"""
CSV file ingestion and validation for ctOS Service 1
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
import uuid

from core.database import DatabaseManager
from core.models import Product, ProcessingStatus
from data.data_mapper import DataMapper
from data.data_validator import DataValidator
from utils.logger import setup_logger
from utils.exceptions import CSVLoadError, DataValidationError

logger = setup_logger("ctOS.csv_loader")


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names"""
    df = df.copy()
    # Convert to lowercase and replace spaces/special chars with underscores
    df.columns = df.columns.str.lower().str.replace(r"[^\w]", "_", regex=True)
    # Remove multiple underscores
    df.columns = df.columns.str.replace(r"_+", "_", regex=True)
    # Remove leading/trailing underscores
    df.columns = df.columns.str.strip("_")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle null/empty values consistently"""
    df = df.copy()
    # Replace empty strings with NaN for consistency
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    return df


def deduplicate_products(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicates by item_id"""
    if "item_id" in df.columns:
        initial_count = len(df)
        df = df.drop_duplicates(subset=["item_id"], keep="first")
        final_count = len(df)
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} duplicate products")
    return df


class CSVLoader:
    """Main CSV loading functionality"""

    def __init__(self, csv_path: str, db_manager: DatabaseManager):
        self.csv_path = Path(csv_path)
        self.db_manager = db_manager
        self.data_mapper = DataMapper()
        self.data_validator = DataValidator(db_manager)
        self.load_stats = {}

    def load_csv(self) -> pd.DataFrame:
        """Load and validate CSV file"""
        try:
            logger.info(f"Loading CSV from {self.csv_path}")

            if not self.csv_path.exists():
                raise CSVLoadError(f"CSV file not found: {self.csv_path}")

            # Load CSV with error handling
            df = pd.read_csv(self.csv_path, encoding="utf-8")
            logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

            # Normalize and clean data
            df = normalize_column_names(df)
            df = handle_missing_values(df)
            df = deduplicate_products(df)

            # Validate structure
            is_valid, errors = self.validate_csv_structure(df)
            if not is_valid:
                raise CSVLoadError(f"CSV structure validation failed: {errors}")

            logger.info(f"CSV loaded successfully: {len(df)} products")
            return df

        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            raise CSVLoadError(f"CSV loading failed: {e}")

    def validate_csv_structure(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Structure validation"""
        errors = []

        # Check minimum required columns
        required_columns = ["item_id", "item_description"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        # Check for empty DataFrame
        if df.empty:
            errors.append("CSV file is empty")

        # Check for reasonable number of columns
        if len(df.columns) < 2:
            errors.append("CSV has too few columns")

        return len(errors) == 0, errors

    def batch_insert_products(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """Insert products in batches"""
        total_inserted = 0
        total_failed = 0

        try:
            # Map DataFrame to Product objects
            products = self.data_mapper.batch_map_products(df)

            logger.info(
                f"Inserting {len(products)} products in batches of {batch_size}"
            )

            with self.db_manager.get_connection() as conn:
                for i in range(0, len(products), batch_size):
                    batch = products[i : i + batch_size]
                    batch_inserted, batch_failed = self._insert_product_batch(
                        conn, batch
                    )
                    total_inserted += batch_inserted
                    total_failed += batch_failed

                    if (i // batch_size + 1) % 10 == 0:  # Log every 10 batches
                        logger.info(f"Processed {i + len(batch)} products...")

            # Update statistics
            self.load_stats = {
                "total_processed": len(products),
                "successfully_inserted": total_inserted,
                "failed_insertions": total_failed,
                "success_rate": total_inserted / len(products) if products else 0.0,
            }

            logger.info(
                f"Batch insert completed: {total_inserted} inserted, {total_failed} failed"
            )
            return total_inserted

        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            raise CSVLoadError(f"Failed to insert products: {e}")

    def _insert_product_batch(self, conn, batch: List[Product]) -> Tuple[int, int]:
        """Insert a single batch of products"""
        inserted = 0
        failed = 0

        for product in batch:
            try:
                product_dict = self.data_mapper.product_to_db_dict(product)

                # Insert product
                placeholders = ", ".join(["?" for _ in product_dict])
                columns = ", ".join(product_dict.keys())
                values = list(product_dict.values())

                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO products ({columns}) 
                    VALUES ({placeholders})
                """,
                    values,
                )

                # Create processing status record
                conn.execute(
                    """
                    INSERT OR REPLACE INTO processing_status 
                    (item_id, status, processing_attempts) 
                    VALUES (?, ?, ?)
                """,
                    (product.item_id, "pending", 0),
                )

                inserted += 1

            except Exception as e:
                logger.warning(f"Failed to insert product {product.item_id}: {e}")
                failed += 1

        return inserted, failed

    def get_load_statistics(self) -> Dict:
        """Loading statistics and summary"""
        return {
            **self.load_stats,
            "csv_path": str(self.csv_path),
            "load_timestamp": pd.Timestamp.now(),
        }

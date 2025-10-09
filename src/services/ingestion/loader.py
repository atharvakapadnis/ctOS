"""
CSV loading logic with encoding handling and validation
"""

import pandas as pd
from pathlib import Path
from typing import List, Optional
import logging
from .config import PRODUCT_COLUMNS, LOG_FORMAT, LOG_DATE_FORMAT
from .models import ProductRecord

# Configure logger
logger = logging.getLogger(__name__)


class CSVLoader:
    """Loads and parses CSV files into ProductRecord instances"""

    def __init__(self):
        self.expected_columns = PRODUCT_COLUMNS

    def load(self, csv_path: Path) -> pd.DataFrame:
        """
        Load CSV file with encoding detection and validation

        Args:
            csv_path: Path to CSV file

        Returns:
            DataFrame with loaded data

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV has wrong structure
        """
        csv_path = Path(csv_path)

        # Check file exists
        if not csv_path.exists():
            error_msg = f"CSV file not found: {csv_path.absolute()}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        logger.info(f"Starting CSV ingestion from: {csv_path.absolute()}")

        # Get file size
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)
        logger.debug(f"CSV file size: {file_size_mb:.2f} MB")

        # Try loading with UTF-8 first, fallback to latin-1
        df = None
        encoding_used = None

        try:
            logger.debug("Attempting to load CSV with UTF-8 encoding...")
            df = pd.read_csv(csv_path, encoding="utf-8")
            encoding_used = "utf-8"
            logger.debug("Successfully loaded with UTF-8 encoding")
        except UnicodeDecodeError as e:
            logger.warning(f"UTF-8 encoding failed: {e}")
            logger.debug("Attempting to load CSV with latin-1 encoding...")
            try:
                df = pd.read_csv(csv_path, encoding="latin-1")
                encoding_used = "latin-1"
                logger.warning("Loaded with latin-1 encoding (fallback)")
            except Exception as e2:
                error_msg = (
                    f"Failed to load CSV with both UTF-8 and latin-1 encodings: {e2}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Failed to load CSV: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Log basic info
        logger.info(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns")
        logger.debug(f"Encoding used: {encoding_used}")
        logger.debug(f"Columns found: {list(df.columns)}")

        # Log data types detected by pandas
        logger.debug(f"Data types detected:\n{df.dtypes.to_dict()}")

        # Log sample of first 3 rows
        if len(df) > 0:
            sample = df.head(3).to_dict("records")
            logger.debug(f"Sample first row: {sample[0]}")
            if len(sample) > 1:
                logger.debug(f"Sample second row: {sample[1]}")
            if len(sample) > 2:
                logger.debug(f"Sample third row: {sample[2]}")

        # Verify expected columns exist
        missing_columns = [
            col for col in self.expected_columns if col not in df.columns
        ]
        if missing_columns:
            error_msg = f"CSV is missing required columns: {missing_columns}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check for extra columns (warn but don't fail)
        extra_columns = [col for col in df.columns if col not in self.expected_columns]
        if extra_columns:
            logger.warning(
                f"CSV contains extra columns (will be ignored): {extra_columns}"
            )

        # Keep only expected columns in correct order
        df = df[self.expected_columns]

        logger.info(f"CSV validation passed, {len(df)} records ready for processing")

        return df

    def to_product_records(self, df: pd.DataFrame) -> List[ProductRecord]:
        """
        Convert DataFrame to list of ProductRecord instances

        Args:
            df: DataFrame with product data

        Returns:
            List of validated ProductRecord instances

        Raises:
            ValueError: If any record fails validation
        """
        logger.info(
            f"Converting {len(df)} DataFrame rows to ProductRecord instances..."
        )

        records = []
        failed_rows = []

        for idx, row in df.iterrows():
            try:
                # Convert row to dict, replacing NaN with None
                row_dict = row.where(pd.notna(row), None).to_dict()

                # Convert all values to strings (since database uses TEXT)
                for key, value in row_dict.items():
                    if value is not None:
                        row_dict[key] = str(value).strip()

                # Create ProductRecord (Pydantic will validate)
                record = ProductRecord(**row_dict)
                records.append(record)

            except Exception as e:
                failed_rows.append(
                    {
                        "row": int(idx) + 2,  # +2 for 1-based index and header
                        "error": str(e),
                        "data": row.to_dict(),
                    }
                )
                logger.warning(f"Row {idx + 2} failed validation: {e}")

        if failed_rows:
            logger.error(f"Failed to convert {len(failed_rows)} rows to ProductRecord")
            for failed in failed_rows[:10]:  # Log first 10 failures
                logger.error(f"Row {failed['row']}: {failed['error']}")

            # Raise exception with details
            raise ValueError(
                f"Failed to convert {len(failed_rows)} rows to ProductRecord. "
                f"First error: Row {failed_rows[0]['row']}: {failed_rows[0]['error']}"
            )

        logger.info(f"Successfully converted {len(records)} records")
        return records

    def load_and_convert(self, csv_path: Path) -> List[ProductRecord]:
        """
        Convenience method: Load CSV and convert to ProductRecords in one step

        Args:
            csv_path: Path to CSV file

        Returns:
            List of ProductRecord instances
        """
        df = self.load(csv_path)
        records = self.to_product_records(df)
        return records

    def get_sample_records(self, csv_path: Path, n: int = 10) -> List[ProductRecord]:
        """
        Load only the first N records for testing/debugging

        Args:
            csv_path: Path to CSV file
            n: Number of records to load

        Returns:
            List of first N ProductRecord instances
        """
        logger.info(f"Loading sample of {n} records from {csv_path}")
        df = self.load(csv_path)
        df_sample = df.head(n)
        records = self.to_product_records(df_sample)
        logger.info(f"Loaded {len(records)} sample records")
        return records

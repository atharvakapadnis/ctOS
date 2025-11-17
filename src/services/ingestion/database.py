"""
SQLite database operations for products and processing results
Two-table design: products (immutable) + processing_results (mutable)
"""

from asyncio import SelectorEventLoop
from functools import total_ordering
import sqlite3
from pathlib import Path
from typing import List, Optional, Dict, Any
from contextlib import contextmanager
import logging
from datetime import datetime, timezone
import json

from pydantic.type_adapter import P

from .config import (
    DATABASE_PATH,
    PRODUCTS_TABLE,
    PROCESSING_TABLE,
    CREATE_PRODUCTS_TABLE_SQL,
    CREATE_PROCESSING_TABLE_SQL,
    CREATE_INDEXES_SQL,
    BATCH_SIZE,
    VALID_CONFIDENCE_LEVELS,
)
from .models import (
    ProductRecord,
    ProcessingResults,
    DatabaseStatistics,
    IntegrityReport,
    ProductWithProcessing,
    UpdateProcessingInput,
)

# Configure logger
logger = logging.getLogger(__name__)


class ProductDatabase:
    """Manages SQLite database operations for products and processing results"""

    def __init__(self, db_path: Path = DATABASE_PATH):
        self.db_path = Path(db_path)
        self.products_table = PRODUCTS_TABLE
        self.processing_table = PROCESSING_TABLE

        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Database initialized at: {self.db_path.absolute()}")

    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        Ensures proper connection cleanup
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Access columns by name
        try:
            logger.debug(f"Database connection opened: {self.db_path}")
            yield conn
            conn.commit()
            logger.debug("Transaction committed")
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
        finally:
            conn.close()
            logger.debug("Database connection closed")

    def create_schema(self) -> None:
        """
        Create database tables and indexes
        Idempotent - safe to call multiple times
        """
        logger.info("Creating database schema...")

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Create products table
            logger.debug(f"Executing: {CREATE_PRODUCTS_TABLE_SQL}")
            cursor.execute(CREATE_PRODUCTS_TABLE_SQL)
            logger.info(f" Table '{self.products_table}' created/verified")

            # Create processing_results table
            logger.debug(f"Executing: {CREATE_PROCESSING_TABLE_SQL}")
            cursor.execute(CREATE_PROCESSING_TABLE_SQL)
            logger.info(f" Table '{self.processing_table}' created/verified")

            # Create indexes
            logger.info("Creating indexes...")
            for idx, sql in enumerate(CREATE_INDEXES_SQL, 1):
                logger.debug(f"Executing index {idx}/{len(CREATE_INDEXES_SQL)}: {sql}")
                cursor.execute(sql)
            logger.info(f" All {len(CREATE_INDEXES_SQL)} indexes created")

        # Create search optimization indexes
        self.create_search_indexes()

        logger.info(" Database schema created successfully")

    def insert_products(self, products: List[ProductRecord]) -> int:
        """
        Batch insert products into database

        Args:
            products: List of ProductRecord instances

        Returns:
            Number of records inserted

        Raises:
            sqlite3.IntegrityError: If duplicate item_id found
        """
        if not products:
            logger.warning("No products to insert")
            return 0

        logger.info(f"Starting batch insert of {len(products)} products...")

        # Prepare data for insertion
        columns = list(products[0].model_dump().keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(columns)

        insert_sql = f"""
            INSERT INTO {self.products_table} ({column_names})
            VALUES ({placeholders})
        """
        logger.debug(f"Insert SQL: {insert_sql}")

        # Prepare rows
        rows = [tuple(p.model_dump().values()) for p in products]

        inserted_count = 0
        failed_count = 0

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Insert in batches with progress logging
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]

                try:
                    cursor.executemany(insert_sql, batch)
                    inserted_count += len(batch)

                    # Log progress
                    progress_pct = (inserted_count / len(products)) * 100
                    logger.info(
                        f"Progress: {inserted_count}/{len(products)} ({progress_pct:.1f}%)"
                    )

                except sqlite3.IntegrityError as e:
                    logger.error(f"Batch insert failed at position {i}: {e}")
                    logger.error(
                        f"Problematic batch sample: {batch[0] if batch else 'empty'}"
                    )
                    failed_count += len(batch)
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error during batch insert: {e}")
                    failed_count += len(batch)
                    raise

        logger.info(
            f" Batch insert complete: {inserted_count} records inserted, {failed_count} failed"
        )
        return inserted_count

    def get_product_by_id(self, item_id: str) -> Optional[ProductWithProcessing]:
        """
        Retrieve single product with its processing results (if any)

        Args:
            item_id: Product identifier

        Returns:
            ProductWithProcessing or None if not found
        """
        query = f"""
            SELECT 
                p.*,
                pr.enhanced_description,
                pr.confidence_score,
                pr.confidence_level,
                pr.extracted_customer_name,
                pr.extracted_dimensions,
                pr.extracted_product,
                pr.rules_applied,
                pr.last_processed_pass,
                pr.last_processed_at
            FROM {self.products_table} p
            LEFT JOIN {self.processing_table} pr ON p.item_id = pr.item_id
            WHERE p.item_id = ?
        """

        start_time = datetime.now()
        logger.debug(f"Executing query: {query} with item_id={item_id}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (item_id,))
            row = cursor.fetchone()

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.debug(f"Query executed in {execution_time:.4f} seconds")

        if row:
            logger.debug(f"Product found: {item_id}")
            return ProductWithProcessing(**dict(row))
        else:
            logger.debug(f"Product not found: {item_id}")
            return None

    def get_products_by_hts(self, hts_code: str) -> List[ProductWithProcessing]:
        """
        Filter products by HTS code (exact match)

        Args:
            hts_code: HTS code to search for

        Returns:
            List of ProductWithProcessing
        """
        query = f"""
            SELECT 
                p.*,
                pr.enhanced_description,
                pr.confidence_score,
                pr.confidence_level,
                pr.extracted_customer_name,
                pr.extracted_dimensions,
                pr.extracted_product,
                pr.rules_applied,
                pr.last_processed_pass,
                pr.last_processed_at
            FROM {self.products_table} p
            LEFT JOIN {self.processing_table} pr ON p.item_id = pr.item_id
            WHERE p.final_hts = ?
        """

        start_time = datetime.now()
        logger.debug(f"Executing query for HTS: {hts_code}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (hts_code,))
            rows = cursor.fetchall()

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.debug(
            f"Query executed in {execution_time:.4f} seconds, found {len(rows)} matches"
        )

        return [ProductWithProcessing(**dict(row)) for row in rows]

    def get_products_by_ids(self, item_ids: List[str]) -> List[ProductWithProcessing]:
        """
        Retrieve multiple products by their IDs with processing results

        Args:
            item_ids: List of product identifiers

        Returns:
            List of ProductWithProcessing objects (only found products)
        """

        if not item_ids:
            logger.warning("get_products_by_ids called with empty item_ids list")
            return []

        logger.warning(f"Retrieving {len(item_ids)} products by IDs")

        # Create placeholders for SQL IN clause
        placeholders = ", ".join(["?"] * len(item_ids))

        query = f"""
            SELECT 
                p.*,
                pr.enhanced_description,
                pr.confidence_score,
                pr.confidence_level,
                pr.extracted_customer_name,
                pr.extracted_dimensions,
                pr.extracted_product,
                pr.rules_applied,
                pr.last_processed_pass,
                pr.last_processed_at
            FROM {self.products_table} p
            LEFT JOIN {self.processing_table} pr ON p.item_id = pr.item_id
            WHERE p.item_id IN ({placeholders})
        """

        start_time = datetime.now()

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, item_ids)
            rows = cursor.fetchall()

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.debug(
            f"Query executed in {execution_time:.4f} seconds, "
            f"requested {len(item_ids)}, found {len(rows)} products"
        )

        # Log if some products not found
        if len(rows) < len(item_ids):
            found_ids = {row["item_id"] for row in rows}
            missing_ids = set(item_ids) - found_ids
            logger.warning(
                f"{len(missing_ids)} products not found in database: {list(missing_ids)[:5]}..."
            )

        return [ProductWithProcessing(**dict(row)) for row in rows]

    def get_products_by_confidence(
        self, confidence_level: str
    ) -> List[ProductWithProcessing]:
        """
        Filter products by confidence level
        Only returns processed products

        Args:
            confidence_level: 'Low', 'Medium', or 'High'

        Returns:
            List of ProductWithProcessing
        """
        if confidence_level not in VALID_CONFIDENCE_LEVELS:
            raise ValueError(
                f"Invalid confidence_level. Must be one of: {VALID_CONFIDENCE_LEVELS}"
            )

        query = f"""
            SELECT 
                p.*,
                pr.enhanced_description,
                pr.confidence_score,
                pr.confidence_level,
                pr.extracted_customer_name,
                pr.extracted_dimensions,
                pr.extracted_product,
                pr.rules_applied,
                pr.last_processed_pass,
                pr.last_processed_at
            FROM {self.products_table} p
            INNER JOIN {self.processing_table} pr ON p.item_id = pr.item_id
            WHERE pr.confidence_level = ?
        """

        start_time = datetime.now()
        logger.debug(f"Executing query for confidence level: {confidence_level}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (confidence_level,))
            rows = cursor.fetchall()

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.debug(
            f"Query executed in {execution_time:.4f} seconds, found {len(rows)} matches"
        )

        return [ProductWithProcessing(**dict(row)) for row in rows]

    def get_unprocessed_products(
        self, limit: Optional[int] = None
    ) -> List[ProductWithProcessing]:
        """
        Get products that have not been processed yet
        (no entry in processing_results table)

        Args:
            limit: Optional limit for pagination

        Returns:
            List of ProductWithProcessing (with NULL processing fields)
        """
        query = f"""
            SELECT 
                p.*,
                NULL as enhanced_description,
                NULL as confidence_score,
                NULL as confidence_level,
                NULL as extracted_customer_name,
                NULL as extracted_dimensions,
                NULL as extracted_product,
                NULL as rules_applied,
                NULL as last_processed_pass,
                NULL as last_processed_at
            FROM {self.products_table} p
            LEFT JOIN {self.processing_table} pr ON p.item_id = pr.item_id
            WHERE pr.item_id IS NULL
        """

        if limit:
            query += f" LIMIT {limit}"

        start_time = datetime.now()
        logger.debug(f"Executing unprocessed query (limit={limit})")

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # First get total unprocessed count
            count_query = f"""
                SELECT COUNT(*) as total
                FROM {self.products_table} p
                LEFT JOIN {self.processing_table} pr ON p.item_id = pr.item_id
                WHERE pr.item_id IS NULL
            """
            cursor.execute(count_query)
            total_unprocessed = cursor.fetchone()["total"]

            # Then get the records
            cursor.execute(query)
            rows = cursor.fetchall()

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.debug(f"Query executed in {execution_time:.4f} seconds")
        logger.debug(f"Total unprocessed: {total_unprocessed}, Returned: {len(rows)}")

        return [ProductWithProcessing(**dict(row)) for row in rows]

    def update_processing_results(
        self, item_id: str, results: UpdateProcessingInput
    ) -> bool:
        """
        Update or insert processing results for a product

        Args:
            item_id: Product identifier
            results: Processing results to update

        Returns:
            True if successful, False if item_id not found in products table
        """
        logger.debug(f"Updating processing results for item_id: {item_id}")

        # First verify product exists
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT item_id FROM {self.products_table} WHERE item_id = ?",
                (item_id,),
            )
            if not cursor.fetchone():
                logger.warning(f"Product not found: {item_id}")
                return False

        # Set timestamp
        timestamp = datetime.now(timezone.utc).isoformat()

        # Prepare data
        data = {
            "item_id": item_id,
            "enhanced_description": results.enhanced_description,
            "confidence_score": results.confidence_score,
            "confidence_level": results.confidence_level,
            "extracted_customer_name": results.extracted_customer_name,
            "extracted_dimensions": results.extracted_dimensions,
            "extracted_product": results.extracted_product,
            "rules_applied": results.rules_applied,
            "last_processed_pass": results.pass_number,
            "last_processed_at": timestamp,
        }

        # Log before values
        before = self.get_product_by_id(item_id)
        logger.debug(
            f"Before update - confidence: {before.confidence_level if before else 'N/A'}"
        )

        # INSERT OR REPLACE (upsert)
        upsert_sql = f"""
            INSERT OR REPLACE INTO {self.processing_table} (
                item_id, enhanced_description, confidence_score, confidence_level,
                extracted_customer_name, extracted_dimensions, extracted_product,
                rules_applied, last_processed_pass, last_processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        logger.debug(f"Executing: {upsert_sql}")
        logger.debug(f"Values: {tuple(data.values())}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(upsert_sql, tuple(data.values()))

        # Log after values
        after = self.get_product_by_id(item_id)
        logger.debug(
            f"After update - confidence: {after.confidence_level if after else 'N/A'}"
        )

        logger.info(f" Processing results updated for item_id: {item_id}")
        return True

    def get_database_statistics(self) -> DatabaseStatistics:
        """
        Get comprehensive database statistics

        Returns:
            DatabaseStatistics with all metrics
        """
        logger.info("Calculating database statistics...")

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Total products
            cursor.execute(f"SELECT COUNT(*) as total FROM {self.products_table}")
            total_products = cursor.fetchone()["total"]
            logger.debug(f"Total products: {total_products}")

            # Processed count (has entry in processing_results)
            cursor.execute(f"SELECT COUNT(*) as processed FROM {self.processing_table}")
            processed_count = cursor.fetchone()["processed"]
            logger.debug(f"Processed count: {processed_count}")

            # Unprocessed count
            unprocessed_count = total_products - processed_count
            logger.debug(f"Unprocessed count: {unprocessed_count}")

            # Confidence distribution
            cursor.execute(
                f"""
                SELECT confidence_level, COUNT(*) as count
                FROM {self.processing_table}
                WHERE confidence_level IS NOT NULL
                GROUP BY confidence_level
            """
            )
            confidence_rows = cursor.fetchall()
            confidence_distribution = {
                row["confidence_level"]: row["count"] for row in confidence_rows
            }
            logger.debug(f"Confidence distribution: {confidence_distribution}")

            # Average confidence score
            cursor.execute(
                f"""
                SELECT AVG(CAST(confidence_score AS REAL)) as avg_score
                FROM {self.processing_table}
                WHERE confidence_score IS NOT NULL
            """
            )
            avg_result = cursor.fetchone()
            average_confidence_score = (
                round(avg_result["avg_score"], 3) if avg_result["avg_score"] else None
            )
            logger.debug(f"Average confidence score: {average_confidence_score}")

            # Unique HTS codes
            cursor.execute(
                f"SELECT COUNT(DISTINCT final_hts) as unique_hts FROM {self.products_table}"
            )
            unique_hts_codes = cursor.fetchone()["unique_hts"]
            logger.debug(f"Unique HTS codes: {unique_hts_codes}")

            # Pass distribution
            cursor.execute(
                f"""
                SELECT last_processed_pass, COUNT(*) as count
                FROM {self.processing_table}
                WHERE last_processed_pass IS NOT NULL
                GROUP BY last_processed_pass
            """
            )
            pass_rows = cursor.fetchall()
            pass_distribution = {
                row["last_processed_pass"]: row["count"] for row in pass_rows
            }
            logger.debug(f"Pass distribution: {pass_distribution}")

        stats = DatabaseStatistics(
            total_products=total_products,
            processed_count=processed_count,
            unprocessed_count=unprocessed_count,
            confidence_distribution=confidence_distribution,
            average_confidence_score=average_confidence_score,
            unique_hts_codes=unique_hts_codes,
            pass_distribution=pass_distribution,
        )

        logger.info(" Database statistics calculated")
        return stats

    def verify_database_integrity(self) -> IntegrityReport:
        """
        Verify database integrity (development/debugging tool)

        Returns:
            IntegrityReport with any issues found
        """
        logger.info("Running database integrity checks...")

        issues = []

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Total counts
            cursor.execute(f"SELECT COUNT(*) as total FROM {self.products_table}")
            total_products = cursor.fetchone()["total"]

            cursor.execute(f"SELECT COUNT(*) as total FROM {self.processing_table}")
            total_processing = cursor.fetchone()["total"]

            logger.debug(
                f"Products: {total_products}, Processing records: {total_processing}"
            )

            # 1. Check for orphaned processing records (item_id in processing but not in products)
            cursor.execute(
                f"""
                SELECT pr.item_id
                FROM {self.processing_table} pr
                LEFT JOIN {self.products_table} p ON pr.item_id = p.item_id
                WHERE p.item_id IS NULL
            """
            )
            orphaned = [row["item_id"] for row in cursor.fetchall()]
            if orphaned:
                issues.append(f"Found {len(orphaned)} orphaned processing records")
                logger.warning(f"Orphaned processing records: {orphaned[:10]}")

            # 2. Products without processing (informational)
            products_without_processing = total_products - total_processing
            logger.debug(f"Products without processing: {products_without_processing}")

            # 3. NULL item_ids in products (should be impossible due to PRIMARY KEY)
            cursor.execute(
                f"SELECT COUNT(*) as count FROM {self.products_table} WHERE item_id IS NULL"
            )
            null_ids_products = cursor.fetchone()["count"]
            if null_ids_products > 0:
                issues.append(
                    f"Found {null_ids_products} NULL item_ids in products table"
                )
                logger.error(f"NULL item_ids in products: {null_ids_products}")

            # 4. NULL item_ids in processing (should be impossible due to PRIMARY KEY)
            cursor.execute(
                f"SELECT COUNT(*) as count FROM {self.processing_table} WHERE item_id IS NULL"
            )
            null_ids_processing = cursor.fetchone()["count"]
            if null_ids_processing > 0:
                issues.append(
                    f"Found {null_ids_processing} NULL item_ids in processing table"
                )
                logger.error(f"NULL item_ids in processing: {null_ids_processing}")

            # 5. Duplicate item_ids in products (should be impossible due to PRIMARY KEY)
            cursor.execute(
                f"""
                SELECT item_id, COUNT(*) as count
                FROM {self.products_table}
                GROUP BY item_id
                HAVING count > 1
            """
            )
            dup_products = cursor.fetchall()
            if dup_products:
                issues.append(
                    f"Found {len(dup_products)} duplicate item_ids in products"
                )
                logger.error(
                    f"Duplicate item_ids in products: {[d['item_id'] for d in dup_products]}"
                )

            # 6. Duplicate item_ids in processing (should be impossible due to PRIMARY KEY)
            cursor.execute(
                f"""
                SELECT item_id, COUNT(*) as count
                FROM {self.processing_table}
                GROUP BY item_id
                HAVING count > 1
            """
            )
            dup_processing = cursor.fetchall()
            if dup_processing:
                issues.append(
                    f"Found {len(dup_processing)} duplicate item_ids in processing"
                )
                logger.error(
                    f"Duplicate item_ids in processing: {[d['item_id'] for d in dup_processing]}"
                )

            # 7. Invalid confidence scores
            cursor.execute(
                f"""
                SELECT item_id, confidence_score
                FROM {self.processing_table}
                WHERE confidence_score IS NOT NULL
            """
            )
            invalid_scores = []
            for row in cursor.fetchall():
                try:
                    score = float(row["confidence_score"])
                    if not 0.0 <= score <= 1.0:
                        invalid_scores.append(
                            {
                                "item_id": row["item_id"],
                                "value": row["confidence_score"],
                            }
                        )
                except ValueError:
                    invalid_scores.append(
                        {"item_id": row["item_id"], "value": row["confidence_score"]}
                    )

            if invalid_scores:
                issues.append(f"Found {len(invalid_scores)} invalid confidence scores")
                logger.warning(f"Invalid confidence scores: {invalid_scores[:10]}")

            # 8. Invalid confidence levels
            cursor.execute(
                f"""
                SELECT item_id, confidence_level
                FROM {self.processing_table}
                WHERE confidence_level IS NOT NULL
                AND confidence_level NOT IN ('Low', 'Medium', 'High')
            """
            )
            invalid_levels = [
                {"item_id": row["item_id"], "value": row["confidence_level"]}
                for row in cursor.fetchall()
            ]
            if invalid_levels:
                issues.append(f"Found {len(invalid_levels)} invalid confidence levels")
                logger.warning(f"Invalid confidence levels: {invalid_levels[:10]}")

        integrity_passed = len(issues) == 0

        report = IntegrityReport(
            total_products=total_products,
            total_processing_records=total_processing,
            orphaned_processing_records=orphaned,
            products_without_processing=products_without_processing,
            null_item_ids_in_products=null_ids_products,
            null_item_ids_in_processing=null_ids_processing,
            duplicate_item_ids_in_products=len(dup_products) if dup_products else 0,
            duplicate_item_ids_in_processing=(
                len(dup_processing) if dup_processing else 0
            ),
            invalid_confidence_scores=invalid_scores,
            invalid_confidence_levels=invalid_levels,
            integrity_passed=integrity_passed,
            issues_found=issues,
        )

        if integrity_passed:
            logger.info(" Database integrity check passed - no issues found")
        else:
            logger.warning(f"✗ Database integrity check found {len(issues)} issues")

        return report

    def export_sample_records(
        self, n: int = 10, output_path: Optional[Path] = None
    ) -> List[Dict[str, Any]]:
        """
        Export first N records as JSON for debugging

        Args:
            n: Number of records to export
            output_path: Optional path to save JSON file

        Returns:
            List of product dictionaries with processing results
        """
        logger.info(f"Exporting {n} sample records...")

        query = f"""
            SELECT 
                p.*,
                pr.enhanced_description,
                pr.confidence_score,
                pr.confidence_level,
                pr.extracted_customer_name,
                pr.extracted_dimensions,
                pr.extracted_product,
                pr.rules_applied,
                pr.last_processed_pass,
                pr.last_processed_at
            FROM {self.products_table} p
            LEFT JOIN {self.processing_table} pr ON p.item_id = pr.item_id
            LIMIT ?
        """

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (n,))
            rows = cursor.fetchall()

        records = [dict(row) for row in rows]

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w") as f:
                json.dump(records, f, indent=2)

            logger.info(f" Exported {len(records)} records to {output_path.absolute()}")

        return records

    def search_products(
        self, query: str, search_type: str = "auto", limit: Optional[int] = None
    ) -> List[ProductWithProcessing]:
        """
        Search products across entire database by Item ID, HTS Code, or description keywords.

        This method enables finding products beyondf the standard 500-product display limit
        by querying the entire database. It supports mutliple search types with automatic
        detection of search intent.

        Args:
            query (str): Search query string
            search_type (str, optional): Type of search to perform. Defaults to "auto"
            limit (Optional[int], optional): Maximum results to return. Defaults to None (no limit)

        Returns:
            List[ProductWithProcessing]: List of matching products with processing results
        """
        import re

        start_time = datetime.now()
        logger.info(
            f"Starting search: query='{query}', type='{search_type}', limit='{limit}'"
        )

        if not query or not query.strip():
            logger.warning("Empty search query provided")
            return []

        query = query.strip()

        # Auto-detect search type if needed
        if search_type == "auto":
            # Check for HTS code pattern first (most specific)
            if re.match(r"^\d{4}\.\d{2}(\.\d{2})?(\.\d{2})?$", query):
                search_type = "hts_code"
                logger.debug(f"Auto-detected search type: hts_code")
            # Check for item ID pattern (alphanumeric with optional dashes/underscores)
            elif re.match(r"^[A-Z0-9]+[-_]?[A-Z0-9]+$", query, re.IGNORECASE):
                search_type = "item_id"
                logger.debug(f"Auto-detected search type: item_id")
            else:
                search_type = "description"
                logger.debug(f"Auto-detected search type: description")

        # Build base query
        base_query = f"""
            SELECT
                p.*,
                pr.enhanced_description,
                pr.confidence_score,
                pr.confidence_level,
                pr.extracted_customer_name,
                pr.extracted_dimensions,
                pr.extracted_product,
                pr.rules_applied,
                pr.last_processed_pass,
                pr.last_processed_at
            FROM {self.products_table} p
            LEFT JOIN {self.processing_table} pr ON p.item_id = pr.item_id
        """

        # Build where clause based on the search type
        where_clause = ""
        params = []

        if search_type == "item_id":
            where_clause = "WHERE p.item_id LIKE ? COLLATE NOCASE"
            params = [f"%{query}%"]
            logger.debug(f"Search type: item_id, pattern: {query}%")

        elif search_type == "hts_code":
            where_clause = "WHERE p.final_hts LIKE ?"
            params = [f"{query}%"]
            logger.debug(f"Search type: hts_code, pattern: {query}%")

        elif search_type == "description":
            # Split into keywords and search for all
            keywords = query.lower().split()
            conditions = []
            for keyword in keywords:
                conditions.append("LOWER(p.item_description) LIKE ?")
                params.append(f"%{keyword}%")
            where_clause = "WHERE " + " AND ".join(conditions)
            logger.debug(f"Search type: description, keywords: {keywords}")

        elif search_type == "multi":
            # Search across multiple columns
            where_clause = """
                WHERE p.item_id LIKE ? COLLATE NOCASE
                OR LOWER(p.item_description) LIKE ?
                OR p.final_hts LIKE ?
            """
            query_lower = query.lower()
            params = [f"%{query}%", f"%{query_lower}%", f"%{query}%"]
            logger.debug(f"Search type: multi-column")

        # Combine query parts
        full_query = base_query + where_clause

        if limit:
            full_query += f" LIMIT {limit}"

        logger.warning(f"Executing query: {full_query}")
        logger.debug(f"Parameters: {params}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(full_query, params)
            rows = cursor.fetchall()

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Search complete: found {len(rows)} results in {execution_time:.4f} seconds"
        )

        return [ProductWithProcessing(**dict(row)) for row in rows]

    def filter_products(
        self, filters: Dict[str, Any], limit: int = 500
    ) -> List[ProductWithProcessing]:
        """
        Filter products by multiple criteria, return first N of filtered results.

        Purpose: Filter product by multiple criteria, reutrn first N of filtered results.

        Args:
            filters (Dict): Dicitonary of filter critera
            limit (int): Maximum results to return. Defaults to 500

        Filter Dictionary Structure:
        {
            "hts_range": {
                "start": "7303.11.00",
                "end": "7307.99.00"
            },
            "product_group": "GROUP_A",
            "material_class": "Ductile Iron",
            "confidence_levels": ["Low", "Medium"],
            "status": "unprocessed",
            "description_keywords": ["spacer", "ring"]
        }

        Returns:
            List[ProductWithProcessing]: List of matching products (max limit)
        """
        start_time = datetime.now()
        logger.info(f"Starting filter with {len(filters)} criteria, limit={limit}")
        logger.debug(f"Filters: {filters}")

        # Build base query
        base_query = f"""
            SELECT
                p.*,
                pr.enhanced_description,
                pr.confidence_score,
                pr.confidence_level,
                pr.extracted_customer_name,
                pr.extracted_dimensions,
                pr.extracted_product,
                pr.rules_applied,
                pr.last_processed_pass,
                pr.last_processed_at
            FROM {self.products_table} p
            LEFT JOIN {self.processing_table} pr ON p.item_id = pr.item_id
        """

        where_clauses = []
        params = []

        # HTS Range filter
        if "hts_range" in filters and filters["hts_range"]:
            hts_range = filters["hts_range"]
            if hts_range.get("start") and hts_range.get("end"):
                where_clauses.append("p.final_hts BETWEEN ? AND ?")
                params.extend([hts_range["start"], hts_range["end"]])
                logger.debug(
                    f"Filter: HTS Range {hts_range['start']} to {hts_range['end']}"
                )
            elif hts_range.get("start"):
                where_clauses.append("p.final_hts >= ?")
                params.append(hts_range["start"])
                logger.debug(f"Filter: HTS Range >= {hts_range['start']}")
            elif hts_range.get("end"):
                where_clauses.append("p.final_hts <= ?")
                params.append(hts_range["end"])
                logger.debug(f"Filter: HTS <= {hts_range['end']}")

        # Product group filter
        if filters.get("product_group"):
            where_clauses.append("p.product_group = ?")
            params.append(filters["product_group"])
            logger.debug(f"Filter: Product Group {filters['product_group']}")

        # Material class filter
        if filters.get("material_class"):
            where_clauses.append("p.material_class = ?")
            params.append(filters["material_class"])
            logger.debug(f"Filter: Material Class {filters['material_class']}")

        # Status filter
        status = filters.get("status", "all")
        if status == "unprocessed":
            where_clauses.append("pr.item_id IS NULL")
            logger.debug(f"Filter: Status = Unprocessed")
        elif status == "processed":
            where_clauses.append("pr.item_id IS NOT NULL")
            logger.debug(f"Filter: Status = Processed")

        # Confidence Level filter (only for processed products)
        if filters.get("confidence_levels") and status != "unprocessed":
            levels = filters["confidence_levels"]
            if levels:
                placeholders = ", ".join(["?" for _ in levels])
                where_clauses.append(f"pr.confidence_level IN ({placeholders})")
                params.extend(levels)
                logger.debug(f"Filter: Confidence Levels = {levels}")

        # Description Keywords Filter
        if filters.get("description_keywords"):
            keywords = filters["description_keywords"]
            for keyword in keywords:
                where_clauses.append(f"LOWER(p.item_description) LIKE ?")
                params.append(f"%{keyword.lower()}%")
            logger.debug(f"Filter: Desccription Keywords = {keywords}")

        # Combine where clauses
        if where_clauses:
            full_query = base_query + " WHERE " + " AND ".join(where_clauses)
        else:
            full_query = base_query

        # Add limit
        full_query += f" LIMIT {limit}"

        logger.debug(f"Executing query: {full_query}")
        logger.debug(f"Parameters: {params}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(full_query, params)
            rows = cursor.fetchall()

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Filter complete: found {len(rows)} results in {execution_time:.4f} seconds"
        )

        return [ProductWithProcessing(**dict(row)) for row in rows]

    def count_filtered_products(self, filters: Dict[str, any]) -> int:
        """
        Count total products matching filters WITHOUT loading them.

        Purpose: Count total products matching filters WITHOUT loading them

        Args:
            filters (Dict): Dictionary of filter criteria

        Returns:
            int: Total number of products matching filters
        """
        start_time = datetime.now()
        logger.debug(f"Counting filtered products with filters: {filters}")

        # Build count query
        base_query = f"""
            SELECT COUNT(*) as total
            FROM {self.products_table} p
            LEFT JOIN {self.processing_table} pr on p.item_id = pr.item_id
        """

        where_clauses = []
        params = []

        # HTS Range Filter
        if "hts_range" in filters and filters["hts_range"]:
            hts_range = filters["hts_range"]
            if hts_range.get("start") and hts_range.get("end"):
                where_clauses.append("p.final_hts BETWEEN ? AND ?")
                params.extend([hts_range["start"], hts_range["end"]])
            elif hts_range.get("start"):
                where_clauses.append("p.final_hts >= ?")
                params.append(hts_range["start"])
            elif hts_range.get("end"):
                where_clauses.append("p.final_hts <= ?")
                params.append(hts_range["end"])

        # Product Group filter
        if filters.get("product_group"):
            where_clauses.append("p.product_group = ?")
            params.append(filters["product_group"])

        # Material class filter
        if filters.get("material_class"):
            where_clauses.append("p.material_class = ?")
            params.append(filters["material_class"])

        # Status Filter
        status = filters.get("status", "all")
        if status == "unprocessed":
            where_clauses.append("pr.item_id IS NULL")
        elif status == "processed":
            where_clauses.append("pr.item_id IS NOT NULL")

        # Confidence levels filter
        if filters.get("confidence_levels") and status != "unprocessed":
            levels = filters["confidence_levels"]
            if levels:
                placeholders = ", ".join(["?" for _ in levels])
                where_clauses.append(f"pr.confidence_level IN ({placeholders})")
                params.extend(levels)

        # Description Keywords Filter
        if filters.get("description_keyboards"):
            keywords = filters["description_keywords"]
            for keyword in keywords:
                where_clauses.append(f"LOWER(p.item_description) LIKE ?")
                params.append(f"%{keyword.lower()}%")

        # Combine WHERE clauses
        if where_clauses:
            full_query = base_query + " WHERE " + " AND ".join(where_clauses)
        else:
            full_query = base_query

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(full_query, params)
            result = cursor.fetchone()
            total = result["total"] if result else 0

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.debug(
            f"Count complete: found {total} results in {execution_time:.4f} seconds"
        )

        return total

    def get_unique_product_groups(self) -> List[str]:
        """
        Get list of unique product groups for dropdown population

        Returns:
            List[str]: Sorted list of unique product groups
        """
        query = f"""
            SELECT DISTINCT product_group
            FROM {self.products_table}
            WHERE product_group IS NOT NULL
            ORDER BY product_group
        """

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

        result = [row["product_group"] for row in rows]
        logger.debug(f"Found {len(result)} unique product groups")

        return result

    def get_unique_material_classes(self) -> List[str]:
        """
        Get list of unique material classes for dropdown population.

        Returns:
            List[str]: Sorted list of unique material classes
        """
        query = f"""
            SELECT DISTINCT material_class 
            FROM {self.products_table} 
            WHERE material_class IS NOT NULL 
            ORDER BY material_class
        """

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

        result = [row["material_class"] for row in rows]
        logger.debug(f"Found {len(result)} unique material classes")
        return result

    def get_unique_hts_codes(self) -> List[str]:
        """
        Get list of unique HTS codes for dropdown population.

        Returns:
            List[str]: Sorted list of unique HTS codes
        """
        query = f"""
            SELECT DISTINCT final_hts 
            FROM {self.products_table} 
            WHERE final_hts IS NOT NULL 
            ORDER BY final_hts
        """

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

        result = [row["final_hts"] for row in rows]
        logger.debug(f"Found {len(result)} unique HTS codes")
        return result

    def get_hts_code_ranges(self) -> Dict[str, str]:
        """
        Get min/max HTS codes for input validation.

        Returns:
            Dict[str, str]: Dictionary with min/max HTS codes
            Example: {"min": "7307.11.00", "max": "7307.99.90"}
        """
        query = f"""
            SELECT 
                MIN(final_hts) as min_hts, 
                MAX(final_hts) as max_hts 
            FROM {self.products_table}
            WHERE final_hts IS NOT NULL
        """

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()

        if row:
            result = {"min": row["min_hts"], "max": row["max_hts"]}
            logger.debug(f"HTS range: {result}")
            return result
        else:
            logger.warning("No HTS codes found in database")
            return {"min": "", "max": ""}

    def create_search_indexes(self) -> None:
        """
        Create additional indexes for search optimization.
        These are separate from the base indexes created in create_schema().
        """
        logger.info("Creating search optimization indexes...")

        search_indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_products_item_id_search ON {self.products_table}(item_id COLLATE NOCASE)",
            f"CREATE INDEX IF NOT EXISTS idx_products_material_class ON {self.products_table}(material_class)",
        ]

        with self.get_connection() as conn:
            cursor = conn.cursor()
            for idx, sql in enumerate(search_indexes, 1):
                logger.debug(
                    f"Executing search index {idx}/{len(search_indexes)}: {sql}"
                )
                cursor.execute(sql)

        logger.info(f"Created {len(search_indexes)} search indexes successfully")

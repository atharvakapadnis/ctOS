"""
Demo Data Generation Script
Creates demo_seed.db with 250 random products from production database
"""

import sqlite3
import sys
from pathlib import Path
import logging
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.ingestion.config import (
    DATABASE_PATH,
    PRODUCTS_TABLE,
    PROCESSING_TABLE,
    DATA_DIR,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_demo_database():
    """Create demo_seed.db with 250 random products"""

    prod_db_path = DATABASE_PATH
    demo_seed_path = DATA_DIR / "demo_seed.db"

    if not prod_db_path.exists():
        logger.error(f"Production database not found: {prod_db_path}")
        sys.exit(1)

    logger.info(f"Creating demo database from: {prod_db_path}")
    logger.info(f"Output: {demo_seed_path}")

    # Connect to production database
    prod_conn = sqlite3.connect(prod_db_path)
    prod_cursor = prod_conn.cursor()

    # Check total products available
    prod_cursor.execute(f"SELECT COUNT(*) FROM {PRODUCTS_TABLE}")
    total_products = prod_cursor.fetchone()[0]

    logger.info(f"Total products in production: {total_products}")

    if total_products < 250:
        logger.warning(
            f"Less than 250 products available. Using all {total_products} products."
        )
        sample_size = total_products
    else:
        sample_size = 250

    # Get random sample of products
    logger.info(f"Selecting {sample_size} random products...")
    prod_cursor.execute(
        f"""
        SELECT * FROM {PRODUCTS_TABLE}
        ORDER BY RANDOM()
        LIMIT {sample_size}
    """
    )

    products = prod_cursor.fetchall()
    product_ids = [p[0] for p in products]  # item_id is first column

    # Get column names for products table
    prod_cursor.execute(f"PRAGMA table_info({PRODUCTS_TABLE})")
    product_columns = [col[1] for col in prod_cursor.fetchall()]

    # Create demo database
    if demo_seed_path.exists():
        logger.warning(f"Removing existing demo seed database...")
        demo_seed_path.unlink()

    demo_conn = sqlite3.connect(demo_seed_path)
    demo_cursor = demo_conn.cursor()

    # Create products table schema
    logger.info("Creating products table schema...")
    prod_cursor.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{PRODUCTS_TABLE}'"
    )
    create_products_sql = prod_cursor.fetchone()[0]
    demo_cursor.execute(create_products_sql)
    logger.info(f"  Products table created")

    # Create processing_results table schema
    logger.info("Creating processing_results table schema...")
    prod_cursor.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{PROCESSING_TABLE}'"
    )
    processing_result = prod_cursor.fetchone()

    if processing_result is None:
        logger.error(
            f"ERROR: {PROCESSING_TABLE} table not found in production database"
        )
        demo_conn.close()
        prod_conn.close()
        sys.exit(1)

    create_processing_sql = processing_result[0]
    demo_cursor.execute(create_processing_sql)
    logger.info(f"  Processing_results table created")

    # Copy indexes for both tables
    logger.info("Creating indexes...")
    prod_cursor.execute(
        f"""
        SELECT sql FROM sqlite_master 
        WHERE type='index' 
        AND tbl_name IN ('{PRODUCTS_TABLE}', '{PROCESSING_TABLE}')
        AND sql IS NOT NULL
    """
    )
    indexes = prod_cursor.fetchall()

    index_count = 0
    for idx_sql_tuple in indexes:
        if idx_sql_tuple[0]:  # Skip None values
            try:
                demo_cursor.execute(idx_sql_tuple[0])
                index_count += 1
            except sqlite3.OperationalError as e:
                # Skip if index already exists or other non-critical errors
                logger.debug(f"Index creation skipped: {e}")
                pass

    logger.info(f"  {index_count} indexes created")

    # Insert products
    logger.info(f"Inserting {len(products)} products...")
    placeholders = ",".join(["?" for _ in product_columns])
    demo_cursor.executemany(
        f"INSERT INTO {PRODUCTS_TABLE} VALUES ({placeholders})", products
    )
    logger.info(f"  {len(products)} products inserted")

    # Insert processing records in unprocessed state
    logger.info("Creating processing records (unprocessed state)...")
    inserted_count = 0
    for product_id in product_ids:
        demo_cursor.execute(
            f"""
            INSERT INTO {PROCESSING_TABLE} (
                item_id,
                enhanced_description,
                confidence_score,
                confidence_level,
                extracted_customer_name,
                extracted_dimensions,
                extracted_product,
                rules_applied,
                last_processed_pass,
                last_processed_at
            ) VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """,
            (product_id,),
        )
        inserted_count += 1

    logger.info(f"  {inserted_count} processing records created")

    # Commit changes
    demo_conn.commit()

    # Vacuum database to optimize size
    logger.info("Optimizing database size...")
    demo_cursor.execute("VACUUM")
    demo_conn.commit()

    # Get statistics
    demo_cursor.execute(f"SELECT COUNT(*) FROM {PRODUCTS_TABLE}")
    demo_product_count = demo_cursor.fetchone()[0]

    demo_cursor.execute(f"SELECT COUNT(*) FROM {PROCESSING_TABLE}")
    demo_processing_count = demo_cursor.fetchone()[0]

    demo_cursor.execute(f"SELECT COUNT(DISTINCT final_hts) FROM {PRODUCTS_TABLE}")
    demo_hts_count = demo_cursor.fetchone()[0]

    file_size_mb = demo_seed_path.stat().st_size / (1024 * 1024)

    # Close connections
    demo_conn.close()
    prod_conn.close()

    # Summary
    logger.info("=" * 60)
    logger.info("DEMO DATABASE CREATION COMPLETED")
    logger.info("=" * 60)
    logger.info(f"Products table: {demo_product_count} records")
    logger.info(f"Processing table: {demo_processing_count} records")
    logger.info(f"Unique HTS codes: {demo_hts_count}")
    logger.info(f"File size: {file_size_mb:.2f} MB")
    logger.info(f"Location: {demo_seed_path.absolute()}")
    logger.info(f"All products set to unprocessed state")
    logger.info("=" * 60)


if __name__ == "__main__":
    create_demo_database()

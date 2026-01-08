"""
Demo Data Generation Script
Created demo_seed.db with 250 random prodycts from production database
"""

from ntpath import samefile
import sqlite3
import sys
from pathlib import Path
import logging
from datetime import datetime

# Add prohect root to the path
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

    logger.info(f"Total prodcuts in production: {total_products}")

    if total_products < 250:
        logger.warning(
            f"Less than 250 products available. Using all {total_products} products"
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
    product_ids = [p[0] for p in products]  # item_id is the first column

    # Get column names
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

    # Create processing results table schema
    logger.info("Creating indexes...")
    prod_cursor.execute(
        f"SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name IN ('{PRODUCTS_TABLE}', '{PROCESSING_TABLE}')"
    )
    create_processing_sql = prod_cursor.fetchone()[0]
    demo_cursor.execute(create_processing_sql)

    # Copy indexes
    logger.info("Copying indexes...")
    prod_cursor.execute(
        f"SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name IN ('{PRODUCTS_TABLE}', '{PROCESSING_TABLE}')"
    )
    indexes = prod_cursor.fetchall()
    for idx_sql in indexes:
        if idx_sql[0]:
            try:
                demo_cursor.execute(idx_sql[0])
            except sqlite3.OperationalError:
                pass  # Skip if index already exists

    # Insert products
    logger.info(f"Inserting {len(products)} products...")
    placeholders = ",".join(["?" for _ in product_columns])
    demo_cursor.executemany(
        f"INSERT INTO {PRODUCTS_TABLE} VALUES ({placeholders})", products
    )

    # Reset all processing data (set to unprocessed state)
    logger.info("Resetting processing data to unprocessed state...")
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

    # Commit and close
    demo_conn.commit()

    # Vaccum database to optimize size
    logger.info("Optimizing database size...")
    demo_cursor.execute("VACUUM")
    demo_conn.commit()

    # Get statistics
    demo_cursor.execute(f"SELECT COUNT(*) FROM {PRODUCTS_TABLE}")
    demo_product_count = demo_cursor.fetchone()[0]

    demo_cursor.execute(f"SELECT COUNT(DISTINCT final_hts) FROM {PRODUCTS_TABLE}")
    demo_hts_count = demo_cursor.fetchone()[0]

    file_size_mb = demo_seed_path.stat().st_size / (1024 * 1024)

    # Close connections
    demo_conn.close()
    prod_conn.close()

    # Summary
    logger.info("DEMO DATABASE CREATION COMPLETED")
    logger.info("=" * 60)
    logger.info(f"Products included: {demo_product_count}")
    logger.info(f"Unique HTS codes: {demo_hts_count}")
    logger.info(f"File size: {file_size_mb:.2f} MB")
    logger.info(f"Location: {demo_seed_path.absolute()}")
    logger.info(f"All products set to unprocessed state")
    logger.info("=" * 60)


if __name__ == "__main__":
    create_demo_database()

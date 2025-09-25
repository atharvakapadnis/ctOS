"""
Initialize database schema and validate setup for ctOS Service 1
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.config import get_config, validate_paths
from core.database import DatabaseManager
from utils.logger import setup_logger
from utils.exceptions import ConfigurationError, DatabaseError


def main():
    """Main initialization routine"""
    logger = setup_logger("ctOS.init")

    try:
        logger.info("Starting ctOS Service 1 initialization...")

        # Validate configuration
        if not validate_paths():
            raise ConfigurationError("Invalid configuration - check file paths")

        config = get_config()
        logger.info(f"Using database: {config.DATABASE_PATH}")

        # Initialize database
        db_manager = DatabaseManager(config.DATABASE_PATH)

        # Create schema
        create_database_schema(db_manager, logger)

        # Validate setup
        validate_database_setup(db_manager, logger)

        logger.info("ctOS Service 1 initialization completed successfully")
        return True

    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return False


def create_database_schema(db_manager: DatabaseManager, logger):
    """Create all tables and indexes"""
    logger.info("Creating database schema...")
    try:
        db_manager.create_schema()
        logger.info("Database schema created successfully")
    except Exception as e:
        raise DatabaseError(f"Failed to create schema: {e}")


def validate_database_setup(db_manager: DatabaseManager, logger):
    """Verify schema creation"""
    logger.info("Validating database setup...")

    required_tables = ["products", "processing_status", "processing_history"]

    with db_manager.get_connection() as conn:
        # Check tables exist
        for table in required_tables:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            )
            if not cursor.fetchone():
                raise DatabaseError(f"Table {table} not found")

        # Check table schemas
        for table in required_tables:
            info = db_manager.get_table_info(table)
            logger.info(f"Table {table}: {len(info['columns'])} columns")

    logger.info("Database validation completed")


def create_sample_data(db_manager: DatabaseManager, logger):
    """Insert test data for validation"""
    logger.info("Creating sample data...")

    with db_manager.get_connection() as conn:
        # Insert sample product
        conn.execute(
            """
            INSERT OR REPLACE INTO products 
            (item_id, item_description, material_class, final_hts)
            VALUES (?, ?, ?, ?)
        """,
            ("TEST001", "Sample Product", "Iron", "7301.10.00.00"),
        )

        # Insert processing status
        conn.execute(
            """
            INSERT OR REPLACE INTO processing_status 
            (item_id, status)
            VALUES (?, ?)
        """,
            ("TEST001", "pending"),
        )

    logger.info("Sample data created")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

"""
Load CSV data into database for ctOS Service 1
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.config import get_config
from core.database import DatabaseManager
from data.csv_loader import CSVLoader
from data.data_validator import DataValidator
from utils.logger import setup_logger


def main():
    """Main CSV loading routine"""
    logger = setup_logger("ctOS.csv_load")

    try:
        config = get_config()
        logger.info(f"Starting CSV data loading from {config.CSV_INPUT_PATH}")

        # Initialize components
        db_manager = DatabaseManager(config.DATABASE_PATH)
        csv_loader = CSVLoader(config.CSV_INPUT_PATH, db_manager)

        # Load and validate CSV
        df = load_and_validate_csv(csv_loader, logger)

        # Insert into database
        inserted_count = csv_loader.batch_insert_products(df, config.BATCH_SIZE)

        # Generate loading report
        generate_loading_report(csv_loader, db_manager, logger)

        logger.info(
            f"CSV loading completed successfully: {inserted_count} products loaded"
        )
        return True

    except Exception as e:
        logger.error(f"CSV loading failed: {e}")
        return False


def load_and_validate_csv(csv_loader: CSVLoader, logger) -> pd.DataFrame:
    """Load CSV with validation"""
    df = csv_loader.load_csv()

    # Run data validation
    validator = DataValidator()
    quality_report = validator.generate_quality_report(df)

    logger.info(
        f"Data quality score: {quality_report['summary']['data_quality_score']:.2f}"
    )

    if not quality_report["summary"]["validation_passed"]:
        logger.warning("Data quality issues detected:")
        for error in quality_report["issues"]["errors"][:5]:
            logger.warning(f"  - {error}")

    return df


def generate_loading_report(csv_loader: CSVLoader, db_manager: DatabaseManager, logger):
    """Create data loading summary"""
    stats = csv_loader.get_load_statistics()

    # Get database counts
    with db_manager.get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) as count FROM products")
        total_products = cursor.fetchone()["count"]

        cursor = conn.execute(
            """
            SELECT status, COUNT(*) as count 
            FROM processing_status 
            GROUP BY status
        """
        )
        status_counts = {row["status"]: row["count"] for row in cursor.fetchall()}

    logger.info("=== Loading Summary ===")
    logger.info(f"Total products in database: {total_products}")
    logger.info(f"Successfully loaded: {stats.get('successfully_inserted', 0)}")
    logger.info(f"Failed insertions: {stats.get('failed_insertions', 0)}")
    logger.info(f"Success rate: {stats.get('success_rate', 0.0):.1%}")
    logger.info(f"Processing status distribution: {status_counts}")


def handle_loading_errors(error: Exception, logger):
    """Error recovery and reporting"""
    logger.error(f"Loading error encountered: {error}")
    # In a real implementation, might implement retry logic or partial recovery


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

"""
Comprehensive Service 1 validation for ctOS
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.config import get_config, validate_paths
from core.database import DatabaseManager
from hts.hts_loader import HTSLoader
from hts.hts_hierarchy import HTSHierarchy
from data.data_validator import DataValidator
from utils.logger import setup_logger


def main():
    """Main validation routine"""
    logger = setup_logger("ctOS.validation")

    results = {
        "database_connectivity": False,
        "csv_loading": False,
        "hts_hierarchy": False,
        "data_quality": False,
    }

    try:
        logger.info("=" * 60)
        logger.info("ctOS Service 1 Comprehensive Validation")
        logger.info("=" * 60)

        # Validate configuration
        config = get_config()
        if not validate_paths():
            logger.error("Configuration validation failed")
            return False

        logger.info("✓ Configuration validated")

        # Test database connectivity
        results["database_connectivity"] = validate_database_connectivity(
            config, logger
        )

        # Test CSV loading
        results["csv_loading"] = validate_csv_loading(config, logger)

        # Test HTS hierarchy
        results["hts_hierarchy"] = validate_hts_hierarchy(config, logger)

        # Test data quality
        results["data_quality"] = validate_data_quality(config, logger)

        # Generate validation report
        generate_validation_report(results, logger)

        all_passed = all(results.values())
        if all_passed:
            logger.info("=" * 60)
            logger.info("✓ ALL VALIDATIONS PASSED")
            logger.info("=" * 60)
        else:
            logger.error("=" * 60)
            logger.error("✗ SOME VALIDATIONS FAILED")
            logger.error("=" * 60)

        return all_passed

    except Exception as e:
        logger.error(f"Validation failed with error: {e}")
        return False


def validate_database_connectivity(config, logger) -> bool:
    """Test database operations"""
    logger.info("\n[1/4] Testing Database Connectivity...")

    try:
        db_manager = DatabaseManager(config.DATABASE_PATH)

        # Test connection
        with db_manager.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) as count FROM products")
            product_count = cursor.fetchone()["count"]

            cursor = conn.execute("SELECT COUNT(*) as count FROM processing_status")
            status_count = cursor.fetchone()["count"]

            logger.info(f"  Products in database: {product_count}")
            logger.info(f"  Processing status records: {status_count}")

        # Test schema inspection
        tables = ["products", "processing_status", "processing_history"]
        for table in tables:
            info = db_manager.get_table_info(table)
            logger.info(f"  Table '{table}': {len(info['columns'])} columns")

        logger.info("✓ Database connectivity validated")
        return True

    except Exception as e:
        logger.error(f"✗ Database validation failed: {e}")
        return False


def validate_csv_loading(config, logger) -> bool:
    """Test CSV loading functionality"""
    logger.info("\n[2/4] Testing CSV Loading...")

    try:
        db_manager = DatabaseManager(config.DATABASE_PATH)

        # Check if data was loaded
        with db_manager.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) as count FROM products")
            count = cursor.fetchone()["count"]

            if count == 0:
                logger.warning(
                    "  No products in database - CSV may not have been loaded"
                )
                logger.info("  Run: python scripts/load_csv_data.py")
                return False

            # Sample a few products
            cursor = conn.execute("SELECT * FROM products LIMIT 5")
            samples = cursor.fetchall()

            logger.info(f"  Total products loaded: {count}")
            logger.info(
                f"  Sample product: {dict(samples[0])['item_id']} - {dict(samples[0])['item_description'][:50]}"
            )

        logger.info("✓ CSV loading validated")
        return True

    except Exception as e:
        logger.error(f"✗ CSV loading validation failed: {e}")
        return False


def validate_hts_hierarchy(config, logger) -> bool:
    """Test HTS reference functionality"""
    logger.info("\n[3/4] Testing HTS Hierarchy...")

    try:
        # Load HTS data
        hts_loader = HTSLoader(config.HTS_JSON_PATH)
        hts_data = hts_loader.load_hts_data()

        # Get statistics
        stats = hts_loader.get_hts_statistics()
        logger.info(f"  Total HTS entries: {stats['total_entries']}")
        logger.info(f"  Unique codes: {stats['unique_codes']}")
        logger.info(f"  Indent levels: {len(stats['indent_levels'])}")
        logger.info(f"  Chapters: {stats['total_chapters']}")

        # Initialize hierarchy
        hts_hierarchy = HTSHierarchy(hts_data)

        # Test hierarchy navigation with a known code
        db_manager = DatabaseManager(config.DATABASE_PATH)
        with db_manager.get_connection() as conn:
            cursor = conn.execute(
                "SELECT final_hts FROM products WHERE final_hts IS NOT NULL LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                test_code = row["final_hts"]

                # Test get_classification_context
                context = hts_hierarchy.get_classification_context(test_code)

                if "error" not in context:
                    logger.info(f"  Test HTS code: {test_code}")
                    logger.info(f"  Description: {context['description'][:60]}...")
                    logger.info(f"  Hierarchy depth: {len(context['hierarchy_path'])}")
                    logger.info(f"  Children: {context['total_children']}")
                else:
                    logger.warning(
                        f"  HTS code {test_code} not found in reference data"
                    )

        # Test search functionality
        search_results = hts_hierarchy.find_similar_codes(
            ["iron", "pipe"], max_results=3
        )
        logger.info(f"  Search test ('iron', 'pipe'): {len(search_results)} results")

        logger.info("✓ HTS hierarchy validated")
        return True

    except Exception as e:
        logger.error(f"✗ HTS hierarchy validation failed: {e}")
        return False


def validate_data_quality(config, logger) -> bool:
    """Test data validation"""
    logger.info("\n[4/4] Testing Data Quality...")

    try:
        db_manager = DatabaseManager(config.DATABASE_PATH)
        validator = DataValidator(db_manager)

        # Generate quality report
        quality_report = validator.generate_quality_report()

        logger.info(f"  Total products: {quality_report['summary']['total_products']}")
        logger.info(
            f"  Data quality score: {quality_report['summary']['data_quality_score']:.2f}"
        )
        logger.info(
            f"  Validation passed: {quality_report['summary']['validation_passed']}"
        )

        logger.info(
            f"  Overall completeness: {quality_report['completeness']['overall_completeness']:.2%}"
        )
        logger.info(
            f"  Key fields completeness: {quality_report['completeness']['key_fields_completeness']:.2%}"
        )

        # Show any errors
        if quality_report["issues"]["errors"]:
            logger.warning(
                f"  Data quality issues found: {len(quality_report['issues']['errors'])}"
            )
            for error in quality_report["issues"]["errors"][:3]:
                logger.warning(f"    - {error}")

        logger.info("✓ Data quality validated")
        return True

    except Exception as e:
        logger.error(f"✗ Data quality validation failed: {e}")
        return False


def generate_validation_report(results: dict, logger):
    """Create comprehensive validation report"""
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)

    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"{test_name.replace('_', ' ').title()}: {status}")

    passed_count = sum(results.values())
    total_count = len(results)
    logger.info(f"\nTotal: {passed_count}/{total_count} tests passed")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

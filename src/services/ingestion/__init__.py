"""
Data Ingestion Service - Public API

This service loads CSV product data, validates it, and stores it in SQLite
with a two-table design: products (immutable) + processing_results (mutable)

Usage:
    from src.services.ingestion import ingest_products, ProductDatabase

    # Full ingestion pipeline
    report = ingest_products('data/input/cleaned_test_ch73.csv')

    # Direct database access
    db = ProductDatabase()
    product = db.get_product_by_id('ITEM123')
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

from .config import (
    DATABASE_PATH,
    CSV_PATH,
    LOG_DIR,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    DEBUG_DIR,
)
from .models import (
    ProductRecord,
    ProcessingResults,
    ValidationReport,
    DatabaseStatistics,
    IntegrityReport,
    ProductWithProcessing,
    UpdateProcessingInput,
)
from .loader import CSVLoader
from .validator import DataValidator
from .database import ProductDatabase

# Configure logging
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Main log file (all levels)
main_log_handler = logging.FileHandler(LOG_DIR / "ingestion.log", mode="w")
main_log_handler.setLevel(logging.DEBUG)
main_log_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))

# Error log file (errors only)
error_log_handler = logging.FileHandler(LOG_DIR / "ingestion_errors.log", mode="w")
error_log_handler.setLevel(logging.ERROR)
error_log_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))

# Console handler (colored if possible)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))

# Configure root logger for this module
logger = logging.getLogger("src.services.ingestion")
logger.setLevel(logging.DEBUG)
logger.addHandler(main_log_handler)
logger.addHandler(error_log_handler)
logger.addHandler(console_handler)

# Prevent duplicate logs
logger.propagate = False


def ingest_products(
    csv_path: Optional[Path] = None,
    db_path: Optional[Path] = None,
    validate: bool = True,
) -> ValidationReport:
    """
    Main ingestion pipeline: Load CSV → Validate → Create DB → Insert

    This is the primary entry point for the ingestion service.

    Args:
        csv_path: Path to CSV file (defaults to config.CSV_PATH)
        db_path: Path to database file (defaults to config.DATABASE_PATH)
        validate: Whether to run validation (default: True)

    Returns:
        ValidationReport with detailed validation results

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If validation fails critically
        sqlite3.IntegrityError: If duplicate item_ids found

    Example:
        >>> report = ingest_products('data/input/cleaned_test_ch73.csv')
        >>> print(f"Loaded {report.total_records} records")
        >>> print(f"Quality score: {report.quality_score}")
    """
    csv_path = Path(csv_path) if csv_path else CSV_PATH
    db_path = Path(db_path) if db_path else DATABASE_PATH

    logger.info("=" * 80)
    logger.info("STARTING DATA INGESTION SERVICE")
    logger.info("=" * 80)

    # Step 1: Load CSV
    logger.info("STEP 1: Loading CSV...")
    loader = CSVLoader()
    df = loader.load(csv_path)

    # Step 2: Validate data
    validation_report = None
    if validate:
        logger.info("STEP 2: Validating data...")
        validator = DataValidator()
        validation_report = validator.validate(df)

        # Check if validation passed
        if not validation_report.validation_passed:
            logger.error("✗ Validation failed! Critical issues found:")
            for issue in validation_report.critical_issues[:10]:
                logger.error(f"  - {issue.message}")
            raise ValueError(
                f"Validation failed with {len(validation_report.critical_issues)} critical issues. "
                f"First issue: {validation_report.critical_issues[0].message if validation_report.critical_issues else 'Unknown'}"
            )

        logger.info(
            f" Validation passed! Quality score: {validation_report.quality_score:.2f}"
        )
    else:
        logger.warning("STEP 2: Validation skipped (validate=False)")
        # Create minimal validation report
        from .models import ValidationReport

        validation_report = ValidationReport(
            total_records=len(df),
            expected_columns=[],
            found_columns=list(df.columns),
            missing_columns=[],
            extra_columns=[],
            valid_hts_count=0,
            invalid_hts_count=0,
            valid_hts_percentage=0.0,
            sample_invalid_hts=[],
            null_counts={},
            rows_with_null_required_fields=[],
            complete_required_fields_count=len(df),
            complete_required_fields_percentage=100.0,
            duplicate_count=0,
            duplicate_item_ids=[],
            completeness_by_column={},
            low_completeness_columns=[],
            quality_score=1.0,
            quality_score_breakdown={},
            critical_issues=[],
            warnings=[],
            validation_passed=True,
        )

    # Step 3: Convert to ProductRecords
    logger.info("STEP 3: Converting to ProductRecord instances...")
    products = loader.to_product_records(df)
    logger.info(f" Converted {len(products)} records")

    # Step 4: Create database schema
    logger.info("STEP 4: Creating database schema...")
    db = ProductDatabase(db_path)
    db.create_schema()

    # Step 5: Insert products
    logger.info("STEP 5: Inserting products into database...")
    inserted_count = db.insert_products(products)
    logger.info(f" Inserted {inserted_count} products")

    # Step 6: Verify insertion
    logger.info("STEP 6: Verifying database...")
    stats = db.get_database_statistics()
    logger.info(f" Database statistics:")
    logger.info(f"  Total products: {stats.total_products}")
    logger.info(f"  Processed: {stats.processed_count}")
    logger.info(f"  Unprocessed: {stats.unprocessed_count}")
    logger.info(f"  Unique HTS codes: {stats.unique_hts_codes}")

    # Calculate database file size
    if db_path.exists():
        db_size_mb = db_path.stat().st_size / (1024 * 1024)
        logger.info(f"  Database file size: {db_size_mb:.2f} MB")

    logger.info("=" * 80)
    logger.info("INGESTION COMPLETE!")
    logger.info("=" * 80)

    return validation_report


def export_debug_sample(
    n: int = 10, db_path: Optional[Path] = None, output_path: Optional[Path] = None
) -> List[Dict[str, Any]]:
    """
    Export first N records to JSON for debugging

    Args:
        n: Number of records to export (default: 10)
        db_path: Path to database file (defaults to config.DATABASE_PATH)
        output_path: Output JSON file path (defaults to data/debug/sample.json)

    Returns:
        List of product dictionaries

    Example:
        >>> records = export_debug_sample(n=20)
        >>> print(f"Exported {len(records)} records")
    """
    db_path = Path(db_path) if db_path else DATABASE_PATH
    output_path = Path(output_path) if output_path else (DEBUG_DIR / "sample.json")

    logger.info(f"Exporting {n} debug samples to {output_path}")

    db = ProductDatabase(db_path)
    records = db.export_sample_records(n=n, output_path=output_path)

    logger.info(f" Exported {len(records)} records")
    return records


def get_database_info(db_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Get comprehensive database information

    Args:
        db_path: Path to database file (defaults to config.DATABASE_PATH)

    Returns:
        Dictionary with statistics and integrity report

    Example:
        >>> info = get_database_info()
        >>> print(f"Total products: {info['statistics'].total_products}")
        >>> print(f"Integrity passed: {info['integrity'].integrity_passed}")
    """
    db_path = Path(db_path) if db_path else DATABASE_PATH

    logger.info("Gathering database information...")

    db = ProductDatabase(db_path)

    stats = db.get_database_statistics()
    integrity = db.verify_database_integrity()

    return {
        "statistics": stats,
        "integrity": integrity,
        "database_path": str(db_path.absolute()),
        "database_size_mb": (
            db_path.stat().st_size / (1024 * 1024) if db_path.exists() else 0
        ),
    }


# Public exports
__all__ = [
    # Main functions
    "ingest_products",
    "export_debug_sample",
    "get_database_info",
    # Core classes
    "CSVLoader",
    "DataValidator",
    "ProductDatabase",
    # Models
    "ProductRecord",
    "ProcessingResults",
    "ValidationReport",
    "DatabaseStatistics",
    "IntegrityReport",
    "ProductWithProcessing",
    "UpdateProcessingInput",
    # Config paths
    "DATABASE_PATH",
    "CSV_PATH",
]

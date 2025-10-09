"""
Ingest the real product data
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.ingestion import ingest_products, get_database_info


def main():
    csv_path = Path("data/input/cleaned_test_ch73.csv")

    # Check if file exists
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path.absolute()}")
        print(f"Please place your cleaned_test_ch73.csv in: data/input/")
        return

    print("=" * 80)
    print("STARTING REAL DATA INGESTION")
    print("=" * 80)
    print(f"CSV file: {csv_path.absolute()}")
    print()

    # Run ingestion
    try:
        report = ingest_products(csv_path=csv_path)

        print("\n" + "=" * 80)
        print("VALIDATION REPORT SUMMARY")
        print("=" * 80)
        print(f"Total records: {report.total_records}")
        print(f"Quality score: {report.quality_score:.2f}")
        print(
            f"Valid HTS codes: {report.valid_hts_count}/{report.total_records} ({report.valid_hts_percentage:.1f}%)"
        )
        print(
            f"Complete required fields: {report.complete_required_fields_count}/{report.total_records} ({report.complete_required_fields_percentage:.1f}%)"
        )
        print(f"Duplicates found: {report.duplicate_count}")
        print(f"Validation passed: {'YES' if report.validation_passed else 'NO'}")

        if report.critical_issues:
            print(f"\n Critical issues: {len(report.critical_issues)}")
            for issue in report.critical_issues[:5]:
                print(f"  - {issue.message}")

        if report.warnings:
            print(f"\n Warnings: {len(report.warnings)}")
            for warning in report.warnings[:5]:
                print(f"  - {warning.message}")

        # Get database info
        print("\n" + "=" * 80)
        print("DATABASE INFO")
        print("=" * 80)
        info = get_database_info()
        stats = info["statistics"]
        print(f"Database path: {info['database_path']}")
        print(f"Database size: {info['database_size_mb']:.2f} MB")
        print(f"Total products: {stats.total_products}")
        print(f"Processed: {stats.processed_count}")
        print(f"Unprocessed: {stats.unprocessed_count}")
        print(f"Unique HTS codes: {stats.unique_hts_codes}")

        print("\nINGESTION COMPLETE!")
        print(f"Check logs at: data/logs/ingestion.log")

    except Exception as e:
        print(f"\nERROR: {e}")
        print(f"Check error log at: data/logs/ingestion_errors.log")
        raise


if __name__ == "__main__":
    main()

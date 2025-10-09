"""
Data validation logic for CSV input
Performs comprehensive validation with detailed checkpoints
"""

import pandas as pd
from typing import List, Dict, Any
import logging
from .config import (
    PRODUCT_COLUMNS,
    REQUIRED_FIELDS,
    HTS_PATTERN,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
)
from .models import ValidationReport, ValidationIssue

# Configure logger
logger = logging.getLogger(__name__)


class DataValidator:
    """Validates CSV data before database insertion"""

    def __init__(self):
        self.hts_pattern = HTS_PATTERN
        self.expected_columns = PRODUCT_COLUMNS
        self.required_fields = REQUIRED_FIELDS

    def validate(self, df: pd.DataFrame) -> ValidationReport:
        """
        Perform comprehensive validation on DataFrame

        Args:
            df: DataFrame loaded from CSV

        Returns:
            ValidationReport with all validation results
        """
        logger.info("Starting validation checks...")

        critical_issues = []
        warnings = []

        # 1. Column validation
        logger.info("Running column validation...")
        column_results = self.validate_columns(df)
        if column_results["missing_columns"]:
            for col in column_results["missing_columns"]:
                critical_issues.append(
                    ValidationIssue(
                        severity="ERROR",
                        message=f"Missing required column: {col}",
                        column=col,
                    )
                )
        if column_results["extra_columns"]:
            for col in column_results["extra_columns"]:
                warnings.append(
                    ValidationIssue(
                        severity="WARNING",
                        message=f"Extra column found (will be ignored): {col}",
                        column=col,
                    )
                )
        logger.info(
            f" Column validation complete: {len(column_results['found_columns'])}/{len(self.expected_columns)} expected columns found"
        )

        # 2. HTS code validation
        logger.info("Running HTS code validation...")
        hts_results = self.validate_hts_codes(df)
        logger.info(
            f" HTS validation: {hts_results['valid_count']}/{len(df)} valid ({hts_results['valid_percentage']:.1f}%)"
        )

        if hts_results["sample_invalid"]:
            logger.debug(
                f"Sample invalid HTS codes: {hts_results['sample_invalid'][:10]}"
            )
            for invalid in hts_results["sample_invalid"][:10]:
                warnings.append(
                    ValidationIssue(
                        severity="WARNING",
                        message=f"Invalid HTS code format",
                        row_number=invalid["row"],
                        column="final_hts",
                        value=invalid["value"],
                    )
                )

        # 3. Required fields validation
        logger.info("Running required fields validation...")
        required_results = self.validate_required_fields(df)
        logger.info(
            f" Required fields: {required_results['complete_count']}/{len(df)} complete ({required_results['complete_percentage']:.1f}%)"
        )

        if required_results["rows_with_nulls"]:
            logger.warning(
                f"{len(required_results['rows_with_nulls'])} rows with NULL required fields: {required_results['rows_with_nulls'][:20]}"
            )
            for row_num in required_results["rows_with_nulls"][:20]:
                critical_issues.append(
                    ValidationIssue(
                        severity="ERROR",
                        message=f"Row has NULL in required fields",
                        row_number=row_num,
                    )
                )

        # 4. Duplicate detection
        logger.info("Running duplicate detection...")
        duplicate_results = self.detect_duplicates(df)

        if duplicate_results["duplicate_count"] > 0:
            logger.warning(
                f"✗ Duplicate check: {duplicate_results['duplicate_count']} duplicates found"
            )
            for dup in duplicate_results["duplicates"][:10]:
                critical_issues.append(
                    ValidationIssue(
                        severity="ERROR",
                        message=f"Duplicate item_id found",
                        column="item_id",
                        value=dup["item_id"],
                    )
                )
        else:
            logger.info(" Duplicate check: No duplicates found")

        # 5. Data completeness check
        logger.info("Running data completeness check...")
        completeness_results = self.check_completeness(df)

        for col in completeness_results["low_completeness_columns"]:
            completeness = completeness_results["completeness_by_column"][col]
            warnings.append(
                ValidationIssue(
                    severity="WARNING",
                    message=f"Column has low completeness: {completeness:.1f}%",
                    column=col,
                )
            )
            logger.warning(f"Column '{col}' has low completeness: {completeness:.1f}%")

        logger.debug(
            f"Completeness by column: {completeness_results['completeness_by_column']}"
        )

        # 6. Calculate quality score
        logger.info("Calculating overall quality score...")
        quality_results = self.calculate_quality_score(
            hts_results, required_results, duplicate_results
        )
        logger.info(
            f" Quality score: {quality_results['score']:.2f} (HTS: {quality_results['breakdown']['hts']:.2f}, Completeness: {quality_results['breakdown']['completeness']:.2f}, Duplicates: {quality_results['breakdown']['duplicates']:.2f})"
        )

        # Determine if validation passed
        validation_passed = (
            len(column_results["missing_columns"]) == 0
            and duplicate_results["duplicate_count"] == 0
            and len(required_results["rows_with_nulls"]) == 0
        )

        # Build report
        report = ValidationReport(
            total_records=len(df),
            expected_columns=self.expected_columns,
            found_columns=column_results["found_columns"],
            missing_columns=column_results["missing_columns"],
            extra_columns=column_results["extra_columns"],
            valid_hts_count=hts_results["valid_count"],
            invalid_hts_count=hts_results["invalid_count"],
            valid_hts_percentage=hts_results["valid_percentage"],
            sample_invalid_hts=hts_results["sample_invalid"],
            null_counts=required_results["null_counts"],
            rows_with_null_required_fields=required_results["rows_with_nulls"],
            complete_required_fields_count=required_results["complete_count"],
            complete_required_fields_percentage=required_results["complete_percentage"],
            duplicate_count=duplicate_results["duplicate_count"],
            duplicate_item_ids=duplicate_results["duplicates"],
            completeness_by_column=completeness_results["completeness_by_column"],
            low_completeness_columns=completeness_results["low_completeness_columns"],
            quality_score=quality_results["score"],
            quality_score_breakdown=quality_results["breakdown"],
            critical_issues=critical_issues,
            warnings=warnings,
            validation_passed=validation_passed,
        )

        logger.info(f"Validation complete! Passed: {validation_passed}")
        return report

    def validate_columns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that all expected columns exist

        Returns:
            Dict with found, missing, and extra columns
        """
        found_columns = list(df.columns)
        missing_columns = [
            col for col in self.expected_columns if col not in found_columns
        ]
        extra_columns = [
            col for col in found_columns if col not in self.expected_columns
        ]

        logger.debug(f"Expected columns: {self.expected_columns}")
        logger.debug(f"Found columns: {found_columns}")

        return {
            "found_columns": found_columns,
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
        }

    def validate_hts_codes(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate HTS code format (####.##.##.##)

        Returns:
            Dict with valid/invalid counts and samples
        """
        if "final_hts" not in df.columns:
            return {
                "valid_count": 0,
                "invalid_count": len(df),
                "valid_percentage": 0.0,
                "sample_invalid": [],
            }

        # Check for valid HTS codes
        valid_mask = df["final_hts"].astype(str).str.match(self.hts_pattern, na=False)
        valid_count = valid_mask.sum()
        invalid_count = len(df) - valid_count
        valid_percentage = (valid_count / len(df) * 100) if len(df) > 0 else 0.0

        # Get sample of invalid codes
        invalid_df = df[~valid_mask].reset_index()
        sample_invalid = [
            {
                "row": int(row["index"]) + 2,  # +2 for 1-based index and header
                "value": str(row["final_hts"]),
            }
            for _, row in invalid_df.head(10).iterrows()
        ]

        logger.debug(
            f"First 10 valid HTS codes: {df[valid_mask]['final_hts'].head(10).tolist()}"
        )
        logger.debug(f"First 10 invalid HTS codes: {sample_invalid}")

        return {
            "valid_count": int(valid_count),
            "invalid_count": int(invalid_count),
            "valid_percentage": valid_percentage,
            "sample_invalid": sample_invalid,
        }

    def validate_required_fields(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check for NULL values in required fields

        Returns:
            Dict with null counts and rows with issues
        """
        null_counts = {}
        rows_with_nulls = []

        for field in self.required_fields:
            if field in df.columns:
                null_mask = df[field].isna() | (df[field].astype(str).str.strip() == "")
                null_count = null_mask.sum()
                null_counts[field] = int(null_count)

                if null_count > 0:
                    null_rows = df[null_mask].index.tolist()
                    rows_with_nulls.extend(
                        [int(r) + 2 for r in null_rows]
                    )  # +2 for 1-based and header
                    logger.debug(
                        f"Column '{field}' has {null_count} NULL values at rows: {null_rows[:10]}"
                    )

        # Remove duplicates and sort
        rows_with_nulls = sorted(list(set(rows_with_nulls)))

        # Count complete rows
        complete_count = len(df) - len(rows_with_nulls)
        complete_percentage = (complete_count / len(df) * 100) if len(df) > 0 else 0.0

        return {
            "null_counts": null_counts,
            "rows_with_nulls": rows_with_nulls,
            "complete_count": complete_count,
            "complete_percentage": complete_percentage,
        }

    def detect_duplicates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect duplicate item_id values

        Returns:
            Dict with duplicate count and details
        """
        if "item_id" not in df.columns:
            return {"duplicate_count": 0, "duplicates": []}

        # Find duplicates
        duplicate_mask = df.duplicated(subset=["item_id"], keep=False)
        duplicate_df = df[duplicate_mask].sort_values("item_id")

        # Group duplicates
        duplicates = []
        if len(duplicate_df) > 0:
            for item_id, group in duplicate_df.groupby("item_id"):
                rows = [
                    int(idx) + 2 for idx in group.index
                ]  # +2 for 1-based and header
                duplicates.append({"item_id": str(item_id), "rows": rows})
                logger.debug(f"Duplicate item_id '{item_id}' found at rows: {rows}")

        return {"duplicate_count": len(duplicates), "duplicates": duplicates}

    def check_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate data completeness for each column

        Returns:
            Dict with completeness percentages
        """
        completeness_by_column = {}
        low_completeness_columns = []

        for col in df.columns:
            non_null_count = df[col].notna().sum()
            completeness = (non_null_count / len(df) * 100) if len(df) > 0 else 0.0
            completeness_by_column[col] = round(completeness, 2)

            if completeness < 80.0:
                low_completeness_columns.append(col)

        return {
            "completeness_by_column": completeness_by_column,
            "low_completeness_columns": low_completeness_columns,
        }

    def calculate_quality_score(
        self,
        hts_results: Dict[str, Any],
        required_results: Dict[str, Any],
        duplicate_results: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Calculate overall data quality score (0.0 to 1.0)

        Weights:
        - Valid HTS codes: 0.4
        - Complete required fields: 0.4
        - No duplicates: 0.2

        Returns:
            Dict with score and breakdown
        """
        # HTS score (0.0 to 1.0)
        hts_score = hts_results["valid_percentage"] / 100.0

        # Completeness score (0.0 to 1.0)
        completeness_score = required_results["complete_percentage"] / 100.0

        # Duplicate score (1.0 if no duplicates, 0.0 if any duplicates)
        duplicate_score = 1.0 if duplicate_results["duplicate_count"] == 0 else 0.0

        # Weighted total
        total_score = hts_score * 0.4 + completeness_score * 0.4 + duplicate_score * 0.2

        logger.debug(
            f"Quality score breakdown - HTS: {hts_score:.2f}, Completeness: {completeness_score:.2f}, Duplicates: {duplicate_score:.2f}, Total: {total_score:.2f}"
        )

        return {
            "score": round(total_score, 2),
            "breakdown": {
                "hts": round(hts_score, 2),
                "completeness": round(completeness_score, 2),
                "duplicates": round(duplicate_score, 2),
            },
        }

"""
Data quality validation and reporting for ctOS Service 1
"""

import pandas as pd
import re
from typing import Dict, List, Tuple
from dataclasses import dataclass

from ..core.database import DatabaseManager
from ..utils.logger import setup_logger
from ..utils.exceptions import DataValidationError

logger = setup_logger("ctOS.validator")


@dataclass
class ValidationResult:
    """Validation result container"""

    is_valid: bool
    errors: List[str]
    warnings: List[str]
    statistics: Dict


class DataValidator:
    """Data quality checks and validation"""

    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager

        # HTS code pattern (e.g., 7301.10.00.00)
        self.hts_pattern = re.compile(r"^\d{4}\.\d{2}\.\d{2}\.\d{2}$")

    def validate_product_data(self, df: pd.DataFrame = None) -> Dict:
        """Comprehensive product validation"""
        if df is None and self.db_manager:
            # Load data from database
            with self.db_manager.get_connection() as conn:
                df = pd.read_sql_query("SELECT * FROM products", conn)

        if df is None or df.empty:
            return {
                "total_products": 0,
                "data_quality_score": 0.0,
                "errors": ["No data to validate"],
            }

        results = {
            "total_products": len(df),
            "validation_timestamp": pd.Timestamp.now(),
            "errors": [],
            "warnings": [],
        }

        # Check required fields
        required_valid, required_errors = self.check_required_fields(df)
        results["required_fields_valid"] = required_valid
        results["errors"].extend(required_errors)

        # Check data types and formats
        types_valid, type_errors = self.check_data_types(df)
        results["data_types_valid"] = types_valid
        results["errors"].extend(type_errors)

        # Check HTS codes
        hts_valid, hts_errors = self.check_hts_codes(df)
        results["hts_codes_valid"] = hts_valid
        results["errors"].extend(hts_errors)

        # Calculate quality metrics
        results.update(self._calculate_quality_metrics(df))

        # Overall quality score
        results["data_quality_score"] = self._calculate_quality_score(results)

        logger.info(
            f"Validated {results['total_products']} products. Quality score: {results['data_quality_score']:.2f}"
        )

        return results

    def check_required_fields(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Required field validation"""
        errors = []
        required_fields = ["item_id", "item_description"]

        for field in required_fields:
            if field not in df.columns:
                errors.append(f"Missing required column: {field}")
                continue

            null_count = df[field].isna().sum()
            empty_count = (df[field].astype(str).str.strip() == "").sum()

            if null_count > 0:
                errors.append(f"Column {field} has {null_count} null values")
            if empty_count > 0:
                errors.append(f"Column {field} has {empty_count} empty values")

        return len(errors) == 0, errors

    def check_data_types(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Data type validation"""
        errors = []

        # Check item_id uniqueness
        if "item_id" in df.columns:
            duplicate_count = df["item_id"].duplicated().sum()
            if duplicate_count > 0:
                errors.append(f"Found {duplicate_count} duplicate item_ids")

        # Check string length constraints
        string_columns = ["item_description", "material_class", "material_detail"]
        for col in string_columns:
            if col in df.columns:
                max_length = df[col].astype(str).str.len().max()
                if max_length > 500:  # Reasonable limit
                    errors.append(
                        f"Column {col} has values longer than 500 characters (max: {max_length})"
                    )

        return len(errors) == 0, errors

    def check_hts_codes(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """HTS code format validation"""
        errors = []

        if "final_hts" not in df.columns:
            errors.append("Column 'final_hts' not found")
            return False, errors

        # Filter out null/empty HTS codes
        hts_values = df["final_hts"].dropna()
        hts_values = hts_values[hts_values.astype(str).str.strip() != ""]

        if len(hts_values) == 0:
            errors.append("No HTS codes found to validate")
            return False, errors

        # Validate HTS format
        invalid_hts = []
        for idx, hts_code in hts_values.items():
            if not self.hts_pattern.match(str(hts_code).strip()):
                invalid_hts.append(f"Row {idx}: '{hts_code}'")

        if invalid_hts:
            errors.append(f"Invalid HTS code format found in {len(invalid_hts)} rows")
            # Log first few examples
            for example in invalid_hts[:3]:
                errors.append(f"  Example: {example}")

        return len(errors) == 0, errors

    def _calculate_quality_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate data quality metrics"""
        metrics = {}

        # Completeness metrics
        total_cells = len(df) * len(df.columns)
        null_cells = df.isna().sum().sum()
        metrics["completeness_score"] = (
            1.0 - (null_cells / total_cells) if total_cells > 0 else 0.0
        )

        # Key field completeness
        key_fields = ["item_id", "item_description", "final_hts", "material_class"]
        key_completeness = []
        for field in key_fields:
            if field in df.columns:
                field_completeness = 1.0 - (df[field].isna().sum() / len(df))
                key_completeness.append(field_completeness)
                metrics[f"{field}_completeness"] = field_completeness

        metrics["key_fields_avg_completeness"] = (
            sum(key_completeness) / len(key_completeness) if key_completeness else 0.0
        )

        # Uniqueness (for item_id)
        if "item_id" in df.columns:
            unique_ids = df["item_id"].nunique()
            metrics["item_id_uniqueness"] = unique_ids / len(df) if len(df) > 0 else 0.0

        return metrics

    def _calculate_quality_score(self, results: Dict) -> float:
        """Calculate overall data quality score"""
        score_components = []

        # Completeness weight: 40%
        if "completeness_score" in results:
            score_components.append(results["completeness_score"] * 0.4)

        # Key field completeness weight: 30%
        if "key_fields_avg_completeness" in results:
            score_components.append(results["key_fields_avg_completeness"] * 0.3)

        # Validity (fewer errors = higher score) weight: 20%
        error_count = len(results.get("errors", []))
        validity_score = max(0.0, 1.0 - (error_count * 0.1))  # Penalty for each error
        score_components.append(validity_score * 0.2)

        # Uniqueness weight: 10%
        if "item_id_uniqueness" in results:
            score_components.append(results["item_id_uniqueness"] * 0.1)

        return sum(score_components) if score_components else 0.0

    def generate_quality_report(self, df: pd.DataFrame = None) -> Dict:
        """Generate comprehensive data quality summary report"""
        validation_results = self.validate_product_data(df)

        report = {
            "summary": {
                "total_products": validation_results["total_products"],
                "data_quality_score": validation_results["data_quality_score"],
                "validation_passed": validation_results["data_quality_score"] >= 0.7,
                "validation_timestamp": validation_results["validation_timestamp"],
            },
            "completeness": {
                "overall_completeness": validation_results.get(
                    "completeness_score", 0.0
                ),
                "key_fields_completeness": validation_results.get(
                    "key_fields_avg_completeness", 0.0
                ),
            },
            "validity": {
                "required_fields_valid": validation_results.get(
                    "required_fields_valid", False
                ),
                "data_types_valid": validation_results.get("data_types_valid", False),
                "hts_codes_valid": validation_results.get("hts_codes_valid", False),
            },
            "issues": {
                "errors": validation_results["errors"],
                "warnings": validation_results.get("warnings", []),
            },
        }

        return report

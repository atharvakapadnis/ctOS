"""
Rule validation logic for rules management service
"""

import re
import logging
from typing import Dict, List, Tuple, Any

from .models import ValidationResult, ValidationReport
from .config import ALLOWED_RULE_TYPES, RULE_ID_PATTERN

logger = logging.getLogger(__name__)


class RuleValidator:
    """Validates rules and rule sets"""

    def validate_rule(self, rule: Dict) -> ValidationResult:
        """
        Validate a single rule

        Args:
            rule: Dictionary containing rule data

        Returns:
            ValidationResult with validation status and errors
        """
        errors = []
        warnings = []
        rule_id = rule.get("rule_id", "UNKNOWN")

        # Check required fields
        required_fields = [
            "rule_id",
            "rule_name",
            "rule_content",
            "rule_type",
            "active",
        ]
        for field in required_fields:
            if field not in rule:
                errors.append(f"Missing required field: {field}")

        # Validate field types
        if "rule_id" in rule and not isinstance(rule["rule_id"], str):
            errors.append(f"rule_id must be a string, got: {type(rule['rule_id'])}")
        if "rule_name" in rule and not isinstance(rule["rule_name"], str):
            errors.append(f"rule_name must be a string, got: {type(rule['rule_name'])}")
        if "rule_content" in rule and not isinstance(rule["rule_content"], str):
            errors.append(
                f"rule_content must be a string, got: {type(rule['rule_content'])}"
            )
        if "rule_type" in rule and not isinstance(rule["rule_type"], str):
            errors.append(f"rule_type must be a string, got: {type(rule['rule_type'])}")
        if "active" in rule and not isinstance(rule["active"], bool):
            errors.append(f"active must be a boolean, got: {type(rule['active'])}")

        # Validate rule_id pattern
        if "rule_id" in rule and isinstance(rule["rule_id"], str):
            if not self.validate_rule_id(rule["rule_id"]):
                errors.append(f"rule_id must match pattern {RULE_ID_PATTERN}")

        # Validate rule_type
        if "rule_type" in rule and isinstance(rule["rule_type"], str):
            if not self.validate_rule_type(rule["rule_type"]):
                errors.append(f"rule_type must be one of {ALLOWED_RULE_TYPES}")

        # Validate content not empty
        if "rule_name" in rule and isinstance(rule["rule_name"], str):
            if not rule["rule_name"].strip():
                errors.append("rule_name cannot be empty")

        if "rule_content" in rule and isinstance(rule["rule_content"], str):
            if not rule["rule_content"].strip():
                errors.append("rule_content cannot be empty")

        # Check Optional fields
        if "created_at" not in rule:
            warnings.append("Optional field, 'created_at' not provided")

        if "description" not in rule:
            warnings.append("Optional field, 'description' not provided")

        return ValidationResult(
            valid=len(errors) == 0, rule_id=rule_id, errors=errors, warnings=warnings
        )

    def validate_rule_set(self, rules: Dict) -> ValidationReport:
        """
        Validate entire rule set

        Args:
            rules: List of rule dictionaries

        Returns:
            ValidationReport with comprehensive validation results
        """
        total_rules = len(rules)
        valid_rules = 0
        invalid_rules = 0
        all_errors = []
        all_warnings = []

        # Validate each rule
        for rule in rules:
            result = self.validate_rule(rule)
            if result.valid:
                valid_rules += 1
            else:
                invalid_rules += 1
                for error in result.errors:
                    all_errors.append(f"Rule {result.rule_id}: {error}")

            for warning in result.warnings:
                all_warnings.append(f"Rule {result.rule_id}: {warning}")

        # Check for duplicate IDs
        is_unique, duplicate_ids = self.check_duplicate_ids(rules)
        if not is_unique:
            all_errors.append(f"Duplicate rule IDs found: {duplicate_ids}")

        return ValidationReport(
            valid=(invalid_rules == 0 and is_unique),
            total_rules=total_rules,
            valid_rules=valid_rules,
            invalid_rules=invalid_rules,
            errors=all_errors,
            warnings=all_warnings,
            duplicate_ids=duplicate_ids,
        )

    def check_unique_ids(self, rules: List[Dict]) -> Tuple[bool, List[str]]:
        """
        Check all rule_ids are unique

        Args:
            rules: List of rule dictionaries

        Returns:
            Tuple of (is_unique, duplicate_ids)
        """
        rule_ids = [rule.get("rule_id") for rule in rules if "rule_id" in rule]
        duplicates = []
        seen = set()

        for rule_id in rule_ids:
            if rule_id in seen:
                if rule_id not in duplicates:
                    duplicates.append(rule_id)

            else:
                seen.add(rule_id)

        return len(duplicates) == 0, duplicates

    def validate_rule_id_format(self, rule_id: str) -> bool:
        """
        Check rule_id matches pattern

        Args:
            rule_id: Rule ID string

        Returns:
            True if matches pattern, False otherwise
        """
        return bool(re.match(RULE_ID_PATTERN, rule_id))

    def validate_rule_type(self, rule_type: str) -> bool:
        """
        Check rule_type is allowed

        Args:
            rule_type: Rule type string

        Returns:
            True if allowed, False otherwise
        """
        return rule_type in ALLOWED_RULE_TYPES

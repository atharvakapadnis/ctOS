"""
Rule manager - CRUD operations for Rule management Service
"""

import re
import json
import logging
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path

from .models import Rule, RuleSet, ValidationReport
from .validator import RuleValidator
from .config import (
    RULES_FILE,
    LOG_FILE,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    RULE_ID_PATTERN,
    ALLOWED_RULE_TYPES,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


class RuleManager:
    """Manages rules: loading, validation, caching and retrieval"""

    def __init__(self, rules_file: Optional[Path] = None):
        """
        Initialize Rule Manager

        Args:
            rules_file: Path to rules JSON file (uses default if None)
        """
        self.rules_file = rules_file or RULES_FILE
        self.validator = RuleValidator()
        self._rules_cache: List[Rule] = []
        self._cache_loaded = False

        logger.info(f"RuleManager initialized with file: {self.rules_file}")

    def load_rules(self) -> List[Rule]:
        """
        Load all rules from JSON file

        Returns:
            List of Rule objects (both active and inactive)
        """
        # Return from cached if already loaded
        if self._cache_loaded:
            logger.debug(f"Returning {len(self._rules_cache)} rules from cache")
            return self._rules_cache

        # Check if file exists
        if not self.rules_file.exists():
            logger.warning(f"Ruels file not found: {self.rules_file}")
            logger.info("Starting with empty rule set. Create rules.json to add rules.")
            self._rules_cache = []
            self._cache_loaded = True
            return []

        try:
            # Load JSON file
            with open(self.rules_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Extract rules array
            rules_data = data.get("rules", [])

            if not rules_data:
                logger.warning("Rules file exists but contains no rules")
                self._rules_cache = []
                self._cache_loaded = True
                return []

            # Validated rules
            validation_report = self.validator.validate_rule_set(rules_data)

            if not validation_report.valid:
                logger.error("Rules validation failed:")
                for error in validation_report.errors:
                    logger.error(f"  - {error}")

                # Log warnings but continue
                for warning in validation_report.warnings:
                    logger.warning(f"  - {warning}")

                # Use only valid rules
                logger.warning(
                    f"Loading only valid rules: {validation_report.valid_rules}/{validation_report.total_rules}"
                )

            # Conver to Pydantic models (skip invalid rules)
            rules = []
            for rule_data in rules_data:
                try:
                    rule = Rule(**rule_data)
                    rules.append(rule)
                except Exception as e:
                    logger.warning(
                        f"Failed to parse rule {rule_data.get('rule_id', 'UNKNOWN')}: {e}"
                    )

            # Cache and return
            self._rules_cache = rules
            self._cache_loaded = True

            logger.info(f"Successfully loaded {len(rules)} rules from file")

            return rules

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in rules file: {e}")
            self._rules_cache = []
            self._cache_loaded = True
            return []

        except Exception as e:
            logger.error(f"Error loading rules file: {e}")
            self._rules_cache = []
            self._cache_loaded = True
            return []

    def get_active_rules(self) -> List[Rule]:
        """
        Get only active rules

        Returns:
            List of active Rule objects, sorted by rule_id
        """
        all_rules = self.load_rules()
        active_rules = [rule for rule in all_rules if rule.active]
        active_rules.sort(key=lambda r: r.rule_id)

        logger.debug(f"Returning {len(active_rules)} active rules")
        return active_rules

    def get_rule_by_id(self, rule_id: str) -> Optional[Rule]:
        """
        Get a single rule by its ID

        Args:
            rule_id: ID of the rule to retrieve

        Returns:
            Rule object or None if not found
        """
        all_rules = self.load_rules()
        for rule in all_rules:
            if rule.rule_id == rule_id:
                logger.debug(f"Found rule {rule_id}")
                return rule

        logger.debug(f"Rule {rule_id} not found")
        return None

    def get_rules_by_ids(self, rule_ids: List[str]) -> List[Rule]:
        """
        Get multiple rule by list of IDs

        Args:
            rule_ids: List of rule IDs to retrieve

        Returns:
            List of found Rule objects (skips invalid IDs)
        """

        all_rules = self.load_rules()
        rule_map = {rule.rule_id: rule for rule in all_rules}

        found_rules = []
        for rule_id in rule_ids:
            if rule_id in rule_map:
                found_rules.append(rule_map[rule_id])
            else:
                logger.warning(f"Rule ID not found, skipping: {rule_id}")

        logger.debug(f"Returning {len(found_rules)}/{len(rule_ids)} requested rules")
        return found_rules

    def get_rules_by_type(self, rule_type: str) -> List[Rule]:
        """
        Get all rules of a specific type

        Args:
            rule_type: Rule type to filter by

        Returns:
            List of Rule objects matching the type, sorted by rule_id
        """
        all_rules = self.load_rules()
        filtered_rules = [rule for rule in all_rules if rule.rule_type == rule_type]
        filtered_rules.sort(key=lambda r: r.rule_id)

        logger.debug(f"Returning {len(filtered_rules)} rules of type '{rule_type}'")
        return filtered_rules

    def get_all_rule_types(self) -> List[str]:
        """
        Get list of all rule types present in file

        Returns:
            List of unique rule types
        """
        all_rules = self.load_rules()
        rule_types = list(set(rule.rule_type for rule in all_rules))
        rule_types.sort()

        logger.debug(f"Found {len(rule_types)} unique rule types: {rule_types}")
        return rule_types

    def validate_rules_file(self) -> ValidationReport:
        """
        Validate entire rules.json file

        Returns:
            ValidationReport with validation results
        """
        logger.info("Validating rules file...")

        if not self.rules_file.exists():
            logger.error("Rules file does not exist")
            return ValidationReport(
                valid=False,
                total_rules=0,
                valid_rules=0,
                invalid_rules=0,
                errors=["Rules file does not exist"],
                warnings=[],
                duplicate_ids=[],
            )

        try:
            with open(self.rules_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            rules_data = data.get("rules", [])
            report = self.validator.validate_rule_set(rules_data)

            logger.info(
                f"Validation complete: {report.valid_rules}/{report.total_rules} valid"
            )
            return report

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            return ValidationReport(
                valid=False,
                total_rules=0,
                valid_rules=0,
                invalid_rules=0,
                errors=[f"Invalid JSOn: {str(e)}"],
                warnings=[],
                duplicate_ids=[],
            )

        except Exception as e:
            logger.error(f"Validation error: {e}")
            return ValidationReport(
                valid=False,
                total_rules=0,
                valid_rules=0,
                invalid_rules=0,
                errors=[f"Validation error: {str(e)}"],
                warnings=[],
                duplicate_ids=[],
            )

    def reload_rules(self) -> int:
        """
        Reload rules from file (clear cache)

        Returns:
            Count of loaded rules
        """
        logger.info("Reloading rules from file...")
        self._cache_loaded = False
        self._rules_cache = []

        rules = self.load_rules()
        logger.info(f"Reloaded {len(rules)} rules")

        return len(rules)

    def get_rules_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about loaded rules

        Returns:
            Dictionary with statistics
        """
        all_rules = self.load_rules()
        active_rules = [rule for rule in all_rules if rule.active]
        inactive_rules = [rule for rule in all_rules if not rule.active]

        # Count rules by type
        rules_by_type = {}
        for rule in all_rules:
            rule_type = rule.rule_type
            rules_by_type[rule_type] = rules_by_type.get(rule_type, 0) + 1

        stats = {
            "total_rules": len(all_rules),
            "active_rules": len(active_rules),
            "inactive_rules": len(inactive_rules),
            "rules_by_type": rules_by_type,
        }

        logger.debug(f"Statistics: {stats}")
        return stats

    def format_rules_for_prompt(self, rules: List[Rule]) -> str:
        """
        Format list of rules into text for LLM prompt

        Args:
            rules: List of Rule objects

        Returns:
            Formatted string for prompt injection
        """
        if not rules:
            return ""

        formatted_lines = ["Rules to Apply:"]
        for rule in rules:
            formatted_lines.append(f"- [{rule.rule_id}] {rule.rule_content}")

        formatted_text = "\n".join(formatted_lines)
        logger.debug(f"Formatted {len(rules)} rules for prompt")
        return formatted_text

    def get_next_rule_id(self) -> str:
        """
        Generare next available rule ID

        Returns:
            Next available rule ID in format R###
        """
        all_rules = self.load_rules()
        if not all_rules:
            logger.info("No rules found, starting with R001")
            return "R001"

        # Extract numeric parts and find max
        max_id = 0
        for rule in all_rules:
            try:
                numeric_part = int(rule.rule_id[1:])
                max_id = max(max_id, numeric_part)
            except (ValueError, IndexError):
                logger.warning(f"Invalid rule ID format: {rule.rule_id}")
                continue

        next_id = f"R{str(max_id + 1).zfill(3)}"
        logger.info(f"Next available rule ID: {next_id}")
        return next_id

    def validate_rule_for_save(
        self, rule_dict: Dict, rule_id_to_update: Optional[str] = None
    ) -> Tuple[bool, List[str]]:
        """
        Comprehensice validation before any save operation

        Args:
            rule_dict: Dictionary containing rule data
            rule_id_to_update: Optional rule ID to update (for duplicate check)

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Check required fields
        required_fields = [
            "rule_id",
            "rule_name",
            "rule_content",
            "rule_type",
            "active",
        ]
        for field in required_fields:
            if field not in rule_dict:
                errors.append(f"Missing required field: {field}")

        if errors:
            return False, errors

        # Validate field types
        if not isinstance(rule_dict["rule_id"], str):
            errors.append("rule_id must be string")
        if not isinstance(rule_dict["rule_name"], str):
            errors.append("rule_name must be string")
        if not isinstance(rule_dict["rule_content"], str):
            errors.append("rule_content must be string")
        if not isinstance(rule_dict["rule_type"], str):
            errors.append("rule_type must be string")
        if not isinstance(rule_dict["active"], bool):
            errors.append("active must be boolean")

        # Validate rule_id format
        if isinstance(rule_dict["rule_id"], str):
            if not re.match(RULE_ID_PATTERN, rule_dict["rule_id"]):
                errors.append(
                    f"rule_id must match pattern R###: {rule_dict['rule_id']}"
                )

        # Check rule_id uniqueness
        if isinstance(rule_dict["rule_id"], str):
            existing_rule = self.get_rule_by_id(rule_dict["rule_id"])
            if existing_rule:
                if rule_id_to_update is None:
                    errors.append(f"rule_id {rule_dict['rule_id']} already exists")
                elif rule_id_to_update != rule_dict["rule_id"]:
                    errors.append(
                        f"Cannot change rule_id from {rule_id_to_update} to {rule_dict['rule_id']}"
                    )

        # Validate rule_type
        if rule_dict.get("rule_type") not in ALLOWED_RULE_TYPES:
            errors.append(f"rule_type must be one of {ALLOWED_RULE_TYPES}")

        # Validate non-empty strings
        if not rule_dict.get("rule_name", "").strip():
            errors.append("rule_name cannot be empty")
        if not rule_dict.get("rule_content", "").strip():
            errors.append("rule_content cannot be empty")

        is_valid = len(errors) == 0
        return is_valid, errors

    def add_rule(self, rule_dict: Dict) -> Tuple[bool, str, Optional[Rule]]:
        """
        Add new rule to rules.json file

        Args:
            rule_dict: Dictionary with rule fields

        Returns:
            Tuple of (success: bool, message: str, created_rule: Optional[Rule])
        """
        logger.info(f"Attempting to add rule: {rule_dict.get('rule_id', 'NO_ID')}")

        # Validate before save
        is_valid, errors = self.validate_rule_for_save(rule_dict)
        if not is_valid:
            error_msg = "; ".join(errors)
            logger.error(f"Validation failed: {error_msg}")
            return False, error_msg, None

        try:
            # Add created_at timestamp if not provided
            if "created_at" not in rule_dict or not rule_dict["created_at"]:
                from datetime import datetime, timezone

                rule_dict["created_at"] = datetime.now(timezone.utc).isoformat()

            # Validate using Pydantic model
            new_rule = Rule(**rule_dict)

            # Load current rules.json
            if not self.rules_file.exists():
                data = {
                    "rules": [],
                    "metadata": {
                        "version": "1.0",
                        "last_updated": "",
                        "total_rules": 0,
                        "active_rules": 0,
                    },
                }
            else:
                with open(self.rules_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

            # Append new rule
            data["rules"].append(rule_dict)

            # Update metadata
            from datetime import datetime, timezone

            data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
            data["metadata"]["total_rules"] = len(data["rules"])
            data["metadata"]["active_rules"] = sum(
                1 for r in data["rules"] if r.get("active", False)
            )

            # Save back to file
            with open(self.rules_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Reload cache
            self._cache_loaded = False
            self.load_rules()

            logger.info(f"Successfully added rule {new_rule.rule_id}")
            return True, f"Rule {new_rule.rule_id} created successfully", new_rule

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return False, f"Validation error: {str(e)}", None
        except Exception as e:
            logger.error(f"Error adding rule: {e}")
            return False, f"Error adding rule: {str(e)}", None

    def get_rule_for_edit(self, rule_id: str) -> Optional[Dict]:
        """
        Get rule as dictionary suitable for pre-filling edit form

        Args:
            rule_id: Rule ID to retrieve

        Returns:
            Dictionary with rule data or None if not found
        """
        rule = self.get_rule_by_id(rule_id)
        if not rule:
            logger.warning(f"Rule {rule_id} not found for editing")
            return None

        # Convert Rule model to dictionary
        rule_dict = {
            "rule_id": rule.rule_id,
            "rule_name": rule.rule_name,
            "rule_content": rule.rule_content,
            "rule_type": rule.rule_type,
            "active": rule.active,
            "created_at": rule.created_at,
            "description": rule.description,
        }

        logger.debug(f"Retrieved rule {rule_id} for editing")
        return rule_dict

    def update_rule(
        self, rule_id: str, updated_fields: Dict
    ) -> Tuple[bool, str, Optional[Rule]]:
        """
        Update existing rule in rules.json

        Args:
            rule_id: Rule ID to update
            updated_fields: Dictionary of fields to update

        Returns:
            Tuple of (success: bool, message: str, updated_rule: Optional[Rule])
        """
        logger.info(f"Attempting to update rule: {rule_id}")

        # Check rule exists
        existing_rule = self.get_rule_by_id(rule_id)
        if not existing_rule:
            logger.error(f"Rule {rule_id} not found")
            return False, f"Rule {rule_id} not found", None

        try:
            # Load current rules.json
            with open(self.rules_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Find rule by rule_id
            rule_index = None
            for i, rule in enumerate(data["rules"]):
                if rule["rule_id"] == rule_id:
                    rule_index = i
                    break

            if rule_index is None:
                return False, f"Rule {rule_id} not found in file", None

            # Merge updated fields with existing rule (preserve created_at)
            merged_rule = data["rules"][rule_index].copy()
            merged_rule.update(updated_fields)

            # Ensure created_at is preserved
            if "created_at" not in merged_rule:
                merged_rule["created_at"] = existing_rule.created_at

            # Ensure rule_id cannot change
            merged_rule["rule_id"] = rule_id

            # Validate merged rule
            is_valid, errors = self.validate_rule_for_save(
                merged_rule, rule_id_to_update=rule_id
            )
            if not is_valid:
                error_msg = "; ".join(errors)
                logger.error(f"Validation failed: {error_msg}")
                return False, error_msg, None

            # Validate using Pydantic model
            updated_rule = Rule(**merged_rule)

            # Replace rule in array
            data["rules"][rule_index] = merged_rule

            # Update metadata
            from datetime import datetime, timezone

            data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
            data["metadata"]["active_rules"] = sum(
                1 for r in data["rules"] if r.get("active", False)
            )

            # Save back to file
            with open(self.rules_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Reload cache
            self._cache_loaded = False
            self.load_rules()

            logger.info(f"Successfully updated rule {rule_id}")
            return True, f"Rule {rule_id} updated successfully", updated_rule

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return False, f"Validation error: {str(e)}", None
        except Exception as e:
            logger.error(f"Error updating rule: {e}")
            return False, f"Error updating rule: {str(e)}", None

    def delete_rule(self, rule_id: str) -> Tuple[bool, str]:
        """
        Remove single rule from rules.json

        Args:
            rule_id: Rule ID to delete

        Returns:
            Tuple of (success: bool, message: str)
        """
        logger.info(f"Attempting to delete rule: {rule_id}")

        # Check rule exists
        existing_rule = self.get_rule_by_id(rule_id)
        if not existing_rule:
            logger.error(f"Rule {rule_id} not found")
            return False, f"Rule {rule_id} not found"

        try:
            # Load current rules.json
            with open(self.rules_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Filter out rule with matching rule_id
            original_count = len(data["rules"])
            data["rules"] = [r for r in data["rules"] if r["rule_id"] != rule_id]

            # Check if any rules were actually removed
            if len(data["rules"]) == original_count:
                return False, f"Rule {rule_id} not found in file"

            # Update metadata
            from datetime import datetime, timezone

            data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
            data["metadata"]["total_rules"] = len(data["rules"])
            data["metadata"]["active_rules"] = sum(
                1 for r in data["rules"] if r.get("active", False)
            )

            # Save back to file
            with open(self.rules_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Reload cache
            self._cache_loaded = False
            self.load_rules()

            logger.info(f"Successfully deleted rule {rule_id}")
            return True, f"Rule {rule_id} deleted successfully"

        except Exception as e:
            logger.error(f"Error deleting rule: {e}")
            return False, f"Error deleting rule: {str(e)}"

    def delete_rules(self, rule_ids: List[str]) -> Dict[str, Any]:
        """
        Remove multiple rules in one operation

        Args:
            rule_ids: List of rule IDs to delete

        Returns:
            Dictionary with results
        """
        logger.info(f"Attempting to delete {len(rule_ids)} rules")

        try:
            # Load current rules.json
            with open(self.rules_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Track which IDs were actually found and deleted
            original_ids = {r["rule_id"] for r in data["rules"]}
            deleted_ids = []
            not_found_ids = []

            for rule_id in rule_ids:
                if rule_id in original_ids:
                    deleted_ids.append(rule_id)
                else:
                    not_found_ids.append(rule_id)

            # Filter out all rules with IDs in list
            data["rules"] = [r for r in data["rules"] if r["rule_id"] not in rule_ids]

            # Update metadata
            from datetime import datetime, timezone

            data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
            data["metadata"]["total_rules"] = len(data["rules"])
            data["metadata"]["active_rules"] = sum(
                1 for r in data["rules"] if r.get("active", False)
            )

            # Save back to file
            with open(self.rules_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Reload cache
            self._cache_loaded = False
            self.load_rules()

            result = {
                "deleted_count": len(deleted_ids),
                "not_found": not_found_ids,
                "success": True,
                "message": f"Successfully deleted {len(deleted_ids)} rule(s)",
            }

            logger.info(
                f"Batch delete complete: {len(deleted_ids)} deleted, {len(not_found_ids)} not found"
            )
            return result

        except Exception as e:
            logger.error(f"Error in batch delete: {e}")
            return {
                "deleted_count": 0,
                "not_found": rule_ids,
                "success": False,
                "message": f"Error deleting rules: {str(e)}",
            }

    def toggle_rule_status(self, rule_id: str) -> Tuple[bool, str, bool]:
        """
        Flip active status of a rule

        Args:
            rule_id: Rule ID to toggle

        Returns:
            Tuple of (success: bool, message: str, new_status: bool)
        """
        logger.info(f"Attempting to toggle status for rule: {rule_id}")

        # Check rule exists
        existing_rule = self.get_rule_by_id(rule_id)
        if not existing_rule:
            logger.error(f"Rule {rule_id} not found")
            return False, f"Rule {rule_id} not found", False

        try:
            # Load current rules.json
            with open(self.rules_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Find rule and flip active status
            rule_found = False
            new_status = False
            for rule in data["rules"]:
                if rule["rule_id"] == rule_id:
                    rule["active"] = not rule.get("active", False)
                    new_status = rule["active"]
                    rule_found = True
                    break

            if not rule_found:
                return False, f"Rule {rule_id} not found in file", False

            # Update metadata
            from datetime import datetime, timezone

            data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
            data["metadata"]["active_rules"] = sum(
                1 for r in data["rules"] if r.get("active", False)
            )

            # Save back to file
            with open(self.rules_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Reload cache
            self._cache_loaded = False
            self.load_rules()

            status_text = "ACTIVE" if new_status else "INACTIVE"
            logger.info(f"Successfully toggled rule {rule_id} to {status_text}")
            return True, f"Rule {rule_id} is now {status_text}", new_status

        except Exception as e:
            logger.error(f"Error toggling rule status: {e}")
            return False, f"Error toggling status: {str(e)}", False

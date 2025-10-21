"""
Rule manager - CRUD operations for Rule management Service
"""

import json
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path

from .models import Rule, RuleSet, ValidationReport
from .validator import RuleValidator
from .config import RULES_FILE, LOG_FILE, LOG_FORMAT, LOG_DATE_FORMAT

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

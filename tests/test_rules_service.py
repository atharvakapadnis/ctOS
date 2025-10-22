"""
Test suite for Service 4: Rules Management Service
Tests rule loading, validation, CRUD operations, and prompt formatting
"""

import pytest
import json
import tempfile
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.rules.manager import RuleManager
from src.services.rules.validator import RuleValidator
from src.services.rules.models import Rule, ValidationReport, ValidationResult
from src.services.rules.config import ALLOWED_RULE_TYPES, RULE_ID_PATTERN


class TestRuleValidator:
    """Test rule validation functionality"""

    def setup_method(self):
        """Setup test environment"""
        self.validator = RuleValidator()

    def test_validate_valid_rule(self):
        """Test validation of a valid rule"""
        valid_rule = {
            "rule_id": "R001",
            "rule_name": "Test Rule",
            "rule_content": "This is a test rule content",
            "rule_type": "material",
            "active": True,
        }

        result = self.validator.validate_rule(valid_rule)

        assert result.valid is True
        assert len(result.errors) == 0
        assert result.rule_id == "R001"

    def test_validate_missing_required_field(self):
        """Test validation fails when required field is missing"""
        invalid_rule = {
            "rule_id": "R002",
            "rule_name": "Test Rule",
            "rule_type": "material",
            "active": True,
        }

        result = self.validator.validate_rule(invalid_rule)

        assert result.valid is False
        assert any("rule_content" in error for error in result.errors)

    def test_validate_invalid_rule_id_format(self):
        """Test validation fails with invalid rule_id format"""
        invalid_rule = {
            "rule_id": "INVALID",
            "rule_name": "Test Rule",
            "rule_content": "Test content",
            "rule_type": "material",
            "active": True,
        }

        result = self.validator.validate_rule(invalid_rule)

        assert result.valid is False
        assert any("rule_id" in error.lower() for error in result.errors)

    def test_validate_invalid_rule_type(self):
        """Test validation fails with invalid rule_type"""
        invalid_rule = {
            "rule_id": "R003",
            "rule_name": "Test Rule",
            "rule_content": "Test content",
            "rule_type": "invalid_type",
            "active": True,
        }

        result = self.validator.validate_rule(invalid_rule)

        assert result.valid is False
        assert any("rule_type" in error.lower() for error in result.errors)

    def test_validate_empty_content(self):
        """Test validation fails with empty content"""
        invalid_rule = {
            "rule_id": "R004",
            "rule_name": "Test Rule",
            "rule_content": "",
            "rule_type": "material",
            "active": True,
        }

        result = self.validator.validate_rule(invalid_rule)

        assert result.valid is False
        assert any("empty" in error.lower() for error in result.errors)

    def test_validate_wrong_field_type(self):
        """Test validation fails with wrong field type"""
        invalid_rule = {
            "rule_id": "R005",
            "rule_name": "Test Rule",
            "rule_content": "Test content",
            "rule_type": "material",
            "active": "yes",
        }

        result = self.validator.validate_rule(invalid_rule)

        assert result.valid is False
        assert any("boolean" in error.lower() for error in result.errors)

    def test_validate_rule_set_with_duplicates(self):
        """Test validation detects duplicate rule IDs"""
        rules = [
            {
                "rule_id": "R001",
                "rule_name": "Rule 1",
                "rule_content": "Content 1",
                "rule_type": "material",
                "active": True,
            },
            {
                "rule_id": "R001",
                "rule_name": "Rule 2",
                "rule_content": "Content 2",
                "rule_type": "customer",
                "active": True,
            },
        ]

        report = self.validator.validate_rule_set(rules)

        assert report.valid is False
        assert len(report.duplicate_ids) == 1
        assert "R001" in report.duplicate_ids

    def test_validate_rule_set_all_valid(self):
        """Test validation passes with all valid rules"""
        rules = [
            {
                "rule_id": "R001",
                "rule_name": "Rule 1",
                "rule_content": "Content 1",
                "rule_type": "material",
                "active": True,
            },
            {
                "rule_id": "R002",
                "rule_name": "Rule 2",
                "rule_content": "Content 2",
                "rule_type": "customer",
                "active": True,
            },
        ]

        report = self.validator.validate_rule_set(rules)

        assert report.valid is True
        assert report.total_rules == 2
        assert report.valid_rules == 2
        assert report.invalid_rules == 0

    def test_check_unique_ids(self):
        """Test unique ID checking"""
        rules = [{"rule_id": "R001"}, {"rule_id": "R002"}, {"rule_id": "R003"}]

        is_unique, duplicates = self.validator.check_unique_ids(rules)

        assert is_unique is True
        assert len(duplicates) == 0

    def test_validate_rule_id_format(self):
        """Test rule ID format validation"""
        assert self.validator.validate_rule_id_format("R001") is True
        assert self.validator.validate_rule_id_format("R123") is True
        assert self.validator.validate_rule_id_format("R9999") is True
        assert self.validator.validate_rule_id_format("INVALID") is False
        assert self.validator.validate_rule_id_format("R12") is False
        assert self.validator.validate_rule_id_format("123") is False

    def test_validate_rule_type(self):
        """Test rule type validation"""
        assert self.validator.validate_rule_type("material") is True
        assert self.validator.validate_rule_type("dimension") is True
        assert self.validator.validate_rule_type("customer") is True
        assert self.validator.validate_rule_type("product") is True
        assert self.validator.validate_rule_type("general") is True
        assert self.validator.validate_rule_type("invalid") is False


class TestRuleManager:
    """Test rule manager functionality"""

    def setup_method(self):
        """Setup test environment with temporary directory"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def create_test_rules_file(self, rules_data):
        """Helper to create a test rules file"""
        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(rules_data, f)

    def test_load_rules_file_not_exists(self):
        """Test loading when file doesn't exist returns empty list"""
        rules = self.manager.load_rules()

        assert isinstance(rules, list)
        assert len(rules) == 0

    def test_load_rules_valid_file(self):
        """Test loading valid rules file"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Test Rule 1",
                    "rule_content": "Test content 1",
                    "rule_type": "material",
                    "active": True,
                },
                {
                    "rule_id": "R002",
                    "rule_name": "Test Rule 2",
                    "rule_content": "Test content 2",
                    "rule_type": "customer",
                    "active": False,
                },
            ]
        }

        self.create_test_rules_file(test_data)
        rules = self.manager.load_rules()

        assert len(rules) == 2
        assert all(isinstance(rule, Rule) for rule in rules)
        assert rules[0].rule_id == "R001"
        assert rules[1].rule_id == "R002"

    def test_load_rules_caching(self):
        """Test that rules are cached after first load"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Test Rule",
                    "rule_content": "Test content",
                    "rule_type": "material",
                    "active": True,
                }
            ]
        }

        self.create_test_rules_file(test_data)

        rules1 = self.manager.load_rules()
        rules2 = self.manager.load_rules()

        assert rules1 is rules2

    def test_load_rules_invalid_json(self):
        """Test loading handles invalid JSON gracefully"""
        with open(self.rules_file, "w") as f:
            f.write("{ invalid json")

        rules = self.manager.load_rules()

        assert isinstance(rules, list)
        assert len(rules) == 0

    def test_get_active_rules(self):
        """Test getting only active rules"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Active Rule",
                    "rule_content": "Content 1",
                    "rule_type": "material",
                    "active": True,
                },
                {
                    "rule_id": "R002",
                    "rule_name": "Inactive Rule",
                    "rule_content": "Content 2",
                    "rule_type": "customer",
                    "active": False,
                },
                {
                    "rule_id": "R003",
                    "rule_name": "Active Rule 2",
                    "rule_content": "Content 3",
                    "rule_type": "dimension",
                    "active": True,
                },
            ]
        }

        self.create_test_rules_file(test_data)
        active_rules = self.manager.get_active_rules()

        assert len(active_rules) == 2
        assert all(rule.active for rule in active_rules)
        assert active_rules[0].rule_id == "R001"
        assert active_rules[1].rule_id == "R003"

    def test_get_rule_by_id_found(self):
        """Test getting rule by ID when it exists"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Test Rule",
                    "rule_content": "Test content",
                    "rule_type": "material",
                    "active": True,
                }
            ]
        }

        self.create_test_rules_file(test_data)
        rule = self.manager.get_rule_by_id("R001")

        assert rule is not None
        assert rule.rule_id == "R001"
        assert rule.rule_name == "Test Rule"

    def test_get_rule_by_id_not_found(self):
        """Test getting rule by ID when it doesn't exist"""
        test_data = {"rules": []}

        self.create_test_rules_file(test_data)
        rule = self.manager.get_rule_by_id("R999")

        assert rule is None

    def test_get_rules_by_ids(self):
        """Test getting multiple rules by IDs"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Rule 1",
                    "rule_content": "Content 1",
                    "rule_type": "material",
                    "active": True,
                },
                {
                    "rule_id": "R002",
                    "rule_name": "Rule 2",
                    "rule_content": "Content 2",
                    "rule_type": "customer",
                    "active": True,
                },
                {
                    "rule_id": "R003",
                    "rule_name": "Rule 3",
                    "rule_content": "Content 3",
                    "rule_type": "dimension",
                    "active": True,
                },
            ]
        }

        self.create_test_rules_file(test_data)
        rules = self.manager.get_rules_by_ids(["R001", "R003"])

        assert len(rules) == 2
        assert rules[0].rule_id == "R001"
        assert rules[1].rule_id == "R003"

    def test_get_rules_by_ids_with_invalid(self):
        """Test getting rules by IDs with some invalid IDs"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Rule 1",
                    "rule_content": "Content 1",
                    "rule_type": "material",
                    "active": True,
                }
            ]
        }

        self.create_test_rules_file(test_data)
        rules = self.manager.get_rules_by_ids(["R001", "R999", "R888"])

        assert len(rules) == 1
        assert rules[0].rule_id == "R001"

    def test_get_rules_by_type(self):
        """Test getting rules by type"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Material Rule 1",
                    "rule_content": "Content 1",
                    "rule_type": "material",
                    "active": True,
                },
                {
                    "rule_id": "R002",
                    "rule_name": "Customer Rule",
                    "rule_content": "Content 2",
                    "rule_type": "customer",
                    "active": True,
                },
                {
                    "rule_id": "R003",
                    "rule_name": "Material Rule 2",
                    "rule_content": "Content 3",
                    "rule_type": "material",
                    "active": False,
                },
            ]
        }

        self.create_test_rules_file(test_data)
        material_rules = self.manager.get_rules_by_type("material")

        assert len(material_rules) == 2
        assert all(rule.rule_type == "material" for rule in material_rules)

    def test_get_all_rule_types(self):
        """Test getting all rule types"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Rule 1",
                    "rule_content": "Content 1",
                    "rule_type": "material",
                    "active": True,
                },
                {
                    "rule_id": "R002",
                    "rule_name": "Rule 2",
                    "rule_content": "Content 2",
                    "rule_type": "customer",
                    "active": True,
                },
                {
                    "rule_id": "R003",
                    "rule_name": "Rule 3",
                    "rule_content": "Content 3",
                    "rule_type": "dimension",
                    "active": True,
                },
            ]
        }

        self.create_test_rules_file(test_data)
        rule_types = self.manager.get_all_rule_types()

        assert len(rule_types) == 3
        assert "material" in rule_types
        assert "customer" in rule_types
        assert "dimension" in rule_types

    def test_validate_rules_file(self):
        """Test validating rules file"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Valid Rule",
                    "rule_content": "Valid content",
                    "rule_type": "material",
                    "active": True,
                }
            ]
        }

        self.create_test_rules_file(test_data)
        report = self.manager.validate_rules_file()

        assert report.valid is True
        assert report.total_rules == 1
        assert report.valid_rules == 1

    def test_reload_rules(self):
        """Test reloading rules from file"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Rule 1",
                    "rule_content": "Content 1",
                    "rule_type": "material",
                    "active": True,
                }
            ]
        }

        self.create_test_rules_file(test_data)

        self.manager.load_rules()

        test_data["rules"].append(
            {
                "rule_id": "R002",
                "rule_name": "Rule 2",
                "rule_content": "Content 2",
                "rule_type": "customer",
                "active": True,
            }
        )

        self.create_test_rules_file(test_data)

        count = self.manager.reload_rules()

        assert count == 2

        rules = self.manager.load_rules()
        assert len(rules) == 2

    def test_get_rules_statistics(self):
        """Test getting rules statistics"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Rule 1",
                    "rule_content": "Content 1",
                    "rule_type": "material",
                    "active": True,
                },
                {
                    "rule_id": "R002",
                    "rule_name": "Rule 2",
                    "rule_content": "Content 2",
                    "rule_type": "material",
                    "active": False,
                },
                {
                    "rule_id": "R003",
                    "rule_name": "Rule 3",
                    "rule_content": "Content 3",
                    "rule_type": "customer",
                    "active": True,
                },
            ]
        }

        self.create_test_rules_file(test_data)
        stats = self.manager.get_rules_statistics()

        assert stats["total_rules"] == 3
        assert stats["active_rules"] == 2
        assert stats["inactive_rules"] == 1
        assert stats["rules_by_type"]["material"] == 2
        assert stats["rules_by_type"]["customer"] == 1

    def test_format_rules_for_prompt_empty(self):
        """Test formatting empty rules list"""
        formatted = self.manager.format_rules_for_prompt([])

        assert formatted == ""

    def test_format_rules_for_prompt(self):
        """Test formatting rules for LLM prompt"""
        rules = [
            Rule(
                rule_id="R001",
                rule_name="Rule 1",
                rule_content="DI means Ductile Iron",
                rule_type="material",
                active=True,
            ),
            Rule(
                rule_id="R002",
                rule_name="Rule 2",
                rule_content="MJ means Mechanical Joint",
                rule_type="material",
                active=True,
            ),
        ]

        formatted = self.manager.format_rules_for_prompt(rules)

        assert "Rules to Apply:" in formatted
        assert "[R001]" in formatted
        assert "DI means Ductile Iron" in formatted
        assert "[R002]" in formatted
        assert "MJ means Mechanical Joint" in formatted


class TestPydanticModels:
    """Test Pydantic model validation"""

    def test_rule_model_valid(self):
        """Test creating valid Rule model"""
        rule = Rule(
            rule_id="R001",
            rule_name="Test Rule",
            rule_content="Test content",
            rule_type="material",
            active=True,
        )

        assert rule.rule_id == "R001"
        assert rule.active is True

    def test_rule_model_invalid_id(self):
        """Test Rule model rejects invalid rule_id"""
        with pytest.raises(ValueError):
            Rule(
                rule_id="INVALID",
                rule_name="Test",
                rule_content="Content",
                rule_type="material",
                active=True,
            )

    def test_rule_model_invalid_type(self):
        """Test Rule model rejects invalid rule_type"""
        with pytest.raises(ValueError):
            Rule(
                rule_id="R001",
                rule_name="Test",
                rule_content="Content",
                rule_type="invalid_type",
                active=True,
            )

    def test_rule_model_empty_content(self):
        """Test Rule model rejects empty content"""
        with pytest.raises(ValueError):
            Rule(
                rule_id="R001",
                rule_name="Test",
                rule_content="",
                rule_type="material",
                active=True,
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

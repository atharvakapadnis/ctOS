"""
Test suite for Service 4: Rules CRUD Operations
Tests create, read, update, delete operations for RuleManager
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
from src.services.rules.models import Rule
from src.services.rules.config import ALLOWED_RULE_TYPES


class TestGetNextRuleId:
    """Test get_next_rule_id method"""

    def setup_method(self):
        """Setup test environment with temporary directory"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def create_test_rules_file(self, rules_data):
        """Helper to create a test rules file"""
        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(rules_data, f)

    def test_next_id_no_existing_rules(self):
        """Test generating next ID when no rules exist"""
        next_id = self.manager.get_next_rule_id()
        assert next_id == "R001"

    def test_next_id_with_sequential_rules(self):
        """Test generating next ID with sequential existing rules"""
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
                    "active": True,
                },
                {
                    "rule_id": "R003",
                    "rule_name": "Rule 3",
                    "rule_content": "Content 3",
                    "rule_type": "material",
                    "active": True,
                },
            ]
        }

        self.create_test_rules_file(test_data)
        next_id = self.manager.get_next_rule_id()
        assert next_id == "R004"

    def test_next_id_with_non_sequential_rules(self):
        """Test generating next ID with non-sequential existing rules"""
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
                    "rule_id": "R003",
                    "rule_name": "Rule 3",
                    "rule_content": "Content 3",
                    "rule_type": "material",
                    "active": True,
                },
                {
                    "rule_id": "R010",
                    "rule_name": "Rule 10",
                    "rule_content": "Content 10",
                    "rule_type": "material",
                    "active": True,
                },
            ]
        }

        self.create_test_rules_file(test_data)
        next_id = self.manager.get_next_rule_id()
        assert next_id == "R011"

    def test_next_id_with_large_numbers(self):
        """Test generating next ID with large ID numbers"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R999",
                    "rule_name": "Rule 999",
                    "rule_content": "Content",
                    "rule_type": "material",
                    "active": True,
                }
            ]
        }

        self.create_test_rules_file(test_data)
        next_id = self.manager.get_next_rule_id()
        assert next_id == "R1000"


class TestValidateRuleForSave:
    """Test validate_rule_for_save method"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def test_validate_valid_rule(self):
        """Test validation of a completely valid rule"""
        valid_rule = {
            "rule_id": "R001",
            "rule_name": "Test Rule",
            "rule_content": "This is test content",
            "rule_type": "material",
            "active": True,
        }

        is_valid, errors = self.manager.validate_rule_for_save(valid_rule)
        assert is_valid is True
        assert len(errors) == 0

    def test_validate_missing_required_fields(self):
        """Test validation fails when required fields are missing"""
        invalid_rule = {
            "rule_id": "R001",
            "rule_name": "Test Rule",
        }

        is_valid, errors = self.manager.validate_rule_for_save(invalid_rule)
        assert is_valid is False
        assert any("rule_content" in error for error in errors)
        assert any("rule_type" in error for error in errors)
        assert any("active" in error for error in errors)

    def test_validate_invalid_rule_id_format(self):
        """Test validation fails with invalid rule_id format"""
        invalid_rule = {
            "rule_id": "INVALID123",
            "rule_name": "Test Rule",
            "rule_content": "Content",
            "rule_type": "material",
            "active": True,
        }

        is_valid, errors = self.manager.validate_rule_for_save(invalid_rule)
        assert is_valid is False
        assert any("pattern" in error.lower() for error in errors)

    def test_validate_invalid_rule_type(self):
        """Test validation fails with invalid rule_type"""
        invalid_rule = {
            "rule_id": "R001",
            "rule_name": "Test Rule",
            "rule_content": "Content",
            "rule_type": "invalid_type",
            "active": True,
        }

        is_valid, errors = self.manager.validate_rule_for_save(invalid_rule)
        assert is_valid is False
        assert any("rule_type" in error for error in errors)

    def test_validate_empty_strings(self):
        """Test validation fails with empty strings"""
        invalid_rule = {
            "rule_id": "R001",
            "rule_name": "",
            "rule_content": "   ",
            "rule_type": "material",
            "active": True,
        }

        is_valid, errors = self.manager.validate_rule_for_save(invalid_rule)
        assert is_valid is False
        assert any("rule_name" in error for error in errors)
        assert any("rule_content" in error for error in errors)

    def test_validate_duplicate_rule_id_create(self):
        """Test validation fails when rule_id already exists (create)"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Existing Rule",
                    "rule_content": "Content",
                    "rule_type": "material",
                    "active": True,
                }
            ]
        }

        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        self.manager.load_rules()

        duplicate_rule = {
            "rule_id": "R001",
            "rule_name": "New Rule",
            "rule_content": "Content",
            "rule_type": "material",
            "active": True,
        }

        is_valid, errors = self.manager.validate_rule_for_save(duplicate_rule)
        assert is_valid is False
        assert any("already exists" in error for error in errors)

    def test_validate_same_rule_id_update(self):
        """Test validation passes when updating rule with same ID"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Existing Rule",
                    "rule_content": "Content",
                    "rule_type": "material",
                    "active": True,
                }
            ]
        }

        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        self.manager.load_rules()

        updated_rule = {
            "rule_id": "R001",
            "rule_name": "Updated Rule",
            "rule_content": "New Content",
            "rule_type": "material",
            "active": True,
        }

        is_valid, errors = self.manager.validate_rule_for_save(
            updated_rule, rule_id_to_update="R001"
        )
        assert is_valid is True
        assert len(errors) == 0


class TestAddRule:
    """Test add_rule method"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def test_add_valid_rule_success(self):
        """Test successfully adding a valid rule"""
        new_rule = {
            "rule_id": "R001",
            "rule_name": "Test Rule",
            "rule_content": "This is a test rule",
            "rule_type": "material",
            "active": True,
            "description": "Test description",
        }

        success, message, created_rule = self.manager.add_rule(new_rule)

        assert success is True
        assert "created successfully" in message.lower()
        assert created_rule is not None
        assert created_rule.rule_id == "R001"
        assert created_rule.rule_name == "Test Rule"

    def test_add_rule_creates_file_if_not_exists(self):
        """Test adding rule creates rules.json if it doesn't exist"""
        new_rule = {
            "rule_id": "R001",
            "rule_name": "First Rule",
            "rule_content": "Content",
            "rule_type": "material",
            "active": True,
        }

        success, message, created_rule = self.manager.add_rule(new_rule)

        assert success is True
        assert self.rules_file.exists()

    def test_add_rule_reject_duplicate_id(self):
        """Test rejecting rule with duplicate rule_id"""
        first_rule = {
            "rule_id": "R001",
            "rule_name": "First Rule",
            "rule_content": "Content",
            "rule_type": "material",
            "active": True,
        }

        self.manager.add_rule(first_rule)

        duplicate_rule = {
            "rule_id": "R001",
            "rule_name": "Duplicate Rule",
            "rule_content": "Different Content",
            "rule_type": "material",
            "active": True,
        }

        success, message, created_rule = self.manager.add_rule(duplicate_rule)

        assert success is False
        assert "already exists" in message.lower()
        assert created_rule is None

    def test_add_rule_reject_invalid_type(self):
        """Test rejecting rule with invalid rule_type"""
        invalid_rule = {
            "rule_id": "R001",
            "rule_name": "Invalid Rule",
            "rule_content": "Content",
            "rule_type": "invalid_type",
            "active": True,
        }

        success, message, created_rule = self.manager.add_rule(invalid_rule)

        assert success is False
        assert created_rule is None

    def test_add_rule_reject_empty_content(self):
        """Test rejecting rule with empty content"""
        invalid_rule = {
            "rule_id": "R001",
            "rule_name": "Empty Content Rule",
            "rule_content": "",
            "rule_type": "material",
            "active": True,
        }

        success, message, created_rule = self.manager.add_rule(invalid_rule)

        assert success is False
        assert created_rule is None

    def test_add_rule_updates_metadata(self):
        """Test that adding rule updates metadata correctly"""
        rule1 = {
            "rule_id": "R001",
            "rule_name": "Rule 1",
            "rule_content": "Content 1",
            "rule_type": "material",
            "active": True,
        }

        rule2 = {
            "rule_id": "R002",
            "rule_name": "Rule 2",
            "rule_content": "Content 2",
            "rule_type": "material",
            "active": False,
        }

        self.manager.add_rule(rule1)
        self.manager.add_rule(rule2)

        with open(self.rules_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["metadata"]["total_rules"] == 2
        assert data["metadata"]["active_rules"] == 1


class TestGetRuleForEdit:
    """Test get_rule_for_edit method"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def test_get_existing_rule(self):
        """Test retrieving existing rule for editing"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Test Rule",
                    "rule_content": "Test content",
                    "rule_type": "material",
                    "active": True,
                    "description": "Test description",
                    "created_at": "2025-01-01T00:00:00Z",
                }
            ]
        }

        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        self.manager.load_rules()

        rule_dict = self.manager.get_rule_for_edit("R001")

        assert rule_dict is not None
        assert rule_dict["rule_id"] == "R001"
        assert rule_dict["rule_name"] == "Test Rule"
        assert rule_dict["rule_content"] == "Test content"
        assert rule_dict["rule_type"] == "material"
        assert rule_dict["active"] is True
        assert rule_dict["description"] == "Test description"
        assert rule_dict["created_at"] == "2025-01-01T00:00:00Z"

    def test_get_non_existent_rule(self):
        """Test retrieving non-existent rule returns None"""
        rule_dict = self.manager.get_rule_for_edit("R999")
        assert rule_dict is None


class TestUpdateRule:
    """Test update_rule method"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def create_initial_rule(self):
        """Helper to create an initial rule"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Original Name",
                    "rule_content": "Original Content",
                    "rule_type": "material",
                    "active": True,
                    "description": "Original Description",
                    "created_at": "2025-01-01T00:00:00Z",
                }
            ],
            "metadata": {
                "version": "1.0",
                "last_updated": "2025-01-01T00:00:00Z",
                "total_rules": 1,
                "active_rules": 1,
            },
        }

        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        self.manager.load_rules()

    def test_update_rule_name_success(self):
        """Test successfully updating rule_name"""
        self.create_initial_rule()

        updated_fields = {"rule_name": "Updated Name"}

        success, message, updated_rule = self.manager.update_rule(
            "R001", updated_fields
        )

        assert success is True
        assert updated_rule.rule_name == "Updated Name"
        assert updated_rule.rule_content == "Original Content"

    def test_update_rule_content_success(self):
        """Test successfully updating rule_content"""
        self.create_initial_rule()

        updated_fields = {"rule_content": "Updated Content"}

        success, message, updated_rule = self.manager.update_rule(
            "R001", updated_fields
        )

        assert success is True
        assert updated_rule.rule_content == "Updated Content"

    def test_update_rule_type_success(self):
        """Test successfully updating rule_type"""
        self.create_initial_rule()

        updated_fields = {"rule_type": "customer"}

        success, message, updated_rule = self.manager.update_rule(
            "R001", updated_fields
        )

        assert success is True
        assert updated_rule.rule_type == "customer"

    def test_update_active_status(self):
        """Test successfully changing active status"""
        self.create_initial_rule()

        updated_fields = {"active": False}

        success, message, updated_rule = self.manager.update_rule(
            "R001", updated_fields
        )

        assert success is True
        assert updated_rule.active is False

        with open(self.rules_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["metadata"]["active_rules"] == 0

    def test_update_preserves_created_at(self):
        """Test that update preserves original created_at timestamp"""
        self.create_initial_rule()

        updated_fields = {"rule_name": "Updated Name"}

        success, message, updated_rule = self.manager.update_rule(
            "R001", updated_fields
        )

        assert success is True
        assert updated_rule.created_at == "2025-01-01T00:00:00Z"

    def test_update_non_existent_rule(self):
        """Test updating non-existent rule fails"""
        self.create_initial_rule()

        updated_fields = {"rule_name": "Updated Name"}

        success, message, updated_rule = self.manager.update_rule(
            "R999", updated_fields
        )

        assert success is False
        assert "not found" in message.lower()
        assert updated_rule is None

    def test_update_reject_invalid_fields(self):
        """Test updating with invalid fields fails"""
        self.create_initial_rule()

        updated_fields = {"rule_type": "invalid_type"}

        success, message, updated_rule = self.manager.update_rule(
            "R001", updated_fields
        )

        assert success is False
        assert updated_rule is None


class TestDeleteRule:
    """Test delete_rule method"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def create_test_rules(self):
        """Helper to create test rules"""
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
                    "active": True,
                },
            ],
            "metadata": {
                "version": "1.0",
                "last_updated": "2025-01-01T00:00:00Z",
                "total_rules": 2,
                "active_rules": 2,
            },
        }

        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        self.manager.load_rules()

    def test_delete_existing_rule_success(self):
        """Test successfully deleting an existing rule"""
        self.create_test_rules()

        success, message = self.manager.delete_rule("R001")

        assert success is True
        assert "deleted successfully" in message.lower()

        rules = self.manager.load_rules()
        assert len(rules) == 1
        assert rules[0].rule_id == "R002"

    def test_delete_non_existent_rule(self):
        """Test attempting to delete non-existent rule"""
        self.create_test_rules()

        success, message = self.manager.delete_rule("R999")

        assert success is False
        assert "not found" in message.lower()

    def test_delete_updates_metadata(self):
        """Test that deleting rule updates metadata correctly"""
        self.create_test_rules()

        self.manager.delete_rule("R001")

        with open(self.rules_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["metadata"]["total_rules"] == 1
        assert data["metadata"]["active_rules"] == 1


class TestDeleteRules:
    """Test delete_rules (batch delete) method"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def create_test_rules(self):
        """Helper to create test rules"""
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
                    "active": True,
                },
                {
                    "rule_id": "R003",
                    "rule_name": "Rule 3",
                    "rule_content": "Content 3",
                    "rule_type": "material",
                    "active": False,
                },
            ],
            "metadata": {
                "version": "1.0",
                "last_updated": "2025-01-01T00:00:00Z",
                "total_rules": 3,
                "active_rules": 2,
            },
        }

        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        self.manager.load_rules()

    def test_delete_multiple_rules_success(self):
        """Test successfully deleting multiple rules"""
        self.create_test_rules()

        result = self.manager.delete_rules(["R001", "R002"])

        assert result["success"] is True
        assert result["deleted_count"] == 2
        assert len(result["not_found"]) == 0

        rules = self.manager.load_rules()
        assert len(rules) == 1
        assert rules[0].rule_id == "R003"

    def test_delete_rules_mix_valid_invalid(self):
        """Test deleting with mix of valid and invalid IDs"""
        self.create_test_rules()

        result = self.manager.delete_rules(["R001", "R999", "R002", "R888"])

        assert result["success"] is True
        assert result["deleted_count"] == 2
        assert len(result["not_found"]) == 2
        assert "R999" in result["not_found"]
        assert "R888" in result["not_found"]

    def test_delete_rules_updates_metadata(self):
        """Test batch delete updates metadata correctly"""
        self.create_test_rules()

        self.manager.delete_rules(["R001", "R003"])

        with open(self.rules_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["metadata"]["total_rules"] == 1
        assert data["metadata"]["active_rules"] == 1


class TestToggleRuleStatus:
    """Test toggle_rule_status method"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def create_test_rule(self, active=True):
        """Helper to create a test rule"""
        test_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Test Rule",
                    "rule_content": "Content",
                    "rule_type": "material",
                    "active": active,
                }
            ],
            "metadata": {
                "version": "1.0",
                "last_updated": "2025-01-01T00:00:00Z",
                "total_rules": 1,
                "active_rules": 1 if active else 0,
            },
        }

        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        self.manager.load_rules()

    def test_toggle_from_active_to_inactive(self):
        """Test toggling rule from active to inactive"""
        self.create_test_rule(active=True)

        success, message, new_status = self.manager.toggle_rule_status("R001")

        assert success is True
        assert new_status is False
        assert "INACTIVE" in message

    def test_toggle_from_inactive_to_active(self):
        """Test toggling rule from inactive to active"""
        self.create_test_rule(active=False)

        success, message, new_status = self.manager.toggle_rule_status("R001")

        assert success is True
        assert new_status is True
        assert "ACTIVE" in message

    def test_toggle_updates_metadata(self):
        """Test that toggling updates metadata correctly"""
        self.create_test_rule(active=True)

        self.manager.toggle_rule_status("R001")

        with open(self.rules_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["metadata"]["active_rules"] == 0

    def test_toggle_non_existent_rule(self):
        """Test toggling non-existent rule fails"""
        self.create_test_rule(active=True)

        success, message, new_status = self.manager.toggle_rule_status("R999")

        assert success is False
        assert "not found" in message.lower()


class TestIntegrationCRUDCycle:
    """Integration tests for complete CRUD cycle"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.rules_file = Path(self.temp_dir) / "rules.json"
        self.manager = RuleManager(rules_file=self.rules_file)

    def test_full_crud_cycle(self):
        """Test complete Create -> Read -> Update -> Delete cycle"""

        # CREATE
        new_rule = {
            "rule_id": "R001",
            "rule_name": "Test Rule",
            "rule_content": "Original Content",
            "rule_type": "material",
            "active": True,
        }

        success, message, created_rule = self.manager.add_rule(new_rule)
        assert success is True
        assert created_rule.rule_id == "R001"

        # READ
        rules = self.manager.load_rules()
        assert len(rules) == 1
        assert rules[0].rule_id == "R001"

        rule_for_edit = self.manager.get_rule_for_edit("R001")
        assert rule_for_edit is not None
        assert rule_for_edit["rule_name"] == "Test Rule"

        # UPDATE
        updated_fields = {
            "rule_name": "Updated Rule",
            "rule_content": "Updated Content",
        }

        success, message, updated_rule = self.manager.update_rule(
            "R001", updated_fields
        )
        assert success is True
        assert updated_rule.rule_name == "Updated Rule"
        assert updated_rule.rule_content == "Updated Content"

        # DELETE
        success, message = self.manager.delete_rule("R001")
        assert success is True

        rules = self.manager.load_rules()
        assert len(rules) == 0

    def test_batch_operations(self):
        """Test batch operations with multiple rules"""

        # Create multiple rules
        for i in range(1, 6):
            rule = {
                "rule_id": f"R{str(i).zfill(3)}",
                "rule_name": f"Rule {i}",
                "rule_content": f"Content {i}",
                "rule_type": "material",
                "active": True,
            }
            self.manager.add_rule(rule)

        rules = self.manager.load_rules()
        assert len(rules) == 5

        # Batch delete
        result = self.manager.delete_rules(["R001", "R003", "R005"])
        assert result["deleted_count"] == 3

        rules = self.manager.load_rules()
        assert len(rules) == 2
        assert rules[0].rule_id == "R002"
        assert rules[1].rule_id == "R004"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

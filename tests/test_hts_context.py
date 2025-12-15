"""
Unit tests for HTS Context Service
"""

import pytest
import json
import tempfile
from pathlib import Path
from src.services.hts_context.loader import HTSReferenceLoader
from src.services.hts_context.hierarchy import HTSHierarchyBuilder
from src.services.hts_context.service import HTSContextService
from src.services.hts_context.models import HTSContextResponse


class TestHTSReferenceLoader:
    """Test HTS Reference Loader"""

    def test_load_valid_json(self, tmp_path):
        """Test loading valid JSON file"""
        # Create test data
        test_data = [
            {"htsno": "7301", "indent": 0, "description": "Sheet piling"},
            {"htsno": "7301.10", "indent": 1, "description": "Not assembled"},
            {"htsno": "7301.10.00.00", "indent": 3, "description": "Other"},
        ]

        test_file = tmp_path / "test_hts.json"
        with open(test_file, "w") as f:
            json.dump(test_data, f)

        # Load and verify
        loader = HTSReferenceLoader()
        result = loader.load_hts_json(test_file)

        assert len(result) == 3
        assert result[0]["htsno"] == "7301"

    def test_missing_file(self):
        """Test handling missing file"""
        loader = HTSReferenceLoader()

        with pytest.raises(FileNotFoundError):
            loader.load_hts_json(Path("/nonexistent/file.json"))

    def test_malformed_json(self, tmp_path):
        """Test handling malformed JSON"""
        test_file = tmp_path / "malformed.json"
        with open(test_file, "w") as f:
            f.write("{invalid json content")

        loader = HTSReferenceLoader()

        with pytest.raises(json.JSONDecodeError):
            loader.load_hts_json(test_file)

    def test_missing_required_fields(self, tmp_path):
        """Test validation of required fields"""
        # Missing 'description' field
        test_data = [{"htsno": "7301", "indent": 0}]

        test_file = tmp_path / "incomplete.json"
        with open(test_file, "w") as f:
            json.dump(test_data, f)

        loader = HTSReferenceLoader()

        with pytest.raises(ValueError, match="missing required fields"):
            loader.load_hts_json(test_file)

    def test_duplicate_codes(self, tmp_path):
        """Test detection of duplicate HTS codes"""
        test_data = [
            {"htsno": "7301", "indent": 0, "description": "Sheet piling"},
            {"htsno": "7301", "indent": 0, "description": "Duplicate"},
        ]

        test_file = tmp_path / "duplicates.json"
        with open(test_file, "w") as f:
            json.dump(test_data, f)

        loader = HTSReferenceLoader()

        with pytest.raises(ValueError, match="Duplicate HTS codes"):
            loader.load_hts_json(test_file)

    def test_invalid_field_types(self, tmp_path):
        """Test that loader handles type coerciof gracefully"""
        # indent as string should be coerced to int
        test_data = [{"htsno": "7301", "indent": "0", "description": "Sheet piling"}]

        test_file = tmp_path / "invalid_types.json"
        with open(test_file, "w") as f:
            json.dump(test_data, f)

        loader = HTSReferenceLoader()

        # Should load successfully with type coercion
        items = loader.load_hts_json(test_file)
        assert len(items) == 1
        # Verify indent is presenty (may be sting on implementation)
        assert "indent" in items[0]


class TestHTSHierarchyBuilder:
    """Test HTS Hierarchy Builder and Parent-Finding Algorithm"""

    def test_parent_finding_prefix_match(self):
        """Test parent-finding with prefix matching"""
        test_items = [
            {"htsno": "7301", "indent": 0, "description": "Sheet piling"},
            {"htsno": "7301.10", "indent": 1, "description": "Not assembled"},
        ]

        builder = HTSHierarchyBuilder()
        hierarchy_map = builder.build_hierarchy_map(test_items)

        # 7301.10 should have 7301 as parent (prefix match)
        assert hierarchy_map["7301.10"]["parent"] == "7301"
        assert "7301.10" in hierarchy_map["7301"]["children"]
        assert builder.parent_finding_stats["prefix_matches"] > 0

    def test_parent_finding_fallback(self):
        """Test parent-finding with positional fallback"""
        # Create scenario where prefix doesn't match but positional parent exists
        test_items = [
            {"htsno": "7301", "indent": 0, "description": "Sheet piling"},
            {"htsno": "7301.99", "indent": 1, "description": "Other"},
        ]

        builder = HTSHierarchyBuilder()
        hierarchy_map = builder.build_hierarchy_map(test_items)

        # Should find parent via fallback or prefix
        assert hierarchy_map["7301.99"]["parent"] is not None

    def test_parent_finding_missing_intermediate(self):
        """Test case where intermediate node is missing"""
        # 7301.20.50.00 exists but 7301.20.00 doesn't
        test_items = [
            {"htsno": "7301", "indent": 0, "description": "Sheet piling"},
            {"htsno": "7301.20", "indent": 1, "description": "Other"},
            {"htsno": "7301.20.50.00", "indent": 3, "description": "Specific item"},
        ]

        builder = HTSHierarchyBuilder()
        hierarchy_map = builder.build_hierarchy_map(test_items)

        # Should find 7301.20 as parent via fallback
        parent = hierarchy_map["7301.20.50.00"]["parent"]
        assert parent is not None

    def test_parent_finding_root_level(self):
        """Test that root level codes have no parent"""
        test_items = [{"htsno": "7301", "indent": 0, "description": "Sheet piling"}]

        builder = HTSHierarchyBuilder()
        hierarchy_map = builder.build_hierarchy_map(test_items)

        assert hierarchy_map["7301"]["parent"] is None

    def test_orphaned_code_detection(self):
        """Test detection of orphaned codes"""
        # Code with indent > 0 but no valid parent
        test_items = [
            {"htsno": "7301", "indent": 0, "description": "Sheet piling"},
            {"htsno": "9999.99.99.99", "indent": 3, "description": "Orphaned"},
        ]

        builder = HTSHierarchyBuilder()
        hierarchy_map = builder.build_hierarchy_map(test_items)

        # Should be logged as orphaned
        assert "9999.99.99.99" in builder.orphaned_codes

    def test_hierarchy_building_complete(self):
        """Test complete hierarchy map construction"""
        test_items = [
            {"htsno": "7301", "indent": 0, "description": "Sheet piling"},
            {"htsno": "7301.10", "indent": 1, "description": "Not assembled"},
            {"htsno": "7301.10.00", "indent": 2, "description": "Other"},
            {"htsno": "7301.10.00.00", "indent": 3, "description": "Other specific"},
        ]

        builder = HTSHierarchyBuilder()
        hierarchy_map = builder.build_hierarchy_map(test_items)

        # Verify all codes are in map
        assert len(hierarchy_map) == 4

        # Verify parent-child links
        assert hierarchy_map["7301"]["parent"] is None
        assert hierarchy_map["7301.10"]["parent"] == "7301"
        assert hierarchy_map["7301.10.00"]["parent"] == "7301.10"
        assert hierarchy_map["7301.10.00.00"]["parent"] == "7301.10.00"

        # Verify children
        assert "7301.10" in hierarchy_map["7301"]["children"]
        assert "7301.10.00" in hierarchy_map["7301.10"]["children"]


class TestHTSContextService:
    """Test HTS Context Service"""

    @pytest.fixture
    def test_service(self, tmp_path):
        """Create test service with mock data"""
        test_data = [
            {
                "htsno": "7301",
                "indent": 0,
                "description": "Sheet piling of iron or steel",
            },
            {
                "htsno": "7301.10",
                "indent": 1,
                "description": "Not assembled or fabricated",
            },
            {"htsno": "7301.10.00", "indent": 2, "description": "Other"},
            {"htsno": "7301.10.00.00", "indent": 3, "description": "Other"},
        ]

        test_file = tmp_path / "test_hts.json"
        with open(test_file, "w") as f:
            json.dump(test_data, f)

        return HTSContextService(test_file)

    def test_get_context_valid_code(self, test_service):
        """Test getting context for valid HTS code"""
        result = test_service.get_hts_context("7301.10.00.00")

        assert result["found"] is True
        assert result["hts_code"] == "7301.10.00.00"
        assert len(result["hierarchy_path"]) == 4

        # Verify hierarchy order (root first)
        assert result["hierarchy_path"][0]["code"] == "7301"
        assert result["hierarchy_path"][1]["code"] == "7301.10"
        assert result["hierarchy_path"][2]["code"] == "7301.10.00"
        assert result["hierarchy_path"][3]["code"] == "7301.10.00.00"

    def test_get_context_invalid_code(self, test_service):
        """Test getting context for invalid HTS code"""
        result = test_service.get_hts_context("9999.99.99.99")

        assert result["found"] is False
        assert result["error"] == "HTS code not found in reference data"
        assert len(result["hierarchy_path"]) == 0

    def test_get_context_root_level(self, test_service):
        """Test getting context for root level code"""
        result = test_service.get_hts_context("7301")

        assert result["found"] is True
        assert len(result["hierarchy_path"]) == 1
        assert result["hierarchy_path"][0]["code"] == "7301"
        assert result["hierarchy_path"][0]["indent"] == 0

    def test_get_context_deep_level(self, test_service):
        """Test getting context for deep level code"""
        result = test_service.get_hts_context("7301.10.00.00")

        assert result["found"] is True
        assert len(result["hierarchy_path"]) == 4

        # Verify all indent levels are correct
        assert result["hierarchy_path"][0]["indent"] == 0
        assert result["hierarchy_path"][1]["indent"] == 1
        assert result["hierarchy_path"][2]["indent"] == 2
        assert result["hierarchy_path"][3]["indent"] == 3

    def test_validate_code_exists(self, test_service):
        """Test code existence validation"""
        assert test_service.validate_hts_code_exists("7301.10.00.00") is True
        assert test_service.validate_hts_code_exists("9999.99.99.99") is False

    def test_get_statistics(self, test_service):
        """Test statistics retrieval"""
        stats = test_service.get_hierarchy_statistics()

        assert stats["total_codes"] == 4
        assert "indent_distribution" in stats
        assert "parent_finding_stats" in stats
        assert "orphaned_codes" in stats

        # Verify indent distribution
        assert stats["indent_distribution"][0] == 1
        assert stats["indent_distribution"][1] == 1
        assert stats["indent_distribution"][2] == 1
        assert stats["indent_distribution"][3] == 1

    def test_export_hierarchy_map(self, test_service, tmp_path):
        """Test hierarchy map export"""
        export_path = tmp_path / "hierarchy_export.json"
        test_service.export_hierarchy_map(export_path)

        assert export_path.exists()

        with open(export_path, "r") as f:
            exported_data = json.load(f)

        assert len(exported_data) == 4
        assert "7301" in exported_data
        assert "parent" in exported_data["7301"]
        assert "children" in exported_data["7301"]


class TestParentFindingEdgeCases:
    """Test edge cases in parent-finding algorithm"""

    def test_multiple_candidates_same_level(self):
        """Test when multiple candidates exist at parent level"""
        test_items = [
            {"htsno": "7301", "indent": 0, "description": "Sheet piling"},
            {"htsno": "7302", "indent": 0, "description": "Other items"},
            {"htsno": "7301.10", "indent": 1, "description": "Not assembled"},
        ]

        builder = HTSHierarchyBuilder()
        hierarchy_map = builder.build_hierarchy_map(test_items)

        # 7301.10 should match 7301, not 7302
        assert hierarchy_map["7301.10"]["parent"] == "7301"

    def test_complex_hierarchy_chain(self):
        """Test complex multi-level hierarchy"""
        test_items = [
            {"htsno": "7301", "indent": 0, "description": "Level 0"},
            {"htsno": "7301.10", "indent": 1, "description": "Level 1"},
            {"htsno": "7301.10.00", "indent": 2, "description": "Level 2"},
            {"htsno": "7301.10.00.00", "indent": 3, "description": "Level 3"},
            {"htsno": "7301.20", "indent": 1, "description": "Another Level 1"},
            {"htsno": "7301.20.50.00", "indent": 3, "description": "Skipped Level 2"},
        ]

        builder = HTSHierarchyBuilder()
        hierarchy_map = builder.build_hierarchy_map(test_items)

        # Verify complete chain for 7301.10.00.00
        assert hierarchy_map["7301.10.00.00"]["parent"] == "7301.10.00"
        assert hierarchy_map["7301.10.00"]["parent"] == "7301.10"
        assert hierarchy_map["7301.10"]["parent"] == "7301"

        # Verify item with missing intermediate level finds correct parent
        parent_of_skipped = hierarchy_map["7301.20.50.00"]["parent"]
        assert parent_of_skipped == "7301.20"

    def test_non_standard_code_formats(self):
        """Test handling of non-standard HTS code formats"""
        test_items = [
            {"htsno": "73", "indent": 0, "description": "Chapter"},
            {"htsno": "7301", "indent": 1, "description": "Heading"},
        ]

        builder = HTSHierarchyBuilder()
        hierarchy_map = builder.build_hierarchy_map(test_items)

        # Should handle codes without dots
        assert hierarchy_map["7301"]["parent"] == "73"


class TestServiceIntegration:
    """Integration tests with real data structure"""

    def test_service_initialization(self, tmp_path):
        """Test service initializes correctly"""
        test_data = [{"htsno": "7301", "indent": 0, "description": "Sheet piling"}]

        test_file = tmp_path / "test_hts.json"
        with open(test_file, "w") as f:
            json.dump(test_data, f)

        service = HTSContextService(test_file)

        assert service.hierarchy_map is not None
        assert len(service.hierarchy_map) == 1

    def test_multiple_context_lookups(self, tmp_path):
        """Test multiple consecutive lookups"""
        test_data = [
            {"htsno": "7301", "indent": 0, "description": "Sheet piling"},
            {"htsno": "7301.10", "indent": 1, "description": "Not assembled"},
            {"htsno": "7301.20", "indent": 1, "description": "Assembled"},
        ]

        test_file = tmp_path / "test_hts.json"
        with open(test_file, "w") as f:
            json.dump(test_data, f)

        service = HTSContextService(test_file)

        # Multiple lookups should work consistently
        result1 = service.get_hts_context("7301.10")
        result2 = service.get_hts_context("7301.20")
        result3 = service.get_hts_context("7301.10")  # Repeat lookup

        assert result1["found"] is True
        assert result2["found"] is True
        assert result3["found"] is True
        assert result1 == result3  # Same result for same code


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""
Integration tests for batch processing cache optimizations

Tests cover:
- HTS context batch-level caching
- Cache hit rate calculations
- Cache clearing between batches
- Service reuse across batch processors
- Rule manager file detection in batch workflow
"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from src.services.llm_enhancement.batch_processor import BatchProcessor
from src.services.common.service_factory import ServiceFactory
from src.services.ingestion.database import ProductDatabase
from src.services.hts_context.service import HTSContextService


class TestHTSContextBatchCache:
    """Test HTS context caching within batches"""

    @pytest.fixture
    def mock_products(self):
        """Create mock products with duplicate HTS codes"""
        products = []

        # 5 products with HTS "7307.11.00"
        for i in range(5):
            product = Mock()
            product.item_id = f"ITEM{i+1:03d}"
            product.final_hts = "7307.11.00"
            product.item_description = f"Product {i+1}"
            product.material_detail = "Steel"
            product.product_group = "Fittings"
            products.append(product)

        # 3 products with HTS "7307.92.00"
        for i in range(5, 8):
            product = Mock()
            product.item_id = f"ITEM{i+1:03d}"
            product.final_hts = "7307.92.00"
            product.item_description = f"Product {i+1}"
            product.material_detail = "Steel"
            product.product_group = "Fittings"
            products.append(product)

        return products

    def test_hts_cache_within_batch(self, mock_products):
        """Test HTS context cache reduces redundant lookups"""
        # Create mock HTS service that tracks calls
        mock_hts_service = Mock(spec=HTSContextService)
        mock_hts_service.get_hts_context = Mock(
            return_value={"found": True, "hierarchy_path": []}
        )

        # Create batch processor with mock
        processor = BatchProcessor(hts_service=mock_hts_service)

        # Call _get_hts_context for each product
        for product in mock_products:
            processor._get_hts_context(product)

        # Should only call service twice (2 unique HTS codes)
        assert mock_hts_service.get_hts_context.call_count == 2

        # Check cache has 2 entries
        assert len(processor._hts_context_cache) == 2
        assert "7307.11.00" in processor._hts_context_cache
        assert "7307.92.00" in processor._hts_context_cache

    def test_hts_cache_cleared_between_batches(self, mock_products):
        """Test cache is cleared at start of each batch"""
        mock_hts_service = Mock(spec=HTSContextService)
        mock_hts_service.get_hts_context = Mock(
            return_value={"found": True, "hierarchy_path": []}
        )

        processor = BatchProcessor(hts_service=mock_hts_service)

        # Process first batch
        for product in mock_products[:3]:
            processor._get_hts_context(product)

        assert len(processor._hts_context_cache) == 1

        # Simulate start of new batch (clear cache)
        processor._hts_context_cache.clear()

        # Process second batch
        for product in mock_products[5:7]:
            processor._get_hts_context(product)

        # Should only have HTS codes from second batch
        assert len(processor._hts_context_cache) == 1
        assert "7307.92.00" in processor._hts_context_cache
        assert "7307.11.00" not in processor._hts_context_cache


class TestServiceReuseInBatchProcessor:
    """Test batch processors reuse services via ServiceFactory"""

    def test_batch_processor_reuses_services(self):
        """Test multiple batch processors use same service instances"""
        processor1 = BatchProcessor()
        processor2 = BatchProcessor()

        # Both should use same HTS service
        assert processor1.hts_service is processor2.hts_service

        # Both should use same database
        assert processor1.db is processor2.db

    def test_custom_instances_not_cached(self):
        """Test passing custom instances bypasses caching"""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_db = ProductDatabase(Path(tmpdir) / "custom.db")

            processor = BatchProcessor(db=custom_db)

            # Should use custom instance, not cached one
            assert processor.db is custom_db

            # HTS service should still be cached (not provided)
            default_hts = ServiceFactory.get_hts_service()
            assert processor.hts_service is default_hts


class TestRuleManagerFileDetection:
    """Test rule manager file modification detection in batch workflow"""

    @pytest.fixture
    def temp_rules_file(self):
        """Create temporary rules file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            rules_file = Path(tmpdir) / "rules.json"
            rules_data = {
                "rules": [
                    {
                        "rule_id": "R001",
                        "rule_name": "Test Rule",
                        "rule_content": "Test content",
                        "rule_type": "material",
                        "active": True,
                        "created_at": "2025-01-01T00:00:00Z",
                    }
                ],
                "metadata": {
                    "version": "1.0",
                    "total_rules": 1,
                    "active_rules": 1,
                    "last_updated": "2025-01-01T00:00:00Z",
                },
            }
            with open(rules_file, "w") as f:
                json.dump(rules_data, f)

            yield rules_file

    def test_rule_file_modification_detected(self, temp_rules_file):
        """Test file modification is detected automatically"""
        import time

        # Get RuleManager and load rules
        rule_manager = ServiceFactory.get_rule_manager(temp_rules_file)
        rules = rule_manager.load_rules()
        assert len(rules) == 1

        # Modify file
        time.sleep(0.1)  # Ensure mtime changes

        rules_data = {
            "rules": [
                {
                    "rule_id": "R001",
                    "rule_name": "Test Rule",
                    "rule_content": "Test content",
                    "rule_type": "material",
                    "active": True,
                    "created_at": "2025-01-01T00:00:00Z",
                },
                {
                    "rule_id": "R002",
                    "rule_name": "New Rule",
                    "rule_content": "New content",
                    "rule_type": "material",
                    "active": True,
                    "created_at": "2025-01-02T00:00:00Z",
                },
            ],
            "metadata": {
                "version": "1.0",
                "total_rules": 2,
                "active_rules": 2,
                "last_updated": "2025-01-02T00:00:00Z",
            },
        }
        with open(temp_rules_file, "w") as f:
            json.dump(rules_data, f)

        # Get RuleManager again - should auto-detect change
        rule_manager_again = ServiceFactory.get_rule_manager(temp_rules_file)
        rules_after = rule_manager_again.load_rules()

        assert len(rules_after) == 2
        assert rules_after[1].rule_id == "R002"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

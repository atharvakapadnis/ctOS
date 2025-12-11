"""
Unit tests for ServiceFactory - Centralized service instance manager

Tests cover:
- Database caching (default and custom paths)
- HTS service caching (default and custom paths)
- OpenAI client caching (default vs custom API keys)
- RuleManager caching with file modification detection
- Cache management operations
- Thread safety
"""

import pytest
import tempfile
import json
import time
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.services.common.service_factory import ServiceFactory
from src.services.ingestion.database import ProductDatabase
from src.services.hts_context.service import HTSContextService
from src.services.llm_enhancement.api_client import OpenAIClient
from src.services.rules.manager import RuleManager


class TestServiceFactoryDatabase:
    """Test database caching functionality"""

    def test_get_database_default_path(self):
        """Test getting database with default path returns same instance"""
        db1 = ServiceFactory.get_database()
        db2 = ServiceFactory.get_database()

        assert db1 is db2
        assert isinstance(db1, ProductDatabase)

    def test_get_database_custom_path(self):
        """Test getting database with custom path caches per path"""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_path = Path(tmpdir) / "custom.db"

            db1 = ServiceFactory.get_database(custom_path)
            db2 = ServiceFactory.get_database(custom_path)

            assert db1 is db2
            assert isinstance(db1, ProductDatabase)

    def test_get_database_multiple_paths(self):
        """Test different paths create separate cached instances"""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_path = Path(tmpdir) / "custom.db"

            db_default = ServiceFactory.get_database()
            db_custom = ServiceFactory.get_database(custom_path)
            db_default_again = ServiceFactory.get_database()

            assert db_default is db_default_again
            assert db_default is not db_custom

            stats = ServiceFactory.get_cache_stats()
            assert stats["total_instances"] >= 2

    def test_get_database_after_clear(self):
        """Test new instance created after cache clear"""
        db1 = ServiceFactory.get_database()
        instance_id_1 = id(db1)

        ServiceFactory.clear_cache()

        db2 = ServiceFactory.get_database()
        instance_id_2 = id(db2)

        assert instance_id_1 != instance_id_2


class TestServiceFactoryHTSService:
    """Test HTS service caching functionality"""

    def test_get_hts_service_singleton(self):
        """Test HTS service returns same instance (default path)"""
        hts1 = ServiceFactory.get_hts_service()
        hts2 = ServiceFactory.get_hts_service()

        assert hts1 is hts2
        assert isinstance(hts1, HTSContextService)

    def test_get_hts_service_custom_path(self):
        """Test HTS service with custom path caches separately"""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_hts = Path(tmpdir) / "custom_hts.json"

            # Create minimal valid HTS file
            hts_data = [
                {
                    "hts": "7307.11.00",
                    "indent": 0,
                    "description": "Test HTS",
                    "unit": "kg",
                }
            ]
            with open(custom_hts, "w") as f:
                json.dump(hts_data, f)

            hts_default = ServiceFactory.get_hts_service()
            hts_custom = ServiceFactory.get_hts_service(custom_hts)

            assert hts_default is not hts_custom

    def test_get_hts_service_after_clear(self):
        """Test new HTS service created after cache clear"""
        hts1 = ServiceFactory.get_hts_service()
        instance_id_1 = id(hts1)

        ServiceFactory.clear_cache()

        hts2 = ServiceFactory.get_hts_service()
        instance_id_2 = id(hts2)

        assert instance_id_1 != instance_id_2

    def test_hts_service_hierarchy_map_loaded(self):
        """Test HTS service has hierarchy map loaded"""
        hts_service = ServiceFactory.get_hts_service()

        assert hasattr(hts_service, "hierarchy_map")
        assert isinstance(hts_service.hierarchy_map, dict)


class TestServiceFactoryOpenAIClient:
    """Test OpenAI client caching functionality"""

    def test_get_openai_client_default_key(self):
        """Test OpenAI client with default key is cached"""
        client1 = ServiceFactory.get_openai_client()
        client2 = ServiceFactory.get_openai_client()

        assert client1 is client2
        assert isinstance(client1, OpenAIClient)

    def test_get_openai_client_custom_key_not_cached(self):
        """Test OpenAI client with custom key creates fresh instances"""
        client1 = ServiceFactory.get_openai_client(api_key="test-key-1")
        client2 = ServiceFactory.get_openai_client(api_key="test-key-1")

        assert client1 is not client2
        assert isinstance(client1, OpenAIClient)
        assert isinstance(client2, OpenAIClient)

    def test_get_openai_client_mixed_keys(self):
        """Test mixing default and custom keys caches correctly"""
        client_default_1 = ServiceFactory.get_openai_client()
        client_custom = ServiceFactory.get_openai_client(api_key="test-key")
        client_default_2 = ServiceFactory.get_openai_client()

        assert client_default_1 is client_default_2
        assert client_default_1 is not client_custom


class TestServiceFactoryRuleManager:
    """Test RuleManager caching with file modification detection"""

    @pytest.fixture
    def temp_rules_file(self):
        """Create temporary rules file for testing"""
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

    def test_get_rule_manager_singleton(self, temp_rules_file):
        """Test RuleManager returns same instance for same path"""
        mgr1 = ServiceFactory.get_rule_manager(temp_rules_file)
        mgr2 = ServiceFactory.get_rule_manager(temp_rules_file)

        assert mgr1 is mgr2
        assert isinstance(mgr1, RuleManager)

    def test_rule_manager_loads_rules(self, temp_rules_file):
        """Test RuleManager loads rules correctly"""
        rule_manager = ServiceFactory.get_rule_manager(temp_rules_file)
        rules = rule_manager.load_rules()

        assert len(rules) == 1
        assert rules[0].rule_id == "R001"

    def test_reload_rules_after_file_change(self, temp_rules_file):
        """Test RuleManager auto-reloads when file is modified"""
        rule_manager = ServiceFactory.get_rule_manager(temp_rules_file)
        rules = rule_manager.load_rules()
        assert len(rules) == 1

        # Modify rules file
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

        # Get RuleManager again - should detect file change
        rule_manager_again = ServiceFactory.get_rule_manager(temp_rules_file)
        rules_after = rule_manager_again.load_rules()

        assert len(rules_after) == 2
        assert rules_after[1].rule_id == "R002"

    def test_reload_rules_explicit(self, temp_rules_file):
        """Test explicit rule reload works"""
        rule_manager = ServiceFactory.get_rule_manager(temp_rules_file)
        rules = rule_manager.load_rules()
        assert len(rules) == 1

        # Modify rules file
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

        # Force reload
        ServiceFactory.reload_rules(temp_rules_file)

        # Get rules
        rule_manager_again = ServiceFactory.get_rule_manager(temp_rules_file)
        rules_after = rule_manager_again.load_rules()

        assert len(rules_after) == 2


class TestServiceFactoryCacheManagement:
    """Test cache management operations"""

    def test_clear_cache_all_services(self):
        """Test clearing cache removes all services"""
        # Create instances
        ServiceFactory.get_database()
        ServiceFactory.get_hts_service()
        ServiceFactory.get_openai_client()

        stats_before = ServiceFactory.get_cache_stats()
        assert stats_before["total_instances"] >= 3

        # Clear cache
        ServiceFactory.clear_cache()

        stats_after = ServiceFactory.get_cache_stats()
        assert stats_after["total_instances"] == 0

    def test_get_cache_stats(self):
        """Test cache statistics are accurate"""
        # Start fresh
        ServiceFactory.clear_cache()

        # Create instances
        ServiceFactory.get_database()
        ServiceFactory.get_hts_service()
        ServiceFactory.get_openai_client()

        stats = ServiceFactory.get_cache_stats()

        assert isinstance(stats, dict)
        assert stats["total_instances"] >= 3
        assert stats["has_hts_service"] is True
        assert stats["has_openai_client"] is True
        assert "instance_types" in stats
        assert "database_paths" in stats

    def test_cache_stats_empty(self):
        """Test cache stats when cache is empty"""
        ServiceFactory.clear_cache()

        stats = ServiceFactory.get_cache_stats()

        assert stats["total_instances"] == 0
        assert stats["has_hts_service"] is False
        assert stats["has_openai_client"] is False


class TestServiceFactoryThreadSafety:
    """Test thread safety of ServiceFactory"""

    def test_concurrent_access_same_service(self):
        """Test concurrent access to same service returns same instance"""
        instances = []

        def get_db():
            db = ServiceFactory.get_database()
            instances.append(db)

        # Create 10 threads accessing database simultaneously
        threads = [threading.Thread(target=get_db) for _ in range(10)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # All threads should get same instance
        assert len(instances) == 10
        assert all(inst is instances[0] for inst in instances)

    def test_concurrent_access_different_services(self):
        """Test concurrent access to different services works"""
        results = {"db": None, "hts": None, "openai": None}

        def get_db():
            results["db"] = ServiceFactory.get_database()

        def get_hts():
            results["hts"] = ServiceFactory.get_hts_service()

        def get_openai():
            results["openai"] = ServiceFactory.get_openai_client()

        threads = [
            threading.Thread(target=get_db),
            threading.Thread(target=get_hts),
            threading.Thread(target=get_openai),
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # All services should be created successfully
        assert isinstance(results["db"], ProductDatabase)
        assert isinstance(results["hts"], HTSContextService)
        assert isinstance(results["openai"], OpenAIClient)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

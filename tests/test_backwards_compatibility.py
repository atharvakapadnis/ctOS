"""
Backwards compatibility tests for ServiceFactory

Ensures that existing code continues to work without changes
when ServiceFactory is introduced.

Tests cover:
- BatchProcessor with custom instances
- BatchProcessor without instances (using factory)
- Mixed custom and factory instances
- Existing test patterns still work
"""

import pytest
import tempfile
from pathlib import Path

from src.services.llm_enhancement.batch_processor import BatchProcessor
from src.services.ingestion.database import ProductDatabase
from src.services.hts_context.service import HTSContextService
from src.services.llm_enhancement.api_client import OpenAIClient
from src.services.common.service_factory import ServiceFactory


class TestBackwardsCompatibility:
    """Test backwards compatibility with existing code patterns"""

    def test_batch_processor_with_custom_instances(self):
        """Test BatchProcessor accepts custom service instances"""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_db = ProductDatabase(Path(tmpdir) / "custom.db")
            custom_hts = HTSContextService()
            custom_openai = OpenAIClient()

            processor = BatchProcessor(
                db=custom_db, hts_service=custom_hts, openai_client=custom_openai
            )

            # Should use provided instances, not factory
            assert processor.db is custom_db
            assert processor.hts_service is custom_hts
            assert processor.openai_client is custom_openai

    def test_batch_processor_without_instances(self):
        """Test BatchProcessor uses factory when no instances provided"""
        processor = BatchProcessor()

        # Should use factory instances
        factory_hts = ServiceFactory.get_hts_service()
        assert processor.hts_service is factory_hts

        factory_db = ServiceFactory.get_database()
        assert processor.db is factory_db

        factory_openai = ServiceFactory.get_openai_client()
        assert processor.openai_client is factory_openai

    def test_mixed_custom_and_factory(self):
        """Test mixing custom instances and factory instances"""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_db = ProductDatabase(Path(tmpdir) / "custom.db")

            processor = BatchProcessor(db=custom_db)

            # Should use custom database
            assert processor.db is custom_db

            # Should use factory for HTS and OpenAI
            factory_hts = ServiceFactory.get_hts_service()
            assert processor.hts_service is factory_hts

            factory_openai = ServiceFactory.get_openai_client()
            assert processor.openai_client is factory_openai

    def test_multiple_processors_with_custom_instances(self):
        """Test multiple processors can have different custom instances"""
        with tempfile.TemporaryDirectory() as tmpdir:
            db1 = ProductDatabase(Path(tmpdir) / "db1.db")
            db2 = ProductDatabase(Path(tmpdir) / "db2.db")

            processor1 = BatchProcessor(db=db1)
            processor2 = BatchProcessor(db=db2)

            # Should use different database instances
            assert processor1.db is db1
            assert processor2.db is db2
            assert processor1.db is not processor2.db

            # But share HTS service from factory
            assert processor1.hts_service is processor2.hts_service

    def test_hts_context_cache_independent_per_processor(self):
        """Test HTS context cache is per-processor, not global"""
        processor1 = BatchProcessor()
        processor2 = BatchProcessor()

        # Each should have its own cache dictionary
        assert processor1._hts_context_cache is not processor2._hts_context_cache

        # Caches start empty
        assert len(processor1._hts_context_cache) == 0
        assert len(processor2._hts_context_cache) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

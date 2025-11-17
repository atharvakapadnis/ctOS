"""
Unit tests for search and filter functionality
Tests database search and filter methods
"""

import pytest
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime, timezone

from src.services.ingestion.database import ProductDatabase
from src.services.ingestion.models import ProductRecord, UpdateProcessingInput


@pytest.fixture
def temp_db():
    """Create temporary database for testing"""
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test_search.db"
    return db_path


@pytest.fixture
def sample_products():
    """Create sample products for testing"""
    return [
        ProductRecord(
            item_id="ITEM-001",
            item_description="Ductile iron spacer for pipe fitting",
            product_group="FITTINGS",
            product_group_code="FIT001",
            product_group_description="Pipe Fittings",
            material_class="Ductile Iron",
            material_detail="DI Grade 65-45-12",
            manf_class="Casting",
            supplier_id="SUP001",
            supplier_name="ABC Manufacturing",
            country_of_origin="China",
            import_type="Direct",
            port_of_delivery="Los Angeles",
            final_hts="7307.11.00.50",
            hts_description="Fittings of cast iron",
        ),
        ProductRecord(
            item_id="ITEM-002",
            item_description="Steel coupling for water pipe",
            product_group="COUPLINGS",
            product_group_code="COU001",
            product_group_description="Couplings",
            material_class="Steel",
            material_detail="Carbon Steel",
            manf_class="Machined",
            supplier_id="SUP002",
            supplier_name="XYZ Corp",
            country_of_origin="USA",
            import_type="Domestic",
            port_of_delivery="Chicago",
            final_hts="7307.92.30.40",
            hts_description="Couplings",
        ),
        ProductRecord(
            item_id="ITEM-003",
            item_description="Ductile iron ring gasket",
            product_group="GASKETS",
            product_group_code="GAS001",
            product_group_description="Gaskets and Seals",
            material_class="Ductile Iron",
            material_detail="DI Grade 80-55-06",
            manf_class="Molded",
            supplier_id="SUP001",
            supplier_name="ABC Manufacturing",
            country_of_origin="China",
            import_type="Direct",
            port_of_delivery="Los Angeles",
            final_hts="7307.19.00.00",
            hts_description="Other fittings",
        ),
        ProductRecord(
            item_id="TEST-ALPHA",
            item_description="Aluminum bracket for mounting",
            product_group="BRACKETS",
            product_group_code="BRA001",
            product_group_description="Mounting Brackets",
            material_class="Aluminum",
            material_detail="6061-T6",
            manf_class="Extruded",
            supplier_id="SUP003",
            supplier_name="Metal Works Inc",
            country_of_origin="Mexico",
            import_type="NAFTA",
            port_of_delivery="San Diego",
            final_hts="7308.90.00.00",
            hts_description="Other structures",
        ),
    ]


@pytest.fixture
def populated_db(temp_db, sample_products):
    """Create and populate database with sample data"""
    db = ProductDatabase(temp_db)
    db.create_schema()
    db.insert_products(sample_products)

    # Add processing results for some products
    db.update_processing_results(
        "ITEM-001",
        UpdateProcessingInput(
            enhanced_description="Ductile iron spacer",
            confidence_score="0.85",
            confidence_level="High",
            extracted_customer_name="N/A",
            extracted_dimensions="N/A",
            extracted_product="Spacer",
            rules_applied="[]",
            pass_number="1",
        ),
    )

    db.update_processing_results(
        "ITEM-002",
        UpdateProcessingInput(
            enhanced_description="Steel coupling",
            confidence_score="0.55",
            confidence_level="Medium",
            extracted_customer_name="N/A",
            extracted_dimensions="N/A",
            extracted_product="Coupling",
            rules_applied="[]",
            pass_number="1",
        ),
    )

    db.update_processing_results(
        "ITEM-003",
        UpdateProcessingInput(
            enhanced_description="Ductile iron ring",
            confidence_score="0.35",
            confidence_level="Low",
            extracted_customer_name="N/A",
            extracted_dimensions="N/A",
            extracted_product="Ring Gasket",
            rules_applied="[]",
            pass_number="1",
        ),
    )

    return db


class TestSearchProducts:
    """Test search_products method"""

    def test_search_by_item_id_exact(self, populated_db):
        """Test exact item ID search"""
        results = populated_db.search_products("ITEM-001", search_type="item_id")
        assert len(results) == 1
        assert results[0].item_id == "ITEM-001"

    def test_search_by_item_id_partial(self, populated_db):
        """Test partial item ID search"""
        results = populated_db.search_products("ITEM", search_type="item_id")
        assert len(results) == 3
        assert all("ITEM" in r.item_id for r in results)

    def test_search_by_hts_code_prefix(self, populated_db):
        """Test HTS code prefix search"""
        results = populated_db.search_products("7307", search_type="hts_code")
        assert len(results) == 3
        assert all(r.final_hts.startswith("7307") for r in results)

    def test_search_by_hts_code_full(self, populated_db):
        """Test full HTS code search"""
        results = populated_db.search_products("7307.11.00", search_type="hts_code")
        assert len(results) == 1
        assert results[0].final_hts.startswith("7307.11.00")

    def test_search_by_description_single_keyword(self, populated_db):
        """Test description search with single keyword"""
        results = populated_db.search_products("spacer", search_type="description")
        assert len(results) == 1
        assert "spacer" in results[0].item_description.lower()

    def test_search_by_description_multiple_keywords(self, populated_db):
        """Test description search with multiple keywords"""
        results = populated_db.search_products(
            "ductile iron", search_type="description"
        )
        assert len(results) == 2
        for r in results:
            desc_lower = r.item_description.lower()
            assert "ductile" in desc_lower and "iron" in desc_lower

    def test_search_multi_column(self, populated_db):
        """Test multi-column search"""
        results = populated_db.search_products("iron", search_type="multi")
        assert len(results) >= 2

    def test_search_no_results(self, populated_db):
        """Test search with no results"""
        results = populated_db.search_products("nonexistent", search_type="item_id")
        assert len(results) == 0

    def test_search_auto_detect_item_id(self, populated_db):
        """Test auto-detection of item ID search"""
        results = populated_db.search_products("ITEM-001", search_type="auto")
        assert len(results) == 1
        assert results[0].item_id == "ITEM-001"

    def test_search_auto_detect_hts(self, populated_db):
        """Test auto-detection of HTS code search"""
        results = populated_db.search_products("7307.11.00", search_type="auto")
        assert len(results) >= 1

    def test_search_with_limit(self, populated_db):
        """Test search with result limit"""
        results = populated_db.search_products("ITEM", search_type="item_id", limit=2)
        assert len(results) == 2

    def test_search_empty_query(self, populated_db):
        """Test search with empty query"""
        results = populated_db.search_products("", search_type="auto")
        assert len(results) == 0


class TestFilterProducts:
    """Test filter_products method"""

    def test_filter_by_hts_range_both(self, populated_db):
        """Test filtering by HTS range (both start and end)"""
        filters = {"hts_range": {"start": "7307.11.00", "end": "7307.92.99"}}
        results = populated_db.filter_products(filters, limit=500)
        assert len(results) == 3
        for r in results:
            assert "7307.11.00" <= r.final_hts <= "7307.92.99"

    def test_filter_by_hts_range_start_only(self, populated_db):
        """Test filtering by HTS range (start only)"""
        filters = {"hts_range": {"start": "7307.90.00"}}
        results = populated_db.filter_products(filters, limit=500)
        assert all(r.final_hts >= "7307.90.00" for r in results)

    def test_filter_by_hts_range_end_only(self, populated_db):
        """Test filtering by HTS range (end only)"""
        filters = {"hts_range": {"end": "7307.20.00"}}
        results = populated_db.filter_products(filters, limit=500)
        assert all(r.final_hts <= "7307.20.00" for r in results)

    def test_filter_by_product_group(self, populated_db):
        """Test filtering by product group"""
        filters = {"product_group": "FITTINGS"}
        results = populated_db.filter_products(filters, limit=500)
        assert len(results) == 1
        assert all(r.product_group == "FITTINGS" for r in results)

    def test_filter_by_material_class(self, populated_db):
        """Test filtering by material class"""
        filters = {"material_class": "Ductile Iron"}
        results = populated_db.filter_products(filters, limit=500)
        assert len(results) == 2
        assert all(r.material_class == "Ductile Iron" for r in results)

    def test_filter_by_status_unprocessed(self, populated_db):
        """Test filtering by unprocessed status"""
        filters = {"status": "unprocessed"}
        results = populated_db.filter_products(filters, limit=500)
        assert len(results) == 1
        assert all(r.enhanced_description is None for r in results)

    def test_filter_by_status_processed(self, populated_db):
        """Test filtering by processed status"""
        filters = {"status": "processed"}
        results = populated_db.filter_products(filters, limit=500)
        assert len(results) == 3
        assert all(r.enhanced_description is not None for r in results)

    def test_filter_by_confidence_levels(self, populated_db):
        """Test filtering by confidence levels"""
        filters = {"status": "processed", "confidence_levels": ["Low", "Medium"]}
        results = populated_db.filter_products(filters, limit=500)
        assert len(results) == 2
        assert all(r.confidence_level in ["Low", "Medium"] for r in results)

    def test_filter_combined_all_criteria(self, populated_db):
        """Test filtering with all criteria combined"""
        filters = {
            "hts_range": {"start": "7307.00.00", "end": "7307.99.99"},
            "material_class": "Ductile Iron",
            "status": "processed",
            "confidence_levels": ["High", "Low"],
        }
        results = populated_db.filter_products(filters, limit=500)
        assert len(results) == 2

    def test_filter_with_limit(self, populated_db):
        """Test filtering with result limit"""
        filters = {"status": "all"}
        results = populated_db.filter_products(filters, limit=2)
        assert len(results) == 2

    def test_filter_no_results(self, populated_db):
        """Test filtering with criteria that match nothing"""
        filters = {"product_group": "NONEXISTENT"}
        results = populated_db.filter_products(filters, limit=500)
        assert len(results) == 0


class TestCountFilteredProducts:
    """Test count_filtered_products method"""

    def test_count_matches_actual_filter(self, populated_db):
        """Test that count matches actual filtered results"""
        filters = {"material_class": "Ductile Iron"}
        count = populated_db.count_filtered_products(filters)
        actual_results = populated_db.filter_products(filters, limit=500)
        assert count == len(actual_results)

    def test_count_all_products(self, populated_db):
        """Test count with no filters (all products)"""
        filters = {"status": "all"}
        count = populated_db.count_filtered_products(filters)
        assert count == 4

    def test_count_unprocessed(self, populated_db):
        """Test count unprocessed products"""
        filters = {"status": "unprocessed"}
        count = populated_db.count_filtered_products(filters)
        assert count == 1

    def test_count_processed(self, populated_db):
        """Test count processed products"""
        filters = {"status": "processed"}
        count = populated_db.count_filtered_products(filters)
        assert count == 3


class TestUniqueValueMethods:
    """Test methods for getting unique filter values"""

    def test_get_unique_product_groups(self, populated_db):
        """Test getting unique product groups"""
        groups = populated_db.get_unique_product_groups()
        assert len(groups) == 4
        assert "FITTINGS" in groups
        assert "COUPLINGS" in groups
        assert groups == sorted(groups)

    def test_get_unique_material_classes(self, populated_db):
        """Test getting unique material classes"""
        materials = populated_db.get_unique_material_classes()
        assert len(materials) == 3
        assert "Ductile Iron" in materials
        assert "Steel" in materials
        assert "Aluminum" in materials
        assert materials == sorted(materials)

    def test_get_unique_hts_codes(self, populated_db):
        """Test getting unique HTS codes"""
        hts_codes = populated_db.get_unique_hts_codes()
        assert len(hts_codes) == 4
        assert hts_codes == sorted(hts_codes)

    def test_get_hts_code_ranges(self, populated_db):
        """Test getting HTS code min/max ranges"""
        ranges = populated_db.get_hts_code_ranges()
        assert "min" in ranges
        assert "max" in ranges
        assert ranges["min"] == "7307.11.00.50"
        assert ranges["max"] == "7308.90.00.00"


class TestSearchIndexes:
    """Test search index creation"""

    def test_create_search_indexes(self, temp_db):
        """Test that search indexes are created successfully"""
        db = ProductDatabase(temp_db)
        db.create_schema()

        # Verify indexes exist
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = [row[0] for row in cursor.fetchall()]

        # Check for search-specific indexes
        assert "idx_products_item_id_search" in indexes
        assert "idx_products_material_class" in indexes

        # Check for base indexes
        assert "idx_products_final_hts" in indexes
        assert "idx_products_product_group" in indexes

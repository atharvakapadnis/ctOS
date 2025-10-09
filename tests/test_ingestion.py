"""
Basic tests for Data Ingestion Service
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import shutil

from src.services.ingestion import (
    CSVLoader,
    DataValidator,
    ProductDatabase,
    ProductRecord,
    ingest_products,
)
from src.services.ingestion.config import PRODUCT_COLUMNS


class TestCSVLoader:
    """Test CSV loading functionality"""

    def test_load_valid_csv(self, sample_csv):
        """Test loading a valid CSV file"""
        loader = CSVLoader()
        df = loader.load(sample_csv)

        assert len(df) > 0
        assert list(df.columns) == PRODUCT_COLUMNS

    def test_load_missing_file(self):
        """Test loading non-existent file raises error"""
        loader = CSVLoader()

        with pytest.raises(FileNotFoundError):
            loader.load(Path("nonexistent.csv"))

    def test_to_product_records(self, sample_csv):
        """Test converting DataFrame to ProductRecord instances"""
        loader = CSVLoader()
        df = loader.load(sample_csv)
        records = loader.to_product_records(df)

        assert len(records) > 0
        assert all(isinstance(r, ProductRecord) for r in records)


class TestDataValidator:
    """Test data validation"""

    def test_validate_columns(self, sample_df):
        """Test column validation"""
        validator = DataValidator()
        result = validator.validate_columns(sample_df)

        assert "found_columns" in result
        assert "missing_columns" in result
        assert len(result["missing_columns"]) == 0

    def test_hts_validation(self, sample_df):
        """Test HTS code validation"""
        validator = DataValidator()
        result = validator.validate_hts_codes(sample_df)

        assert "valid_count" in result
        assert "invalid_count" in result
        assert result["valid_count"] >= 0

    def test_full_validation(self, sample_df):
        """Test complete validation pipeline"""
        validator = DataValidator()
        report = validator.validate(sample_df)

        assert report.total_records == len(sample_df)
        assert 0.0 <= report.quality_score <= 1.0


class TestProductDatabase:
    """Test database operations"""

    def test_create_schema(self, temp_db):
        """Test schema creation"""
        db = ProductDatabase(temp_db)
        db.create_schema()

        # Verify tables exist
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            assert "products" in tables
            assert "processing_results" in tables

    def test_insert_products(self, temp_db, sample_records):
        """Test product insertion"""
        db = ProductDatabase(temp_db)
        db.create_schema()

        count = db.insert_products(sample_records)
        assert count == len(sample_records)

    def test_get_product_by_id(self, populated_db, sample_records):
        """Test retrieving product by ID"""
        db = populated_db

        # Get first product
        first_item_id = sample_records[0].item_id
        product = db.get_product_by_id(first_item_id)

        assert product is not None
        assert product.item_id == first_item_id

    def test_get_unprocessed_products(self, populated_db, sample_records):
        """Test getting unprocessed products"""
        db = populated_db

        unprocessed = db.get_unprocessed_products()

        # All should be unprocessed initially
        assert len(unprocessed) == len(sample_records)

    def test_get_statistics(self, populated_db, sample_records):
        """Test database statistics"""
        db = populated_db
        stats = db.get_database_statistics()

        assert stats.total_products == len(sample_records)
        assert stats.unprocessed_count == len(sample_records)
        assert stats.processed_count == 0


class TestIntegration:
    """Integration tests"""

    def test_full_ingestion_pipeline(self, sample_csv, temp_db):
        """Test complete ingestion pipeline"""
        report = ingest_products(csv_path=sample_csv, db_path=temp_db)

        assert report.total_records > 0
        assert report.validation_passed


# ============= FIXTURES =============


@pytest.fixture
def sample_csv(tmp_path):
    """Create a minimal sample CSV for testing"""
    csv_content = """item_id,item_description,product_group,product_group_description,product_group_code,material_class,material_detail,manf_class,supplier_id,supplier_name,country_of_origin,import_type,port_of_delivery,final_hts,hts_description
ITEM001,Steel rebar 10mm,Steel,Rebar,SP001,Steel,Carbon,Rebar,SUP001,ABC Steel,China,Import,LA,7301.10.00.00,Rebar
ITEM002,Aluminum sheet 2mm,Aluminum,Sheet,AL001,Aluminum,6061,Sheet,SUP002,XYZ Metals,Canada,Import,Seattle,7301.20.00.00,Aluminum
ITEM003,Copper wire 2.5mm,Copper,Wire,CU001,Copper,Pure,Wire,SUP003,Copper Corp,Mexico,Import,SD,7302.10.00.00,Copper wire
"""

    csv_file = tmp_path / "test_products.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def sample_df(sample_csv):
    """Load sample CSV as DataFrame"""
    return pd.read_csv(sample_csv)


@pytest.fixture
def sample_records(sample_csv):
    """Load sample CSV as ProductRecord list"""
    loader = CSVLoader()
    return loader.load_and_convert(sample_csv)


@pytest.fixture
def temp_db(tmp_path):
    """Create temporary database path"""
    return tmp_path / "test.db"


@pytest.fixture
def populated_db(temp_db, sample_records):
    """Create database with sample data"""
    db = ProductDatabase(temp_db)
    db.create_schema()
    db.insert_products(sample_records)
    return db

"""
Configuration constants for Data Ingestion Service
"""

from pathlib import Path
import re

# Project Root Directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# File paths
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
LOG_DIR = DATA_DIR / "logs"
DEBUG_DIR = DATA_DIR / "debug"
DATABASE_PATH = DATA_DIR / "products.db"
CSV_PATH = INPUT_DIR / "cleaned_test_ch73.csv"

# Database config
PRODUCTS_TABLE = "products"
PROCESSING_TABLE = "processing_results"
BATCH_SIZE = 1000

# PRODUCT COLUMNS
PRODUCT_COLUMNS = [
    "item_id",
    "item_description",
    "product_group",
    "product_group_code",
    "product_group_description",
    "material_class",
    "material_detail",
    "manf_class",
    "supplier_id",
    "supplier_name",
    "country_of_origin",
    "import_type",
    "port_of_delivery",
    "final_hts",
    "hts_description",
]

# PROCESSING COLUMNS
PROCESSING_COLUMNS = [
    "item_id",
    "enhanced_description",
    "confidence_score",
    "confidence_level",
    "extracted_customer_name",
    "extracted_dimensions",
    "extracted_product",
    "rules_applied",
    "last_processed_pass",
    "last_processed_at",
]

# Required fields
REQUIRED_FIELDS = ["item_id", "item_description", "final_hts"]

# Valida confidence levels
VALID_CONFIDENCE_LEVELS = ["Low", "Medium", "High"]

# HTS Code validation patterns
HTS_PATTERN = re.compile(r"^\d{4}\.\d{2}\.\d{2}\.\d{2}$")

# Logging config
LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
)
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# SQL Statements
CREATE_PRODUCTS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PRODUCTS_TABLE} (
    item_id TEXT PRIMARY KEY,
    item_description TEXT NOT NULL,
    product_group TEXT,
    product_group_code TEXT,
    product_group_description TEXT,
    material_class TEXT,
    material_detail TEXT,
    manf_class TEXT,
    supplier_id TEXT,
    supplier_name TEXT,
    country_of_origin TEXT,
    import_type TEXT,
    port_of_delivery TEXT,
    final_hts TEXT NOT NULL,
    hts_description TEXT
)
"""

CREATE_PROCESSING_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PROCESSING_TABLE} (
    item_id TEXT PRIMARY KEY,
    enhanced_description TEXT,
    confidence_score TEXT,
    confidence_level TEXT,
    extracted_customer_name TEXT,
    extracted_dimensions TEXT,
    extracted_product TEXT,
    rules_applied TEXT,
    last_processed_pass TEXT,
    last_processed_at TEXT,
    FOREIGN KEY (item_id) REFERENCES {PRODUCTS_TABLE}(item_id) ON DELETE CASCADE
)
"""

CREATE_INDEXES_SQL = [
    # Product tbale indexes
    f"CREATE INDEX IF NOT EXISTS idx_prodcuts_final_hts ON {PRODUCTS_TABLE}(final_hts)",
    f"CREATE INDEX IF NOT EXISTS idx_products_product_group ON {PRODUCTS_TABLE}(product_group)",
    # Processing table indexes
    f"CREATE INDEX IF NOT EXISTS idx_processing_confidence_level ON {PROCESSING_TABLE}(confidence_level)",
    f"CREATE INDEX IF NOT EXISTS idx_processing_last_pass ON {PROCESSING_TABLE}(last_processed_pass)",
]

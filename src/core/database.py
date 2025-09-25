"""
SQLite database connection and schema management for ctOS
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

# Schema definitions
PRODUCTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS products (
    item_id TEXT PRIMARY KEY,
    item_description TEXT NOT NULL,
    product_group TEXT,
    product_group_description TEXT,
    product_group_code TEXT,
    material_class TEXT,
    material_detail TEXT,
    manf_class TEXT,
    supplier_id TEXT,
    supplier_name TEXT,
    country_of_origin TEXT,
    import_type TEXT,
    port_of_delivery TEXT,
    final_hts TEXT,
    hts_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

PROCESSING_STATUS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS processing_status (
    item_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    confidence_level TEXT,
    last_processed TIMESTAMP,
    processing_attempts INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (item_id) REFERENCES products(item_id)
);
"""

PROCESSING_HISTORY_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS processing_history (
    history_id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL,
    processing_attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (item_id) REFERENCES products(item_id)
);
"""


class DatabaseManager:
    """Core database operations for ctOS Service 1"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_database_exists()

    def _ensure_database_exists(self):
        """Ensure database file and directory exist"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def create_connection(self) -> sqlite3.Connection:
        """Create database connection with proper settings"""
        try:
            conn = sqlite3.Connection(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign keys
            return conn
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = self.create_connection()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def create_schema(self) -> None:
        """Create all required tables and indexes"""
        with self.get_connection() as conn:
            try:
                # Create tables
                conn.execute(PRODUCTS_TABLE_DDL)
                conn.execute(PROCESSING_STATUS_TABLE_DDL)
                conn.execute(PROCESSING_HISTORY_TABLE_DDL)

                # Create indexes for performance
                indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_products_hts ON products(final_hts)",
                    "CREATE INDEX IF NOT EXISTS idx_products_material ON products(material_class, material_detail)",
                    "CREATE INDEX IF NOT EXISTS idx_processing_status ON processing_status(status, confidence_level)",
                    "CREATE INDEX IF NOT EXISTS idx_history_item ON processing_history(item_id, processed_at)",
                ]

                for index_sql in indexes:
                    conn.execute(index_sql)

                logger.info("Database schema created successfully")

            except Exception as e:
                logger.error(f"Failed to create schema: {e}")
                raise

    def drop_schema(self) -> None:
        """Drop all tables (for testing purposes)"""
        with self.get_connection() as conn:
            tables = ["processing_history", "processing_status", "products"]
            for table in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            logger.info("Database schema dropped")

    def get_table_info(self, table_name: str) -> Dict:
        """Get table schema information"""
        with self.get_connection() as conn:
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            return {"table_name": table_name, "columns": [dict(row) for row in columns]}

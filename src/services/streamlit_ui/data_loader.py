"""
Data loading and caching logic for Streamlit UI
"""

import streamlit as st
from typing import List, Optional
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.services.ingestion import ProductDatabase
from src.services.ingestion.models import ProductWithProcessing, DatabaseStatistics


@st.cache_resource
def get_database():
    """Get database instance with caching"""
    return ProductDatabase()


@st.cache_data(ttl=10)
def get_database_statistics():
    """Get database statistics (cached with 10s TTL)"""
    db = get_database()
    stats = db.get_database_statistics()

    # Conver to Pydantic model to dict for pickle serialization
    return {
        "total_products": stats.total_products,
        "processed_count": stats.processed_count,
        "unprocessed_count": stats.unprocessed_count,
        "confidence_distribution": stats.confidence_distribution,
        "average_confidence_score": stats.average_confidence_score,
        "unique_hts_codes": stats.unique_hts_codes,
        "pass_distribution": stats.pass_distribution,
        "timestamp": stats.timestamp,
    }


@st.cache_data(ttl=60)
def load_unprocessed_products(limit: int = 500) -> List[ProductWithProcessing]:
    """Load unprocessed products with caching"""
    db = get_database()
    products = db.get_unprocessed_products(limit=limit)
    return products


@st.cache_data(ttl=60)
def load_processed_producs(limit: int = 500) -> List[ProductWithProcessing]:
    """Load processed products (cached)"""
    db = get_database()
    # Get products weith any confidence level
    products = []
    for level in ["High", "Medium", "Low"]:
        level_products = db.get_products_by_confidence(level)
        products.extend(level_products)
        if len(products) >= limit:
            break
    return products[:limit]


@st.cache_data(ttl=60)
def load_products_by_confidence(
    confidence_level: List[str],
) -> List[ProductWithProcessing]:
    """Load products by confidence level with caching"""
    db = get_database()
    products = []
    for level in confidence_level:
        products.extend(db.get_products_by_confidence(level))
    return products


@st.cache_data(ttl=60)
def load_all_products(limit: int = 500) -> List[ProductWithProcessing]:
    """Load all products (mix of processed and unprocessed)"""
    db = get_database()

    # Get all products
    processed = load_processed_producs(limit // 2)
    unprocessed = load_unprocessed_products(limit // 2)

    return processed + unprocessed


def clear_cache():
    """Clear all cache"""
    st.cache_data.clear()

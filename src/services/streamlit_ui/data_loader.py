"""
Data loading and caching logic for Streamlit UI
"""

import streamlit as st
from typing import List, Optional, Dict, Any
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
def load_unprocessed_products(limit: int = 500) -> List[dict]:
    """Load unprocessed products with caching"""
    db = get_database()
    products = db.get_unprocessed_products(limit=limit)
    return [p.model_dump() for p in products]


@st.cache_data(ttl=60)
def load_processed_products(limit: int = 500) -> List[dict]:
    """Load processed products (cached)"""
    db = get_database()
    # Get products weith any confidence level
    products = []
    for level in ["High", "Medium", "Low"]:
        level_products = db.get_products_by_confidence(level)
        products.extend(level_products)
        if len(products) >= limit:
            break
    return [p.model_dump() for p in products[:limit]]


@st.cache_data(ttl=60)
def load_products_by_confidence(
    confidence_level: List[str],
) -> List[dict]:
    """Load products by confidence level with caching"""
    db = get_database()
    products = []
    for level in confidence_level:
        products.extend(db.get_products_by_confidence(level))
    return [p.model_dump() for p in products]


@st.cache_data(ttl=60)
def load_all_products(limit: int = 500) -> List[dict]:
    """Load all products (mix of processed and unprocessed)"""
    db = get_database()

    # Get all products
    processed = load_processed_products(limit // 2)
    unprocessed = load_unprocessed_products(limit // 2)

    return processed + unprocessed


@st.cache_data(ttl=30)
def search_products_cached(
    query: str, search_type: str = "auto", limit: Optional[int] = None
) -> List[dict]:
    """
    Cached wrapper around datbase search.

    Purpose: Search entire database with short cache TTL (search results can change)

    Args:
        query (str): Search query string
        search_type (str): "auto", "item_id", "hts_code", "description", "multi"
        limit (Optional[int]): Maximum results to return

    Returns:
        List[dict]: List of products as dictionaries (Streamlit serializable)

    Cache:
        30 sectonds TTL (short, results can changes are products are processed)
    """
    db = get_database()
    products = db.search_products(query, search_type, limit)
    return [p.model_dump() for p in products]


@st.cache_data(ttl=60)
def filter_products_cached(filters: Dict[str, Any], limit: int = 500) -> List[dict]:
    """
    Cached wrapper around database filtering.

    Purpose: Filter products with medium cache TTL

    Args:
        filters (Dict): Filter criteria dictionary
        limit (int): Maximum products to return

    Returns:
        List[dict]: List of filtered products as dictionaries

    Cached:
        60 seconds TTL
    """
    db = get_database()
    products = db.filter_products(filters, limit)
    return [p.model_dump() for p in products]


@st.cache_data(ttl=60)
def count_filtered_products_cached(filters: Dict[str, Any]) -> int:
    """
    Cached wrapper for counting filtered products.

    Args:
        filters (Dict): Filter criteria dictionary

    Returns:
        int: Total count of matching products

    Cache:
        60 seconds TTL
    """
    db = get_database()
    return db.count_filtered_products(filters)


@st.cache_data(ttl=3600)
def get_product_groups() -> List[str]:
    """
    Get unique product groups for dropdown population.

    Returns:
        List[str]: Sorted list of unique product groups

    Cache: 1 hour TTL (rarely changes)
    """
    db = get_database()
    return db.get_unique_product_groups()


@st.cache_data(ttl=3600)
def get_material_classes() -> List[str]:
    """
    Get unique material classes for dropwdown population.

    Returns:
        List[str]: Sorted list of unique material classes

    Cache: 1 hour TTL (rarely changes)
    """
    db = get_database()
    return db.get_unique_material_classes()


@st.cache_data(ttl=3600)
def get_hts_codes() -> List[str]:
    """
    Get unique HTS codes for dropdown population.

    Returns:
        List[str]: Sorted list of unique HTS codes

    Cache: 1 hour TTL (rarely changes)
    """
    db = get_database()
    return db.get_unique_hts_codes()


@st.cache_data(ttl=3600)
def get_hts_ranges() -> Dict[str, str]:
    f"""
    Get min/max HTS codes for validation

    Returns:
        Dict[str, str]: {"min": "...", "max": "..."}

    Cache: 1 hour TTL (rarely changes)
    """
    db = get_database()
    return db.get_hts_code_ranges()


def clear_cache():
    """Clear all cache"""
    st.cache_data.clear()

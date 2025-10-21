"""
Reusable UI Components
"""

import streamlit as st
from typing import Dict, Any


def display_section_header(title: str, icon: str = ""):
    """Display section header with icon"""
    st.markdown(f"## {title}")
    st.markdown("---")


def display_metric_row(metrics_dict: Dict[str, Any]):
    """Display metrics in a column"""
    cols = st.columns(len(metrics_dict))
    for col, (label, value) in zip(cols, metrics_dict.items()):
        col.metric(label, value)


def display_confidence_badge(confidence_level: str) -> str:
    """Display confidence level"""
    return confidence_level if confidence_level else "N/A"


def display_processing_status(status: str) -> str:
    """Display processing status"""
    return status


def format_product_for_display(product) -> Dict[str, Any]:
    """Format product for display in table"""
    return {
        "Item ID": product.item_id,
        "Original": (
            product.item_description[:50] + "..."
            if len(product.item_description) > 50
            else product.item_description
        ),
        "Enhanced": (
            product.enhanced_description[:50] + "..."
            if product.enhanced_description and len(product.enhanced_description) > 50
            else product.enhanced_description or "Not processed"
        ),
        "Confidence": product.confidence_level or "N/A",
        "Score": float(product.confidence_score) if product.confidence_score else 0.0,
        "Product": product.extracted_product or "N/A",
        "Pass": product.last_processed_pass or "N/A",
    }

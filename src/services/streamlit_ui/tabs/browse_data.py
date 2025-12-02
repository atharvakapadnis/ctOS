"""
Browse Data Tab - View Only Product Browsing with search and filtering
"""

import streamlit as st
from pathlib import Path
import sys
from typing import List, Optional, Dict, Any

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from ..data_loader import (
    load_all_products,
    load_unprocessed_products,
    load_processed_products,
    load_products_by_confidence,
    search_products_cached,
    filter_products_cached,
    count_filtered_products_cached,
)
from ..components import display_search_bar, display_advanced_filters
from ..config import MAX_PRODUCTS_DISPLAY
from src.services.ingestion.models import ProductWithProcessing


def display_browse_data_tab():
    """Display browse data tab - view-only product browsing with search and filters"""

    st.markdown("### Browse Products")
    st.caption("View-only product database browser")

    # Section A: Search Bar
    query, search_type, search_triggered = display_search_bar("browse_")
    st.markdown("---")

    # Section B: Basic Filter Controls
    st.markdown("#### Basic Filters")

    status_filter = st.radio(
        "Filter by Status",
        options=["All Products", "Unprocessed Only", "Processed Only"],
        horizontal=True,
        key="browse_status_filter",
    )

    confidence_filter = None
    if status_filter == "Processed Only":
        confidence_filter = st.multiselect(
            "Filter by Confidence Level",
            options=["High", "Medium", "Low"],
            default=["High", "Medium", "Low"],
            key="browse_confidence_filter",
        )

    # Section C: Advanced Filters

    show_confidence_in_advanced = status_filter == "Processed Only"
    advanced_filters = display_advanced_filters(
        key_prefix="browse_", show_confidence=show_confidence_in_advanced
    )

    st.markdown("---")

    # Section D: Load and Display products
    try:
        products = []
        display_message = ""

        # Priority Chain: Search > Advanced Filters > Basic Filters

        if (
            search_triggered
            and query
            and st.session_state.get("browse_search_active", False)
        ):
            # Search Mode
            products = search_products_cached(query, search_type, limit=500)
            display_message = f"Search Results: {len(products)} products found"

            if not products:
                st.info(
                    f"No products found matching '{query}'. Try different search terms or use advanced filters."
                )

        elif advanced_filters is not None and st.session_state.get(
            "browse_filters_active", False
        ):
            # Advanced Filter Mode
            total_count = count_filtered_products_cached(advanced_filters)
            products = filter_products_cached(
                advanced_filters, limit=MAX_PRODUCTS_DISPLAY
            )
            display_message = f"Filtered: {len(products)} of {total_count} products (showing first {MAX_PRODUCTS_DISPLAY})"

            if not products:
                st.info(
                    "No products found matching your filter criteria. Try adjusting the filters"
                )

        else:
            # Basic filter mode
            if status_filter == "All Products":
                products = load_all_products(limit=MAX_PRODUCTS_DISPLAY)
                display_message = (
                    f"Showing {len(products)} products (max {MAX_PRODUCTS_DISPLAY})"
                )

            elif status_filter == "Unprocessed Only":
                products = load_unprocessed_products(limit=MAX_PRODUCTS_DISPLAY)
                display_message = f"Showing {len(products)} unprocessed products (max {MAX_PRODUCTS_DISPLAY})"

            elif status_filter == "Processed Only":
                if confidence_filter:
                    products = load_products_by_confidence(confidence_filter)
                    products = products[:MAX_PRODUCTS_DISPLAY]
                    display_message = f"Showing {len(products)} processed products (max {MAX_PRODUCTS_DISPLAY})"
                else:
                    st.warning("Please select at least one confidence level")
                    products = []
                    display_message = "No confidence level selected"

        # Section E: Product Count Display
        if display_message:
            st.markdown(f"**{display_message}**")

        if len(products) >= MAX_PRODUCTS_DISPLAY:
            st.info(
                f"Displaying first {MAX_PRODUCTS_DISPLAY} products. Use search or filters to narrow results"
            )

        # Section F: Product Table
        if products:
            display_data = []

            for p in products:
                # Convert to dict if needed
                if not isinstance(p, dict):
                    # Try model dump
                    if hasattr(p, "model_dump"):
                        p = p.model_dump()
                    elif hasattr(p, "dict"):
                        p = p.dict()
                    else:
                        p = dict(p) if hasattr(p, "__iter__") else p.__dict__

                enhanced_desc = p.get("enhanced_description")
                item_desc = p.get("item_description", "")

                display_data.append(
                    {
                        "Item ID": p.get("item_id", ""),
                        "Original Description": (
                            item_desc[:50] + "..."
                            if item_desc and len(item_desc) > 50
                            else item_desc
                        ),
                        "Enhanced Description": (
                            (
                                enhanced_desc[:50] + "..."
                                if enhanced_desc and len(enhanced_desc) > 50
                                else enhanced_desc
                            )
                            if enhanced_desc
                            else "Not Processed"
                        ),
                        "Confidence Level": p.get("confidence_level") or "N/A",
                        "Confidence Score": (
                            f"{float(p.get('confidence_score')):.2f}"
                            if p.get("confidence_score")
                            else "N/A"
                        ),
                        "Extracted Product": p.get("extracted_product") or "N/A",
                        "Pass Number": p.get("last_processed_pass") or "N/A",
                    }
                )
            # if products:
            #     display_data = []

            #     for p in products:
            #         # Ensure we're working with dict
            #         if not isinstance(p, dict):
            #             p = p if hasattr(p, "__dict__") else p.model_dump()

            #         enhanced_desc = p.get("enhanced_description")
            #         item_desc = p.get("item_description", "")

            #         display_data.append(
            #             {
            #                 "Item ID": p.get("item_id", ""),
            #                 "Original Description": (
            #                     item_desc[:50] + "..."
            #                     if item_desc and len(item_desc) > 50
            #                     else item_desc
            #                 ),
            #                 "Enhanced Description": (
            #                     (
            #                         enhanced_desc[:50] + "..."
            #                         if enhanced_desc and len(enhanced_desc) > 50
            #                         else enhanced_desc
            #                     )
            #                     if enhanced_desc
            #                     else "Not Processed"
            #                 ),
            #                 "Confidence Level": p.get("confidence_level") or "N/A",
            #                 "Confidence Score": (
            #                     f"{float(p.get('confidence_score')):.2f}"
            #                     if p.get("confidence_score")
            #                     else "N/A"
            #                 ),
            #                 "Extracted Product": p.get("extracted_product") or "N/A",
            #                 "Pass Number": p.get("last_processed_pass") or "N/A",
            #             }
            #         )

            st.dataframe(
                display_data,
                width="stretch",
                height=400,
                hide_index=True,
            )
        else:
            if not display_message or "No" not in display_message:
                st.warning("No products found matching the selected criteria")

        st.markdown("---")

        # Section G: Future Placeholder
        st.info("Export features coming soon")

    except Exception as e:
        st.error(f"Error loading products: {str(e)}")
        if st.button("Retry", key="browse_retry"):
            st.rerun()

"""
Browse Data Tab - View-only product browsing
"""

import streamlit as st
from pathlib import Path
import sys
from typing import List

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from ..data_loader import (
    load_all_products,
    load_unprocessed_products,
    load_processed_products,
    load_products_by_confidence,
)
from ..config import MAX_PRODUCTS_DISPLAY
from src.services.ingestion.models import ProductWithProcessing


def display_browse_data_tab():
    """Display browse data tab - view-only product browsing"""

    st.markdown("### Browse Products")
    st.caption("View-only product database browser")

    # Section A: Filter Controls
    st.markdown("#### Filters")

    # Status filter
    status_filter = st.radio(
        "Filter by Status",
        options=["All Products", "Unprocessed Only", "Processed Only"],
        horizontal=True,
    )

    # Confidence filter (only show when Processed selected)
    confidence_filter = None
    if status_filter == "Processed Only":
        confidence_filter = st.multiselect(
            "Filter by Confidence Level",
            options=["High", "Medium", "Low"],
            default=["High", "Medium", "Low"],
        )

    st.markdown("---")

    # Section B & C: Load and display products
    try:
        products = []

        # Load based on filters
        if status_filter == "All Products":
            products = load_all_products(limit=MAX_PRODUCTS_DISPLAY)

        elif status_filter == "Unprocessed Only":
            products = load_unprocessed_products(limit=MAX_PRODUCTS_DISPLAY)

        elif status_filter == "Processed Only":
            if confidence_filter:
                products = load_products_by_confidence(confidence_filter)
                products = products[:MAX_PRODUCTS_DISPLAY]
            else:
                st.warning("Please select at least one confidence level")
                products = []

        # Section B: Product Count
        total_count = len(products)
        st.markdown(
            f"**Showing {total_count} products** (max display: {MAX_PRODUCTS_DISPLAY})"
        )

        if total_count >= MAX_PRODUCTS_DISPLAY:
            st.info(
                f"Displaying first {MAX_PRODUCTS_DISPLAY} products. Use filters to narrow results."
            )

        # Section C: Product Table
        if products:
            display_data = []

            for p in products:
                enhanced_desc = p.get("enhanced_description")
                display_data.append(
                    {
                        "Item ID": p["item_id"],
                        "Original Description": (
                            p["item_description"][:50] + "..."
                            if len(p["item_description"]) > 50
                            else p["item_description"]
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
            #         display_data.append(
            #             {
            #                 "Item ID": p.item_id,
            #                 "Original Description": (
            #                     p.item_description[:50] + "..."
            #                     if len(p.item_description) > 50
            #                     else p.item_description
            #                 ),
            #                 "Enhanced Description": (
            #                     (
            #                         p.enhanced_description[:50] + "..."
            #                         if p.enhanced_description
            #                         and len(p.enhanced_description) > 50
            #                         else p.enhanced_description
            #                     )
            #                     if p.enhanced_description
            #                     else "Not Processed"
            #                 ),
            #                 "Confidence Level": p.confidence_level or "N/A",
            #                 "Confidence Score": (
            #                     f"{float(p.confidence_score):.2f}"
            #                     if p.confidence_score
            #                     else "N/A"
            #                 ),
            #                 "Extracted Product": p.extracted_product or "N/A",
            #                 "Pass Number": p.last_processed_pass or "N/A",
            #             }
            #         )

            st.dataframe(
                display_data,
                width="stretch",
                height=400,
                hide_index=True,
            )
        else:
            st.warning("No products found matching the selected filters")

        st.markdown("---")

        # Section D: Future Placeholder
        st.info("Export and advanced filtering features coming soon")

    except Exception as e:
        st.error(f"Error loading products: {str(e)}")
        if st.button("Retry"):
            st.rerun()

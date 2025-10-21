"""
Main Streamlit Application Logic
"""

import streamlit as st
import sys
from pathlib import Path
from typing import List

# Add src to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .components import (
    display_section_header,
    display_metric_row,
    display_confidence_badge,
    display_processing_status,
    format_product_for_display,
)
from .data_loader import (
    get_database_statistics,
    load_unprocessed_products,
    load_processed_producs,
    load_products_by_confidence,
    load_all_products,
    clear_cache,
)
from .config import (
    PAGE_TITLE,
    SECTION_TITLES,
    DEFAULT_BATCH_SIZE,
    MIN_BATCH_SIZE,
    MAX_BATCH_SIZE,
    MAX_PRODUCTS_DISPLAY,
    MAX_SELECTION_ITEMS,
)

from src.services.llm_enhancement import process_batch


def main():
    """Main Streamlit Application"""

    # App title
    st.title(PAGE_TITLE)
    st.markdown("Basic functionality only for testing services 1,2,4")

    # Initialize session state
    initialize_session_state()

    # Section 1: Statistics
    display_statistics_section()

    # Section 2: Batch Processing Control
    display_batch_control_section()

    # Section 3: Product Data View
    display_product_view_section()

    # Section 4: Reprocessing
    display_reprocessing_section()


def initialize_session_state():
    """Initialize session state"""
    if "selected_items" not in st.session_state:
        st.session_state.selected_items = set()

    if "processing_status" not in st.session_state:
        st.session_state.processing_status = "Idle"

    if "last_batch_result" not in st.session_state:
        st.session_state.last_batch_result = None


def display_statistics_section():
    """Display database statistics"""
    display_section_header("Database Statistics")

    try:
        stats = get_database_statistics()

        # Display main metrics in columns
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Products", stats.total_products)
        with col2:
            st.metric("Processed", stats.processed_count)
        with col3:
            st.metric("Unprocessed", stats.unprocessed_count)

        # Confidence distribution
        st.markdown("### Confidence Distribution")

        dist_col1, dist_col2, dist_col3 = st.columns(3)

        # Extract values frmo confidence_distribution dictionary
        confidence_dist = stats.confidence_distribution
        high_count = confidence_dist.get("High", 0)
        medium_count = confidence_dist.get("Medium", 0)
        low_count = confidence_dist.get("Low", 0)

        total_processed = stats.processed_count if stats.processed_count > 0 else 1

        with dist_col1:
            high_pct = (
                (high_count / total_processed * 100) if stats.processed_count > 0 else 0
            )
            st.metric("High", high_count, f"{high_pct:.1f}%")
        with dist_col2:
            medium_pct = (
                (medium_count / total_processed * 100)
                if stats.processed_count > 0
                else 0
            )
            st.metric("Medium", medium_count, f"{medium_pct:.1f}%")
        with dist_col3:
            low_pct = (
                (low_count / total_processed * 100) if stats.processed_count > 0 else 0
            )
            st.metric("Low", low_count, f"{low_pct:.1f}%")

        # Unique HTS codes
        st.metric("Unique HTS Codes", stats.unique_hts_codes)

        # Refresh button
        if st.button("Refresh Statistics"):
            clear_cache()
            st.rerun()

    except Exception as e:
        st.error(f"Error loading statistics: {str(e)}")
        st.exception(e)


def display_batch_control_section():
    """Display batch processing control"""
    display_section_header("Batch Processing Control (Pass 1)")

    # Batch size control
    batch_size = st.slider(
        "Batch Size",
        min_value=MIN_BATCH_SIZE,
        max_value=MAX_BATCH_SIZE,
        value=DEFAULT_BATCH_SIZE,
        step=10,
        help="Number of products to process in one batch",
    )

    # Start Stop Buttons
    col1, col2 = st.columns(2)

    with col1:
        start_batch = st.button("Start Pass 1 Batch", type="primary")

    with col2:
        stop_btn = st.button("Stop Processing", type="secondary")
        if stop_btn:
            st.warning("Stop functionality coming in future version")

    # Process batch if button clicked
    if start_batch:
        st.session_state.processing_status = "Processing"

        progress_bar = st.progress(0, text="Starting batch processing...")
        status_container = st.empty()
        log_container = st.container()

        try:
            with log_container:
                st.info(f"Processing batch of {batch_size} products (Pass 1)...")

                # Call service 3
                result = process_batch(
                    batch_size=batch_size,
                    pass_number=1,
                    selected_item_ids=None,
                )

                # Update Progress
                progress_bar.progress(1.0, text="Batch processing complete!")

                # Display results
                st.session_state.processing_status = "Complete"
                st.session_state.last_batch_result = result

                status_container.success(
                    f"Batch complete: {result.successful} successful, {result.failed} out of {result.total_processed} processed"
                )

                # Display detailed results
                with log_container:
                    st.json(
                        {
                            "total_processed": result.total_processed,
                            "successful": result.successful,
                            "failed": result.failed,
                            "avg_time_per_product": f"{result.avg_time_per_product:.2f}s",
                            "processing_time": f"{result.processing_time:.2f}s",
                        }
                    )

                # Clear cache to show updated statistics
                clear_cache()

        except Exception as e:
            st.session_state.processing_status = "Error"
            status_container.error(f"Batch processing failed: {str(e)}")
            with log_container:
                st.exception(e)

    # Display current stats
    st.markdown("### Status")
    status_text = display_processing_status(st.session_state.processing_status)
    st.markdown(f"**Current Status:** {status_text}")

    # Display last result if availabel
    if st.session_state.last_batch_result:
        st.markdown("### Last Batch Result")
        result = st.session_state.last_batch_result

        metric_col1, metric_col2, metric_col3 = st.columns(3)

        with metric_col1:
            st.metric("Total Processed", result.total_processed)
        with metric_col2:
            st.metric("Successful", result.successful)
        with metric_col3:
            st.metric("Failed", result.failed)


def display_product_view_section():
    """Display product view"""
    display_section_header("Product View")

    # Filter Controls
    status_filter = st.radio(
        "Filter by Status",
        ["All Products", "Unprocessed Only", "Processed Only"],
        horizontal=True,
    )

    # Confidence Filter (only show if "Processed Only" selected)
    confidence_filters = None
    if status_filter == "Processed Only":
        confidence_filters = st.multiselect(
            "Filter by Confidence",
            ["High", "Medium", "Low"],
            default=["High", "Medium", "Low"],
        )

    try:
        # Load products based on filter
        if status_filter == "All Products":
            products = load_all_products(limit=MAX_PRODUCTS_DISPLAY)
        elif status_filter == "Unprocessed Only":
            products = load_unprocessed_products(limit=MAX_PRODUCTS_DISPLAY)
        else:  # Processed Only
            if confidence_filters:
                products = load_products_by_confidence(confidence_filters)[
                    :MAX_PRODUCTS_DISPLAY
                ]
            else:
                products = []

        st.info(
            f"Showing {len(products)} of products (max display: {MAX_PRODUCTS_DISPLAY})"
        )

        if products:
            # Format for display
            display_data = []
            for p in products:
                display_data.append(
                    {
                        "Item ID": p.item_id,
                        "Original Description": (
                            p.item_description[:50] + "..."
                            if len(p.item_description) > 50
                            else p.item_description
                        ),
                        "Enhanced Description": (
                            (
                                p.enhanced_description[:50] + "..."
                                if p.enhanced_description
                                and len(p.enhanced_description) > 50
                                else p.enhanced_description
                            )
                            if p.enhanced_description
                            else "Not Processed"
                        ),
                        "Confidence": (
                            display_confidence_badge(p.confidence_level)
                            if p.confidence_level
                            else "N/A"
                        ),
                        "Score": (
                            float(p.confidence_score) if p.confidence_score else 0.0
                        ),
                        "Product": p.extracted_product or "N/A",
                        "Pass": p.last_processed_pass or "N/A",
                    }
                )

            # Display as a dataframe
            st.dataframe(display_data, width="stretch", height=400)

        else:
            st.warning("No products found matchin the selected filters.")

    except Exception as e:
        st.error(f"Error loading products: {str(e)}")
        st.exception(e)


def display_reprocessing_section():
    """Display Pass 2+ reprocessing"""
    display_section_header("Pass 2+ Reprocessing")

    # Step 1: Filter products for reprocessing
    st.markdown("### Step 1: Filter products for reprocessing")

    reprocess_confidence = st.multiselect(
        "Filter by Confidence",
        ["Low", "Medium", "High"],
        default=["Low", "Medium"],
        help="Select which confidence levels to show for reprocessing",
    )

    try:
        # Load eligible products
        eligible_products = (
            load_products_by_confidence(reprocess_confidence)
            if reprocess_confidence
            else []
        )

        st.info(f"Showing {len(eligible_products)} products eligible for reprocessing")

        # Step 2: Select products to reprocess
        st.markdown("### Step 2: Select products to reprocess")

        if eligible_products:
            # Select All / Deselect All
            col1, col2 = st.columns(2)
            select_all = col1.button("Select All")
            deselect_all = col2.button("Deselect All")

            if select_all:
                st.session_state.selected_items = set(
                    p.item_id for p in eligible_products[:MAX_SELECTION_ITEMS]
                )
                st.rerun()

            if deselect_all:
                st.session_state.selected_items = set()
                st.rerun()

            # Display checkboxes (limit to MAX_SELECTION_ITEMS for performance)
            display_products = eligible_products[:MAX_SELECTION_ITEMS]

            if len(eligible_products) > MAX_SELECTION_ITEMS:
                st.warning(
                    f"Showing first {MAX_SELECTION_ITEMS} products for selection (performance limit)"
                )

            for product in display_products:
                is_selected = st.checkbox(
                    f"{product.item_id} - {product.item_description[:40]}... - confidence: {product.confidence_score} ({product.confidence_level})",
                    value=product.item_id in st.session_state.selected_items,
                    key=f"select_{product.item_id}",
                )

                if is_selected:
                    st.session_state.selected_items.add(product.item_id)
                else:
                    st.session_state.selected_items.discard(product.item_id)

            st.markdown(
                f"**Selected: {len(st.session_state.selected_items)} products**"
            )

            # Step 3: Reprocess selected items
            st.markdown("### Step 3: Reprocess selected items")

            pass_number = st.number_input(
                "Pass Number",
                min_value=2,
                max_value=10,
                value=2,
                help="Pass number for this reprocessing run",
            )

            reprocess_batch_size = st.slider(
                "Batch Size",
                min_value=MIN_BATCH_SIZE,
                max_value=MAX_BATCH_SIZE,
                value=50,
            )

            st.info("Note: Rules will be applied when Service 4 is implemented")

            if st.button("Reprocess Selected Items", type="primary"):
                if not st.session_state.selected_items:
                    st.error("No items selected for reprocessing")
                else:
                    progress_bar = st.progress(0, text="Starting reprocessing...")
                    status_container = st.empty()

                    try:
                        status_container.info(
                            f"Reprocessing {len(st.session_state.selected_items)} items (Pass {pass_number})..."
                        )

                        # Call Service 3 with selected items
                        result = process_batch(
                            batch_size=reprocess_batch_size,
                            pass_number=pass_number,
                            selected_item_ids=list(st.session_state.selected_items),
                        )

                        progress_bar.progress(1.0, text="Reprocessing complete!")

                        status_container.success(
                            f"Reprocessing complete: {result.successful} successful, {result.failed} failed"
                        )

                        st.json(
                            {
                                "total_processed": result.total_processed,
                                "successful": result.successful,
                                "failed": result.failed,
                                "avg_time_per_product": f"{result.avg_time_per_product:.2f}s",
                            }
                        )

                        # Clear selections after processing
                        st.session_state.selected_items = set()

                        # Clear cache to show updated data
                        clear_cache()

                        st.rerun()

                    except Exception as e:
                        status_container.error(f"Reprocessing failed: {str(e)}")
                        st.exception(e)
        else:
            st.warning(
                "No products available for reprocessing with selected confidence levels."
            )

    except Exception as e:
        st.error(f"Error loading reprocessing candidates: {str(e)}")
        st.exception(e)

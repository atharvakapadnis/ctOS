"""
Processing Tab - Pass 1 and Pass 2+ Processing Operations
All processing operation happen here
"""

import streamlit as st
from pathlib import Path
import sys
from typing import List, Set

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from ..data_loader import (
    load_unprocessed_products,
    load_products_by_confidence,
    clear_cache,
    search_products_cached,
    filter_products_cached,
    count_filtered_products_cached,
)
from ..components import display_search_bar, display_advanced_filters
from ..config import (
    DEFAULT_BATCH_SIZE,
    MIN_BATCH_SIZE,
    MAX_BATCH_SIZE,
    MAX_SELECTION_ITEMS,
)
from src.services.llm_enhancement import process_batch
from src.services.rules import RuleManager


def display_processing_tab():
    """Display processing tab with Pass 1 and Pass 2+ processing oeprations"""

    st.markdown("### Processing Operations")
    st.caption("Process products through LLM Enhancement")

    # Initialize session state for processing
    initialize_processing_session_state()

    # Section A: Pass 1 Processing
    display_pass_1_section()

    st.markdown("---")

    # Section B: Pass 2+ Processing
    display_pass2_section()


def initialize_processing_session_state():
    """Initialize session state variables for processing tab"""

    if "pass1_mode" not in st.session_state:
        st.session_state.pass1_mode = "Process all unprocessed products"

    if "pass1_selected_products" not in st.session_state:
        st.session_state.pass1_selected_products = set()

    if "pass2_selected_products" not in st.session_state:
        st.session_state.pass2_selected_products = set()

    if "selected_rule_ids" not in st.session_state:
        st.session_state.selected_rule_ids = set()

    if "pass1_last_result" not in st.session_state:
        st.session_state.pass1_last_result = None

    if "pass2_last_result" not in st.session_state:
        st.session_state.pass2_last_result = None

    if "processing_status" not in st.session_state:
        st.session_state.processing_status = "Idle"


def display_pass_1_section():
    """Display Pass 1 processing section"""

    st.markdown("## Pass 1: Initial Processing")
    st.caption("Process unprocessed products for the first time")

    # Subesection 1: Product Selection Mode
    st.markdown("#### Step 1: Select Processing Mode")

    mode = st.radio(
        "Choose how to select products:",
        options=[
            "Process all unprocessed products",
            "Select specific products to process",
        ],
        key="pass1_mode_radio",
        horizontal=False,
    )
    st.session_state.pass1_mode = mode

    # Variables to track
    unprocessed = []
    batch_size = DEFAULT_BATCH_SIZE

    # Mode specific UI
    if mode == "Process all unprocessed products":
        # Mode A: Process all unprocessed products
        st.info("Will process the next batch of unprocessed products from the database")

        st.markdown("---")
        st.markdown("#### Step 2: Configure Batch")

        # Show unprocessed count
        try:
            from ..data_loader import get_database_statistics

            stats = get_database_statistics()
            unprocessed_count = stats["unprocessed_count"]
            st.metric("Unprocessed Products Available", unprocessed_count)
        except:
            pass

        # Number input instead of slider
        batch_size = st.number_input(
            "Number of products to process",
            min_value=MIN_BATCH_SIZE,
            max_value=MAX_BATCH_SIZE,
            value=DEFAULT_BATCH_SIZE,
            step=10,
            help="Enter the number of products to process in this batch",
            key="pass1_batch_size_mode_a",
        )
    elif mode == "Selecte specific products to process":
        # Mode B: Select specific products with search and filters
        st.markdown("---")
        st.markdown("#### Step 2: Find Products to Process")

        # Search Bar
        query, search_type, search_triggered = display_search_bar(key_prefix="pass1_")
        st.markdown("---")

        # Advanced Filters (No confidence filter for unprocessed)
        advanced_filters = display_advanced_filters(
            key_prefix="pass1_",
            show_confidence=False,
        )

        st.markdown("---")
        st.markdown("#### Step 3: Select Products")

        try:
            products_to_display = []
            display_message = ""

            # Priority: Search> Filters> Default
            if (
                search_triggered
                and query
                and st.session_state.get("pass1_search_active", False)
            ):
                # Search Mode
                products_to_display = search_products_cached(
                    query, search_type, limit=500
                )

                # Filter out already processed products
                products_to_display = [
                    p for p in products_to_display if not p.get("enhanced_description")
                ]

                display_message = f"Search Results: {len(products_to_display)} unprocessed products found"

                if not products_to_display:
                    st.info(
                        f"No unprocessed products found matching '{query}'. Try different search terms."
                    )

            elif advanced_filters is not None and st.session_state.get(
                "pass1_filters_active", False
            ):
                # Filter Mode
                # Ensure status is unprocessed
                advanced_filters["status"] = "unprocessed"

                total_count = count_filtered_products_cached(advanced_filters)
                products_to_display = filter_products_cached(
                    advanced_filters, limit=500
                )

                display_message = f"Filtered: {len(products_to_display)} of {total_count} unprocessed products (showing first 500)"

                if not products_to_display:
                    st.info(
                        f"No unprocessed products found matching your filters. Try adjusting the criteria."
                    )

            else:
                # DEFAULT: Load unprocessed
                products_to_display = load_unprocessed_products(limit=500)
                display_message = f"Showing {len(products_to_display)} unprocessed products (showing first 500)"

            # Display count
            if display_message:
                st.info(display_message)

            if not products_to_display:
                st.warning("No unprocessed products available")
            else:
                # Select All/ Deselect All
                col1, col2, col3 = st.columns([1, 1, 4])

                with col1:
                    if st.button("Select All", key="pass1_select_all"):
                        st.session_state.pass1_selected_products = {
                            p["item_id"] for p in products_to_display
                        }
                        st.rerun()

                with col2:
                    if st.button("Deselect All", key="pass1_deselect_all"):
                        st.session_state.pass1_selected_products = set()
                        st.rerun()

                # Build display data for data_editor
                display_data = []
                for p in products_to_display:
                    display_data.append(
                        {
                            "Select": p["item_id"]
                            in st.session_state.pass1_selected_products,
                            "Item ID": p["item_id"],
                            "Description": (
                                p["item_description"][:80] + "..."
                                if len(p["item_description"]) > 80
                                else p["item_description"]
                            ),
                            "HTS Code": p.get("final_hts", "N/A"),
                            "Product Group": p.get("product_group", "N/A"),
                            "Material": p.get("material_class", "N/A"),
                        }
                    )

                # Use data_editor for selection
                edited_df = st.data_editor(
                    display_data,
                    width="stretch",
                    height=400,
                    hide_index=True,
                    disabled=[
                        "Item ID",
                        "Description",
                        "HTS Code",
                        "Product Group",
                        "Material",
                    ],
                    column_config={
                        "Select": st.column_config.CheckboxColumn(
                            "Select",
                            help="Select products to process",
                            default=False,
                        ),
                        "Description": st.column_config.TextColumn(
                            "Description",
                            width="large",
                        ),
                    },
                    key="pass1_product_selector",
                )

                # Update Session state based on selections
                st.session_state.pass1_selected_products = {
                    row["Item ID"] for row in edited_df if row["Select"]
                }

                # Batch size is determined by selection count
                batch_size = len(st.session_state.pass1_selected_products)

        except Exception as e:
            st.error(f"Error loading products: {str(e)}")

    st.markdown("---")

    # Subsection 3: Execute
    st.markdown("#### Step 3: Start Processing")

    # Validation
    can_process = True
    validation_message = ""

    if st.session_state.pass1_mode == "Select specific products to process":
        if len(st.session_state.pass1_selected_products) == 0:
            can_process = False
            validation_message = "Please select at least one product"
            st.warning(validation_message)
        else:
            st.success(
                f"Ready to process {len(st.session_state.pass1_selected_products)} selected products"
            )
    else:
        st.info(f"Ready to process next {batch_size} unprocessed products")

    if st.button(
        "Start Pass 1 Processing",
        type="primary",
        disabled=not can_process,
        key="pass1_start",
    ):
        # Determine selected_item_ids parameter
        selected_item_ids = None

        if st.session_state.pass1_mode == "Select specific products to process":
            selected_item_ids = list(st.session_state.pass1_selected_products)
            batch_size = len(selected_item_ids)  # Use actual selection count

        # Create containers for status and logs
        status_container = st.empty()
        progress_container = st.empty()
        log_container = st.empty()

        try:
            st.session_state.processing_status = "Processing"
            status_container.info("Processing batch...")

            # Show progress bar
            with progress_container:
                progress_bar = st.progress(0)
                progress_bar.progress(50)

            # Cal batch processor
            result = process_batch(
                batch_size=batch_size,
                pass_number=1,
                selected_item_ids=selected_item_ids,
                selected_rule_ids=None,
            )

            # Update progress
            progress_bar.progress(100)

            # Store result
            st.session_state.pass1_last_result = result
            st.session_state.processing_status = "Complete"

            # Display success
            status_container.success(
                f"Batch complete: {result.successful} succesful, {result.failed} failed "
                f"out of {result.total_processed} processed"
            )

            # Show detailed results
            with log_container:
                st.json(
                    {
                        "total_processed": result.total_processed,
                        "successful": result.successful,
                        "failed": result.failed,
                        "success_rate": f"{result.success_rate:.1%}",
                        "avg_time_per_product": f"{result.avg_time_per_product:.2f}s",
                        "processing_time": f"{result.processing_time:.2f}s",
                        "confidence_distribution": result.confidence_distribution,
                    }
                )

            # Clear cache to show updated statistics
            clear_cache()

            # Clear selections for mode B
            if st.session_state.pass1_mode == "Select specific products to process":
                st.session_state.pass1_selected_products = set()

        except Exception as e:
            st.session_state.processing_status = "Error"
            status_container.error(f"Batch processing failed: {str(e)}")
            with log_container:
                st.exception(e)

    # Subsection 4: Results Display
    if st.session_state.pass1_last_result:
        st.markdown("---")
        st.markdown("#### Last Batch Result")

        result = st.session_state.pass1_last_result

        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

        with metric_col1:
            st.metric("Total Processed", result.total_processed)
        with metric_col2:
            st.metric("Successful", result.successful)
        with metric_col3:
            st.metric("Failed", result.failed)
        with metric_col4:
            st.metric("Success Rate", f"{result.success_rate:.1%}")


def display_pass2_section():
    """Display Pass 2+ reprocessing section with search and filters"""

    st.markdown(f"## Pass 2+: Reprocessing with Rules")
    st.caption("Reprocess products with additional rule guidance")

    # Step 1: Basic Filter - Confidence Levels
    st.markdown("#### Step 1: Filter by Confidence Level")

    confidence_filter = st.multiselect(
        "Select Confidence Levels to Reprocess",
        options=["Low", "Medium", "High"],
        default=["Low", "Medium"],
        key="pass2_confidence_filter",
    )

    # Step 1.5: Search and Advanced Filters
    st.markdown("---")
    st.markdown("#### Refine Search (Optional)")

    # Search Bar
    query, search_type, search_triggered = display_search_bar(key_prefix="pass2_")

    # Advanced Filters
    advanced_filters = display_advanced_filters(
        key_prefix="pass2_",
        show_confidence=True,
    )

    st.markdown("---")

    # Step 2: Load and Select Products
    st.markdown("#### Step 2: Select Products to Reprocess")

    eligible_products = []
    display_message = ""

    try:
        # Priority: Search > Filters > Basic Confidence Filter
        if (
            search_triggered
            and query
            and st.session_state.get("pass2_search_active", False)
        ):
            # Search Mode
            eligible_products = search_products_cached(query, search_type, limit=500)

            # Filter by confidence if specified in basic filter
            if confidence_filter:
                eligible_products = [
                    p
                    for p in eligible_products
                    if p.get("confidence_level") in confidence_filter
                ]

                display_message = f"Search Results: {len(eligible_products)} products found (max 100 for selection)"

                if not eligible_products:
                    st.info(
                        f"No products found matching '{query}' with selected confidence levels"
                    )

        elif advanced_filters is not None and st.session_state.get(
            "pass2_filters_active", False
        ):
            # Advanced Filter Mode
            # Merge basic confidence filter into advanced filters
            if confidence_filter:
                advanced_filters["confidence_levels"] = confidence_filter

            total_count = count_filtered_products_cached(advanced_filters)
            eligible_products = filter_products_cached(advanced_filters, limit=100)

            display_message = f"Filtered: {len(eligible_products)} of {total_count} products (showing first 100)"

            if not eligible_products:
                st.info("No products found matching your filter criteria.")

        elif confidence_filter:
            # BASIC FILTER ONLY (existing behavior)
            eligible_products = load_products_by_confidence(confidence_filter)
            eligible_products = eligible_products[:100]

            display_message = (
                f"Found {len(eligible_products)} products (showing max 100)"
            )

        else:
            st.warning(
                "Please select at least one confidence level or use search/filters"
            )

        # Display count
        if display_message:
            st.info(display_message)

    except Exception as e:
        st.error(f"Error loading products: {str(e)}")

    # Product selection table
    if eligible_products:
        # Select All/ Deselect All buttons
        col1, col2, col3 = st.columns([1, 1, 4])

        with col1:
            if st.button("Select All", key="pass2_select_all"):
                st.session_state.pass2_selected_products = {
                    p["item_id"] for p in eligible_products
                }
                st.rerun()

        with col2:
            if st.button("Deselect All", key="pass2_deselect_all"):
                st.session_state.pass2_selected_products = set()
                st.rerun()

        # Build display data
        display_data = []
        for p in eligible_products:
            display_data.append(
                {
                    "Select": p["item_id"] in st.session_state.pass2_selected_products,
                    "Item ID": p["item_id"],
                    "Original Description": (
                        p["item_description"][:50] + "..."
                        if len(p["item_description"]) > 50
                        else p["item_description"]
                    ),
                    "Enhanced Description": (
                        p.get("enhanced_description", "")[:50] + "..."
                        if p.get("enhanced_description")
                        and len(p.get("enhanced_description")) > 50
                        else p.get("enhanced_description", "")
                    ),
                    "Customer": p.get("extracted_customer_name", "N/A"),
                    "Dimensions": p.get("extracted_dimensions", "N/A"),
                    "Product": p.get("extracted_product", "N/A"),
                    "Rules Applied": p.get("rules_applied", "N/A"),
                    "Confidence": p.get("confidence_level", "N/A"),
                    "Score": (
                        f"{float(p.get('confidence_score', 0)):.2f}"
                        if p.get("confidence_score")
                        else "N/A"
                    ),
                    "Pass": p.get("last_processed_pass", "N/A"),
                }
            )

        # Use data_editor for selection
        edited_df = st.data_editor(
            display_data,
            width="stretch",
            height=400,
            hide_index=True,
            disabled=[
                "Item ID",
                "Original Description",
                "Enhanced Description",
                "Customer",
                "Dimensions",
                "Product",
                "Rules Applied",
                "Confidence",
                "Score",
                "Pass",
            ],
            column_config={
                "Select": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select products to reprocess",
                    default=False,
                ),
                "Original Description": st.column_config.TextColumn(
                    "Original Description",
                    width="medium",
                ),
                "Enhanced Description": st.column_config.TextColumn(
                    "Enhanced Description",
                    width="medium",
                ),
            },
            key="pass2_product_selector",
        )

        # Update session state based on selections
        st.session_state.pass2_selected_products = {
            row["Item ID"] for row in edited_df if row["Select"]
        }
    else:
        st.info(
            "No products available. Please adjust filters or search criteria above."
        )

    st.markdown("---")

    # Step 2.5: Rule Selection (exisiting logic continues below...)
    st.markdown("#### Step 2.5: Select Rules (Optional)")

    try:
        rule_manager = RuleManager()
        all_rules = rule_manager.get_all_rules()

        if not all_rules:
            st.info("No rules available. Create rules in the Rules tab.")
        else:
            st.info(f"Found {len(all_rules)} active rules.")

            # Select All/ Deselect All for rules
            rule_col1, rule_col2, rule_col3 = st.columns([1, 1, 4])

            with rule_col1:
                if st.button("Select All Rules", key="pass2_select_all_rules"):
                    st.session_state.selected_rule_ids = {
                        r["rule_id"] for r in all_rules
                    }
                    st.rerun()

            with rule_col2:
                if st.button("Deselect All Rules", key="pass2_deselect_all_rules"):
                    st.session_state.selected_rule_ids = set()
                    st.rerun()

            # Display rules in data_editor
            rule_display_data = []
            for rule in all_rules:
                rule_display_data.append(
                    {
                        "Select": rule["rule_id"] in st.session_state.selected_rule_ids,
                        "Rule ID": rule["rule_id"],
                        "Name": rule["rule_name"],
                        "Type": rule["rule_type"],
                        "Status": "Active" if rule["active"] else "Inactive",
                        "Content": (
                            rule["rule_content"][:60] + "..."
                            if len(rule["rule_content"]) > 60
                            else rule["rule_content"]
                        ),
                    }
                )

            edited_rules_df = st.data_editor(
                rule_display_data,
                use_container_width=True,
                height=300,
                hide_index=True,
                disabled=["Rule ID", "Name", "Type", "Status", "Content"],
                column_config={
                    "Select": st.column_config.CheckboxColumn(
                        "Select",
                        help="Select rules to apply",
                        default=False,
                    ),
                    "Content": st.column_config.TextColumn(
                        "Content",
                        width="large",
                    ),
                },
                key="pass2_rule_selector",
            )

            # Update session state
            st.session_state.selected_rule_ids = {
                row["Rule ID"] for row in edited_rules_df if row["Select"]
            }

    except Exception as e:
        st.error(f"Error loading rules: {str(e)}")

    st.markdown("---")

    # Step 3: Configuration
    st.markdown("#### Step 3: Configuration")

    pass_number = st.number_input(
        "Pass Number",
        min_value=2,
        max_value=10,
        value=2,
        help="Specify the pass number for tracking",
        key="pass2_pass_number",
    )

    st.markdown("---")

    # Step 4: Execute
    st.markdown("#### Step 4: Start Reprocessing")

    # Validation
    can_reprocess = len(st.session_state.pass2_selected_products) > 0

    if not can_reprocess:
        st.warning("Please select at least one product to reprocess")
    else:
        selected_count = len(st.session_state.pass2_selected_products)
        rule_count = len(st.session_state.selected_rule_ids)
        st.success(
            f"Ready to reprocess {selected_count} products " f"with {rule_count} rules"
        )

    if st.button(
        "Start Pass 2+ Reprocessing",
        type="primary",
        disabled=not can_reprocess,
        key="pass2_start",
    ):
        # Create containers
        status_container = st.empty()
        progress_container = st.empty()
        log_container = st.empty()

        try:
            st.session_state.processing_status = "Processing"
            status_container.info("Reprocessing batch...")

            # Show progress
            with progress_container:
                progress_bar = st.progress(0)
                progress_bar.progress(50)

            # Call batch processor
            result = process_batch(
                batch_size=len(st.session_state.pass2_selected_products),
                pass_number=pass_number,
                selected_item_ids=list(st.session_state.pass2_selected_products),
                selected_rule_ids=(
                    list(st.session_state.selected_rule_ids)
                    if st.session_state.selected_rule_ids
                    else None
                ),
            )

            # Update progress
            progress_bar.progress(100)

            # Store result
            st.session_state.pass2_last_result = result
            st.session_state.processing_status = "Complete"

            # Display success
            status_container.success(
                f"Reprocessing complete: {result.successful} successful, {result.failed} failed "
                f"out of {result.total_processed} processed"
            )

            # Show detailed results
            with log_container:
                st.json(
                    {
                        "total_processed": result.total_processed,
                        "successful": result.successful,
                        "failed": result.failed,
                        "success_rate": f"{result.success_rate:.1%}",
                        "avg_time_per_product": f"{result.avg_time_per_product:.2f}s",
                        "processing_time": f"{result.processing_time:.2f}s",
                        "confidence_distribution": result.confidence_distribution,
                    }
                )

            # Clear cache
            clear_cache()

            # Clear selections
            st.session_state.pass2_selected_products = set()

        except Exception as e:
            st.session_state.processing_status = "Error"
            status_container.error(f"Reprocessing failed: {str(e)}")
            with log_container:
                st.exception(e)

    # Step 5: Results Display
    if st.session_state.pass2_last_result:
        st.markdown("---")
        st.markdown("#### Last Reprocessing Result")

        result = st.session_state.pass2_last_result

        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

        with metric_col1:
            st.metric("Total Processed", result.total_processed)
        with metric_col2:
            st.metric("Successful", result.successful)
        with metric_col3:
            st.metric("Failed", result.failed)
        with metric_col4:
            st.metric("Success Rate", f"{result.success_rate:.1%}")

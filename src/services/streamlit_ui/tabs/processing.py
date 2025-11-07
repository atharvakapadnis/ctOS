"""
Processing Tab - Pass 1 and Pass 2+ Processing Operations
All processing operation happen here
"""

from tkinter import N
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
)
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

    elif mode == "Select specific products to process":
        # Mode B: Select specific products
        st.markdown("#### Step 2: Select Products")

        try:
            # Load unprocessed products
            unprocessed = load_unprocessed_products(limit=500)

            if not unprocessed:
                st.warning("No unprocessed products available")
            else:
                st.info(
                    f"Found {len(unprocessed)} unprocessed products (showing up to 500)"
                )

                # Select All/ Deselect All
                col1, col2, col3 = st.columns([1, 1, 4])

                with col1:
                    if st.button("Select All", key="pass1_select_all"):
                        st.session_state.pass1_selected_products = {
                            p["item_id"] for p in unprocessed
                        }
                        st.rerun()

                with col2:
                    if st.button("Deselect All", key="pass1_deselect_all"):
                        st.session_state.pass1_selected_products = set()
                        st.rerun()

                # Display selection count
                display_data = []
                for p in unprocessed:
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
                        }
                    )

                # Use data_editor for selection
                edited_df = st.data_editor(
                    display_data,
                    width="stretch",
                    height=400,
                    hide_index=True,
                    disabled=["Item ID", "Description", "HTS Code"],
                    column_config={
                        "Select": st.column_config.CheckboxColumn(
                            "Select",
                            help="Select products to process",
                            default=False,
                        )
                    },
                    key="pass1_product_selector",
                )

                # Update session state based on selections
                st.session_state.pass1_selected_products = {
                    row["Item ID"] for row in edited_df if row["Select"]
                }

                if len(unprocessed) > 500:
                    st.warning(f"Showing first 500 of {len(unprocessed)} products")

                # Batch size is determined by seelction count
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
    """Display Pass 2+ reprocessing section"""

    st.markdown("## Pass 2+: Reprocessing with Rules")
    st.caption("Reprocess products with additional rule guidance")

    # Step 1: Filter Products
    st.markdown("#### Step 1: Filter Products for Reprocessing")

    confidence_filter = st.multiselect(
        "Select Confidence Levels to Reprocess",
        options=["Low", "Medium", "High"],
        default=["Low", "Medium"],
        key="pass2_confidence_filter",
    )

    eligible_products = []

    if confidence_filter:
        try:
            eligible_products = load_products_by_confidence(confidence_filter)
            eligible_products = eligible_products[:MAX_SELECTION_ITEMS]

            st.info(
                f"Found {len(eligible_products)} eligible products for reprocessing"
            )

            if len(eligible_products) >= MAX_SELECTION_ITEMS:
                st.warning(f"Showing first {MAX_SELECTION_ITEMS} products")

        except Exception as e:
            st.error(f"Error loading products: {str(e)}")
    else:
        st.warning("Please select at least one confidence level")

    st.markdown("---")

    # Step 2: Select Products
    st.markdown("#### Step 2: Select Products to Reprocess")

    if eligible_products:
        # Select All / Deselect All buttons
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

        # Display selection count
        selected_count = len(st.session_state.pass2_selected_products)
        st.markdown(f"**Selected: {selected_count} products**")

        # Create dataframe with selection column
        display_data = []
        for p in eligible_products:
            confidence_score = p.get("confidence_score")
            confidence_score_display = (
                f"{float(confidence_score):.2f}" if confidence_score else "N/A"
            )

            # Get original description
            original_desc = p.get("item_description", "")
            original_display = (
                original_desc[:50] + "..." if len(original_desc) > 50 else original_desc
            )

            # Get enhanced description
            enhanced_desc = p.get("enhanced_description", "")
            enhanced_display = (
                enhanced_desc[:50] + "..."
                if enhanced_desc and len(enhanced_desc) > 50
                else enhanced_desc or "N/A"
            )

            # Get extracted fields
            customer_name = p.get("extracted_customer_name") or "N/A"
            dimensions = p.get("extracted_dimensions") or "N/A"
            product = p.get("extracted_product") or "N/A"

            rules_applied = p.get("rules_applied", "N/A")
            rules_display = None

            if rules_applied:
                try:
                    import json

                    rule_ids = json.loads(rules_applied)
                    rules_display = ", ".join(rule_ids) if rule_ids else "None"
                except:
                    rules_display = "None"

            display_data.append(
                {
                    "Select": p["item_id"] in st.session_state.pass2_selected_products,
                    "Item ID": p["item_id"],
                    "Original Description": original_display,
                    "Enhanced Description": enhanced_display,
                    "Customer": customer_name,
                    "Dimensions": dimensions,
                    "Product": product,
                    "Rules Applied": rules_display,
                    "Confidence": p.get("confidence_level", "N/A"),
                    "Score": confidence_score_display,
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
                "Customer": st.column_config.TextColumn(
                    "Customer",
                    width="small",
                ),
                "Dimensions": st.column_config.TextColumn(
                    "Dimensions",
                    width="small",
                ),
                "Product": st.column_config.TextColumn(
                    "Product",
                    width="small",
                ),
                "Rules Applied": st.column_config.TextColumn(
                    "Rules Applied",
                    width="small",
                ),
                "Confidence": st.column_config.TextColumn(
                    "Confidence",
                    width="small",
                ),
                "Score": st.column_config.TextColumn(
                    "Score",
                    width="small",
                ),
                "Pass": st.column_config.TextColumn(
                    "Pass",
                    width="small",
                ),
            },
            key="pass2_product_selector",
        )

        # Update session state based on selections
        st.session_state.pass2_selected_products = {
            row["Item ID"] for row in edited_df if row["Select"]
        }
    else:
        st.info("No products available. Please adjust filters above.")

    st.markdown("---")

    # Step 2.5: Select Rules
    st.markdown("#### Step 2.5: Select Rules to Apply")

    try:
        rule_manager = RuleManager()
        rules = rule_manager.load_rules()
        active_rules = [r for r in rules if r.active]

        if active_rules:
            st.info(f"Found {len(active_rules)} active rules")

            # Select All / Deselect All buttons
            col1, col2, col3 = st.columns([1, 1, 4])

            with col1:
                if st.button("Select All Rules", key="pass2_select_all_rules"):
                    st.session_state.selected_rule_ids = {
                        r.rule_id for r in active_rules
                    }
                    st.rerun()

            with col2:
                if st.button("Deselect All Rules", key="pass2_deselect_all_rules"):
                    st.session_state.selected_rule_ids = set()
                    st.rerun()

            # Display selection count
            selected_rule_count = len(st.session_state.selected_rule_ids)
            st.markdown(f"**Selected: {selected_rule_count} rules**")

            # Rule selection with checkboxes
            for rule in active_rules:
                is_selected = st.checkbox(
                    f"{rule.rule_id} - {rule.rule_name} ({rule.rule_type})",
                    value=rule.rule_id in st.session_state.selected_rule_ids,
                    key=f"pass2_rule_{rule.rule_id}",
                    help=rule.rule_content,
                )

                if is_selected:
                    st.session_state.selected_rule_ids.add(rule.rule_id)
                else:
                    st.session_state.selected_rule_ids.discard(rule.rule_id)
        else:
            st.warning("No active rules found. Go to Rules tab to create rules.")

    except Exception as e:
        st.error(f"Error loading rules: {str(e)}")

    st.markdown("---")

    # Step 3: Reprocessing Configuration
    st.markdown("#### Step 3: Configure Reprocessing")

    col1, col2 = st.columns(2)

    with col1:
        pass_number = st.number_input(
            "Pass Number",
            min_value=2,
            max_value=10,
            value=2,
            step=1,
            help="Pass number for this reprocessing operation",
            key="pass2_pass_number",
        )

    with col2:
        batch_size = st.slider(
            "Batch Size",
            min_value=MIN_BATCH_SIZE,
            max_value=MAX_BATCH_SIZE,
            value=min(50, DEFAULT_BATCH_SIZE),
            step=10,
            help="Number of products to process in this batch",
            key="pass2_batch_size",
        )

    st.markdown("---")

    # Step 4: Execute
    st.markdown("#### Step 4: Start Reprocessing")

    # Validation
    can_reprocess = True
    validation_messages = []

    if len(st.session_state.pass2_selected_products) == 0:
        can_reprocess = False
        validation_messages.append("Please select at least one product")

    if len(st.session_state.selected_rule_ids) == 0:
        validation_messages.append(
            "Warning: No rules selected. Processing will continue without rule guidance."
        )

    if not can_reprocess:
        st.error(validation_messages[0])
    elif len(validation_messages) > 0:
        st.warning(validation_messages[0])

    if st.button(
        "Reprocess Selected Items",
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

            # Show progress bar
            with progress_container:
                progress_bar = st.progress(0)
                progress_bar.progress(50)

            # Call batch processor
            result = process_batch(
                batch_size=batch_size,
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
                        "pass_number": result.pass_number,
                        "total_processed": result.total_processed,
                        "successful": result.successful,
                        "failed": result.failed,
                        "success_rate": f"{result.success_rate:.1%}",
                        "avg_time_per_product": f"{result.avg_time_per_product:.2f}s",
                        "processing_time": f"{result.processing_time:.2f}s",
                        "confidence_distribution": result.confidence_distribution,
                        "rules_applied": (
                            list(st.session_state.selected_rule_ids)
                            if st.session_state.selected_rule_ids
                            else []
                        ),
                    }
                )

            # Clear cache
            clear_cache()

            # Clear selections after successful reprocessing
            st.session_state.pass2_selected_products = set()
            st.session_state.selected_rule_ids = set()

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

        st.markdown(f"**Pass Number:** {result.pass_number}")

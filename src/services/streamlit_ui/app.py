"""
Main Streamlit Application Logic
"""

import streamlit as st
import sys
from pathlib import Path
from typing import Any, List

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
    load_processed_products,
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

    # Section 3.5: Rules Management
    display_rules_section()

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

    # Rules CRUD session state
    if "selected_rule_ids" not in st.session_state:
        st.session_state.selected_rule_ids = set()

    if "show_create_form" not in st.session_state:
        st.session_state.show_create_form = False

    if "show_edit_form" not in st.session_state:
        st.session_state.show_edit_form = False

    if "editing_rule_id" not in st.session_state:
        st.session_state.editing_rule_id = None

    if "form_data" not in st.session_state:
        st.session_state.form_data = {}


def display_statistics_section():
    """Display database statistics"""
    display_section_header("Database Statistics")

    try:
        stats = get_database_statistics()  # Returns a dict now

        # Display main metrics in columns
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Products", stats["total_products"])
        with col2:
            st.metric("Processed", stats["processed_count"])
        with col3:
            st.metric("Unprocessed", stats["unprocessed_count"])

        # Confidence distribution
        st.markdown("### Confidence Distribution")

        dist_col1, dist_col2, dist_col3 = st.columns(3)

        # Extract values frmo confidence_distribution dictionary
        confidence_dist = stats["confidence_distribution"]
        high_count = confidence_dist.get("High", 0)
        medium_count = confidence_dist.get("Medium", 0)
        low_count = confidence_dist.get("Low", 0)

        total_processed = (
            stats["processed_count"] if stats["processed_count"] > 0 else 1
        )

        with dist_col1:
            high_pct = (
                (high_count / total_processed * 100)
                if stats["processed_count"] > 0
                else 0
            )
            st.metric("High", high_count, f"{high_pct:.1f}%")
        with dist_col2:
            medium_pct = (
                (medium_count / total_processed * 100)
                if stats["processed_count"] > 0
                else 0
            )
            st.metric("Medium", medium_count, f"{medium_pct:.1f}%")
        with dist_col3:
            low_pct = (
                (low_count / total_processed * 100)
                if stats["processed_count"] > 0
                else 0
            )
            st.metric("Low", low_count, f"{low_pct:.1f}%")

        # Unique HTS codes
        st.metric("Unique HTS Codes", stats["unique_hts_codes"])

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


def display_rules_section():
    """Display rules management with CRUD operations"""
    st.markdown("---")
    st.header("Rules Management")
    st.caption("Create, view, edit, and delete rules for Pass 2+ reprocessing")

    # Initialize variables before try block
    rules = []
    stats = {"total_rules": 0, "active_rules": 0, "inactive_rules": 0}

    try:
        from src.services.rules import RuleManager

        rule_manager = RuleManager()

        # Initialize session state for rules CRUD
        if "selected_rule_ids" not in st.session_state:
            st.session_state.selected_rule_ids = set()
        if "show_create_form" not in st.session_state:
            st.session_state.show_create_form = False
        if "show_edit_form" not in st.session_state:
            st.session_state.show_edit_form = False
        if "editing_rule_id" not in st.session_state:
            st.session_state.editing_rule_id = None
        if "form_data" not in st.session_state:
            st.session_state.form_data = {}

        # Load rules and stats
        rules = rule_manager.load_rules()
        stats = rule_manager.get_rules_statistics()

        # A. Statistics Row
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Rules", stats["total_rules"])
        with col2:
            st.metric("Active Rules", stats["active_rules"])
        with col3:
            st.metric("Inactive Rules", stats["inactive_rules"])

        # B. Action Buttons Row
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Create New Rule", width="stretch"):
                st.session_state.show_create_form = True
                st.session_state.show_edit_form = False
                st.rerun()
        with col2:
            if st.button("Reload from File", width="stretch"):
                rule_manager.reload_rules()
                st.success("Rules reloaded successfully")
                st.rerun()

        # C. Rules Table with Selection
        st.markdown("### Rules")

        if rules:
            # Create display data
            display_data = []
            for rule in rules:
                display_data.append(
                    {
                        "Rule ID": rule.rule_id,
                        "Name": rule.rule_name,
                        "Type": rule.rule_type.title(),
                        "Content": (
                            rule.rule_content[:60] + "..."
                            if len(rule.rule_content) > 60
                            else rule.rule_content
                        ),
                        "Status": "Active" if rule.active else "Inactive",
                        "Created": rule.created_at[:10] if rule.created_at else "N/A",
                    }
                )

            # Display dataframe with checkboxes
            st.dataframe(display_data, width="stretch", height=300)

            # Selection checkboxes below table
            st.markdown("#### Select Rules for Actions")
            for rule in rules:
                is_selected = st.checkbox(
                    f"{rule.rule_id} - {rule.rule_name}",
                    value=rule.rule_id in st.session_state.selected_rule_ids,
                    key=f"select_rule_{rule.rule_id}",
                )

                if is_selected:
                    st.session_state.selected_rule_ids.add(rule.rule_id)
                else:
                    st.session_state.selected_rule_ids.discard(rule.rule_id)
        else:
            st.warning("No rules found")

        # D. Bulk Actions Row
        selected_count = len(st.session_state.selected_rule_ids)
        if selected_count > 0:
            st.markdown(f"**Selected: {selected_count} rule(s)**")

            col1, col2, col3 = st.columns([2, 2, 6])

            with col1:
                if st.button("Delete Selected", width="stretch", type="primary"):
                    st.session_state.confirm_delete_pending = True

            with col2:
                if st.button("Clear Selection", width="stretch"):
                    st.session_state.selected_rule_ids = set()
                    st.rerun()

            # Delete confirmation
            if st.session_state.get("confirm_delete_pending", False):
                st.warning(f"Are you sure you want to delete {selected_count} rule(s)?")
                st.markdown("**Rules to be deleted:**")
                for rule_id in st.session_state.selected_rule_ids:
                    st.markdown(f"- {rule_id}")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Confirm Delete", width="stretch"):
                        result = rule_manager.delete_rules(
                            list(st.session_state.selected_rule_ids)
                        )
                        if result["success"]:
                            st.success(result["message"])
                            if result["not_found"]:
                                st.warning(
                                    f"Not found: {', '.join(result['not_found'])}"
                                )
                        else:
                            st.error(result["message"])

                        st.session_state.selected_rule_ids = set()
                        st.session_state.confirm_delete_pending = False
                        st.rerun()

                with col2:
                    if st.button("Cancel", width="stretch"):
                        st.session_state.confirm_delete_pending = False
                        st.rerun()

        # E. Single Rule Actions
        if selected_count == 1:
            selected_id = list(st.session_state.selected_rule_ids)[0]

            col1, col2 = st.columns(2)

            with col1:
                if st.button("Edit Rule", width="stretch"):
                    st.session_state.show_edit_form = True
                    st.session_state.show_create_form = False
                    st.session_state.editing_rule_id = selected_id
                    st.rerun()

            with col2:
                if st.button("Toggle Status", width="stretch"):
                    success, message, new_status = rule_manager.toggle_rule_status(
                        selected_id
                    )
                    if success:
                        status_text = "ACTIVE" if new_status else "INACTIVE"
                        st.success(f"{message}")
                        st.session_state.selected_rule_ids = set()
                        st.rerun()
                    else:
                        st.error(message)

        # F. Create Form
        if st.session_state.show_create_form:
            st.markdown("---")
            st.markdown("### Create New Rule")

            with st.form("create_rule_form"):
                suggested_id = rule_manager.get_next_rule_id()

                rule_id = st.text_input(
                    "Rule ID",
                    value=suggested_id,
                    help="Auto-suggested. Change only if needed. Format: R###",
                )

                rule_name = st.text_input(
                    "Rule Name", placeholder="e.g., Ductile Iron Abbreviation"
                )

                rule_type = st.selectbox(
                    "Rule Type",
                    ["material", "dimension", "customer", "product", "general"],
                )

                rule_content = st.text_area(
                    "Rule Content",
                    placeholder="Enter the rule guidance for the LLM...",
                    height=100,
                )

                description = st.text_input(
                    "Description (Optional)",
                    placeholder="Brief description of what this rule does",
                )

                active = st.checkbox(
                    "Active (rule will be available for selection)", value=True
                )

                col1, col2 = st.columns(2)

                with col1:
                    submit = st.form_submit_button(
                        "Create Rule", width="stretch", type="primary"
                    )

                with col2:
                    cancel = st.form_submit_button("Cancel", width="stretch")

                if cancel:
                    st.session_state.show_create_form = False
                    st.rerun()

                if submit:
                    from datetime import datetime, timezone

                    new_rule = {
                        "rule_id": rule_id.strip(),
                        "rule_name": rule_name.strip(),
                        "rule_content": rule_content.strip(),
                        "rule_type": rule_type,
                        "active": active,
                        "description": description.strip() if description else None,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }

                    success, message, created_rule = rule_manager.add_rule(new_rule)

                    if success:
                        st.success(message)
                        st.session_state.show_create_form = False
                        st.rerun()
                    else:
                        st.error(f"Failed to create rule: {message}")

        # G. Edit Form
        if st.session_state.show_edit_form and st.session_state.editing_rule_id:
            st.markdown("---")
            st.markdown(f"### Edit Rule: {st.session_state.editing_rule_id}")

            existing_rule = rule_manager.get_rule_for_edit(
                st.session_state.editing_rule_id
            )

            if existing_rule:
                with st.form("edit_rule_form"):
                    st.text_input(
                        "Rule ID (Read-only)",
                        value=existing_rule["rule_id"],
                        disabled=True,
                    )

                    rule_name = st.text_input(
                        "Rule Name", value=existing_rule["rule_name"]
                    )

                    rule_type = st.selectbox(
                        "Rule Type",
                        ["material", "dimension", "customer", "product", "general"],
                        index=[
                            "material",
                            "dimension",
                            "customer",
                            "product",
                            "general",
                        ].index(existing_rule["rule_type"]),
                    )

                    rule_content = st.text_area(
                        "Rule Content", value=existing_rule["rule_content"], height=100
                    )

                    description = st.text_input(
                        "Description (Optional)",
                        value=existing_rule.get("description", "") or "",
                    )

                    active = st.checkbox(
                        "Active (rule will be available for selection)",
                        value=existing_rule["active"],
                    )

                    st.info(f"Created: {existing_rule.get('created_at', 'Unknown')}")

                    col1, col2 = st.columns(2)

                    with col1:
                        submit = st.form_submit_button(
                            "Save Changes", width="stretch", type="primary"
                        )

                    with col2:
                        cancel = st.form_submit_button("Cancel", width="stretch")

                    if cancel:
                        st.session_state.show_edit_form = False
                        st.session_state.editing_rule_id = None
                        st.session_state.selected_rule_ids = set()
                        st.rerun()

                    if submit:
                        updated_fields = {
                            "rule_name": rule_name.strip(),
                            "rule_content": rule_content.strip(),
                            "rule_type": rule_type,
                            "active": active,
                            "description": description.strip() if description else None,
                        }

                        success, message, updated_rule = rule_manager.update_rule(
                            st.session_state.editing_rule_id, updated_fields
                        )

                        if success:
                            st.success(message)
                            st.session_state.show_edit_form = False
                            st.session_state.editing_rule_id = None
                            st.session_state.selected_rule_ids = set()
                            st.rerun()
                        else:
                            st.error(f"Failed to update rule: {message}")
            else:
                st.error(f"Rule {st.session_state.editing_rule_id} not found")
                st.session_state.show_edit_form = False
                st.session_state.editing_rule_id = None

    except Exception as e:
        st.error(f"Error in rules management: {str(e)}")
        with st.expander("Error Details"):
            st.exception(e)


# def display_rules_section():
#     """Display available rules (read only)"""
#     st.markdown("---")
#     st.header("Rules Management")
#     st.caption("View and manage rules for Pass 2+ reprocessing")

#     try:
#         from src.services.rules import RuleManager

#         rule_manager = RuleManager()
#         rules = rule_manager.load_rules()
#         stats = rule_manager.get_rules_statistics()

#         # Display statistis
#         col1, col2, col3 = st.columns(3)
#         with col1:
#             st.metric("Total Rules", stats["total_rules"])
#         with col2:
#             st.metric("Active Rules", stats["active_rules"])
#         with col3:
#             st.metric("Inactive Rules", stats["inactive_rules"])

#         # Display rules table
#         st.markdown("### Available Rules")

#         if rules:
#             display_data = []
#             for rule in rules:
#                 display_data.append(
#                     {
#                         "Rule ID": rule.rule_id,
#                         "Name": rule.rule_name,
#                         "Type": rule.rule_type.title(),
#                         "Content": (
#                             rule.rule_content[:80] + "..."
#                             if len(rule.rule_content) > 80
#                             else rule.rule_content
#                         ),
#                         "Status": "Active" if rule.active else "Inactive",
#                         "Description": rule.description or "N/A",
#                     }
#                 )
#             st.dataframe(display_data, width="stretch", height=300)
#         else:
#             st.warning("No rules found")

#         # Instructuing for editing
#         st.info("Rules are stored in rules.json. Edit to add or modify rules.")

#         # Reload button
#         if st.button("Reload Rules from File"):
#             rule_manager.reload_rules()
#             st.success("Rules reloaded successfully")
#             st.rerun()

#     except Exception as e:
#         st.error(f"Error loading rules: {str(e)}")
#         with st.expander("Error Details"):
#             st.exception(e)


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

            # Step 2.5: Selecting rules to apply
            st.markdown("### Step 2.5: Selecting rules to apply")

            try:
                from src.services.rules import RuleManager

                rule_manager = RuleManager()
                available_rules = rule_manager.get_active_rules()

                if available_rules:
                    st.info(f"{len(available_rules)} active rules available")

                    # Initialize session state for rule selection
                    if "selected_rule_ids" not in st.session_state:
                        st.session_state.selected_rule_ids = set()

                    # Select/ Deselect All buttons
                    col1, col2 = st.columns(2)
                    if col1.button("Select All Rules"):
                        st.session_state.selected_rule_ids = set(
                            r.rule_id for r in available_rules
                        )
                        st.rerun()
                    if col2.button("Deselect All Rules"):
                        st.session_state.selected_rule_ids = set()
                        st.rerun()
                    # if col1.button("Select All Rules"):
                    #     st.session_state.selected_rule_ids = [
                    #         r.rule_id for r in available_rules
                    #     ]
                    #     st.rerun()
                    # if col2.button("Deselect All Rules"):
                    #     st.session_state.selected_rule_ids = []
                    #     st.rerun()

                    # Rule checkboxes
                    # selected_rule_ids = []
                    for rule in available_rules:
                        is_selected = st.checkbox(
                            f"**{rule.rule_id}** - {rule.rule_name} ({rule.rule_type})",
                            value=rule.rule_id in st.session_state.selected_rule_ids,
                            key=f"rule_{rule.rule_id}",
                            help=rule.rule_content,
                        )

                        if is_selected:
                            selected_rule_ids.append(rule.rule_id)
                        else:
                            st.session_state.selected_rule_ids.discard(rule.rule_id)

                    st.markdown(f"**Selected: {len(selected_rule_ids)} rules**")

                else:
                    st.warning("No active rules available")
                    selected_rule_ids = []

            except Exception as e:
                st.error(f"Error loading rules: {str(e)}")
                selected_rule_ids = []

            # Step 3: Reprocess selected items
            st.markdown("### Step 3: Reprocess selected items")

            if (
                "selected_items" not in st.session_state
                or len(st.session_state.selected_items) == 0
            ):
                st.warning("No items selected. Please select items in Step 2 above")
                return

            st.info(
                f"Ready to reprocess {len(st.session_state.selected_items)} items with {len(st.session_state.get('selected_rule_ids', []))} rules"
            )

            # Reprocess button
            if st.button("Reprocess Selected Items", type="primary"):
                with st.spinner("Reprocessing items..."):
                    try:
                        from src.services.llm_enhancement.service import process_batch

                        # Call service 3 with selected rules
                        result = process_batch(
                            batch_size=len(st.session_state.selected_items),
                            pass_number=2,
                            selected_item_ids=list(st.session_state.selected_items),
                            selected_rule_ids=st.session_state.get(
                                "selected_rule_ids", []
                            ),
                        )

                        # Display results
                        st.success("Reprocessing complete")

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Processed", result.total_processed)
                        with col2:
                            st.metric("Successful", result.successful)
                        with col3:
                            st.metric("Failed", result.failed)

                        # Clear selection
                        st.session_state.selected_items = set()
                        st.session_state.selected_rule_ids = []

                        st.info(
                            "Items reprocessed. Refresh the product view to see updated data."
                        )

                    except Exception as e:
                        st.error(f"Reprocessing failed: {str(e)}")
                        with st.expander("Error Details"):
                            st.exception(e)

        else:
            st.warning(
                "No products available for reprocessing with selected confidence levels."
            )

    except Exception as e:
        st.error(f"Error loading reprocessing candidates: {str(e)}")
        st.exception(e)

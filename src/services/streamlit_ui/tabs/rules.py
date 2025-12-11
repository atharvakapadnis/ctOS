"""
Rules Tab - CRUD Operations for Rules Management
"""

import streamlit as st
from pathlib import Path
from ...common.service_factory import ServiceFactory
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.services.rules import RuleManager


def display_rules_tab():
    """Display rules management tab with full CRUD operations"""

    st.markdown("### Rules Management")
    st.caption("Create, view, edit, and delete rules for Pass 2+ reprocessing")

    # Initialize variables
    rules = []
    stats = {"total_rules": 0, "active_rules": 0, "inactive_rules": 0}

    try:
        rule_manager = ServiceFactory.get_rule_manager()

        # Initialize session state for rules CRUD
        initialize_rules_crud_session_state()

        # Load rules and stats
        rules = rule_manager.load_rules()
        stats = rule_manager.get_rules_statistics()

        # A. Statistics Row
        st.markdown("#### Rules Statistics")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Rules", stats["total_rules"])
        with col2:
            st.metric("Active Rules", stats["active_rules"])
        with col3:
            st.metric("Inactive Rules", stats["inactive_rules"])

        st.markdown("---")

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

        st.markdown("---")

        # C. Rules Table with Selection
        st.markdown("#### Rules List")

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

            # Display dataframe
            st.dataframe(display_data, width="stretch", height=300, hide_index=True)

            # Selection checkboxes below table
            st.markdown("#### Select Rules for Actions")
            for rule in rules:
                is_selected = st.checkbox(
                    f"{rule.rule_id} - {rule.rule_name}",
                    value=rule.rule_id in st.session_state.rules_crud_selected,
                    key=f"crud_select_rule_{rule.rule_id}",
                )

                if is_selected:
                    st.session_state.rules_crud_selected.add(rule.rule_id)
                else:
                    st.session_state.rules_crud_selected.discard(rule.rule_id)
        else:
            st.warning("No rules found")

        st.markdown("---")

        # D. Bulk Actions Row
        selected_count = len(st.session_state.rules_crud_selected)
        if selected_count > 0:
            st.markdown(f"**Selected: {selected_count} rule(s)**")

            col1, col2, col3 = st.columns([2, 2, 6])

            with col1:
                if st.button("Delete Selected", width="stretch", type="primary"):
                    st.session_state.confirm_delete_pending = True

            with col2:
                if st.button("Clear Selection", width="stretch"):
                    st.session_state.rules_crud_selected = set()
                    st.rerun()

            # Delete confirmation
            if st.session_state.get("confirm_delete_pending", False):
                st.warning(f"Are you sure you want to delete {selected_count} rule(s)?")
                st.markdown("**Rules to be deleted:**")
                for rule_id in st.session_state.rules_crud_selected:
                    st.markdown(f"- {rule_id}")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Confirm Delete", width="stretch"):
                        result = rule_manager.delete_rules(
                            list(st.session_state.rules_crud_selected)
                        )
                        if result["success"]:
                            ServiceFactory.reload_rules()
                            st.success(result["message"])
                            if result["not_found"]:
                                st.warning(
                                    f"Not found: {', '.join(result['not_found'])}"
                                )
                        else:
                            st.error(result["message"])

                        st.session_state.rules_crud_selected = set()
                        st.session_state.confirm_delete_pending = False
                        st.rerun()

                with col2:
                    if st.button("Cancel", width="stretch"):
                        st.session_state.confirm_delete_pending = False
                        st.rerun()

        st.markdown("---")

        # E. Single Rule Actions
        if selected_count == 1:
            selected_id = list(st.session_state.rules_crud_selected)[0]

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
                        ServiceFactory.reload_rules()
                        st.success(f"{message}")
                        st.session_state.rules_crud_selected = set()
                        st.rerun()
                    else:
                        st.error(message)

        st.markdown("---")

        # F. Create Form
        if st.session_state.show_create_form:
            display_create_form(rule_manager)

        # G. Edit Form
        if st.session_state.show_edit_form:
            display_edit_form(rule_manager)

    except Exception as e:
        st.error(f"Error in rules management: {str(e)}")
        st.exception(e)


def initialize_rules_crud_session_state():
    """Initialize session state for rules CRUD operations"""

    if "rules_crud_selected" not in st.session_state:
        st.session_state.rules_crud_selected = set()

    if "show_create_form" not in st.session_state:
        st.session_state.show_create_form = False

    if "show_edit_form" not in st.session_state:
        st.session_state.show_edit_form = False

    if "editing_rule_id" not in st.session_state:
        st.session_state.editing_rule_id = None

    if "form_data" not in st.session_state:
        st.session_state.form_data = {}

    if "confirm_delete_pending" not in st.session_state:
        st.session_state.confirm_delete_pending = False


def display_create_form(rule_manager):
    """Display create rule form"""

    st.markdown("### Create New Rule")

    with st.form("create_rule_form"):
        suggested_id = rule_manager.get_next_rule_id()

        rule_id = st.text_input(
            "Rule ID",
            value=suggested_id,
            help="Auto-suggested. Change only if needed.",
        )

        rule_name = st.text_input(
            "Rule Name",
            placeholder="e.g., Ductile Iron Abbreviation",
            help="Short descriptive name for the rule",
        )

        rule_type = st.selectbox(
            "Rule Type",
            options=["material", "dimension", "customer", "product", "general"],
            help="Category of the rule",
        )

        rule_content = st.text_area(
            "Rule Content",
            placeholder="Enter the rule guidance for the LLM...",
            help="The actual rule text that will be included in the prompt",
            height=150,
        )

        active = st.checkbox("Active", value=True, help="Whether this rule is active")

        col1, col2 = st.columns(2)

        with col1:
            submitted = st.form_submit_button(
                "Create Rule", type="primary", width="stretch"
            )

        with col2:
            cancel = st.form_submit_button("Cancel", width="stretch")

        if cancel:
            st.session_state.show_create_form = False
            st.rerun()

        if submitted:
            # Validation
            if not rule_id or not rule_name or not rule_content:
                st.error("Rule ID, Name, and Content are required")
            else:
                # Create rule
                new_rule = {
                    "rule_id": rule_id,
                    "rule_name": rule_name,
                    "rule_content": rule_content,
                    "rule_type": rule_type,
                    "active": active,
                }

                success, message, created_rule = rule_manager.add_rule(new_rule)

                if success:
                    ServiceFactory.reload_rules()
                    st.success(message)
                    st.session_state.show_create_form = False
                    st.rerun()
                else:
                    st.error(message)


def display_edit_form(rule_manager):
    """Display edit rule form"""

    st.markdown("### Edit Rule")

    rule_id = st.session_state.editing_rule_id

    # Load rule data
    rule_data = rule_manager.get_rule_for_edit(rule_id)

    if not rule_data:
        st.error(f"Rule {rule_id} not found")
        st.session_state.show_edit_form = False
        return

    with st.form("edit_rule_form"):
        st.text_input("Rule ID", value=rule_data["rule_id"], disabled=True)

        rule_name = st.text_input(
            "Rule Name",
            value=rule_data["rule_name"],
            help="Short descriptive name for the rule",
        )

        rule_type = st.selectbox(
            "Rule Type",
            options=["material", "dimension", "customer", "product", "general"],
            index=["material", "dimension", "customer", "product", "general"].index(
                rule_data["rule_type"]
            ),
            help="Category of the rule",
        )

        rule_content = st.text_area(
            "Rule Content",
            value=rule_data["rule_content"],
            help="The actual rule text that will be included in the prompt",
            height=150,
        )

        active = st.checkbox(
            "Active",
            value=rule_data["active"],
            help="Whether this rule is active",
        )

        col1, col2 = st.columns(2)

        with col1:
            submitted = st.form_submit_button(
                "Update Rule", type="primary", width="stretch"
            )

        with col2:
            cancel = st.form_submit_button("Cancel", width="stretch")

        if cancel:
            st.session_state.show_edit_form = False
            st.session_state.editing_rule_id = None
            st.rerun()

        if submitted:
            # Validation
            if not rule_name or not rule_content:
                st.error("Rule Name and Content are required")
            else:
                # Update rule
                updated_fields = {
                    "rule_name": rule_name,
                    "rule_content": rule_content,
                    "rule_type": rule_type,
                    "active": active,
                }

                success, message, updated_rule = rule_manager.update_rule(
                    rule_id, updated_fields
                )

                if success:
                    ServiceFactory.reload_rules()
                    st.success(message)
                    st.session_state.show_edit_form = False
                    st.session_state.editing_rule_id = None
                    st.session_state.rules_crud_selected = set()
                    st.rerun()
                else:
                    st.error(message)

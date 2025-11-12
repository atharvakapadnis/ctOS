"""
Reusable UI Components
"""

import streamlit as st
import re
from typing import Dict, Any, Tuple, Optional


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


def display_search_bar(key_prefix: str) -> Tuple[str, str, bool]:
    f"""
    Reusable search bar component.

    Purpose: Unified search interface for finding products by Item ID, HTS, or keywords

    Args:
        key_prefix (str): Unique prefix for session staate keys

    Returns:
        Tuple[str, str, bool]: (query, search_type, search_triggered)
            - query: Search query text
            - search_type: Mapped search type for database
            - search_triggered: True if search button clicked

    Session State Keys:
        - {key_prefix}search_query: str
        - {key_prefix}search_type: str
        - {key_prefix}search_active: bool
    """

    # Initialize session state
    if f"{key_prefix}search_query" not in st.session_state:
        st.session_state[f"{key_prefix}search_query"] = ""
    if f"{key_prefix}search_type" not in st.session_state:
        st.session_state[f"{key_prefix}search_type"] = "Auto (detect automatically)"
    if f"{key_prefix}search_active" not in st.session_state:
        st.session_state[f"{key_prefix}search_active"] = False

    st.markdown("#### Search Products")

    # Three column layout
    col1, col2, col3 = st.columns([6, 2, 2])

    with col1:
        query = st.text_input(
            "Search Query",
            value=st.session_state[f"{key_prefix}search_query"],
            placeholder="Enter Item ID, HTS Code, or keywords...",
            key=f"{key_prefix}search_input",
            label_visibility="collapsed",
            help="Search by Item ID (e.g., ITEM-1234), HTS Code (e.g., 7307.11.00), or description keywords (e.g., ductile iron spacer)",
        )
        st.session_state[f"{key_prefix}search_query"] = query

    with col2:
        search_type_display = st.selectbox(
            "Search Type",
            [
                "Auto (detect automatically)",
                "Item ID",
                "HTS Code",
                "Description Keywords",
                "Search All Fields",
            ],
            index=0,
            key=f"{key_prefix}search_type_select",
            label_visibility="collapsed",
        )
        st.session_state[f"{key_prefix}search_type"] = search_type_display

    with col3:
        search_button = st.button(
            "Search",
            key=f"{key_prefix}search_button",
            type="primary",
            use_container_width=True,
        )

        if query and st.session_state[f"{key_prefix}search_active"]:
            clear_button = st.button(
                "Clear",
                key=f"{key_prefix}clear_search_button",
                use_container_width=True,
            )
            if clear_button:
                st.session_state[f"{key_prefix}search_query"] = ""
                st.session_state[f"{key_prefix}search_active"] = False
                st.rerun()

    # Map display names to database values
    search_type_map = {
        "Auto (detect automatically)": "auto",
        "Item ID": "item_id",
        "HTS Code": "hts_code",
        "Description Keywords": "description",
        "Search All Fields": "multi",
    }

    search_type_db = search_type_map[search_type_display]

    # Handle search trigger
    search_triggered = False
    if search_button:
        if not query or not query.strip():
            st.warning("Please enter a search query")
        elif search_type_db == "description" and len(query.strip()) < 2:
            st.warning("Please enter at least 2 characters for description search")
        else:
            st.session_state[f"{key_prefix}search_active"] = True
            search_triggered = True

    return query, search_type_db, search_triggered


def display_advanced_filters(
    key_prefix: str, show_confidence: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Reusable advanced filter component.

    Purpose: Comprehensive filtering interface for narrowing product results

    Args:
        key_prefix (str): Unique prefix for session state keys
        show_confidence (bool): Whether to show confidence level filter (Pass 2+ and Browse Data processed view)

    Returns:
        Dict[str, Any] or None: Filter dictionary if filters active, None otherwise

    Filter Dictionary Structure:
        {
            "hts_range": {"start": "7307.11.00", "end": "7307.99.00"},
            "product_group": "GROUP_A",
            "material_class": "Ductile Iron",
            "status": "unprocessed" | "processed" | "all",
            "confidence_levels": ["Low", "Medium"]
        }

    Session State Keys:
        - {key_prefix}filter_hts_start: str
        - {key_prefix}filter_hts_end: str
        - {key_prefix}filter_product_group: str
        - {key_prefix}filter_material_class: str
        - {key_prefix}filter_status: str
        - {key_prefix}filter_confidence_levels: List[str]
        - {key_prefix}filters_active: bool
    """
    from .data_loader import get_product_groups, get_material_classes

    # Initialize session state
    if f"{key_prefix}filter_hts_start" not in st.session_state:
        st.session_state[f"{key_prefix}filter_hts_start"] = ""
    if f"{key_prefix}filter_hts_end" not in st.session_state:
        st.session_state[f"{key_prefix}filter_hts_end"] = ""
    if f"{key_prefix}filter_product_group" not in st.session_state:
        st.session_state[f"{key_prefix}filter_product_group"] = "All"
    if f"{key_prefix}filter_material_class" not in st.session_state:
        st.session_state[f"{key_prefix}filter_material_class"] = "All"
    if f"{key_prefix}filter_status" not in st.session_state:
        st.session_state[f"{key_prefix}filter_status"] = "All"
    if f"{key_prefix}filter_confidence_levels" not in st.session_state:
        st.session_state[f"{key_prefix}filter_confidence_levels"] = []
    if f"{key_prefix}filters_active" not in st.session_state:
        st.session_state[f"{key_prefix}filters_active"] = False

    with st.expander("Advanced Filters", expanded=False):
        # Action buttons at top
        col_apply, col_clear = st.columns([1, 1])
        with col_apply:
            apply_button = st.button(
                "Apply Filters",
                key=f"{key_prefix}apply_filters_button",
                type="primary",
                use_container_width=True,
            )
        with col_clear:
            clear_button = st.button(
                "Clear Filters",
                key=f"{key_prefix}clear_filters_button",
                use_container_width=True,
            )

        st.markdown("---")

        # Row 1: HTS Code Range + Product Group
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**HTS Code Range**")
            hts_start = st.text_input(
                "From",
                value=st.session_state[f"{key_prefix}filter_hts_start"],
                placeholder="7307.11.00",
                key=f"{key_prefix}hts_start_input",
                help="Enter starting HTS code (format: 1234.56.78)",
            )
            hts_end = st.text_input(
                "To",
                value=st.session_state[f"{key_prefix}filter_hts_end"],
                placeholder="7307.99.00",
                key=f"{key_prefix}hts_end_input",
                help="Enter ending HTS code (format: 1234.56.78)",
            )

        with col2:
            st.markdown("**Product Group**")
            product_groups = ["All"] + get_product_groups()
            product_group = st.selectbox(
                "Select Product Group",
                product_groups,
                index=(
                    product_groups.index(
                        st.session_state[f"{key_prefix}filter_product_group"]
                    )
                    if st.session_state[f"{key_prefix}filter_product_group"]
                    in product_groups
                    else 0
                ),
                key=f"{key_prefix}product_group_select",
                label_visibility="collapsed",
            )

        # Row 2: Material Class + Status
        col3, col4 = st.columns(2)

        with col3:
            st.markdown("**Material Class**")
            material_classes = ["All"] + get_material_classes()
            material_class = st.selectbox(
                "Select Material Class",
                material_classes,
                index=(
                    material_classes.index(
                        st.session_state[f"{key_prefix}filter_material_class"]
                    )
                    if st.session_state[f"{key_prefix}filter_material_class"]
                    in material_classes
                    else 0
                ),
                key=f"{key_prefix}material_class_select",
                label_visibility="collapsed",
            )

        with col4:
            st.markdown("**Processing Status**")
            status = st.radio(
                "Status",
                ["All", "Unprocessed Only", "Processed Only"],
                index=(
                    ["All", "Unprocessed Only", "Processed Only"].index(
                        st.session_state[f"{key_prefix}filter_status"]
                    )
                    if st.session_state[f"{key_prefix}filter_status"]
                    in ["All", "Unprocessed Only", "Processed Only"]
                    else 0
                ),
                key=f"{key_prefix}status_radio",
                label_visibility="collapsed",
            )

        # Row 3: Confidence Levels (conditional)
        if show_confidence:
            st.markdown("**Confidence Levels** (for processed products)")
            col5, col6, col7 = st.columns(3)

            with col5:
                conf_low = st.checkbox(
                    "Low",
                    value="Low"
                    in st.session_state[f"{key_prefix}filter_confidence_levels"],
                    key=f"{key_prefix}conf_low_check",
                )
            with col6:
                conf_medium = st.checkbox(
                    "Medium",
                    value="Medium"
                    in st.session_state[f"{key_prefix}filter_confidence_levels"],
                    key=f"{key_prefix}conf_medium_check",
                )
            with col7:
                conf_high = st.checkbox(
                    "High",
                    value="High"
                    in st.session_state[f"{key_prefix}filter_confidence_levels"],
                    key=f"{key_prefix}conf_high_check",
                )

            confidence_levels = []
            if conf_low:
                confidence_levels.append("Low")
            if conf_medium:
                confidence_levels.append("Medium")
            if conf_high:
                confidence_levels.append("High")
        else:
            confidence_levels = []

        # Validation
        validation_errors = []

        # Validate HTS format
        hts_pattern = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
        if hts_start and not hts_pattern.match(hts_start):
            validation_errors.append("Invalid HTS start format. Expected: 1234.56.78")
        if hts_end and not hts_pattern.match(hts_end):
            validation_errors.append("Invalid HTS end format. Expected: 1234.56.78")

        # Validate HTS range
        if hts_start and hts_end and hts_start > hts_end:
            validation_errors.append("HTS start must be less than or equal to HTS end")

        if validation_errors:
            for error in validation_errors:
                st.error(error)

        # Handle clear button
        if clear_button:
            st.session_state[f"{key_prefix}filter_hts_start"] = ""
            st.session_state[f"{key_prefix}filter_hts_end"] = ""
            st.session_state[f"{key_prefix}filter_product_group"] = "All"
            st.session_state[f"{key_prefix}filter_material_class"] = "All"
            st.session_state[f"{key_prefix}filter_status"] = "All"
            st.session_state[f"{key_prefix}filter_confidence_levels"] = []
            st.session_state[f"{key_prefix}filters_active"] = False
            st.rerun()

        # Handle apply button
        if apply_button:
            if validation_errors:
                st.error("Please fix validation errors before applying filters")
                return None

            # Update session state
            st.session_state[f"{key_prefix}filter_hts_start"] = hts_start
            st.session_state[f"{key_prefix}filter_hts_end"] = hts_end
            st.session_state[f"{key_prefix}filter_product_group"] = product_group
            st.session_state[f"{key_prefix}filter_material_class"] = material_class
            st.session_state[f"{key_prefix}filter_status"] = status
            st.session_state[f"{key_prefix}filter_confidence_levels"] = (
                confidence_levels
            )

            # Build filter dict
            filters = {}

            # HTS Range
            if hts_start or hts_end:
                filters["hts_range"] = {}
                if hts_start:
                    filters["hts_range"]["start"] = hts_start
                if hts_end:
                    filters["hts_range"]["end"] = hts_end

            # Product Group
            if product_group != "All":
                filters["product_group"] = product_group

            # Material Class
            if material_class != "All":
                filters["material_class"] = material_class

            # Status
            status_map = {
                "All": "all",
                "Unprocessed Only": "unprocessed",
                "Processed Only": "processed",
            }
            filters["status"] = status_map[status]

            # Confidence Levels
            if confidence_levels and status == "Processed Only":
                filters["confidence_levels"] = confidence_levels

            # Check if any filters applied
            has_filters = (
                "hts_range" in filters
                or "product_group" in filters
                or "material_class" in filters
                or filters["status"] != "all"
                or "confidence_levels" in filters
            )

            if has_filters:
                st.session_state[f"{key_prefix}filters_active"] = True
                return filters
            else:
                st.info(
                    "No filters selected. Please select at least one filter criterion."
                )
                st.session_state[f"{key_prefix}filters_active"] = False
                return None

    # Return None if filters not active
    return None

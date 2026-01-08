"""
Main Streamlit Application with Tab-Based Navigation
"""

import os
import streamlit as st
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .tabs.dashboard import display_dashboard_tab
from .tabs.browse_data import display_browse_data_tab
from .tabs.processing import display_processing_tab
from .tabs.rules import display_rules_tab

# Demo Mode detection
APP_MODE = os.getenv("APP_MODE", "prod")
IS_DEMO_MODE = APP_MODE == "demo"


def main():
    """Main Streamlit Application with Tab-Based Navigation"""

    # Display demo banner if in demo mode
    if IS_DEMO_MODE:
        st.markdown(
            """
            <div style="
                background-color: #FF4B4B;
                color: white;
                padding: 15px;
                text-align: center;
                font-size: 18px;
                font-weight: bold;
                margin-bottom: 20px;
                border-radius: 5px;
            ">
                DEMO MODE - This is a demonstration environment
            </div>
            """,
            unsafe_allow_html=True,
        )

    # App title
    title_prefix = "[DEMO] " if IS_DEMO_MODE else ""
    st.title(f"{title_prefix}ctOS - Product Enhancement System")
    st.caption("AI-powered product description enhancement with HTS alignment")

    # Initialize gloval session state
    initialize_global_session_state()

    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Dashboard", "Browse Data", "Processing", "Rules"]
    )

    # Tab 1: Dashboard
    with tab1:
        display_dashboard_tab()

    # Tab 2: Browse Data
    with tab2:
        display_browse_data_tab()

    # Tab 3: Processing
    with tab3:
        display_processing_tab()

    # Tab 4: Rules
    with tab4:
        display_rules_tab()


def initialize_global_session_state():
    """Initialize global session state variables"""

    # Processing-related state
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

    # Rules CRUD state (tab-specific)
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


if __name__ == "__main__":
    main()

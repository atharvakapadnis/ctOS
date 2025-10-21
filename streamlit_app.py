"""
ctOS - Product Enhancement System
Streamlit Web Interface (MVP)
"""

import streamlit as st
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.services.streamlit_ui.app import main

# Streamlit page config
st.set_page_config(
    page_title="ctOS", page_icon=None, layout="wide", initial_sidebar_state="collapsed"
)

if __name__ == "__main__":
    main()

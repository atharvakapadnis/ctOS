"""
Tab modules for Streamlit UI
Each tab represents a major section of the application
"""

from .dashboard import display_dashboard_tab
from .browse_data import display_browse_data_tab
from .processing import display_processing_tab
from .rules import display_rules_tab

__all__ = [
    "display_dashboard_tab",
    "display_browse_data_tab",
    "display_processing_tab",
    "display_rules_tab",
]

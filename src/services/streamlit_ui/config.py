"""
Streamlit UI Configuration
"""

# Page Config
PAGE_TITLE = "ctOS"
PAGE_ICON = None
Layout = "wide"

# Data display limit (for MVP performance)
MAX_PRODUCTS_DISPLAY = 500
MAX_SELECTION_ITEMS = 100

# Cache TTL
CACHE_TTL_STATISTICS = 10
CACHE_TTL_PRODUCTS = 60

# Batch processing defaults
DEFAULT_BATCH_SIZE = 100
MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 100

# UI text
SECTION_TITLES = {
    "statistics": "Database Statistics",
    "batch_control": "Batch Processing Control",
    "product_view": "Product Data View",
    "reprocessing": "Pass 2+ Reprocessing",
}

# Colors (for future use)
COLORS = {
    "primary": "#0066CC",
    "success": "#28A745",
    "warning": "#FFC107",
    "danger": "#DC3545",
}

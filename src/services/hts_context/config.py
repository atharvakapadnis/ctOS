"""
Configuration constants for HTS Context Service
"""

from pathlib import Path

# Project Root Directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# File paths
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
LOG_DIR = DATA_DIR / "logs"
DEBUG_DIR = DATA_DIR / "debug"

HTS_REFERENCE_PATH = INPUT_DIR / "htsdata_ch73.json"
LOG_FILE = LOG_DIR / "hts_context.log"
DEBUG_EXPORT_PATH = DEBUG_DIR / "hts_hierarchy_map.json"

# Logging
LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
)
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Ensure directories exist
LOG_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

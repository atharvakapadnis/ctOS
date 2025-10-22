"""
Configuration constants for Rules Management Service
"""

from pathlib import Path
from datetime import datetime, timezone
import re

# Project Root Directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# File paths
DATA_DIR = PROJECT_ROOT / "data"
RULES_DIR = DATA_DIR / "rules"
RULES_FILE = RULES_DIR / "rules.json"
LOG_DIR = DATA_DIR / "logs"

# Ensure directories exist
RULES_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Allowed rule types
ALLOWED_RULE_TYPES = ["material", "dimension", "customer", "product", "general"]

# Rule ID Pattern
RULE_ID_PATTERN = r"^R\d{3,}$"

# Logging Configuration
LOG_FILE = LOG_DIR / "rules.log"
LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
)

LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

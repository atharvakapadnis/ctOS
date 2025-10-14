"""
Configuration constants for LLM Enhancement Service
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project Root Directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# File paths
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = DATA_DIR / "logs"
DATABASE_PATH = DATA_DIR / "products.db"
HTS_REFERENCE_PATH = DATA_DIR / "input" / "htsdata_ch73.json"
RULES_PATH = DATA_DIR / "rules" / "rules.json"

# Ensure directories exist
LOG_DIR.mkdir(parents=True, exist_ok=True)

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = "gpt-4"
OPENAI_TEMPERATURE = 0.3
OPENAI_MAX_TOKENS = 2000
OPENAI_TIMEOUT = 60

# Retry Configuration
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0  # seconds
RETRY_EXPONENTIAL_BASE = 2

# Batch Processing Configuration
BATCH_SIZE_DEFAULT = 100
BATCH_SIZE_DEVELOPMENT = 10

# Logging Configuration
LOG_FILE = LOG_DIR / "llm_enhancement.log"
ERROR_LOG_FILE = LOG_DIR / "llm_enhancement_errors.log"
LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
)
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Development/Production Mode
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
MOCK_OPENAI = os.getenv("MOCK_OPENAI", "false").lower() == "true"

# Adjust settings for development mode
if DEBUG_MODE:
    BATCH_SIZE_DEFAULT = BATCH_SIZE_DEVELOPMENT
    OPENAI_MAX_TOKENS = 500
    LOG_LEVEL = "DEBUG"
else:
    LOG_LEVEL = "INFO"

# System Prompt (constant)
SYSTEM_PROMPT = """You are a product description enhancement assistant for trade compliance.

Your task is to enhance product descriptions by:
1. Expanding abbreviations (e.g., DI → Ductile Iron, MJ → Mechanical Joint)
2. Including material specifications from context
3. Adding dimensions if present in original description
4. Identifying customer names if present
5. Using official HTS classification terminology as reference
6. Clarifying product type/category

CRITICAL RESPONSE FORMAT:
You must respond with ONLY a valid JSON object. No additional text before or after.
Do not use markdown code blocks or backticks.

Required JSON structure:
{
    "enhanced_description": "string - the enhanced product description",
    "confidence_score": "string - numeric value 0.0 to 1.0 representing your confidence",
    "confidence_level": "string - must be exactly 'Low', 'Medium', or 'High'",
    "extracted_features": {
        "customer_name": "string or null - customer name if present",
        "dimensions": "string or null - dimensions if present",
        "product": "string - REQUIRED, the product type/category"
    }
}

Confidence Guidelines:
- High (0.8-1.0): Clear description, all key information present, good HTS alignment
- Medium (0.4-0.8): Some information missing or unclear, partial HTS alignment
- Low (0.0-0.4): Significant information missing, poor description quality

REMEMBER: Response must be ONLY the JSON object, nothing else."""

# Validation
if not OPENAI_API_KEY and not MOCK_OPENAI:
    raise ValueError(
        "OPENAI_API_KEY environment variable not set. Set MOCK_OPENAI=true for testing."
    )

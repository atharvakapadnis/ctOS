"""
Configuration management for ctOS service 1
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class Config:
    """Core configuration settings for ctOS service 1"""

    DATABASE_PATH: str = "data/ctOS.db"
    CSV_INPUT_PATH: str = "data/raw/cleaned_test_ch73.csv"
    HTS_JSON_PATH: str = "data/raw/htsdata_ch73.json"
    LOG_LEVEL: str = "INFO"
    BATCH_SIZE: int = 1000


def get_config() -> Config:
    """Get configuration with environment variable overrides"""
    return Config(
        DATABASE_PATH=os.getenv("CTOS_DATABASE_PATH", "data/ctOS.db"),
        CSV_INPUT_PATH=os.getenv(
            "CTOS_CSV_INPUT_PATH", "data/raw/cleaned_test_ch73.csv"
        ),
        HTS_JSON_PATH=os.getenv("CTOS_HTS_JSON_PATH", "data/raw/htsdata_ch73.json"),
        LOG_LEVEL=os.getenv("CTOS_LOG_LEVEL", "INFO"),
        BATCH_SIZE=int(os.getenv("CTOS_BATCH_SIZE", "1000")),
    )


def validate_paths() -> bool:
    """Ensure all required paths exist"""
    config = get_config()

    # Create path if it doesnt exist
    required_files = [config.CSV_INPUT_PATH, config.HTS_JSON_PATH]
    missing_files = [f for f in required_files if not Path(f).exists()]

    if missing_files:
        print(f"Missing required files: {missing_files}")
        return False
    return True

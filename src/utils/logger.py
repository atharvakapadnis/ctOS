"""
Centralized logging configuration for ctOS Service 1
"""

import logging
import sqlite3
from pathlib import Path
from typing import Dict


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Set up logger with consistent formatting"""
    logger = logging.getLogger(name)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper()))

    # Console handler
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    log_file = Path("logs/ctOS.log")
    log_file.parent.mkdir(exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def log_processing_event(item_id: str, event: str, details: Dict):
    """Log structured processing events"""
    logger = logging.getLogger("ctOS.processing")
    logger.info(f"Item {item_id}: {event} - {details}")


class DatabaseLogHandler(logging.Handler):
    """Custom handler for logging to database"""

    def __init__(self, db_path: str):
        super().__init__()
        self.db_path = db_path

    def emit(self, record):
        """Write log record to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO logs 
                    (timestamp, level, logger, message, item_id) 
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        record.created,
                        record.levelname,
                        record.name,
                        record.getMessage(),
                        getattr(record, "item_id", None),
                    ),
                )
        except Exception:
            # Don't let logging errors break the application
            pass

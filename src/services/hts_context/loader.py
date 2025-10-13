"""
HTS Reference Data Loader
"""

import json
import logging
from pathlib import Path
from typing import List, Dict
from .models import HTSItem
from .config import HTS_REFERENCE_PATH

logger = logging.getLogger(__name__)


class HTSReferenceLoader:
    """Loads and validates HTS reference data from JSON"""

    @staticmethod
    def load_hts_json(file_path: Path = HTS_REFERENCE_PATH) -> List[Dict]:
        """
        Load HTS reference data from JSON file

        Args:
            file_path: Path to HTS JSON file

        Returns:
            List of HTS item dictionaries

        Raises:
            FileNotFoundError: If JSON file doesn't exist
            json.JSONDecodeError: If JSON is malformed
            ValueError: If required fields are missing
        """
        # Validate file exists
        if not file_path.exists():
            raise FileNotFoundError(f"HTS reference file not found: {file_path}")

        logger.info(f"Loading HTS reference from: {file_path}")

        # Get file size for logging
        file_size_kb = file_path.stat().st_size / 1024
        logger.debug(f"File size: {file_size_kb:.2f} KB")

        # Load JSON
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                hts_data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            raise

        if not isinstance(hts_data, list):
            raise ValueError("HTS data must be a list of items")

        logger.info(f"Loaded {len(hts_data)} raw entries from JSON")

        # Filter out entries with empty htsno (section headers/dividers)
        hts_data = HTSReferenceLoader._filter_valid_entries(hts_data)
        logger.info(f"Filtered to {len(hts_data)} valid HTS codes")

        # Convert indent from string to int if necessary
        hts_data = HTSReferenceLoader._normalize_indent_types(hts_data)

        # Validate required fields
        HTSReferenceLoader._validate_required_fields(hts_data)

        # Check for duplicates
        HTSReferenceLoader._check_duplicates(hts_data)

        # Log indent distribution
        indent_dist = {}
        for item in hts_data:
            indent = item.get("indent", 0)
            indent_dist[indent] = indent_dist.get(indent, 0) + 1

        logger.debug(f"Indent distribution: {indent_dist}")

        return hts_data

    @staticmethod
    def _filter_valid_entries(hts_data: List[Dict]) -> List[Dict]:
        """
        Filter out invalid entries (empty htsno, section headers, etc.)

        Args:
            hts_data: List of raw HTS items

        Returns:
            List of valid HTS items only
        """
        valid_entries = []
        filtered_count = 0

        for item in hts_data:
            htsno = item.get("htsno", "").strip()

            # Skip entries with empty htsno
            if not htsno:
                filtered_count += 1
                continue

            # Skip entries that are clearly section headers (no actual code)
            # Valid HTS codes should have at least 4 digits
            if len(htsno) < 4:
                filtered_count += 1
                continue

            valid_entries.append(item)

        if filtered_count > 0:
            logger.debug(
                f"Filtered out {filtered_count} invalid entries (empty codes or section headers)"
            )

        return valid_entries

    @staticmethod
    def _normalize_indent_types(hts_data: List[Dict]) -> List[Dict]:
        """
        Convert indent values from string to integer if necessary

        Args:
            hts_data: List of HTS items

        Returns:
            List of HTS items with normalized indent values
        """
        for item in hts_data:
            if "indent" in item and isinstance(item["indent"], str):
                try:
                    item["indent"] = int(item["indent"])
                except ValueError:
                    logger.warning(
                        f"Could not convert indent to int for {item.get('htsno', 'unknown')}: {item['indent']}"
                    )
                    item["indent"] = 0  # Default to 0 if conversion fails

        return hts_data

    @staticmethod
    def _validate_required_fields(hts_data: List[Dict]) -> None:
        """
        Validate that all items have required fields

        Args:
            hts_data: List of HTS items

        Raises:
            ValueError: If required fields are missing
        """
        required_fields = ["htsno", "indent", "description"]

        for idx, item in enumerate(hts_data):
            missing_fields = [field for field in required_fields if field not in item]

            if missing_fields:
                raise ValueError(
                    f"Item at index {idx} missing required fields: {missing_fields}"
                )

            # Validate field types (indent should now be int after normalization)
            if not isinstance(item["htsno"], str):
                raise ValueError(f"Item at index {idx}: htsno must be string")

            if not isinstance(item["indent"], int):
                raise ValueError(f"Item at index {idx}: indent must be integer")

            if not isinstance(item["description"], str):
                raise ValueError(f"Item at index {idx}: description must be string")

            # Validate indent is non-negative
            if item["indent"] < 0:
                logger.warning(
                    f"Item at index {idx} has negative indent: {item['indent']}"
                )

    @staticmethod
    def _check_duplicates(hts_data: List[Dict]) -> None:
        """
        Check for duplicate HTS codes

        Args:
            hts_data: List of HTS items

        Raises:
            ValueError: If duplicate codes found
        """
        seen_codes = set()
        duplicates = []

        for item in hts_data:
            code = item["htsno"]
            if code in seen_codes:
                duplicates.append(code)
            seen_codes.add(code)

        if duplicates:
            raise ValueError(f"Duplicate HTS codes found: {duplicates}")

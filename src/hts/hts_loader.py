"""
Load HTS JSON reference data for ctOS Service 1
"""

import json
from pathlib import Path
from typing import Dict, List, Tuple

from ..utils.logger import setup_logger
from ..utils.exceptions import HTSLookupError

logger = setup_logger("ctOS.hts_loader")


def filter_chapter_73(hts_data: List[Dict]) -> List[Dict]:
    """Filter for Chapter 73 only"""
    chapter_73 = []
    for entry in hts_data:
        hts_code = entry.get("htsno", "")
        # Chapter 73 codes start with '73'
        if hts_code.startswith("73"):
            chapter_73.append(entry)

    logger.info(
        f"Filtered {len(chapter_73)} Chapter 73 entries from {len(hts_data)} total"
    )
    return chapter_73


def normalize_hts_entries(hts_data: List[Dict]) -> List[Dict]:
    """Standardize HTS entries"""
    normalized = []

    for entry in hts_data:
        normalized_entry = {
            "htsno": str(entry.get("htsno", "")).strip(),
            "description": str(entry.get("description", "")).strip(),
            "indent": int(entry.get("indent", 0)),
            "general": str(entry.get("general", "")).strip(),
            "special": str(entry.get("special", "")).strip(),
            "other": str(entry.get("other", "")).strip(),
            "units": entry.get("units", []),
        }
        normalized.append(normalized_entry)

    return normalized


class HTSLoader:
    """HTS JSON data loading"""

    def __init__(self, json_path: str):
        self.json_path = Path(json_path)
        self.hts_data = None

    def load_hts_data(self) -> List[Dict]:
        """Load HTS JSON file"""
        try:
            logger.info(f"Loading HTS data from {self.json_path}")

            if not self.json_path.exists():
                raise HTSLookupError(f"HTS JSON file not found: {self.json_path}")

            with open(self.json_path, "r", encoding="utf-8") as f:
                hts_data = json.load(f)

            # Handle different JSON structures
            if isinstance(hts_data, dict):
                # If JSON is wrapped in an object, extract the list
                if "data" in hts_data:
                    hts_data = hts_data["data"]
                elif "hts_codes" in hts_data:
                    hts_data = hts_data["hts_codes"]
                else:
                    # Try to find the first list value
                    for value in hts_data.values():
                        if isinstance(value, list):
                            hts_data = value
                            break

            if not isinstance(hts_data, list):
                raise HTSLookupError("HTS data must be a list of dictionaries")

            logger.info(f"Loaded {len(hts_data)} HTS entries")

            # Validate structure
            is_valid, errors = self.validate_hts_structure(hts_data)
            if not is_valid:
                logger.warning(f"HTS structure validation warnings: {errors}")

            # Normalize entries
            hts_data = normalize_hts_entries(hts_data)

            # Filter for Chapter 73 (optional, can be disabled if needed)
            # hts_data = filter_chapter_73(hts_data)

            self.hts_data = hts_data
            logger.info(f"HTS data loaded successfully: {len(self.hts_data)} entries")

            return self.hts_data

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format: {e}")
            raise HTSLookupError(f"Failed to parse HTS JSON: {e}")
        except Exception as e:
            logger.error(f"Failed to load HTS data: {e}")
            raise HTSLookupError(f"HTS data loading failed: {e}")

    def validate_hts_structure(self, hts_data: List[Dict]) -> Tuple[bool, List[str]]:
        """Validate JSON structure"""
        errors = []
        warnings = []

        if not hts_data:
            errors.append("HTS data is empty")
            return False, errors

        # Check required fields in sample entries
        required_fields = ["htsno", "description", "indent"]
        sample_size = min(10, len(hts_data))

        for i, entry in enumerate(hts_data[:sample_size]):
            if not isinstance(entry, dict):
                errors.append(f"Entry {i} is not a dictionary")
                continue

            for field in required_fields:
                if field not in entry:
                    warnings.append(f"Entry {i} missing field: {field}")

        # Check for reasonable data
        hts_codes = [e.get("htsno") for e in hts_data if "htsno" in e]
        unique_codes = len(set(hts_codes))

        if unique_codes < len(hts_codes) * 0.9:  # Less than 90% unique
            warnings.append(f"Possible duplicate HTS codes detected")

        if warnings:
            logger.warning(f"HTS validation warnings: {len(warnings)} issues")

        return len(errors) == 0, errors + warnings

    def get_hts_statistics(self, hts_data: List[Dict] = None) -> Dict:
        """HTS data statistics"""
        if hts_data is None:
            hts_data = self.hts_data

        if not hts_data:
            return {"error": "No HTS data loaded"}

        stats = {
            "total_entries": len(hts_data),
            "unique_codes": len(set(e["htsno"] for e in hts_data if "htsno" in e)),
            "indent_levels": {},
        }

        # Count by indent level
        for entry in hts_data:
            indent = entry.get("indent", 0)
            stats["indent_levels"][indent] = stats["indent_levels"].get(indent, 0) + 1

        # Chapter distribution
        chapters = {}
        for entry in hts_data:
            hts_code = entry.get("htsno", "")
            if len(hts_code) >= 2:
                chapter = hts_code[:2]
                chapters[chapter] = chapters.get(chapter, 0) + 1

        stats["chapters"] = chapters
        stats["total_chapters"] = len(chapters)

        return stats

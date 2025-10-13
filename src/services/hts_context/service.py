"""
HTS Context Service - Main API Implementation
"""

import logging
import json
from pathlib import Path
from typing import Dict, List, Optional
from .config import (
    HTS_REFERENCE_PATH,
    LOG_FILE,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    DEBUG_EXPORT_PATH,
)
from .loader import HTSReferenceLoader
from .hierarchy import HTSHierarchyBuilder
from .models import HTSContextResponse, HTSHierarchyPath, HTSStatistics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


class HTSContextService:
    """
    HTS Context Service - Provides hierarchical HTS classification context

    This service loads HTS reference data and provides fast lookups of
    hierarchical context for any HTS code.
    """

    def __init__(self, hts_file_path: Optional[Path] = None):
        """
        Initialize HTS Context Service

        Args:
            hts_file_path: Optional path to HTS JSON file
        """
        self.hts_file_path = hts_file_path or HTS_REFERENCE_PATH
        self.hts_items = []
        self.hierarchy_map = {}
        self.hierarchy_builder = HTSHierarchyBuilder()

        # Initialize service
        self._initialize()

    def _initialize(self) -> None:
        """Initialize service by loading data and building hierarchy"""
        logger.info("HTS Context Service initializing...")

        # Load HTS reference data
        loader = HTSReferenceLoader()
        self.hts_items = loader.load_hts_json(self.hts_file_path)

        # Build hierarchy map
        self.hierarchy_map = self.hierarchy_builder.build_hierarchy_map(self.hts_items)

        logger.info("HTS Context Service initialized successfully")

    def get_hts_context(self, hts_code: str) -> Dict:
        """
        Get full hierarchical context for an HTS code

        Args:
            hts_code: HTS code to look up

        Returns:
            Dictionary with hierarchy path from root to target code
        """
        logger.info(f"get_hts_context called: {hts_code}")

        # Check if code exists
        if hts_code not in self.hierarchy_map:
            logger.warning(f"HTS code not found: {hts_code}")
            return HTSContextResponse(
                hts_code=hts_code,
                found=False,
                hierarchy_path=[],
                error="HTS code not found in reference data",
            ).model_dump()

        # Walk up the hierarchy to build path
        hierarchy_path = []
        current_code = hts_code
        traversal_log = []

        while current_code is not None:
            traversal_log.append(current_code)

            # Get item info
            node = self.hierarchy_map[current_code]
            item = node["item"]

            hierarchy_path.append(
                HTSHierarchyPath(
                    code=current_code,
                    description=item["description"],
                    indent=item["indent"],
                )
            )

            # Move to parent
            current_code = node["parent"]

        # Reverse so root comes first
        hierarchy_path.reverse()

        logger.debug(f"Traversal: {' -> '.join(reversed(traversal_log))}")
        logger.debug(f"Returned {len(hierarchy_path)}-level hierarchy path")

        return HTSContextResponse(
            hts_code=hts_code, found=True, hierarchy_path=hierarchy_path
        ).model_dump()

    def validate_hts_code_exists(self, hts_code: str) -> bool:
        """
        Quick check if HTS code exists

        Args:
            hts_code: HTS code to validate

        Returns:
            True if code exists, False otherwise
        """
        return hts_code in self.hierarchy_map

    def get_hierarchy_statistics(self) -> Dict:
        """
        Get statistics about loaded HTS data

        Returns:
            Dictionary with statistics
        """
        # Calculate indent distribution
        indent_distribution = {}
        for node in self.hierarchy_map.values():
            indent = node["item"]["indent"]
            indent_distribution[indent] = indent_distribution.get(indent, 0) + 1

        # Get builder statistics
        builder_stats = self.hierarchy_builder.get_statistics()

        return HTSStatistics(
            total_codes=len(self.hierarchy_map),
            indent_distribution=indent_distribution,
            orphaned_codes=builder_stats["orphaned_codes"],
            parent_finding_stats=builder_stats["parent_finding_stats"],
        ).model_dump()

    def export_hierarchy_map(self, output_path: Optional[Path] = None) -> None:
        """
        Export hierarchy map to JSON for debugging

        Args:
            output_path: Optional path for export file
        """
        output_path = output_path or DEBUG_EXPORT_PATH

        logger.info(f"Exporting hierarchy map to: {output_path}")

        # Convert hierarchy map to serializable format
        export_data = {}
        for code, node in self.hierarchy_map.items():
            export_data[code] = {
                "item": node["item"],
                "parent": node["parent"],
                "children": node["children"],
            }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2)

        logger.info(f"Hierarchy map exported successfully")


# Convenience function for direct access
_service_instance = None


def get_hts_context(hts_code: str) -> Dict:
    """
    Convenience function to get HTS context

    Args:
        hts_code: HTS code to look up

    Returns:
        HTS context response dictionary
    """
    global _service_instance

    if _service_instance is None:
        _service_instance = HTSContextService()

    return _service_instance.get_hts_context(hts_code)

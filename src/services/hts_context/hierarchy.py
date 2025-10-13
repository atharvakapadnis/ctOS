"""
HTS Hierarchy Builder with Improved Parent-Finding Algorithm
"""

import logging
from typing import Dict, List, Optional, Set
from .models import HTSItem

logger = logging.getLogger(__name__)


class HTSHierarchyBuilder:
    """Builds HTS hierarchy map with parent-child relationships"""

    def __init__(self):
        self.orphaned_codes = []
        self.parent_finding_stats = {
            "prefix_matches": 0,
            "fallback_matches": 0,
            "failed": 0,
        }

    def build_hierarchy_map(self, hts_items: List[Dict]) -> Dict[str, Dict]:
        """
        Build complete hierarchy map with parent-child relationships

        Args:
            hts_items: List of HTS item dictionaries

        Returns:
            Dictionary mapping HTS codes to hierarchy information
        """
        logger.info("Building hierarchy map...")
        logger.debug(f"Processing {len(hts_items)} codes for parent assignment")

        # Initialize hierarchy map
        hierarchy_map = {}
        for item in hts_items:
            hts_code = item["htsno"]
            hierarchy_map[hts_code] = {"item": item, "parent": None, "children": []}

        # Find parents for each item
        for idx, item in enumerate(hts_items):
            hts_code = item["htsno"]
            indent_level = item["indent"]

            parent_code = self._find_parent_code(
                hts_code, indent_level, idx, hts_items, hierarchy_map
            )

            if parent_code:
                hierarchy_map[hts_code]["parent"] = parent_code
                hierarchy_map[parent_code]["children"].append(hts_code)
            elif indent_level > 0:
                # Orphaned code: has indent > 0 but no parent found
                self.orphaned_codes.append(hts_code)
                logger.warning(
                    f"Orphaned code: {hts_code} (indent={indent_level}, no parent found)"
                )

        logger.info("Hierarchy map built")
        logger.info(
            f"Parent-finding stats: {self.parent_finding_stats['prefix_matches']} prefix, "
            f"{self.parent_finding_stats['fallback_matches']} fallback, "
            f"{self.parent_finding_stats['failed']} failed"
        )

        return hierarchy_map

    def _find_parent_code(
        self,
        hts_code: str,
        indent_level: int,
        current_index: int,
        hts_items: List[Dict],
        hierarchy_map: Dict[str, Dict],
    ) -> Optional[str]:
        """
        Find parent code using improved algorithm

        Algorithm:
        1. Root Check: If indent_level == 0, return None
        2. Prefix Matching: Find candidates with indent = (indent_level - 1)
        - Check if hts_code starts with candidate code
        - Prefer longest match
        3. Positional Fallback with Prefix Check: If no candidates at target indent
        - Search backwards for items with lower indent
        - Prefer items that match prefix
        4. Pure Positional Fallback: If still no match
        - Use closest previous item with lower indent
        5. No Parent Found: Return None and log as orphaned

        Args:
            hts_code: Current HTS code
            indent_level: Current indent level
            current_index: Position in original array
            hts_items: Full list of HTS items
            hierarchy_map: Current hierarchy map being built

        Returns:
            Parent HTS code or None
        """
        # Step 1: Root Check
        if indent_level == 0:
            return None

        # Step 2: Find Parent Candidates (indent = indent_level - 1)
        target_indent = indent_level - 1
        parent_candidates = [
            item for item in hts_items if item["indent"] == target_indent
        ]

        # Step 3: Prefix Matching (Primary Method)
        # Try to find best matching parent by checking if code starts with candidate
        best_match = None
        best_match_length = 0

        for candidate in parent_candidates:
            candidate_code = candidate["htsno"]

            # Check if hts_code starts with candidate code
            if hts_code.startswith(candidate_code):
                # Prefer longer matches (more specific)
                if len(candidate_code) > best_match_length:
                    best_match = candidate_code
                    best_match_length = len(candidate_code)

        if best_match:
            logger.debug(f"Parent: {hts_code} -> {best_match} (prefix match)")
            self.parent_finding_stats["prefix_matches"] += 1
            return best_match

        # Step 4: Positional Fallback with Prefix Check (Secondary Method)
        # If no candidates at exact target indent, search for any lower indent with prefix match
        if not parent_candidates or not best_match:
            # Search backwards from current_index for any item with lower indent
            # First pass: Try to find prefix match with lower indent
            for i in range(current_index - 1, -1, -1):
                item = hts_items[i]
                if item["indent"] < indent_level:
                    candidate_code = item["htsno"]
                    # Check if this lower-indent item is a prefix match
                    if hts_code.startswith(candidate_code):
                        logger.debug(
                            f"Parent: {hts_code} -> {candidate_code} (fallback: prefix match at lower indent)"
                        )
                        self.parent_finding_stats["fallback_matches"] += 1
                        return candidate_code

        # Step 5: Pure Positional Fallback (Last Resort)
        # Only if no prefix matches found at all
        # This should only happen for truly orphaned codes
        for i in range(current_index - 1, -1, -1):
            item = hts_items[i]
            if item["indent"] < indent_level:
                fallback_code = item["htsno"]
                # Only use this if the code seems related (same chapter)
                # Extract first segment (chapter number)
                hts_chapter = (
                    hts_code.split(".")[0] if "." in hts_code else hts_code[:4]
                )
                fallback_chapter = (
                    fallback_code.split(".")[0]
                    if "." in fallback_code
                    else fallback_code[:4]
                )

                if hts_chapter == fallback_chapter:
                    logger.debug(
                        f"Parent: {hts_code} -> {fallback_code} (fallback: same chapter)"
                    )
                    self.parent_finding_stats["fallback_matches"] += 1
                    return fallback_code

        # Step 6: No Parent Found - Truly Orphaned
        logger.debug(f"No parent found for: {hts_code} (indent={indent_level})")
        self.parent_finding_stats["failed"] += 1
        return None

    def get_statistics(self) -> Dict:
        """Get statistics about hierarchy building"""
        return {
            "orphaned_codes": self.orphaned_codes,
            "parent_finding_stats": self.parent_finding_stats,
        }

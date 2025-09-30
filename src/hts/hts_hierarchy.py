"""
HTS Hierarchy Management and Navigation System for ctOS Service 1
Adapted from legacy system
"""

from typing import Dict, List, Optional, Set
from collections import defaultdict

from ..utils.logger import setup_logger
from ..utils.exceptions import HTSLookupError

logger = setup_logger("ctOS.hts_hierarchy")


class HTSHierarchy:
    """
    Manages HTS code hierarchy and provides navigation utilities
    Adapted from legacy system with interface modifications
    """

    def __init__(self, hts_data: List[Dict]):
        """
        Initialize with HTS reference data

        Args:
            hts_data: List of HTS code dictionaries from JSON
        """
        if not hts_data:
            raise HTSLookupError("HTS data cannot be empty")

        self.hts_data = hts_data
        self.codes_by_level = self._organize_by_level()
        self.hierarchy_map = self._build_hierarchy_map()
        self.description_map = self._build_description_map()

        logger.info(f"HTSHierarchy initialized with {len(hts_data)} codes")

    def _organize_by_level(self) -> Dict[int, List[Dict]]:
        """Organize HTS codes by indent level"""
        levels = defaultdict(list)
        for item in self.hts_data:
            levels[item["indent"]].append(item)

        # Sort each level by HTS number
        for level in levels:
            levels[level].sort(key=lambda x: x["htsno"])

        return dict(levels)

    def _build_hierarchy_map(self) -> Dict[str, Dict]:
        """Build mapping of parent-child relationships"""
        hierarchy = {}

        for item in self.hts_data:
            hts_code = item["htsno"]
            hierarchy[hts_code] = {"item": item, "children": [], "parent": None}

        # Build parent-child relationships
        for item in self.hts_data:
            hts_code = item["htsno"]
            parent_code = self._find_parent_code(hts_code, item["indent"])

            if parent_code and parent_code in hierarchy:
                hierarchy[hts_code]["parent"] = parent_code
                hierarchy[parent_code]["children"].append(hts_code)

        return hierarchy

    def _build_description_map(self) -> Dict[str, str]:
        """Build mapping of HTS codes to descriptions"""
        return {item["htsno"]: item["description"] for item in self.hts_data}

    def _find_parent_code(self, hts_code: str, indent_level: int) -> Optional[str]:
        """Find the parent HTS code for a given code"""
        if indent_level == 0:
            return None

        # For HTS codes with dots (e.g., 7301.10.00.00)
        if "." in hts_code:
            parts = hts_code.split(".")
            if len(parts) > 1:
                # Try progressively shorter parent codes
                for i in range(len(parts) - 1, 0, -1):
                    potential_parent = ".".join(parts[:i])
                    if any(item["htsno"] == potential_parent for item in self.hts_data):
                        return potential_parent

        # For 4-digit codes or codes without clear hierarchy
        return None

    def get_codes_by_level(self, level: int) -> List[Dict]:
        """Get all HTS codes at a specific indent level"""
        return self.codes_by_level.get(level, [])

    def get_children(self, hts_code: str) -> List[str]:
        """Get all child codes for a given HTS code"""
        return self.hierarchy_map.get(hts_code, {}).get("children", [])

    def get_parent(self, hts_code: str) -> Optional[str]:
        """Get parent code for a given HTS code"""
        return self.hierarchy_map.get(hts_code, {}).get("parent")

    def get_description(self, hts_code: str) -> Optional[str]:
        """Get description for a given HTS code"""
        return self.description_map.get(hts_code)

    def get_full_item(self, hts_code: str) -> Optional[Dict]:
        """Get complete HTS item information"""
        hierarchy_info = self.hierarchy_map.get(hts_code)
        return hierarchy_info["item"] if hierarchy_info else None

    def validate_hts_code(self, hts_code: str) -> bool:
        """Validate HTS code format and existence"""
        if not hts_code:
            return False

        # Check if code exists in our data
        return hts_code in self.description_map

    def get_path_to_root(self, hts_code: str) -> List[str]:
        """Get the full hierarchical path from code to root"""
        path = [hts_code]
        current_code = hts_code

        # Prevent infinite loops
        max_depth = 10
        depth = 0

        while depth < max_depth:
            parent = self.get_parent(current_code)
            if parent is None:
                break
            path.append(parent)
            current_code = parent
            depth += 1

        return list(reversed(path))  # Root first

    def get_classification_context(self, hts_code: str) -> Dict:
        """
        Get rich context for a specific HTS code including hierarchy

        Returns:
            Dictionary with code info, parents, children, and descriptions
        """
        item = self.get_full_item(hts_code)
        if not item:
            return {"error": f"HTS code {hts_code} not found", "code": hts_code}

        path = self.get_path_to_root(hts_code)
        children = self.get_children(hts_code)

        context = {
            "code": hts_code,
            "description": item["description"],
            "indent_level": item["indent"],
            "hierarchy_path": [
                {
                    "code": code,
                    "description": self.get_description(code),
                    "level": (
                        self.get_full_item(code)["indent"]
                        if self.get_full_item(code)
                        else 0
                    ),
                }
                for code in path
            ],
            "children_codes": [
                {"code": child, "description": self.get_description(child)}
                for child in children[:10]  # Limit to first 10 children
            ],
            "has_more_children": len(children) > 10,
            "total_children": len(children),
            "tariff_info": {
                "general": item.get("general", ""),
                "special": item.get("special", ""),
                "other": item.get("other", ""),
                "units": item.get("units", []),
            },
        }

        return context

    def find_similar_codes(
        self, search_terms: List[str], max_results: int = 10
    ) -> List[Dict]:
        """
        Find HTS codes with descriptions containing search terms

        Args:
            search_terms: List of terms to search for
            max_results: Maximum number of results to return

        Returns:
            List of matching HTS codes with relevance scores
        """
        matches = []
        search_terms_lower = [term.lower() for term in search_terms]

        for item in self.hts_data:
            description_lower = item["description"].lower()

            # Calculate relevance score
            score = 0
            matched_terms = []

            for term in search_terms_lower:
                if term in description_lower:
                    score += 1
                    matched_terms.append(term)

            if score > 0:
                matches.append(
                    {
                        "code": item["htsno"],
                        "description": item["description"],
                        "relevance_score": score,
                        "matched_terms": matched_terms,
                        "indent_level": item["indent"],
                    }
                )

        # Sort by relevance score (descending) and then by code
        matches.sort(key=lambda x: (-x["relevance_score"], x["code"]))

        return matches[:max_results]

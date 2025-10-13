"""
Pydantic models for HTS Context Service
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


class HTSItem(BaseModel):
    """Single HTS entry from JSON"""

    htsno: str = Field(..., description="HTS code")
    indent: int = Field(..., description="Hierarchy Level")
    description: str = Field(..., description="Official classification description")
    units: Optional[str] = None
    general: Optional[str] = None
    special: Optional[str] = None
    other: Optional[str] = None
    footnotes: Optional[str] = None
    quotaQuantity: Optional[str] = None
    additionalDuties: Optional[str] = None
    superior: Optional[str] = None


class HTSHierarchyPath(BaseModel):
    """One level in the hierarchy path"""

    code: str = Field(..., description="HTS code at this level")
    description: str = Field(..., description="Description at this level")
    indent: int = Field(..., description="Indent level")


class HTSContextResponse(BaseModel):
    """Response from get_hts_context()"""

    hts_code: str = Field(..., description="Requested HTS code")
    found: bool = Field(..., description="Whether code was found")
    hierarchy_path: List[HTSHierarchyPath] = Field(
        default_factory=list, description="Full hierarchy from root to code"
    )
    error: Optional[str] = Field(None, description="Error message if not found")


class HTSStatistics(BaseModel):
    """Statistics about loaded HTS data"""

    total_codes: int
    indent_distribution: Dict[int, int]
    orphaned_codes: List[str]
    parent_finding_stats: Dict[str, int]

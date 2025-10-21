"""
Pydantic models for Rules Management Service
"""

from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any


class Rule(BaseModel):
    """Single Rule Model"""

    rule_id: str
    rule_name: str
    rule_content: str
    rule_type: str
    active: bool
    created_at: Optional[str] = None
    description: Optional[str] = None

    @field_validator("rule_id")
    @classmethod
    def validate_rule_id(cls, v: str) -> str:
        import re
        from .config import RULE_ID_PATTERN

        if not re.match(RULE_ID_PATTERN, v):
            raise ValueError(f"Rule ID must match pattern R###: {v}")
        return v

    @field_validator("rule_type")
    @classmethod
    def validate_rule_type(cls, v: str) -> str:
        from .config import ALLOWED_RULE_TYPES

        if v not in ALLOWED_RULE_TYPES:
            raise ValueError(f"Rule type must be one of {ALLOWED_RULE_TYPES}")
        return v

    @field_validator("rule_name", "rule_content")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v or v.strip() == "":
            raise ValueError("Field cannot be empty")
        return v


class RuleSet(BaseModel):
    """Collection of Rules with metadata"""

    rules: List[Rule]
    metadata: Optional[Dict[str, Any]] = None


class ValidationResult(BaseModel):
    """Result of validating a single rule"""

    valid: bool
    rule_id: Optional[str] = None
    errors = List[str] = []
    warnings = List[str] = []


class ValidationReport(BaseModel):
    """Report of validating a rule set"""

    valid: bool
    total_rules: int
    valid_rules: int
    invalid_rules: int
    errors: List[str] = []
    warnings: List[str] = []
    duplicate_ids: List[str] = []

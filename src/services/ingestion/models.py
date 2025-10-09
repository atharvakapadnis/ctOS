"""
Data models for Data Ingestion Service
Pydantic V2 models for validation and type safety
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from datetime import datetime, timezone
import json


class ProductRecord(BaseModel):
    """Model for a single product record from CSV (immutable)"""

    model_config = ConfigDict(str_strip_whitespace=True)

    item_id: str
    item_description: str
    product_group: Optional[str] = None
    product_group_description: Optional[str] = None
    product_group_code: Optional[str] = None
    material_class: Optional[str] = None
    material_detail: Optional[str] = None
    manf_class: Optional[str] = None
    supplier_id: Optional[str] = None
    supplier_name: Optional[str] = None
    country_of_origin: Optional[str] = None
    import_type: Optional[str] = None
    port_of_delivery: Optional[str] = None
    final_hts: str
    hts_description: Optional[str] = None

    @field_validator("item_id", "item_description", "final_hts")
    @classmethod
    def required_fields_not_empty(cls, v: str) -> str:
        if not v or (isinstance(v, str) and v.strip() == ""):
            raise ValueError("Field cannot be empty")
        return v


class ProcessingResults(BaseModel):
    """Model for processing results (mutable)"""

    model_config = ConfigDict(str_strip_whitespace=True)

    item_id: str
    enhanced_description: Optional[str] = None
    confidence_score: Optional[str] = None  # Stored as text, e.g., "0.85"
    confidence_level: Optional[str] = None  # 'Low', 'Medium', 'High'
    extracted_customer_name: Optional[str] = None
    extracted_dimensions: Optional[str] = None
    extracted_product: Optional[str] = None
    rules_applied: Optional[str] = None  # JSON array as string
    last_processed_pass: Optional[str] = None  # Pass number as text
    last_processed_at: Optional[str] = None  # ISO 8601 timestamp

    @field_validator("confidence_level")
    @classmethod
    def validate_confidence_level(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in ["Low", "Medium", "High"]:
            raise ValueError(f"confidence_level must be Low, Medium, or High, got: {v}")
        return v

    @field_validator("confidence_score")
    @classmethod
    def validate_confidence_score(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                score = float(v)
                if not 0.0 <= score <= 1.0:
                    raise ValueError(
                        f"confidence_score must be between 0.0 and 1.0, got: {score}"
                    )
            except ValueError as e:
                raise ValueError(f"confidence_score must be a valid number: {e}")
        return v

    @field_validator("rules_applied")
    @classmethod
    def validate_rules_applied(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                rules = json.loads(v)
                if not isinstance(rules, list):
                    raise ValueError("rules_applied must be a JSON array")
            except json.JSONDecodeError as e:
                raise ValueError(f"rules_applied must be valid JSON: {e}")
        return v


class ValidationIssue(BaseModel):
    """Model for a single validation issue"""

    severity: str  # 'ERROR', 'WARNING', 'INFO'
    message: str
    row_number: Optional[int] = None
    column: Optional[str] = None
    value: Optional[str] = None


class ValidationReport(BaseModel):
    """Comprehensive validation report"""

    total_records: int

    # Column validation
    expected_columns: List[str]
    found_columns: List[str]
    missing_columns: List[str]
    extra_columns: List[str]

    # HTS validation
    valid_hts_count: int
    invalid_hts_count: int
    valid_hts_percentage: float
    sample_invalid_hts: List[Dict[str, Any]]  # [{'row': 42, 'value': '7301.10'}, ...]

    # Required fields validation
    null_counts: Dict[str, int]  # {column_name: null_count}
    rows_with_null_required_fields: List[int]
    complete_required_fields_count: int
    complete_required_fields_percentage: float

    # Duplicate detection
    duplicate_count: int
    duplicate_item_ids: List[
        Dict[str, Any]
    ]  # [{'item_id': 'X', 'rows': [1, 5, 9]}, ...]

    # Data completeness
    completeness_by_column: Dict[str, float]  # {column_name: percentage}
    low_completeness_columns: List[str]  # Columns with <80% completeness

    # Quality score
    quality_score: float
    quality_score_breakdown: Dict[
        str, float
    ]  # {'hts': 0.99, 'completeness': 0.99, 'duplicates': 1.0}

    # Issues
    critical_issues: List[ValidationIssue]
    warnings: List[ValidationIssue]

    validation_passed: bool
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class DatabaseStatistics(BaseModel):
    """Statistics about the database contents"""

    total_products: int
    processed_count: int
    unprocessed_count: int

    # Confidence distribution
    confidence_distribution: Dict[str, int]  # {'Low': 10, 'Medium': 50, 'High': 100}
    average_confidence_score: Optional[float] = None

    # HTS statistics
    unique_hts_codes: int

    # Pass distribution
    pass_distribution: Dict[str, int]  # {'1': 50, '2': 30, '3': 20}

    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class IntegrityReport(BaseModel):
    """Database integrity check report"""

    total_products: int
    total_processing_records: int

    # Foreign key integrity
    orphaned_processing_records: List[str]  # item_ids in processing but not in products
    products_without_processing: int

    # Data integrity
    null_item_ids_in_products: int
    null_item_ids_in_processing: int
    duplicate_item_ids_in_products: int
    duplicate_item_ids_in_processing: int

    # Value validation
    invalid_confidence_scores: List[
        Dict[str, Any]
    ]  # [{'item_id': 'X', 'value': 'bad'}]
    invalid_confidence_levels: List[Dict[str, Any]]

    integrity_passed: bool
    issues_found: List[str]
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class ProductWithProcessing(BaseModel):
    """Combined view of product and its processing results"""

    # Product fields
    item_id: str
    item_description: str
    product_group: Optional[str] = None
    product_group_description: Optional[str] = None
    product_group_code: Optional[str] = None
    material_class: Optional[str] = None
    material_detail: Optional[str] = None
    manf_class: Optional[str] = None
    supplier_id: Optional[str] = None
    supplier_name: Optional[str] = None
    country_of_origin: Optional[str] = None
    import_type: Optional[str] = None
    port_of_delivery: Optional[str] = None
    final_hts: str
    hts_description: Optional[str] = None

    # Processing fields (may all be None if not processed)
    enhanced_description: Optional[str] = None
    confidence_score: Optional[str] = None
    confidence_level: Optional[str] = None
    extracted_customer_name: Optional[str] = None
    extracted_dimensions: Optional[str] = None
    extracted_product: Optional[str] = None
    rules_applied: Optional[str] = None
    last_processed_pass: Optional[str] = None
    last_processed_at: Optional[str] = None


class UpdateProcessingInput(BaseModel):
    """Input model for updating processing results"""

    model_config = ConfigDict(str_strip_whitespace=True)

    enhanced_description: str
    confidence_score: str
    confidence_level: str
    extracted_customer_name: Optional[str] = None
    extracted_dimensions: Optional[str] = None
    extracted_product: str  # Required after processing
    rules_applied: str  # JSON array as string
    pass_number: str

    @field_validator("confidence_level")
    @classmethod
    def validate_confidence_level(cls, v: str) -> str:
        if v not in ["Low", "Medium", "High"]:
            raise ValueError(f"confidence_level must be Low, Medium, or High")
        return v

    @field_validator("confidence_score")
    @classmethod
    def validate_confidence_score(cls, v: str) -> str:
        try:
            score = float(v)
            if not 0.0 <= score <= 1.0:
                raise ValueError(f"confidence_score must be between 0.0 and 1.0")
        except ValueError:
            raise ValueError(f"confidence_score must be a valid number")
        return v

    @field_validator("rules_applied")
    @classmethod
    def validate_rules_applied(cls, v: str) -> str:
        try:
            rules = json.loads(v)
            if not isinstance(rules, list):
                raise ValueError("rules_applied must be a JSON array")
        except json.JSONDecodeError:
            raise ValueError("rules_applied must be valid JSON")
        return v

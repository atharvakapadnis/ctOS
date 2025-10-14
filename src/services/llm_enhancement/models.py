"""
Pydantic models for LLM Enhancement Service
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class ExtractedFeatures(BaseModel):
    """Extracted features from LLM response"""

    customer_name: Optional[str] = None
    dimensions: Optional[str] = None
    product: str  # Requried


class LLMResponse(BaseModel):
    """Parsed LLM output"""

    enhanced_description: str
    confidence_score: str  # String to match DATBASE type
    confidence_level: str  # 'Low', 'Medium', 'High'
    extracted_features: ExtractedFeatures


class BatchConfig(BaseModel):
    """Batch processing configuration"""

    batch_size: int = Field(default=100, ge=1, le=1000)
    pass_number: int = Field(default=1, ge=1)
    selected_item_ids: Optional[List[str]] = None


class ProcessingError(BaseModel):
    """Error tracking for individual product processing"""

    item_id: str
    error_type: str
    error_message: str
    timestamp: datetime = Field(default_factory=datetime.now)


class ProductResult(BaseModel):
    """Result for single product processing"""

    item_id: str
    success: bool
    confidence_level: Optional[str] = None
    confidence_score: Optional[str] = None
    error: Optional[str] = None


class BatchResult(BaseModel):
    """Result for batch processing"""

    pass_number: int
    batch_size: int
    total_processed: int
    successful: int
    failed: int
    success_rate: float
    confidence_distribution: Dict[str, int] = Field(
        default_factory=lambda: {"Low": 0, "Medium": 0, "High": 0}
    )
    processing_time: float
    avg_time_per_product: float
    results: List[ProductResult]


class BatchStatistics(BaseModel):
    """Statistics tracked during batch processing"""

    start_time: datetime = Field(default_factory=datetime.now)
    processed_count: int = 0
    successful_count: int = 0
    failed_count: int = 0
    confidence_distribution: Dict[str, int] = Field(
        default_factory=lambda: {"Low": 0, "Medium": 0, "High": 0}
    )
    error: List[str] = Field(default_factory=list)

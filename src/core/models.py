# src/core/models.py
"""
Data models for ctOS Service 1
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class Product:
    """Product data model"""

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
    final_hts: Optional[str] = None
    hts_description: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class ProcessingStatus:
    """Processing tracking model"""

    item_id: str
    status: str = "pending"  # 'pending', 'processing', 'completed', 'failed'
    confidence_level: Optional[str] = None  # 'Low', 'Medium', 'High'
    last_processed: Optional[datetime] = None
    processing_attempts: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class ProcessingHistory:
    """Processing audit model"""

    history_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    item_id: str = ""
    processing_attempt: int = 0
    status: str = ""
    error_message: Optional[str] = None
    processed_at: datetime = field(default_factory=datetime.now)

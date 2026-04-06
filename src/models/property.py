"""Property model chuẩn dùng xuyên suốt ingestion, CDC và analytics layers."""

from dataclasses import dataclass


@dataclass(slots=True)
class PropertyRecord:
    """Schema property đã normalize sau ingestion và trước transformation."""

    property_id: str
    city: str
    district: str
    price: float
    area_sqm: float
    source_url: str

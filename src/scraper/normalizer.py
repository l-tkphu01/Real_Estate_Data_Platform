"""Contract normalize raw record từ API Chợ Tốt có Validate bằng Pydantic."""

from typing import Any
from pydantic import BaseModel, Field, ValidationError

class RealEstateAd(BaseModel):
    """Xây dựng Data Contract: Bắt buộc cục JSON trả về phải đúng cấu trúc này."""
    list_id: int | str = Field(alias="list_id")
    subject: str = Field(alias="subject", default="Không tiêu đề")
    region_name: str = Field(alias="region_name", default="Unknown")
    area_name: str = Field(alias="area_name", default="Unknown")
    price: float | str = Field(alias="price", default=0.0)
    size: float | str = Field(alias="size", default=0.0)
    rooms: int | str = Field(alias="rooms", default=0)
    list_time: int = Field(alias="list_time", default=0)


def normalize_raw_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    """Normalize payload chằng chịt của Chợ Tốt về schema chuẩn (canonical model)."""
    
    try:
        # 1. Pydantic Validation: Lọc bỏ rác, tự động ép kiểu ngay từ cửa ngõ
        validated_data = RealEstateAd(**raw_record)
        
        # 2. Convert Epoch Time (milliseconds) của Chợ Tốt sang chuẩn ISO 8601 (yyyy-MM-ddTHH:mm:ssZ)
        from datetime import datetime, timezone
        timestamp_s = validated_data.list_time / 1000.0 if validated_data.list_time > 10**10 else validated_data.list_time
        posted_dt = datetime.fromtimestamp(timestamp_s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # 3. Ánh xạ sang Schema nội bộ (Canonical Model)
        return {
            "property_id": str(validated_data.list_id),
            "title": str(validated_data.subject).strip(),
            "city": str(validated_data.region_name).strip(),
            "district": str(validated_data.area_name).strip(),
            "price": float(validated_data.price),
            "area_sqm": float(validated_data.size),
            "bedrooms": int(validated_data.rooms) if str(validated_data.rooms).isdigit() else 0,
            "posted_at": posted_dt
        }
    except ValidationError:
        # Nếu Cấu trúc bị sai (khi API bị đổi) => Tự động trả về dict rỗng.
        # Ở Pipeline Ops sẽ kiểm tra và loại bỏ.
        return {}

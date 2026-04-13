"""Contract normalize raw record từ API Chợ Tốt có Validate bằng Pydantic."""

import logging
from typing import Any
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

class BaseAd(BaseModel):
    """Xây dựng Data Contract chung: Bắt buộc cục JSON trả về phải đúng cấu trúc."""
    list_id: int | str = Field(alias="list_id")
    cg: int | str = Field(alias="cg", default=1000)  # Lấy category id để factory pattern
    subject: str = Field(alias="subject", default="Không tiêu đề")
    region_name: str = Field(alias="region_name", default="Unknown")
    area_name: str = Field(alias="area_name", default="Unknown")
    price: float | str = Field(alias="price", default=0.0)
    list_time: int = Field(alias="list_time", default=0)

class RealEstateAd(BaseAd):
    """Schema riêng cho Bất Động Sản (Category 1000)"""
    size: float | str = Field(alias="size", default=0.0)
    rooms: int | str = Field(alias="rooms", default=0)

class VehicleAd(BaseAd):
    """Schema riêng cho Xe Cộ (Category 1001)"""
    brand: str = Field(alias="carbrand", default="Unknown")     # Brand xe
    year: int | str = Field(alias="mfdate", default=0)          # Năm sx
    mileage: int | str = Field(alias="mileage_v2", default=0)   # Số km

class ElectronicsAd(BaseAd):
    """Schema riêng cho Điện Thoại & Điện Tử (Category 1002)"""
    brand: str = Field(alias="elt_brand", default="Unknown")
    model: str = Field(alias="elt_model", default="Unknown")
    storage: str = Field(alias="storage", default="Unknown")

def normalize_raw_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    """Normalize payload chằng chịt của Chợ Tốt về schema chuẩn theo Factory Pattern."""
    
    try:
        # Xác định Category ID để Switch Model
        cg = int(raw_record.get("cg", 1000))
        
        # 1. Pydantic Validation tuỳ category
        if cg == 1001:
            validated_data = VehicleAd(**raw_record)
            record_type = "vehicle"
        elif cg == 1002:
            validated_data = ElectronicsAd(**raw_record)
            record_type = "electronics"
        else:
            validated_data = RealEstateAd(**raw_record)
            record_type = "property"
        
        # 2. Convert Epoch Time (milliseconds) của Chợ Tốt sang chuẩn ISO 8601 (yyyy-MM-ddTHH:mm:ssZ)
        from datetime import datetime, timezone
        timestamp_s = validated_data.list_time / 1000.0 if validated_data.list_time > 10**10 else validated_data.list_time
        posted_dt = datetime.fromtimestamp(timestamp_s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # 3. Ánh xạ các trường chung (Base Model)
        base_dict = {
            "property_id": str(validated_data.list_id), # Giữ nguyên tên cột 'property_id' để tránh gãy Spark pipeline bên dưới
            "category_id": cg,
            "record_type": record_type,
            "title": str(validated_data.subject).strip(),
            "city": str(validated_data.region_name).strip(),
            "district": str(validated_data.area_name).strip(),
            "price": float(validated_data.price),
            "posted_at": posted_dt
        }
        
        # 4. Ánh xạ các trường đặc thù theo từng category
        if isinstance(validated_data, RealEstateAd):
            base_dict.update({
                "area_sqm": float(validated_data.size),
                "bedrooms": int(validated_data.rooms) if str(validated_data.rooms).isdigit() else 0
            })
        elif isinstance(validated_data, VehicleAd):
            base_dict.update({
                "brand": str(validated_data.brand).strip(),
                "manufacture_year": int(validated_data.year) if str(validated_data.year).isdigit() else 0,
                "mileage": int(validated_data.mileage) if str(validated_data.mileage).isdigit() else 0
            })
        elif isinstance(validated_data, ElectronicsAd):
            base_dict.update({
                "brand": str(validated_data.brand).strip(),
                "model": str(validated_data.model).strip(),
                "storage": str(validated_data.storage).strip()
            })

        return base_dict
        
    except ValidationError:
        # Nếu Cấu trúc bị sai (khi API bị đổi) => Tự động trả về dict rỗng.
        # Ở Pipeline Ops sẽ kiểm tra và loại bỏ.
        return {}
    except Exception as e:
        logger.warning(f"Lỗi không mong đợi khi normalize raw record: {e}")
        return {}

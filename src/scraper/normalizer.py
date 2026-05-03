"""Contract normalize raw record từ API Chợ Tốt — chỉ hỗ trợ Bất Động Sản.

Validate bằng Pydantic. Chỉ hỗ trợ Bất Động Sản (category 1000).
"""

import logging
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)


class RealEstateAd(BaseModel):
    """Data Contract cho tin đăng Bất Động Sản (Category 1000).

    Bắt buộc cục JSON trả về phải đúng cấu trúc này,
    nếu API thay đổi schema thì Pydantic sẽ reject và log warning.
    """

    list_id: int | str = Field(alias="list_id")
    subject: str = Field(alias="subject", default="Không tiêu đề")
    region_name: str = Field(alias="region_name", default="Unknown")
    area_name: str = Field(alias="area_name", default="Unknown")
    price: float | str = Field(alias="price", default=0.0)
    list_time: int = Field(alias="list_time", default=0)
    size: float | str = Field(alias="size", default=0.0)
    rooms: int | str = Field(alias="rooms", default=0)
    cg_name: str = Field(alias="category_name", default="Khác")


def normalize_raw_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    """Normalize payload từ API Chợ Tốt về schema chuẩn cho Bất Động Sản.

    Returns:
        dict chuẩn hóa nếu hợp lệ, dict rỗng nếu validation fail.
    """
    try:
        # 1. Pydantic Validation — chỉ Bất Động Sản
        validated = RealEstateAd(**raw_record)

        # 2. Convert Epoch Time (ms) sang ISO 8601
        timestamp_s = (
            validated.list_time / 1000.0
            if validated.list_time > 10**10
            else validated.list_time
        )
        posted_dt = datetime.fromtimestamp(
            timestamp_s, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        # 3. Ánh xạ về schema chuẩn pipeline
        return {
            "property_id": str(validated.list_id),
            "title": str(validated.subject).strip(),
            "city": str(validated.region_name).strip(),
            "district": str(validated.area_name).strip(),
            "price": float(validated.price),
            "area_sqm": float(validated.size),
            "bedrooms": (
                int(validated.rooms)
                if str(validated.rooms).isdigit()
                else 0
            ),
            "posted_at": posted_dt,
            "source_category": str(validated.cg_name).strip(),
        }

    except ValidationError:
        # Schema sai (API bị đổi) → trả dict rỗng, pipeline ops sẽ filter
        return {}
    except Exception as e:
        logger.warning(f"Lỗi không mong đợi khi normalize raw record: {e}")
        return {}

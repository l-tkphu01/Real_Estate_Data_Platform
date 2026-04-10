"""Contract normalize raw record.

File này ánh xạ payload không đồng nhất từ source về một schema chung (canonical model).
"""

from typing import Any

def normalize_raw_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    """Normalize một raw property record về canonical field names."""
    
    # 1. Trích xuất theo format JSON mà ta đã cào lấy từ API
    _id = raw_record.get("id", "")
    _title = raw_record.get("title", "")
    _city = raw_record.get("city", "")
    _district = raw_record.get("district", "")
    _price_vnd = float(raw_record.get("price_vnd", 0.0))
    _area = float(raw_record.get("area", 0.0))
    _bedrooms = int(raw_record.get("bedrooms", 0))
    _created_at = raw_record.get("created_at", "")

    # 2. Xử lý chuyển đổi kiểu dữ liệu (nếu cần thiết) và ánh xạ về Schema chuẩn
    return {
        "property_id": str(_id),
        "title": str(_title).strip(),
        "city": str(_city).strip(),
        "district": str(_district).strip(),
        "price": _price_vnd,
        "area_sqm": _area,
        "bedrooms": _bedrooms,
        "posted_at": str(_created_at)
    }

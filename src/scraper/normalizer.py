"""Contract normalize raw record.

File này ánh xạ payload không đồng nhất từ source về một schema chung.
"""

from typing import Any


def normalize_raw_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    """Normalize một raw property record về canonical field names.

    TODO:
    - Chuẩn hóa keys (property_id, city, district, price, area_sqm).
    - Parse number/text/date values về format nhất quán.
    """

    raise NotImplementedError("Implement in build phase")

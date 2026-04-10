"""Data quality checks cho mandatory fields và value ranges."""

from typing import Any


def validate_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Tách records thành tập valid và invalid.

    TODO:
    - Kiểm tra required fields (property_id, city, price).
    - Validate numeric constraints (price > 0, area > 0).
    """
    valid: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []

    required_fields = ("property_id", "city", "district", "price", "area_sqm", "bedrooms", "posted_at")
    for record in records:
        if not all(record.get(field) not in {None, ""} for field in required_fields):
            invalid.append({**record, "_dq_reason": "missing_required_fields"})
            continue

        try:
            price = float(record["price"])
            area_sqm = float(record["area_sqm"])
            bedrooms = int(record["bedrooms"])
        except (TypeError, ValueError):
            invalid.append({**record, "_dq_reason": "type_cast_failed"})
            continue

        if price <= 0 or area_sqm <= 0 or bedrooms < 0:
            invalid.append({**record, "_dq_reason": "numeric_constraint_failed"})
            continue

        valid.append(record)

    return valid, invalid

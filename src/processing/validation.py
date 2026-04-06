"""Data quality checks cho mandatory fields và value ranges."""

from typing import Any


def validate_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Tách records thành tập valid và invalid.

    TODO:
    - Kiểm tra required fields (property_id, city, price).
    - Validate numeric constraints (price > 0, area > 0).
    """

    raise NotImplementedError("Implement in build phase")

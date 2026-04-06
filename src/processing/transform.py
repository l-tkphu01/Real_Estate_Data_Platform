"""Contract transformation cho analytics-ready schema."""

from typing import Any


def transform_for_silver(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ánh xạ cleaned records sang silver layer schema."""

    raise NotImplementedError("Implement in build phase")


def transform_for_gold(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Tổng hợp silver records thành BI-ready gold datasets."""

    raise NotImplementedError("Implement in build phase")

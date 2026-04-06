"""Contract analytics aggregations dùng cho BI và dashboards."""

from typing import Any


def build_price_trend_dataset(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Tạo time-series aggregates cho xu hướng giá."""

    raise NotImplementedError("Implement in build phase")


def build_location_comparison_dataset(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Tạo aggregates so sánh city/district cho dashboarding."""

    raise NotImplementedError("Implement in build phase")

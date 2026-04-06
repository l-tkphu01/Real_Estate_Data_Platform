"""Contract cleaning để xử lý nulls, text normalization và typing."""

from typing import Any


def clean_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Clean và chuẩn hóa records trước khi nạp vào curated layers."""

    raise NotImplementedError("Implement in build phase")

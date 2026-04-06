"""Ingestion client với contract retry và error handling."""

from typing import Any


def fetch_raw_records() -> list[dict[str, Any]]:
    """Lấy raw property records từ external source.

    TODO:
    - Triển khai HTTP/API requests hoặc scraping logic.
    - Thêm retry với exponential backoff.
    - Xử lý timeout và các trường hợp response sai định dạng.
    """

    raise NotImplementedError("Implement in build phase")

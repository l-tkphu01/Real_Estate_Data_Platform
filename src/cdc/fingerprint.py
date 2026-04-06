"""CDC logic dựa trên fingerprint.

File này tính deterministic fingerprint (ví dụ: property_id+price) và so sánh
với state trước đó.
"""

from typing import Any


def compute_fingerprint(record: dict[str, Any]) -> str:
    """Tính fingerprint ổn định cho một property record."""

    raise NotImplementedError("Implement in build phase")


def detect_changes(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Chỉ trả về records mới hoặc updated kể từ lần chạy trước."""

    raise NotImplementedError("Implement in build phase")

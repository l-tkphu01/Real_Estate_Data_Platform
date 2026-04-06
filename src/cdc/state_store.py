"""Contract lưu trữ CDC state.

Module này lưu và đọc các fingerprint gần nhất trong object storage.
"""

from typing import Any


def load_cdc_state() -> dict[str, str]:
    """Nạp historical fingerprints theo khóa property_id."""

    raise NotImplementedError("Implement in build phase")


def save_cdc_state(state: dict[str, str]) -> None:
    """Lưu fingerprints đã cập nhật sau mỗi run thành công."""

    raise NotImplementedError("Implement in build phase")

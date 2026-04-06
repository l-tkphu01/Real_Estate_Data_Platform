"""Contract raw storage cho immutable ingestion snapshots."""

from typing import Any


def build_raw_object_key() -> str:
    """Tạo partitioned key cho raw files.

    TODO:
    - Partition theo ingestion date/hour.
    - Bao gồm unique run identifier.
    """

    raise NotImplementedError("Implement in build phase")


def save_raw_snapshot(records: list[dict[str, Any]]) -> str:
    """Lưu raw batch và trả về object key để lineage tracking."""

    raise NotImplementedError("Implement in build phase")

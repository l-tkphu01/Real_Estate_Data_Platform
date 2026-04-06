"""Contract Delta Lake write/upsert.

Module này chịu trách nhiệm cho ACID writes, schema evolution và merge logic.
"""

from typing import Any


def upsert_bronze(records: list[dict[str, Any]]) -> None:
    """Merge incremental records vào bronze Delta table."""

    raise NotImplementedError("Implement in build phase")


def upsert_silver(records: list[dict[str, Any]]) -> None:
    """Merge transformed records vào silver Delta table."""

    raise NotImplementedError("Implement in build phase")


def overwrite_gold(records: list[dict[str, Any]]) -> None:
    """Làm mới analytics snapshot table trong gold layer."""

    raise NotImplementedError("Implement in build phase")

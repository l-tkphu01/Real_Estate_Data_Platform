"""Contract Delta Lake write/upsert bằng PySpark trên Azure."""
from __future__ import annotations

from typing import Any

from delta.tables import DeltaTable


def _overwrite_delta(df: Any, target_path: str) -> None:
    """Ghi đè Delta table để đảm bảo dữ liệu deterministic cho local dev."""
    (  
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )


def _upsert_delta(spark: Any, df: Any, target_path: str, key_columns: list[str]) -> None:
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        merge_condition = " AND ".join([f"t.{col} = s.{col}" for col in key_columns])
        (
            delta_table.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

def upsert_silver(spark: Any, df: Any, target_path: str) -> None:
    """Merge transformed records vào silver Delta table."""
    _upsert_delta(spark, df, target_path, key_columns=["property_id"])

def overwrite_gold(spark: Any, df: Any, target_path: str) -> None:
    """Làm mới analytics snapshot table trong gold layer."""
    _overwrite_delta(df, target_path)

"""Contract Delta Lake write/upsert bằng PySpark trên Azure."""
from typing import Any

def upsert_bronze(spark: Any, df: Any, target_path: str) -> None:
    """Merge incremental records vào bronze Delta table."""
    raise NotImplementedError("Implement Delta merge bằng pyspark")

def upsert_silver(spark: Any, df: Any, target_path: str) -> None:
    """Merge transformed records vào silver Delta table."""
    raise NotImplementedError("Implement Delta merge bằng pyspark")

def overwrite_gold(spark: Any, df: Any, target_path: str) -> None:
    """Làm mới analytics snapshot table trong gold layer."""
    raise NotImplementedError("Implement Delta overwrite bằng pyspark")

"""Contract Delta Lake read/write cho Star Schema Warehouse.

Ghi và đọc các bảng Dim/Fact dạng Delta format trong lakehouse.
Tái sử dụng pattern từ src/lakehouse/delta_writer.py.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Prefix cho warehouse layer (ngang hàng với bronze/silver/gold)
WAREHOUSE_PREFIX = "warehouse"


def _warehouse_path(table_name: str, settings: Any) -> str:
    """Xây dựng đường dẫn Delta cho bảng warehouse dựa trên profile.
    
    Trên local: data/lakehouse/warehouse/<table_name>
    Trên cloud: abfss://<container>@<account>/<warehouse_prefix>/<table_name>
    """
    if settings.runtime.profile == "local":
        target = PROJECT_ROOT / "data" / "lakehouse" / WAREHOUSE_PREFIX / table_name
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target)
    else:
        return (
            f"abfss://{settings.storage.azure_container}"
            f"@{settings.azure_storage_account}.dfs.core.windows.net"
            f"/{WAREHOUSE_PREFIX}/{table_name}"
        )


def write_dimension(spark: Any, df: Any, table_name: str, settings: Any) -> str:
    """Ghi (overwrite) bảng Dimension dạng Delta.
    
    Dimension tables luôn overwrite toàn bộ vì:
    - Kích thước nhỏ (< 10K rows)
    - Cần rebuild surrogate key nhất quán với fact tables
    
    Returns:
        Đường dẫn Delta đã ghi.
    """
    target = _warehouse_path(table_name, settings)
    
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(target)
    
    logger.info(f"[SUCCESS] Đã ghi dimension [{table_name}] → {target} ({df.count()} rows)")
    return target


def write_fact(
    spark: Any,
    df: Any,
    table_name: str,
    settings: Any,
    key_columns: list[str] | None = None,
) -> str:
    """Ghi bảng Fact dạng Delta với chiến lược UPSERT (merge).
    
    - Lần đầu: overwrite (tạo table mới)
    - Lần sau: merge on key_columns
    
    Args:
        spark: SparkSession.
        df: DataFrame fact records.
        table_name: Tên bảng (vd: "fact_listing").
        settings: Config settings.
        key_columns: Danh sách cột dùng cho merge condition.
                     Nếu None, sẽ overwrite toàn bộ.
    
    Returns:
        Đường dẫn Delta đã ghi.
    """
    target = _warehouse_path(table_name, settings)
    
    if key_columns and DeltaTable.isDeltaTable(spark, target):
        # UPSERT: merge on natural key
        delta_table = DeltaTable.forPath(spark, target)
        merge_condition = " AND ".join(
            [f"t.{col} = s.{col}" for col in key_columns]
        )
        (
            delta_table.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(
            f"[SUCCESS] Đã MERGE fact [{table_name}] on {key_columns} → {target}"
        )
    else:
        # First write hoặc full overwrite
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(target)
        logger.info(
            f"[SUCCESS] Đã OVERWRITE fact [{table_name}] → {target} ({df.count()} rows)"
        )
    
    return target


def read_dimension(spark: Any, table_name: str, settings: Any) -> Any:
    """Đọc bảng Dimension từ Delta Lake.
    
    Returns:
        DataFrame hoặc None nếu table chưa tồn tại.
    """
    target = _warehouse_path(table_name, settings)
    
    if not DeltaTable.isDeltaTable(spark, target):
        logger.warning(f"[WARN] Dimension [{table_name}] chưa tồn tại tại {target}")
        return None
    
    return spark.read.format("delta").load(target)

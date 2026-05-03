"""Contract Delta Lake write/upsert bằng PySpark trên Azure."""
from __future__ import annotations

import logging
from typing import Any

from delta.tables import DeltaTable
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


def _overwrite_delta(df: Any, target_path: str) -> None:
    """Ghi đè Delta table để đảm bảo dữ liệu deterministic cho local dev."""
    (  
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )


def _append_delta(df: Any, target_path: str, partition_cols: list[str] | None = None) -> None:
    """Append dữ liệu mới vào Delta table (giữ nguyên dữ liệu cũ).
    
    Dùng cho Gold layer để tích lũy snapshot lịch sử theo thời gian.
    """
    writer = df.write.format("delta").mode("append")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(target_path)


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

def append_bronze(spark: Any, df: Any, target_path: str) -> None:
    """Append Raw JSON vào bảng Bronze Delta Lake.
    Sử dụng partition theo ngày ingest để tối ưu tốc độ đọc lịch sử phân tán.
    """
    df_with_ingest_date = df.withColumn(
        "ingest_date", 
        F.to_date(F.current_timestamp()).cast("string")
    )
    
    (
        df_with_ingest_date.write.format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .save(target_path)
    )
    logger.info(f"✅ Đã Append dữ liệu thô vào Bronze Delta (partitioned by ingest_date)")

def upsert_silver(spark: Any, df: Any, target_path: str) -> None:
    """Merge transformed records vào silver Delta table."""
    _upsert_delta(spark, df, target_path, key_columns=["property_id"])

def append_gold(spark: Any, df: Any, target_path: str) -> None:
    """Append snapshot mới vào Gold Delta table (giữ lịch sử).
    
    Chiến lược: APPEND + MERGE trên (city, district, snapshot_date)
    - Nếu cùng ngày đã có snapshot → cập nhật (tránh duplicate trong ngày)
    - Nếu ngày mới → thêm snapshot mới (tích lũy lịch sử)
    
    Cột snapshot_date được tự động trích từ snapshot_at để partition.
    """
    # Thêm cột snapshot_date để partition + dedup theo ngày
    df_with_date = df.withColumn(
        "snapshot_date",
        F.to_date(F.to_timestamp(F.col("snapshot_at"))).cast("string")
    )
    
    if DeltaTable.isDeltaTable(spark, target_path):
        # MERGE: cùng (city, district, snapshot_date) → update, mới → insert
        delta_table = DeltaTable.forPath(spark, target_path)
        merge_condition = (
            "t.city = s.city AND t.district = s.district "
            "AND t.snapshot_date = s.snapshot_date"
        )
        (
            delta_table.alias("t")
            .merge(df_with_date.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(
            f"✅ Gold MERGE thành công (giữ lịch sử, update snapshot cùng ngày)"
        )
    else:
        # Lần đầu: ghi mới với partition
        (
            df_with_date.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("snapshot_date")
            .save(target_path)
        )
        logger.info(f"✅ Gold table tạo mới (partitioned by snapshot_date)")


def overwrite_gold(spark: Any, df: Any, target_path: str) -> None:
    """[DEPRECATED] Ghi đè toàn bộ Gold — chỉ giữ lại cho backward compatibility.
    
    Khuyến nghị: Dùng append_gold() để giữ lịch sử snapshot.
    """
    logger.warning(
        "⚠️ overwrite_gold() đã deprecated. Dùng append_gold() để giữ lịch sử."
    )
    _overwrite_delta(df, target_path)

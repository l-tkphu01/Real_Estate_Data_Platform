"""Ops cho processing stage.

Luồng kỳ vọng:
1) validate records
2) clean và normalize
3) build silver và gold datasets
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dagster import op

from src.lakehouse.delta_writer import append_gold, upsert_silver, append_bronze
from src.processing.cleaning import clean_records
from src.processing.transform import transform_for_gold, transform_for_silver
from src.processing.validation import validate_records
import pyspark.sql.functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _delta_path(prefix: str, settings: Any) -> str:
    """Xây dựng đường dẫn Delta Lake linh hoạt dựa trên config driven.
    Trên local thì lưu vào thư mục dự án, trên cloud (hoặc Databricks) thì dùng abfss://.
    """
    import os
    # Khi dùng Databricks Connect hoặc Databricks Cluster → luôn dùng đường dẫn ABFSS
    if os.getenv("DATABRICKS_HOST") or "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return f"abfss://{settings.storage.azure_container}@{settings.azure_storage_account}.dfs.core.windows.net/{prefix}"
    
    if "local" in settings.runtime.profile:
        # Chạy ở bất kì profile local nào cũng ghi trực tiếp xuống file disk trong volume 
        # (DeltaLake / PySpark đọc file local nhanh và ổn định hơn rất nhiều so với ABFS Azurite giả lập)
        target = PROJECT_ROOT / "data" / "lakehouse" / prefix
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target)
    else:
        # Đường dẫn Cloud Azure (abfss://) dựa trên config-driven
        return f"abfss://{settings.storage.azure_container}@{settings.azure_storage_account}.dfs.core.windows.net/{prefix}"


@op(required_resource_keys={"settings", "storage"}) # Cần storage để đọc raw snapshot, settings để log và config
def op_load_latest_raw_records(context) -> list[dict[str, Any]]:
    """Đọc snapshot raw mới nhất từ Azurite/Azure để xử lý downstream."""
    settings = context.resources.settings 
    storage = context.resources.storage 

    latest_key = storage.get_latest_key(settings.storage.raw_prefix) # Tìm blob mới nhất trong prefix raw/ để nạp dữ liệu
    if not latest_key:
        context.log.warning("Không tìm thấy raw snapshot để xử lý.")
        return []

    payload = storage.get_json(latest_key)
    if not isinstance(payload, list):
        context.log.warning("Payload raw không đúng định dạng list records.")
        return []

    records = [item for item in payload if isinstance(item, dict)]
    context.log.info(f"Đã nạp {len(records)} raw records từ blob: {latest_key}")
    return records


@op(required_resource_keys={"settings", "spark"})
def op_write_bronze_delta(context, raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ghi trực tiếp raw data JSON vào Bronze Delta Lake để lưu trữ lịch sử thô vĩnh viễn."""
    if not raw_records:
        return []
        
    spark = context.resources.spark
    settings = context.resources.settings
    
    df = spark.createDataFrame(raw_records)
    
    # Ép tất cả thành chữ (String) để bảng Bronze vĩnh viễn không bị lỗi schema mâu thuẫn (Schema Evolution)
    for col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast("string"))
        
    target_path = _delta_path(settings.storage.bronze_prefix, settings)
    append_bronze(spark, df, target_path)
    context.log.info(f"[SUCCESS] Đã lưu trữ lịch sử cục raw vào Bronze layer tại: {target_path}")
    
    # Push nguyên trạng records để chạy tiếp hướng CDC ở chốt tiếp theo
    return raw_records 



@op(required_resource_keys={"settings", "spark"}) 
def op_validate_records(context, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Chạy basic data quality checks bằng PySpark và tách invalid records."""
    spark = context.resources.spark
    if not records:
        return {"valid_records": [], "invalid_records": [], "quality_metrics": {"total_records": 0, "valid_records": 0, "invalid_records": 0, "quality_rate": 0.0}}
        
    df = spark.createDataFrame(records)
    valid_df, invalid_df = validate_records(df)
    
    # Collect back to Python objects for logging/passing between Dagster default ops
    # Trong môi trường production thực tế sẽ dùng PySparkIOManager hoặc truyền Delta Lake Paths
    valid_records = [row.asDict() for row in valid_df.collect()]
    invalid_records = [row.asDict() for row in invalid_df.collect()]
    
    total = len(records)
    valid_count = len(valid_records)
    invalid_count = len(invalid_records)
    quality_rate = round((valid_count / total), 4) if total else 0.0
    return {
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "quality_metrics": {
            "total_records": total,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "quality_rate": quality_rate,
        },
    }


@op(required_resource_keys={"settings", "storage"}) # Ghi lại báo cáo chất lượng dữ liệu để theo dõi health của pipeline, đặc biệt khi có nhiều nguồn dữ liệu hoặc schema phức tạp.
def op_publish_data_quality_report(context, validation_result: dict[str, Any]) -> str:
    """Lưu data quality report và mẫu lỗi để giám sát ingestion health."""
    settings = context.resources.settings
    storage = context.resources.storage

    metrics = dict(validation_result["quality_metrics"])
    invalid_records = list(validation_result["invalid_records"]) # Phân tích lý do lỗi để cải thiện pipeline, đếm số lượng lỗi theo loại để ưu tiên fix những vấn đề phổ biến nhất. Lưu lại một số mẫu bản ghi lỗi để debug khi cần thiết.
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") # Timestamp để version hóa báo cáo, dễ dàng truy vết theo thời gian và so sánh chất lượng giữa các lần chạy.
    report_key = f"{settings.storage.cdc_state_prefix}/quality/quality_report_{ts}.json" # Lưu báo cáo vào prefix cdc_state/quality/ để dễ dàng truy cập và phân loại với các loại dữ liệu khác trong storage, đồng thời đảm bảo báo cáo được version hóa theo thời gian.

    storage.put_json(
        report_key,
        {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "profile": settings.runtime.profile,
            "metrics": metrics,
            "invalid_reason_counts": _count_invalid_reasons(invalid_records),
            "invalid_samples": invalid_records[:20],
        },
    )

    context.log.info(f"Đã ghi quality report tại: {report_key}")
    return report_key


def _count_invalid_reasons(invalid_records: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in invalid_records:
        reason = str(row.get("_dq_reason", "unknown"))
        counts[reason] = counts.get(reason, 0) + 1
    return counts


@op(required_resource_keys={"settings", "spark"})
def op_clean_records(context, validation_result: dict[str, Any]) -> list[dict[str, Any]]:
    """Chuẩn hóa kiểu dữ liệu và text phân tán bằng PySpark.
    Tích hợp cơ chế Targeted Reprocessing (Quarantine DLQ).
    """
    spark = context.resources.spark
    settings = context.resources.settings
    records = validation_result["valid_records"]
    
    # 1. Khởi tạo DataFrame mới từ Kafka/Bronze
    if records:
        df_new = spark.createDataFrame(records)
    else:
        # Tạo DataFrame rỗng với schema cơ bản nếu không có data mới
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField("property_id", StringType(), True)])
        df_new = spark.createDataFrame([], schema)

    # 2. Đọc bảng Quarantine cũ (nếu có) để xử lý lại
    quarantine_path = _delta_path("quarantine", settings)
    try:
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(spark, quarantine_path):
            df_quarantine = spark.read.format("delta").load(quarantine_path)
            # Gộp data mới và data lỗi cũ lại thành 1 mẻ
            df_combined = df_new.unionByName(df_quarantine, allowMissingColumns=True)
            context.log.info(f"Đã nạp {df_quarantine.count()} bản ghi từ Quarantine để chạy lại MDM.")
        else:
            df_combined = df_new
    except Exception as e:
        context.log.warning(f"Không thể đọc Quarantine table: {e}")
        df_combined = df_new

    if df_combined.count() == 0:
        return []

    # 3. Chạy thuật toán dọn dẹp và áp dụng luật MDM
    cleaned_df = clean_records(df_combined, getattr(settings, "mdm", None))
    
    # 4. Phân luồng: Tách MAPPED (Thành công) và UNMAPPED (Lỗi)
    if "mapping_status" in cleaned_df.columns:
        df_success = cleaned_df.filter(F.col("mapping_status") == "MAPPED")
        df_failed = cleaned_df.filter(F.col("mapping_status") == "UNMAPPED")
        
        # Ghi đè (Overwrite) lại bảng Quarantine: Xóa lỗi cũ đã fix, cập nhật lỗi mới
        df_failed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(quarantine_path)
        
        fail_count = df_failed.count()
        if fail_count > 0:
            context.log.error(f"CẢNH BÁO: Phát hiện {fail_count} bản ghi địa lý không hợp lệ (UNMAPPED). Đã đưa vào Quarantine!")
            
        # Chỉ những bản ghi MAPPED mới được đi tiếp vào Silver
        cleaned_df = df_success.drop("mapping_status")
    
    # === BÁO CÁO THỐNG KÊ HIỆU SUẤT MDM LÊN DAGSTER ===
    total_records = cleaned_df.count()
    if total_records > 0 and "mapping_source" in cleaned_df.columns:
        context.log.info(f"📊 BÁO CÁO HIỆU SUẤT HYBRID FALLBACK ({total_records} bản ghi):")
        stats_df = cleaned_df.groupBy("mapping_source").count().collect()
        for row in stats_df:
            percentage = round((row["count"] / total_records) * 100, 2)
            context.log.info(f" 🎯 {row['mapping_source']}: {row['count']} căn ({percentage}%)")
            
    # Ghi log 10 dòng mẫu ra Dagster UI để kiểm chứng
    context.log.info("🏙️ KẾT QUẢ SAMPLE 10 DÒNG (ĐÃ MAPPED THÀNH CÔNG):")
    if "mapping_source" in cleaned_df.columns:
        # Include listing_type nếu có
        select_cols = ["property_id", "title", "property_type", "city", "district", "mapping_source"]
        if "listing_type" in cleaned_df.columns:
            select_cols.append("listing_type")
        sample_rows = cleaned_df.select(*select_cols).limit(10).collect()
        for row in sample_rows:
            lt_info = f" | Listing: {row.listing_type}" if hasattr(row, "listing_type") else ""
            context.log.info(f"[SAMPLE] Title: '{row.title}' -> Type: {row.property_type} [{row.mapping_source}]{lt_info} | Loc: {row.city} - {row.district}")

    # === BÁO CÁO ML LISTING TYPE ===
    if "listing_type" in cleaned_df.columns:
        lt_stats = cleaned_df.groupBy("listing_type").count().collect()
        lt_total = sum(r["count"] for r in lt_stats)
        context.log.info(f"🏷️ BÁO CÁO ML LISTING TYPE ({lt_total} bản ghi):")
        for row in lt_stats:
            pct = round((row["count"] / lt_total) * 100, 1) if lt_total > 0 else 0
            context.log.info(f"  🏷️ {row['listing_type']}: {row['count']} tin ({pct}%)")
        
    # Không drop mapping_source vì dim_property_type cần dùng nó
    pass
        
    return [row.asDict() for row in cleaned_df.collect()]


@op(required_resource_keys={"spark"})
def op_transform_silver(context, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Transform sang silver schema bằng PySpark DataFrame."""
    spark = context.resources.spark
    if not records: return []
    
    df = spark.createDataFrame(records)
    silver_df = transform_for_silver(df)
    return [row.asDict() for row in silver_df.collect()]


@op(required_resource_keys={"settings", "spark"})
def op_write_silver_delta(context, records: list[dict[str, Any]]) -> str:
    """Ghi dữ liệu silver dạng Delta table bằng PySpark."""
    if not records:
        context.log.warning("Silver records rỗng, bỏ qua ghi Delta.")
        return "empty"

    spark = context.resources.spark
    settings = context.resources.settings
    target_path = _delta_path(settings.storage.silver_prefix, settings)

    df = spark.createDataFrame(records)
    upsert_silver(spark, df, target_path)
    context.log.info(f"Đã ghi Silver Delta table tại: {target_path}")
    return target_path


@op(required_resource_keys={"spark"})
def op_transform_gold(context, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Tạo analytics-ready aggregates cho gold layer phân tán bằng PySpark."""
    spark = context.resources.spark
    if not records: return []
    
    df = spark.createDataFrame(records)
    gold_df = transform_for_gold(df)
    return [row.asDict() for row in gold_df.collect()]


@op(required_resource_keys={"settings", "spark"})
def op_write_gold_delta(context, records: list[dict[str, Any]]) -> str:
    """Ghi dữ liệu tổng hợp Gold dạng Delta table (APPEND mode — giữ lịch sử).

    Chiến lược: MERGE trên (city, district, snapshot_date)
    - Cùng ngày: update snapshot (tránh duplicate)
    - Ngày mới: append snapshot mới (tích lũy lịch sử)
    """
    if not records:
        context.log.warning("Gold records rỗng, bỏ qua ghi Delta.")
        return "empty"

    spark = context.resources.spark
    settings = context.resources.settings
    target_path = _delta_path(settings.storage.gold_prefix, settings)

    df = spark.createDataFrame(records)
    append_gold(spark, df, target_path)
    context.log.info(
        f"Đã ghi Gold Delta table (append mode) tại: {target_path}"
    )
    return target_path

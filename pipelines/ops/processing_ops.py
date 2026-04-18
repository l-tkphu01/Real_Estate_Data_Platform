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

from src.lakehouse.delta_writer import overwrite_gold, upsert_silver
from src.processing.cleaning import clean_records
from src.processing.transform import transform_for_gold, transform_for_silver
from src.processing.validation import validate_records


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _delta_path(prefix: str, settings: Any) -> str:
    """Xây dựng đường dẫn Delta Lake linh hoạt dựa trên config driven.
    Trên local thì lưu vào thư mục dự án, trên cloud thì dùng path từ settings.
    """
    if "local" in settings.runtime.profile:
        # Chạy ở bất kì profile local nào cũng ghi trực tiếp xuống file disk trong volume 
        # (DeltaLake / PySpark đọc file local nhanh và ổn định hơn rất nhiều so với ABFS Azurite giả lập)
        target = PROJECT_ROOT / "data" / "lakehouse" / prefix
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target)
    else:
        # Giả lập đường dẫn Cloud Azure (abfs://) dựa trên config-driven
        return f"abfs://{settings.storage.azure_container}@{settings.azure_storage_account}.dfs.core.windows.net/{prefix}"


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
    """Chuẩn hóa kiểu dữ liệu và text phân tán bằng PySpark."""
    spark = context.resources.spark
    settings = context.resources.settings
    records = validation_result["valid_records"]
    if not records: return []
    
    df = spark.createDataFrame(records)
    cleaned_df = clean_records(df, getattr(settings, "mdm", None))
    
    # === BÁO CÁO THỐNG KÊ HIỆU SUẤT MDM LÊN DAGSTER ===
    total_records = cleaned_df.count()
    if total_records > 0 and "mapping_source" in cleaned_df.columns:
        context.log.info(f"📊 BÁO CÁO HIỆU SUẤT HYBRID FALLBACK ({total_records} bản ghi):")
        stats_df = cleaned_df.groupBy("mapping_source").count().collect()
        for row in stats_df:
            percentage = round((row["count"] / total_records) * 100, 2)
            context.log.info(f" 🎯 {row['mapping_source']}: {row['count']} căn ({percentage}%)")
            
    # Ghi log 10 dòng mẫu ra Dagster UI để kiểm chứng
    context.log.info("🏙️ KẾT QUẢ SAMPLE 10 DÒNG:")
    if "mapping_source" in cleaned_df.columns:
        sample_rows = cleaned_df.select("property_id", "title", "property_type", "mapping_source").limit(10).collect()
        for row in sample_rows:
            context.log.info(f"[SAMPLE] Tiêu đề: '{row.title}' | Ánh xạ ra: '{row.property_type}' [{row.mapping_source}]")
        
    # Xoá cột tracking dọn rác trước khi xuất kho
    if "mapping_source" in cleaned_df.columns:
        cleaned_df = cleaned_df.drop("mapping_source")
        
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
    """Ghi dữ liệu tổng hợp Gold dạng Delta table."""
    if not records:
        context.log.warning("Gold records rỗng, bỏ qua ghi Delta.")
        return "empty"

    spark = context.resources.spark
    settings = context.resources.settings
    target_path = _delta_path(settings.storage.gold_prefix, settings)

    df = spark.createDataFrame(records)
    overwrite_gold(spark, df, target_path)
    context.log.info(f"Đã ghi Gold Delta table tại: {target_path}")
    return target_path

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


def _delta_path(prefix: str) -> str:
    target = PROJECT_ROOT / "data" / "lakehouse" / prefix
    target.parent.mkdir(parents=True, exist_ok=True)
    return str(target)


@op(required_resource_keys={"settings", "storage"})
def op_load_latest_raw_records(context) -> list[dict[str, Any]]:
    """Đọc snapshot raw mới nhất từ Azurite/Azure để xử lý downstream."""
    settings = context.resources.settings
    storage = context.resources.storage

    latest_key = storage.get_latest_key(settings.storage.raw_prefix)
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


@op
def op_validate_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Chạy basic data quality checks và tách invalid records."""
    valid_records, invalid_records = validate_records(records)
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


@op(required_resource_keys={"settings", "storage"})
def op_publish_data_quality_report(context, validation_result: dict[str, Any]) -> str:
    """Lưu data quality report và mẫu lỗi để giám sát ingestion health."""
    settings = context.resources.settings
    storage = context.resources.storage

    metrics = dict(validation_result["quality_metrics"])
    invalid_records = list(validation_result["invalid_records"])
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_key = f"{settings.storage.cdc_state_prefix}/quality/quality_report_{ts}.json"

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


@op
def op_clean_records(validation_result: dict[str, Any]) -> list[dict[str, Any]]:
    """Chuẩn hóa kiểu dữ liệu và text cho bản ghi hợp lệ."""
    return clean_records(validation_result["valid_records"])


@op
def op_transform_silver(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Transform validated records sang silver schema."""
    return transform_for_silver(records)


@op(required_resource_keys={"settings", "spark"})
def op_write_silver_delta(context, records: list[dict[str, Any]]) -> str:
    """Ghi dữ liệu silver dạng Delta table bằng PySpark."""
    if not records:
        context.log.warning("Silver records rỗng, bỏ qua ghi Delta.")
        return "empty"

    spark = context.resources.spark
    settings = context.resources.settings
    target_path = _delta_path(settings.storage.silver_prefix)

    df = spark.createDataFrame(records)
    upsert_silver(spark, df, target_path)
    context.log.info(f"Đã ghi Silver Delta table tại: {target_path}")
    return target_path


@op
def op_transform_gold(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Tạo analytics-ready aggregates cho gold layer."""
    return transform_for_gold(records)


@op(required_resource_keys={"settings", "spark"})
def op_write_gold_delta(context, records: list[dict[str, Any]]) -> str:
    """Ghi dữ liệu tổng hợp Gold dạng Delta table."""
    if not records:
        context.log.warning("Gold records rỗng, bỏ qua ghi Delta.")
        return "empty"

    spark = context.resources.spark
    settings = context.resources.settings
    target_path = _delta_path(settings.storage.gold_prefix)

    df = spark.createDataFrame(records)
    overwrite_gold(spark, df, target_path)
    context.log.info(f"Đã ghi Gold Delta table tại: {target_path}")
    return target_path

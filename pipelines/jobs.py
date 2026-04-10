"""Contract Dagster jobs cho end-to-end real estate pipeline."""

from dagster import job
from pipelines.ops.ingestion_ops import op_fetch_source_data, op_store_raw_snapshot
from pipelines.ops.processing_ops import (
    op_clean_records,
    op_load_latest_raw_records,
    op_publish_data_quality_report,
    op_transform_gold,
    op_transform_silver,
    op_validate_records,
    op_write_gold_delta,
    op_write_silver_delta,
)
from pipelines.resources import settings_resource, spark_resource, storage_resource


@job(resource_defs={
    "settings": settings_resource, 
    "storage": storage_resource
})
def ingestion_job():
    """Định nghĩa job cho fetch -> raw storage -> cập nhật CDC state."""
    
    # Bước 1: Fetch
    data = op_fetch_source_data()
    
    # Bước 2: Đẩy vào Raw Azurite
    op_store_raw_snapshot(data)


@job(
    resource_defs={
        "settings": settings_resource,
        "storage": storage_resource,
        "spark": spark_resource,
    }
)
def processing_job():
    """Định nghĩa job cho clean/validate -> silver/gold writes."""
    raw_records = op_load_latest_raw_records()
    validation_result = op_validate_records(raw_records)
    op_publish_data_quality_report(validation_result)
    cleaned_records = op_clean_records(validation_result)

    silver_records = op_transform_silver(cleaned_records)
    op_write_silver_delta(silver_records)

    gold_records = op_transform_gold(silver_records)
    op_write_gold_delta(gold_records)

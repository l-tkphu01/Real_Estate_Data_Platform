"""Contract Dagster jobs cho end-to-end real estate pipeline."""

from dagster import job
from pipelines.ops.ingestion_ops import op_ingest_and_store_raw
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


# Job config cho ingestion (batching)
INGESTION_JOB_CONFIG = {
    "ops": {},
    "resources": {}
}

# Job config cho PySpark processing 
PROCESSING_JOB_CONFIG = {
    "ops": {},
    "resources": {}
}


@job(
    resource_defs={
        "settings": settings_resource, 
        "storage": storage_resource
    },
    config=INGESTION_JOB_CONFIG
)
def ingestion_job():
    """Định nghĩa job cho fetch -> raw storage -> cập nhật CDC state.
    
    Timeout: 300s (từ 30s) để support:
    - 50 pages (1000 records) batching
    - 2s delay giữa batch (anti-ban)
    - Network latency buffer
    """
    
    # Bước 1 & 2 được gộp chung để chống OOM RAM Dagster (OOM Prevention)
    # Orchestrator sẽ không phải chuyển qua lại List[dict] khổng lồ trên Memory nữa.
    op_ingest_and_store_raw()


@job(
    resource_defs={
        "settings": settings_resource,
        "storage": storage_resource,
        "spark": spark_resource,
    },
    config=PROCESSING_JOB_CONFIG
)
def processing_job():
    """Định nghĩa job cho clean/validate -> silver/gold writes.
    
    Timeout: 300s (từ 30s) để support:
    - Spark processing 1000 records (2GB memory)
    - Silver deduplication
    - Gold transformation
    - Delta Lake writes
    """
    raw_records = op_load_latest_raw_records()
    validation_result = op_validate_records(raw_records)
    op_publish_data_quality_report(validation_result)
    cleaned_records = op_clean_records(validation_result)

    silver_records = op_transform_silver(cleaned_records)
    op_write_silver_delta(silver_records)

    gold_records = op_transform_gold(silver_records)
    op_write_gold_delta(gold_records)

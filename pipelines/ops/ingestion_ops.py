"""Ops cho ingestion stage.

Luồng:
1) fetch source records (Async + Batching)
2) normalize payload (Pydantic validation)
3) ghi immutable raw snapshot trực tiếp vào Azurite
"""

from dagster import op
from datetime import datetime

from src.scraper.client import fetch_raw_records
from src.scraper.normalizer import normalize_raw_record


@op(required_resource_keys={"settings", "storage"})
def op_ingest_and_store_raw(context) -> str:
    """Fetch dữ liệu BĐS, chuẩn hóa, và Write trực tiếp vào Zone RAW."""
    settings = context.resources.settings
    storage = context.resources.storage

    # 1. Cào dữ liệu API (Async + batching)
    raw_data = fetch_raw_records(settings)

    if not raw_data:
        context.log.warning("[WARN] Không tải được bản ghi nào hoặc danh sách rỗng.")
        return "empty"

    # 2. Normalize qua Pydantic schema
    normalized_data = []
    for row in raw_data:
        norm_row = normalize_raw_record(row)
        if norm_row:
            normalized_data.append(norm_row)

    if not normalized_data:
        context.log.warning("Tất cả bản ghi đều invalid (100% rác).")
        return "empty"

    context.log.info(
        f"[SUCCESS] Đã chuẩn hóa thành công {len(normalized_data)}/{len(raw_data)} bản ghi."
    )

    # 3. Tạo blob key: raw/real_estate_YYYYMMDD_HHMMSS.json
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{settings.storage.raw_prefix}_{now_str}.json"

    # 4. Ghi trực tiếp xuống Azurite (tránh OOM Dagster IPC)
    storage.put_json(blob_name, normalized_data)

    context.log.info(
        f"[SUCCESS] Upload thành công: {blob_name} ({len(normalized_data)} records)"
    )

    return blob_name

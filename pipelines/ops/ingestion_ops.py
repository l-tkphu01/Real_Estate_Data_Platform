"""Ops cho ingestion stage.

Luồng kỳ vọng:
1) fetch source records
2) normalize payload
3) ghi immutable raw snapshot
"""

from dagster import op
from datetime import datetime

from src.scraper.client import fetch_raw_records
from src.scraper.normalizer import normalize_raw_record

@op(required_resource_keys={"settings"})
def op_fetch_source_data(context) -> list[dict]:
    """Fetch records từ API (hoặc Mock Data) với retry logic."""
    settings = context.resources.settings
    
    # 1. Cào dữ liệu theo định dạng gốc
    raw_data = fetch_raw_records(settings)
    
    # 2. Làm sạch / Ánh xạ sang Schema chuẩn
    normalized_data = []
    for row in raw_data:
        norm_row = normalize_raw_record(row)
        if norm_row:  # Lọc bỏ rác (dict rỗng) bị văng ra từ Pydantic Validation
            normalized_data.append(norm_row)
    
    context.log.info(f"Đã chuẩn hóa thành công {len(normalized_data)} bản ghi chuẩn.")
    return normalized_data

@op(required_resource_keys={"storage", "settings"})
def op_store_raw_snapshot(context, data: list[dict]) -> str:
    """Lưu trữ dữ liệu vào Zone RAW của Azurite (hoặc Azure Datalake)."""
    settings = context.resources.settings
    storage = context.resources.storage
    
    if not data:
        context.log.warning("Không có dữ liệu nào để lưu.")
        return "empty"
        
    # Tạo tên file (Blob Name) ví dụ: raw/real_estate_20260409_153022.json
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{settings.storage.raw_prefix}_{now_str}.json"
    
    storage.put_json(blob_name, data)
    
    context.log.info(f"✅ Upload thành công: {blob_name}")
    return blob_name

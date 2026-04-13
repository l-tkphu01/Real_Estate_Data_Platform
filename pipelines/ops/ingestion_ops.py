"""Ops cho ingestion stage.

Luồng kỳ vọng (Đã tối ưu OOM):
1) fetch source records
2) normalize payload
3) ghi immutable raw snapshot trực tiếp KHÔNG qua in-memory truyền của Graph.
"""

from dagster import op
from datetime import datetime

from src.scraper.client import fetch_raw_records
from src.scraper.normalizer import normalize_raw_record

@op(required_resource_keys={"settings", "storage"})
def op_ingest_and_store_raw(context) -> str:
    """Fetch dữ liệu, chuẩn hóa, và Write trực tiếp vào Zone RAW (Optimal Memory)."""
    settings = context.resources.settings
    storage = context.resources.storage
    
    # 1. Cào dữ liệu API (Nội bộ chạy Async Fast)
    raw_data = fetch_raw_records(settings)
    
    if not raw_data:
        context.log.warning("Không tải được bản ghi nào hoặc danh sách rỗng.")
        return "empty"
    
    # 2. Làm sạch / Validation qua Pydantic schema
    normalized_data = []
    for row in raw_data:
        norm_row = normalize_raw_record(row)
        if norm_row:  
            normalized_data.append(norm_row)
            
    if not normalized_data:
        context.log.warning("Tất cả bản ghi đều invalid (100% rác).")
        return "empty"
    
    context.log.info(f"Đã chuẩn hóa thành công {len(normalized_data)} bản ghi chuẩn.")
    
    # 3. Create blob_name: raw/real_estate_20260413_153022.json
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{settings.storage.raw_prefix}_{now_str}.json"
    
    # 4. Ghi TRỰC TIẾP luồng list lớn xuống Azurite thay vì return Node
    storage.put_json(blob_name, normalized_data)
    
    context.log.info(f"✅ Upload thành công: {blob_name} ({len(normalized_data)} records)")
    
    # Only return string -> minimal IPC network load Dagster orchestration!
    return blob_name


# ═══════════════════════════════════════════════════════════════════════════════
# 🚀 TRAINING DATA EXPORT - COMMENTED OUT FOR FUTURE AI MODEL TRAINING
# ═══════════════════════════════════════════════════════════════════════════════
# TO ACTIVATE:
# 1. Uncomment the function below
# 2. Change APP_PROFILE to one of:
#    - APP_PROFILE=local.property (Category 1000 - BĐS only)
#    - APP_PROFILE=local.vehicle (Category 1001 - Xe only)
#    - APP_PROFILE=local.electronics (Category 1002 - Điện thoại only)
# 3. Run: python -m dagster dev
# 4. Data will be exported to:
#    - Azurite: /training/{category_name}/{category_name}_*.json
#    - Local: data/training/{category_name}/{category_name}_*.json
# 5. Use exported data to train separate ML models per category
# ═══════════════════════════════════════════════════════════════════════════════

# @op(required_resource_keys={"storage", "settings"})
# def op_export_training_data(context, data: list[dict]) -> str:
#     """Export normalized data vào thư mục training cho từng category.
#     
#     Logic:
#     - Detect category từ config (settings.ingestion.categories)
#     - Nếu single category: export vào /training/{category_prefix}/
#     - Nếu multi-category: skip (dành cho production data processing)
#     
#     Output paths:
#     - property (1000): /training/property/property_*.json
#     - vehicle (1001): /training/vehicle/vehicle_*.json
#     - electronics (1002): /training/electronics/electronics_*.json
#     """
#     settings = context.resources.settings
#     storage = context.resources.storage
#     
#     if not data:
#         context.log.warning("❌ Không có dữ liệu để export.")
#         return "empty"
#     
#     # Detect category từ config
#     categories = settings.ingestion.categories
#     
#     # ONLY export nếu là single-category training mode
#     if len(categories) > 1:
#         context.log.info("⏭️ Multi-category mode detected - skipping training export (production data)")
#         return "multi_category_skipped"
#     
#     category_id = categories[0]
#     
#     # Map category ID → folder prefix
#     category_map = {
#         1000: "property",
#         1001: "vehicle",
#         1002: "electronics"
#     }
#     
#     category_prefix = category_map.get(category_id, f"category_{category_id}")
#     
#     # Tạo blob name: training/{category_prefix}/{category_prefix}_YYYYMMDD_HHMMSS.json
#     now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
#     blob_name = f"training/{category_prefix}/{category_prefix}_{now_str}.json"
#     
#     # Export vào Azurite
#     storage.put_json(blob_name, data)
#     
#     context.log.info(
#         f"✅ Training data exported: {blob_name} "
#         f"({len(data)} records, category={category_id})"
#     )
#     
#     # Also save locally để dễ access
#     # import json
#     # import os
#     # local_dir = f"data/training/{category_prefix}"
#     # os.makedirs(local_dir, exist_ok=True)
#     # local_path = f"{local_dir}/{category_prefix}_{now_str}.json"
#     # with open(local_path, 'w', encoding='utf-8') as f:
#     #     json.dump(data, f, ensure_ascii=False, indent=2)
#     # context.log.info(f"💾 Lưu local: {local_path}")
#     
#     return blob_name

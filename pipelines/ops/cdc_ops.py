from dagster import op
from src.cdc.fingerprint import generate_fingerprint
from src.cdc.state_store import CdcStateStore
from typing import Any

@op(required_resource_keys={"storage", "settings"})
def op_detect_changes(context, raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Bộ lọc CDC: Cửa ngõ ngăn chặn các bản ghi trùng lặp bào tiền Cloud của hệ thống."""
    if not raw_records:
        return []
        
    storage = context.resources.storage
    settings = context.resources.settings
    
    # Khởi tạo Sổ tay bảo vệ
    store = CdcStateStore(storage, state_file=f"{settings.storage.cdc_state_prefix}/fingerprints.json")
    
    new_or_updated_records = []
    
    for row in raw_records:
        prop_id = str(row.get("ad_id", ""))
        if not prop_id:
            continue
            
        current_hash = generate_fingerprint(row)
        
        if store.is_changed(prop_id, current_hash):
            new_or_updated_records.append(row)
            store.update_state(prop_id, current_hash)
            
    # Ghi nhận lại cuốn sổ tay mới cho ngày mai đọc tiếp
    store.save()
    
    total = len(raw_records)
    passed = len(new_or_updated_records)
    dropped = total - passed
    
    context.log.info(f"🛡️ [CDC FINGERPRINT] Đã quét {total} tin nhắn.")
    context.log.info(f"🗑️ Đã chặn {dropped} tin đăng trùng bài/không đổi giá trị.")
    context.log.info(f"✅ Đưa vào máy cày PySpark {passed} tin đăng hoàn toàn Mới/Update.")
    
    return new_or_updated_records

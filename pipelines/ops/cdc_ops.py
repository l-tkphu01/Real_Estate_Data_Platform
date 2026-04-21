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
    state_path = f"{settings.storage.cdc_state_prefix}/{settings.cdc.state_filename}"
    store = CdcStateStore(storage, state_file=state_path)
    
    new_or_updated_records = []
    
    stats_new = 0
    stats_updated = 0
    stats_unchanged = 0
    
    id_field = settings.cdc.id_field
    
    for row in raw_records:
        prop_id = str(row.get(id_field, ""))
        if not prop_id:
            continue
            
        current_hash = generate_fingerprint(row, settings)
        status = store.check_status(prop_id, current_hash)
        
        if status == "NEW":
            stats_new += 1
            new_or_updated_records.append(row)
            store.update_state(prop_id, current_hash)
        elif status == "UPDATED":
            stats_updated += 1
            new_or_updated_records.append(row)
            store.update_state(prop_id, current_hash)
        else:
            stats_unchanged += 1
            
    # Ghi nhận lại cuốn sổ tay mới cho ngày mai đọc tiếp
    store.save()
    
    total = len(raw_records)
    
    context.log.info("="*50)
    context.log.info(f"📊 BÁO CÁO HOẠT ĐỘNG CDC FINGERPRINT")
    context.log.info("="*50)
    context.log.info(f"📥 Tổng số tin nhắn quét đầu vào : {total} bản ghi")
    context.log.info(f"🆕 Số tin hoàn toàn mới (NEW)    : {stats_new} bản ghi (Sẽ Insert)")
    context.log.info(f"🔄 Số tin bị giấu đổi giá (UP)   : {stats_updated} bản ghi (Sẽ Upsert đè giá lên Delta Lake)")
    context.log.info(f"🗑️ Số tin rác trùng lặp (DROP)   : {stats_unchanged} bản ghi (Bị CHẶN đứng ở cửa ngõ)")
    context.log.info("-" * 50)
    context.log.info(f"✅ Đưa vào máy cày PySpark tổng cộng: {stats_new + stats_updated} tin đăng.")
    context.log.info("="*50)
    
    return new_or_updated_records

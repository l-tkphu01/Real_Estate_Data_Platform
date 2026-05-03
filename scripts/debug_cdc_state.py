"""Debug CDC - chạy TRỰC TIẾP trong Docker container với env đúng."""
import os
os.environ["AZURE_ENDPOINT"] = "http://azurite:10000/devstoreaccount1"
os.environ.setdefault("APP_PROFILE", "local")

from src.config import load_settings
from src.storage.azure_client import AzureStorageClient
from src.cdc.fingerprint import generate_fingerprint

settings = load_settings()
client = AzureStorageClient(settings)

# 1. Liệt kê TẤT CẢ file trong prefix CDC
print("\n========== TẤT CẢ FILE CDC TRONG AZURITE ==========")
try:
    cdc_blobs = client.list_keys(settings.storage.cdc_state_prefix)
    if not cdc_blobs:
        print("  KHÔNG CÓ FILE NÀO (Sổ tay trống rỗng)")
    for b in cdc_blobs:
        print(f"  📄 {b}")
except Exception as e:
    print(f"  Lỗi list: {e}")

# 2. Thử đọc fingerprints.json
fp_file = f"{settings.storage.cdc_state_prefix}/{settings.cdc.state_filename}"
print(f"\n========== ĐỌC: {fp_file} ==========")
fingerprint_data = {}
try:
    fingerprint_data = client.get_json(fp_file)
    if isinstance(fingerprint_data, dict):
        print(f"  🔑 Tổng ID trong sổ tay: {len(fingerprint_data)}")
        for i, (k, v) in enumerate(fingerprint_data.items()):
            if i >= 5:
                print(f"  ... và {len(fingerprint_data) - 5} ID khác")
                break
            print(f"  ID={k}  Hash={v}")
    else:
        print(f"  Định dạng lạ: {type(fingerprint_data)}")
except Exception as e:
    print(f"  Không đọc được (có thể chưa tồn tại): {e}")

# 3. Đọc Raw data mới nhất
print(f"\n========== RAW DATA MỚI NHẤT ==========")
latest_key = client.get_latest_key(settings.storage.raw_prefix)
print(f"  📂 File: {latest_key}")

if latest_key:
    raw_data = client.get_json(latest_key)
    if isinstance(raw_data, list) and len(raw_data) > 0:
        print(f"  📊 Tổng records: {len(raw_data)}")
        sample = raw_data[0]
        keys_available = list(sample.keys())
        print(f"  🗝️ Các trường: {keys_available}")
        
        ad_id_val = sample.get("ad_id")
        print(f"  🏠 ad_id mẫu: {ad_id_val} (type: {type(ad_id_val).__name__})")
        
        # Tính fingerprint cho record đầu tiên
        fp = generate_fingerprint(sample, settings)
        print(f"  🔐 Fingerprint tính được: {fp}")
        
        # So sánh với sổ tay
        if isinstance(fingerprint_data, dict) and ad_id_val is not None:
            stored_hash = fingerprint_data.get(str(ad_id_val))
            print(f"  📋 Hash trong sổ tay: {stored_hash}")
            print(f"  ❓ Giống nhau? -> {stored_hash == fp}")

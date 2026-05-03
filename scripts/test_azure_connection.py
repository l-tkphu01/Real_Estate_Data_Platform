import os
import sys
from pathlib import Path

# Đảm bảo Python tìm thấy module 'src'
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.config import load_settings
from src.storage.azure_client import AzureStorageClient

def run_smoke_test():
    print("1. Đang nạp cấu hình (YAML)...")
    # Ép dùng profile local để test
    os.environ["APP_PROFILE"] = "local"
    os.environ["CONFIG_DIR"] = "pipelines/config"
    os.environ["AZURE_ENDPOINT"] = "http://127.0.0.1:10000/devstoreaccount1"
    settings = load_settings()
    
    print(f"   -> Profile hiện tại: {settings.runtime.profile}")
    print(f"   -> Storage Account: {settings.azure_storage_account}")
    print(f"   -> Endpoint: {settings.azure_endpoint}")
    print(f"   -> Connection String đã gen: {settings.azure_connection_string()[:60]}...")
    
    print("\n2. Đang khởi tạo AzureStorageClient và kết nối Azurite...")
    client = AzureStorageClient(settings)
    print("   -> Đã khởi tạo thành công và đảm bảo Container tồn tại.")
    
    print("\n3. Thử ghi dữ liệu mẫu bằng hàm put_json()...")
    test_data = {"message": "Hello from smoke test!", "status": "success"}
    test_blob_name = "raw/smoke_test/test_file.json"
    client.put_json(test_blob_name, test_data)
    print(f"   -> Đã ghi file: {test_blob_name}")
    
    print("\n4. Kiểm tra danh sách file bằng hàm list_keys()...")
    keys = client.list_keys("raw/smoke_test/")
    for k in keys:
        print(f"   - Tìm thấy: {k}")
        
    if test_blob_name in keys:
        print("\nTẤT CẢ CÁC BƯỚC TEST ĐẦU CUỐI ĐÃ THÀNH CÔNG RỰC RỠ!")
    else:
        print("\nLỖI: Không tìm thấy file vừa ghi trong Container.")

if __name__ == "__main__":
    run_smoke_test()
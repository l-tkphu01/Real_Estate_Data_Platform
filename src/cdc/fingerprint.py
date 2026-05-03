import hashlib
import json
from typing import Any
from src.config import Settings

def generate_fingerprint(record: dict[str, Any], settings: Settings) -> str:
    """Tạo mã hash (Sinh trắc vân tay - Config-Driven) cho các trường đã được chỉ định từ settings.
    
    Hash thuật toán và danh sách các trường được định nghĩa linh hoạt trong file base.yaml (hoặc local.yaml).
    """
    hash_fields = settings.cdc.hash_fields
    core_fields = {field: str(record.get(field, "")) for field in hash_fields}
    
    # Sắp xếp key để hash luôn luôn ổn định
    serialized = json.dumps(core_fields, sort_keys=True, ensure_ascii=False)
    
    algo = settings.cdc.hash_algorithm.lower()
    if algo == "sha256":
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    elif algo == "sha1":
        return hashlib.sha1(serialized.encode("utf-8")).hexdigest()
    else:
        return hashlib.md5(serialized.encode("utf-8")).hexdigest()

# sha256: 64 kí tự 
# sha1: 40 kí tự
# md5: 32 kí tự (đủ dùng cho dự án hiện tại ròi)
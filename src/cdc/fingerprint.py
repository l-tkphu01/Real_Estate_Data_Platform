import hashlib
import json
from typing import Any

def generate_fingerprint(record: dict[str, Any]) -> str:
    """Tạo mã MD5 hash (Sinh trắc vân tay) dựa trên các trường cốt lõi."""
    core_fields = {
        "property_id": str(record.get("ad_id", "")),
        "title": str(record.get("subject", "")),
        "price": str(record.get("price", "")),
        "area": str(record.get("size", ""))
    }
    # Sắp xếp key để hash luôn luôn ổn định
    serialized = json.dumps(core_fields, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(serialized.encode("utf-8")).hexdigest()

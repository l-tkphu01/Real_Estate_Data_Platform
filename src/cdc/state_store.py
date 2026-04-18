from typing import Any

class CdcStateStore:
    """Quản lý trạng thái băm (Sổ tay bảo vệ) lưu trên hệ thống Storage."""
    
    def __init__(self, storage_client: Any, state_file: str = "cdc_state/fingerprints.json"):
        self.storage = storage_client
        self.state_file = state_file
        self._cache = self._load()
        
    def _load(self) -> dict[str, str]:
        """Đọc cuốn sổ tay cũ từ hôm qua lên."""
        try:
            data = self.storage.get_json(self.state_file)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def is_changed(self, record_id: str, new_hash: str) -> bool:
        """Nếu record_id chưa có hoặc mã hash khác với mã cũ -> Coi là MỚI hoặc CÓ SỬA ĐỔI."""
        return self._cache.get(str(record_id)) != new_hash

    def update_state(self, record_id: str, new_hash: str) -> None:
        """Ghi đè mã vân tay mới vào dòng tương ứng trong sổ."""
        self._cache[str(record_id)] = new_hash

    def save(self) -> None:
        """Cất cuốn sổ tay vào thư mục lưu trữ cho ngày mai đọc lại."""
        self.storage.put_json(self.state_file, self._cache)

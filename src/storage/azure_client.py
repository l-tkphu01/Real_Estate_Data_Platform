"""Azure Storage client abstraction."""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from src.config import Settings


AZURE_BLOB_API_VERSION = "2023-11-03"
logger = logging.getLogger(__name__)

class AzureStorageClient:
    """Unified storage client interface cho Azure Data Lake / Azurite.
    
    Optimized: Loại bỏ check Container exist lúc Init để tránh block IO Dagster và
    tránh request dư thừa lên Azure gây tốn transaction cost. Tự tạo container khi upload lần đầu.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.get_connection_string(),
            api_version=AZURE_BLOB_API_VERSION,
        )
        self.container_client = self.blob_service_client.get_container_client(self.settings.azure_container)
        self._container_checked = False

    def _ensure_container_dynamic(self) -> None:
        """Dynamic container creation - lazy init khi write data thực tế."""
        if self._container_checked:
            return
        try:
            if not self.container_client.exists(): # Kiểm tra đã có container chưa (thư mục gốc trong Blob Storage)
                self.container_client.create_container()
                logger.info(f"🚀 Container '{self.settings.azure_container}' created successfully.")
        except Exception as e:
            logger.warning(f"Could not create container: {e}")
        finally:
            self._container_checked = True

    def get_connection_string(self) -> str:
        return self.settings.azure_connection_string()

    def put_json(self, key: str, payload: Any) -> None:
        """Ghi dữ liệu dưới định dạng JSON vào Azurite/Azure Blob."""
        self._ensure_container_dynamic() # Xử lý dynamic creation
        blob_client = self.container_client.get_blob_client(key)
        json_data = json.dumps(payload, ensure_ascii=False)
        blob_client.upload_blob(json_data, overwrite=True)

    def put_parquet(self, key: str, records: list[dict[str, Any]]) -> None:
        """Dành cho giai đoạn Pyspark lưu giữ sau này."""
        raise NotImplementedError("Sử dụng PySpark Delta writer thay vì hàm này.")

    def list_keys(self, prefix: str) -> list[str]:
        """Liệt kê các blob (file) trong một thư mục nhất định."""
        blob_list = self.container_client.list_blobs(name_starts_with=prefix)
        return [blob.name for blob in blob_list]

    def get_json(self, key: str) -> Any:
        """Đọc blob JSON và parse về Python object."""
        blob_client = self.container_client.get_blob_client(key)
        payload = blob_client.download_blob().readall().decode("utf-8")
        return json.loads(payload)

    def get_latest_key(self, prefix: str, suffix: str = ".json") -> str | None:
        """Lấy blob key mới nhất theo last_modified với prefix chỉ định."""
        candidates = [
            blob
            for blob in self.container_client.list_blobs(name_starts_with=prefix)
            if blob.name.endswith(suffix)
        ]
        if not candidates:
            return None

        # Sử dụng  timezone-aware datetime.min (UTC) để tránh lỗi khi so sánh với blob.last_modified
        min_time = datetime.min.replace(tzinfo=timezone.utc)
        latest_blob = max(candidates, key=lambda blob: blob.last_modified or min_time)
        return latest_blob.name

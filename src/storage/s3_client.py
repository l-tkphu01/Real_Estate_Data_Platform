"""S3 client abstraction.

Module này cần ẩn chi tiết phụ thuộc provider để MinIO và AWS S3 dùng cùng
interface.
"""

from typing import Any

from src.config import Settings


class S3StorageClient:
    """Unified storage client interface dùng cho toàn bộ pipeline layers."""

    def __init__(self, settings: Settings) -> None:
        """Nhận settings từ lớp config-driven để dùng nhất quán toàn hệ thống."""

        self.settings = settings

    def get_connection_config(self) -> dict[str, Any]:
        """Trả về cấu hình kết nối S3-compatible từ settings hiện tại."""

        return self.settings.boto3_client_kwargs()

    def put_json(self, key: str, payload: Any) -> None:
        """Ghi JSON data vào object storage theo key được chỉ định."""

        raise NotImplementedError("Implement in build phase")

    def put_parquet(self, key: str, records: list[dict[str, Any]]) -> None:
        """Ghi tabular records dạng Parquet vào object storage."""

        raise NotImplementedError("Implement in build phase")

    def list_keys(self, prefix: str) -> list[str]:
        """Liệt kê object keys theo prefix để phục vụ incremental processing."""

        raise NotImplementedError("Implement in build phase")

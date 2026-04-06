"""S3 client abstraction.

Module này cần ẩn chi tiết phụ thuộc provider để MinIO và AWS S3 dùng cùng
interface.
"""

from typing import Any


class S3StorageClient:
    """Unified storage client interface dùng cho toàn bộ pipeline layers."""

    def put_json(self, key: str, payload: Any) -> None:
        """Ghi JSON data vào object storage theo key được chỉ định."""

        raise NotImplementedError("Implement in build phase")

    def put_parquet(self, key: str, records: list[dict[str, Any]]) -> None:
        """Ghi tabular records dạng Parquet vào object storage."""

        raise NotImplementedError("Implement in build phase")

    def list_keys(self, prefix: str) -> list[str]:
        """Liệt kê object keys theo prefix để phục vụ incremental processing."""

        raise NotImplementedError("Implement in build phase")

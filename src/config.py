"""Contract cấu hình ứng dụng cho toàn bộ pipeline modules.

File này tập trung các environment variables bắt buộc để việc chuyển từ MinIO
(local) sang AWS S3 (cloud) không cần thay đổi business logic.
"""

from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    """Cấu trúc settings có kiểu dữ liệu cho ingestion, storage và orchestration."""

    # Settings bắt buộc cho object storage
    s3_bucket: str
    s3_region: str
    s3_endpoint: str | None
    s3_access_key: str
    s3_secret_key: str


def load_settings() -> Settings:
    """Nạp environment variables vào đối tượng Settings.

    TODO:
    - Đọc từ file .env và OS environment.
    - Validate các biến bắt buộc.
    - Trả về đối tượng Settings đã được khởi tạo đầy đủ.
    """

    raise NotImplementedError("Implement in build phase")

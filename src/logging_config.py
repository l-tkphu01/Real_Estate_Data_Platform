"""Thiết lập logging dùng chung để bảo đảm pipeline observability nhất quán."""


def setup_logging() -> None:
    """Cấu hình log format và log level chuẩn cho toàn bộ services.

    TODO:
    - Thêm structured format có timestamp.
    - Đọc log level từ environment.
    - Bảo đảm Dagster ops và Python modules dùng cùng logger policy.
    """

    raise NotImplementedError("Implement in build phase")

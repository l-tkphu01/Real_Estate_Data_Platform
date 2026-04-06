"""Thiết lập logging dùng chung để bảo đảm pipeline observability nhất quán."""

from __future__ import annotations

import logging

from src.config import load_settings


DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(level: str | None = None) -> None:
    """Cấu hình log format và log level chuẩn cho toàn bộ services.

    Nếu không truyền level, hàm sẽ ưu tiên đọc từ config-driven settings.
    """

    resolved_level = level
    resolved_format = DEFAULT_LOG_FORMAT

    if resolved_level is None:
        try:
            settings = load_settings()
            resolved_level = settings.logging.level
            resolved_format = settings.logging.fmt
        except Exception:
            resolved_level = "INFO"

    logging.basicConfig(level=resolved_level.upper(), format=resolved_format, force=True)

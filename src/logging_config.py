"""Thiết lập logging dùng chung để bảo đảm pipeline observability nhất quán."""

from __future__ import annotations

import logging

from src.config import load_settings


DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def _validate_log_format(fmt: str) -> None:
    """Validate logging format string giống cách logging.Formatter() sử dụng.
    
    Raise ValueError nếu format string có syntax lỗi.
    """
    try:
        logging.Formatter(fmt)
    except Exception as e:
        raise ValueError(f"Invalid logging format string: {fmt}. Error: {e}") from e


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

    # Validate format string before using it
    _validate_log_format(resolved_format)

    logging.basicConfig(level=resolved_level.upper(), format=resolved_format, force=True)

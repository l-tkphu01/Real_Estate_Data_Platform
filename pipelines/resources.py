"""Khai báo Dagster resources.

Resources cung cấp shared dependencies như settings, logging và storage client
cho từng op.
"""

from __future__ import annotations

from src.config import Settings, load_settings
from src.logging_config import setup_logging
from src.storage.s3_client import S3StorageClient


def build_settings_resource(profile: str | None = None, config_dir: str | None = None) -> Settings:
    """Trả về settings resource theo config-driven profile."""

    settings = load_settings(profile=profile, config_dir=config_dir)
    setup_logging(level=settings.logging.level)
    return settings


def build_storage_resource(profile: str | None = None, config_dir: str | None = None) -> S3StorageClient:
    """Trả về S3 storage client resource dùng chung cho Dagster ops."""

    settings = build_settings_resource(profile=profile, config_dir=config_dir)
    return S3StorageClient(settings)

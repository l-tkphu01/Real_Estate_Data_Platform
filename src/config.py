"""Contract cấu hình ứng dụng theo hướng config-driven.

Nguồn cấu hình được nạp theo thứ tự ưu tiên:
1) base.yaml
2) profile yaml (ví dụ: local.minio.yaml)
3) environment variables
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_DIR = PROJECT_ROOT / "pipelines" / "config"


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """Runtime settings dùng cho metadata và orchestration services."""

    project_name: str
    profile: str
    dagster_home: str
    superset_secret_key: str


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    """Logging settings dùng xuyên suốt các modules."""

    level: str
    fmt: str


@dataclass(frozen=True, slots=True)
class StorageSettings:
    """Storage settings cho S3-compatible object storage."""

    s3_bucket: str
    s3_region: str
    s3_endpoint: str | None
    s3_access_key: str
    s3_secret_key: str
    s3_secure: bool
    raw_prefix: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    cdc_state_prefix: str


@dataclass(frozen=True, slots=True)
class IngestionSettings:
    """Ingestion settings cho retry, timeout và data source endpoint."""

    data_source_url: str
    request_timeout_seconds: int
    ingestion_max_retries: int
    ingestion_backoff_seconds: int


@dataclass(frozen=True, slots=True)
class Settings:
    """Settings tổng hợp cho toàn bộ pipeline."""

    runtime: RuntimeSettings
    logging: LoggingSettings
    storage: StorageSettings
    ingestion: IngestionSettings

    @property
    def s3_bucket(self) -> str:
        """Giữ tương thích ngược với các module đang dùng khóa cũ."""

        return self.storage.s3_bucket

    @property
    def s3_region(self) -> str:
        """Giữ tương thích ngược với các module đang dùng khóa cũ."""

        return self.storage.s3_region

    @property
    def s3_endpoint(self) -> str | None:
        """Giữ tương thích ngược với các module đang dùng khóa cũ."""

        return self.storage.s3_endpoint

    @property
    def s3_access_key(self) -> str:
        """Giữ tương thích ngược với các module đang dùng khóa cũ."""

        return self.storage.s3_access_key

    @property
    def s3_secret_key(self) -> str:
        """Giữ tương thích ngược với các module đang dùng khóa cũ."""

        return self.storage.s3_secret_key

    @property
    def log_level(self) -> str:
        """Giữ tương thích ngược với các module đang dùng khóa cũ."""

        return self.logging.level

    def is_aws_mode(self) -> bool:
        """Trả về True khi chạy với AWS S3 thay vì endpoint local."""

        return not bool(self.storage.s3_endpoint)

    def boto3_client_kwargs(self) -> dict[str, Any]:
        """Sinh boto3 client kwargs để lớp storage không phụ thuộc provider."""

        kwargs: dict[str, Any] = {
            "service_name": "s3",
            "region_name": self.storage.s3_region,
            "aws_access_key_id": self.storage.s3_access_key,
            "aws_secret_access_key": self.storage.s3_secret_key,
        }
        if self.storage.s3_endpoint:
            kwargs["endpoint_url"] = self.storage.s3_endpoint
        return kwargs


def _parse_bool(value: str) -> bool:
    """Parse chuỗi boolean từ environment variables."""

    return value.strip().lower() in {"1", "true", "yes", "on"}


def _load_yaml(path: Path) -> dict[str, Any]:
    """Nạp một file YAML và trả về dict rỗng nếu file trống."""

    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file config: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Merge 2 dict theo chiều sâu để profile override base config."""

    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _set_nested(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    """Gán value vào dict nhiều tầng theo đường dẫn path."""

    cursor = target
    for key in path[:-1]:
        cursor = cursor.setdefault(key, {})
    cursor[path[-1]] = value


def _load_env_overrides() -> dict[str, Any]:
    """Nạp các overrides từ environment variables đã chuẩn hóa."""

    mapping: dict[str, tuple[tuple[str, ...], Any]] = {
        "PROJECT_NAME": (("runtime", "project_name"), str),
        "LOG_LEVEL": (("logging", "level"), str),
        "LOG_FORMAT": (("logging", "fmt"), str),
        "DAGSTER_HOME": (("runtime", "dagster_home"), str),
        "SUPERSET_SECRET_KEY": (("runtime", "superset_secret_key"), str),
        "S3_BUCKET": (("storage", "s3_bucket"), str),
        "S3_REGION": (("storage", "s3_region"), str),
        "S3_ENDPOINT": (("storage", "s3_endpoint"), str),
        "S3_ACCESS_KEY": (("storage", "s3_access_key"), str),
        "S3_SECRET_KEY": (("storage", "s3_secret_key"), str),
        "S3_SECURE": (("storage", "s3_secure"), _parse_bool),
        "RAW_PREFIX": (("storage", "raw_prefix"), str),
        "BRONZE_PREFIX": (("storage", "bronze_prefix"), str),
        "SILVER_PREFIX": (("storage", "silver_prefix"), str),
        "GOLD_PREFIX": (("storage", "gold_prefix"), str),
        "CDC_STATE_PREFIX": (("storage", "cdc_state_prefix"), str),
        "DATA_SOURCE_URL": (("ingestion", "data_source_url"), str),
        "REQUEST_TIMEOUT_SECONDS": (("ingestion", "request_timeout_seconds"), int),
        "INGESTION_MAX_RETRIES": (("ingestion", "ingestion_max_retries"), int),
        "INGESTION_BACKOFF_SECONDS": (("ingestion", "ingestion_backoff_seconds"), int),
    }

    overrides: dict[str, Any] = {}
    for env_name, (path, parser) in mapping.items():
        raw = os.getenv(env_name)
        if raw is None:
            continue
        if env_name == "S3_ENDPOINT" and raw.strip() == "":
            _set_nested(overrides, path, None)
            continue
        _set_nested(overrides, path, parser(raw))
    return overrides


def _required(section: dict[str, Any], key: str, section_name: str) -> Any:
    """Lấy giá trị bắt buộc hoặc raise ValueError nếu thiếu."""

    value = section.get(key)
    if value is None or value == "":
        raise ValueError(f"Thiếu cấu hình bắt buộc: {section_name}.{key}")
    return value


def _build_settings(config: dict[str, Any], profile: str) -> Settings:
    """Build đối tượng Settings từ dict config đã merge và validate."""

    runtime_cfg = config.get("runtime", {})
    logging_cfg = config.get("logging", {})
    storage_cfg = config.get("storage", {})
    ingestion_cfg = config.get("ingestion", {})

    runtime = RuntimeSettings(
        project_name=str(_required(runtime_cfg, "project_name", "runtime")),
        profile=profile,
        dagster_home=str(_required(runtime_cfg, "dagster_home", "runtime")),
        superset_secret_key=str(_required(runtime_cfg, "superset_secret_key", "runtime")),
    )

    logging = LoggingSettings(
        level=str(logging_cfg.get("level", "INFO")).upper(),
        fmt=str(logging_cfg.get("fmt", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")),
    )

    endpoint = storage_cfg.get("s3_endpoint")
    endpoint = None if endpoint in {"", None} else str(endpoint)
    storage = StorageSettings(
        s3_bucket=str(_required(storage_cfg, "s3_bucket", "storage")),
        s3_region=str(_required(storage_cfg, "s3_region", "storage")),
        s3_endpoint=endpoint,
        s3_access_key=str(_required(storage_cfg, "s3_access_key", "storage")),
        s3_secret_key=str(_required(storage_cfg, "s3_secret_key", "storage")),
        s3_secure=bool(storage_cfg.get("s3_secure", False)),
        raw_prefix=str(storage_cfg.get("raw_prefix", "raw/real_estate")),
        bronze_prefix=str(storage_cfg.get("bronze_prefix", "bronze/real_estate")),
        silver_prefix=str(storage_cfg.get("silver_prefix", "silver/real_estate")),
        gold_prefix=str(storage_cfg.get("gold_prefix", "gold/real_estate")),
        cdc_state_prefix=str(storage_cfg.get("cdc_state_prefix", "state/cdc")),
    )

    ingestion = IngestionSettings(
        data_source_url=str(_required(ingestion_cfg, "data_source_url", "ingestion")),
        request_timeout_seconds=int(ingestion_cfg.get("request_timeout_seconds", 30)),
        ingestion_max_retries=int(ingestion_cfg.get("ingestion_max_retries", 3)),
        ingestion_backoff_seconds=int(ingestion_cfg.get("ingestion_backoff_seconds", 2)),
    )

    if ingestion.request_timeout_seconds <= 0:
        raise ValueError("ingestion.request_timeout_seconds phải lớn hơn 0")
    if ingestion.ingestion_max_retries < 1:
        raise ValueError("ingestion.ingestion_max_retries phải lớn hơn hoặc bằng 1")
    if ingestion.ingestion_backoff_seconds < 1:
        raise ValueError("ingestion.ingestion_backoff_seconds phải lớn hơn hoặc bằng 1")

    return Settings(runtime=runtime, logging=logging, storage=storage, ingestion=ingestion)


def load_settings(profile: str | None = None, config_dir: str | None = None) -> Settings:
    """Nạp settings theo chuẩn config-driven và validate fail-fast."""

    load_dotenv(PROJECT_ROOT / ".env", override=False)

    selected_profile = profile or os.getenv("APP_PROFILE", "local.minio")
    raw_config_dir = config_dir or os.getenv("CONFIG_DIR", "pipelines/config")
    resolved_dir = Path(raw_config_dir)
    if not resolved_dir.is_absolute():
        resolved_dir = PROJECT_ROOT / resolved_dir

    base_cfg = _load_yaml(resolved_dir / "base.yaml")
    profile_cfg = _load_yaml(resolved_dir / f"{selected_profile}.yaml")
    merged = _deep_merge(base_cfg, profile_cfg)
    merged = _deep_merge(merged, _load_env_overrides())

    return _build_settings(merged, selected_profile)

"""Contract cấu hình ứng dụng theo hướng config-driven.

Nguồn cấu hình được nạp theo thứ tự ưu tiên:
1) base.yaml
2) profile yaml (ví dụ: local.yaml)
3) environment variables (.env)

Chỉ hỗ trợ pipeline Bất Động Sản (category 1000).
"""

from __future__ import annotations

import logging
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
    profile: str  # Ví dụ: local, prod.azure
    dagster_home: str
    superset_secret_key: str

    def __post_init__(self):
        if not self.profile:
            raise ValueError("profile không được để trống")


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    """Logging settings dùng xuyên suốt các modules."""

    level: str  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    fmt: str  # Format string cho logging output


@dataclass(frozen=True, slots=True)
class StorageSettings:
    """Storage settings cho Azure Data Lake / Azurite."""

    azure_container: str
    azure_storage_account: str
    azure_storage_key: str
    azure_endpoint: str | None
    raw_prefix: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    cdc_state_prefix: str

    def __post_init__(self):
        # Validate bằng các hàm đã có bên dưới để đảm bảo không bị dead code
        object.__setattr__(self, 'raw_prefix', _validate_path_prefix(self.raw_prefix, 'raw_prefix'))
        object.__setattr__(self, 'bronze_prefix', _validate_path_prefix(self.bronze_prefix, 'bronze_prefix'))
        object.__setattr__(self, 'silver_prefix', _validate_path_prefix(self.silver_prefix, 'silver_prefix'))
        object.__setattr__(self, 'gold_prefix', _validate_path_prefix(self.gold_prefix, 'gold_prefix'))
        object.__setattr__(self, 'cdc_state_prefix', _validate_path_prefix(self.cdc_state_prefix, 'cdc_state_prefix'))


@dataclass(frozen=True, slots=True)
class IngestionSettings:
    """Ingestion settings cho retry, timeout, batching.

    Chỉ hỗ trợ Bất Động Sản (category 1000) — không cần multi-category.
    """

    data_source_url: str
    request_timeout_seconds: int
    ingestion_max_retries: int
    ingestion_backoff_seconds: int
    max_pages: int
    pages_per_batch: int
    batch_delay_seconds: float
    semaphore_size: int

    def __post_init__(self):
        object.__setattr__(self, 'request_timeout_seconds', _validate_timeout(self.request_timeout_seconds))
        object.__setattr__(self, 'ingestion_max_retries', _validate_retry_count(self.ingestion_max_retries))
        object.__setattr__(self, 'ingestion_backoff_seconds', _validate_backoff(self.ingestion_backoff_seconds))
        object.__setattr__(self, 'max_pages', _validate_max_pages(self.max_pages))
        object.__setattr__(self, 'pages_per_batch', _validate_pages_per_batch(self.pages_per_batch))
        object.__setattr__(self, 'batch_delay_seconds', _validate_batch_delay(self.batch_delay_seconds))
        object.__setattr__(self, 'semaphore_size', _validate_semaphore_size(self.semaphore_size))


@dataclass(frozen=True, slots=True)
class MdmSettings:
    """Settings cho Master Data Management."""
    fuzzy_match_threshold: float
    match_columns: list[str]
    city_mapping: dict[str, list[str]]
    district_mapping: dict[str, list[str]]
    property_type_mapping: dict[str, list[str]]
 

@dataclass(frozen=True, slots=True)
class CdcSettings:
    """Settings cho Change Data Capture (CDC)."""
    id_field: str
    hash_fields: list[str]
    state_filename: str
    hash_algorithm: str

    def __post_init__(self):
        if not self.id_field:
            raise ValueError("cdc.id_field không được để trống")
        if not self.hash_fields:
            raise ValueError("cdc.hash_fields không được để trống")
        if not self.state_filename:
            raise ValueError("cdc.state_filename không được để trống")
        if self.hash_algorithm not in ["md5", "sha256", "sha1"]:
            raise ValueError("cdc.hash_algorithm chỉ hỗ trợ: md5, sha1, sha256")


@dataclass(frozen=True, slots=True)
class AlertSettings:
    """Settings cho việc gửi Alerts/Notifications."""
    email_enabled: bool
    smtp_host: str
    smtp_port: int
    sender_email: str
    recipient_email: str
    smtp_password: str

@dataclass(frozen=True, slots=True)
class Settings:
    """Settings tổng hợp cho toàn bộ pipeline."""

    runtime: RuntimeSettings
    logging: LoggingSettings
    storage: StorageSettings
    ingestion: IngestionSettings
    mdm: MdmSettings
    cdc: CdcSettings
    alerts: AlertSettings

    @property
    def azure_container(self) -> str:
        return self.storage.azure_container

    @property
    def azure_storage_account(self) -> str:
        return self.storage.azure_storage_account

    @property
    def azure_storage_key(self) -> str:
        return self.storage.azure_storage_key

    @property
    def azure_endpoint(self) -> str | None:
        return self.storage.azure_endpoint

    def is_azure_cloud_mode(self) -> bool:
        """Trả về True khi chạy với Azure Cloud thay vì Azurite."""
        return not bool(self.storage.azure_endpoint)

    def azure_connection_string(self) -> str:
        """Build chuỗi kết nối Azure hỗ trợ Account Key (Azurite/Cloud).

        Hỗ trợ:
        - Azurite local (endpoint không None) → http + Account Key
        - Azure Cloud (endpoint None) → https + Account Key
        """
        if self.storage.azure_endpoint:
            return (
                f"DefaultEndpointsProtocol=http;"
                f"AccountName={self.storage.azure_storage_account};"
                f"AccountKey={self.storage.azure_storage_key};"
                f"BlobEndpoint={self.storage.azure_endpoint};"
            )
        else:
            return (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={self.storage.azure_storage_account};"
                f"AccountKey={self.storage.azure_storage_key};"
                f"EndpointSuffix=core.windows.net"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATORS
# ═══════════════════════════════════════════════════════════════════════════════

def _validate_path_prefix(prefix: str, field_name: str) -> str:
    """Validate storage path prefix (không cho phép .., trailing /, hoặc path rỗng)."""
    if not prefix or not isinstance(prefix, str):
        raise ValueError(f"{field_name} không được để trống")
    prefix_stripped = prefix.strip()
    if not prefix_stripped or prefix_stripped.startswith("/") or ".." in prefix_stripped:
        raise ValueError(
            f"{field_name} không hợp lệ: '{prefix}'. "
            f"Không được: rỗng, bắt đầu với '/', hoặc chứa '..'."
        )
    return prefix_stripped


def _validate_timeout(timeout_sec: int) -> int:
    """Validate request timeout (phải trong range 1-300 giây)."""
    if timeout_sec <= 0 or timeout_sec > 300:
        raise ValueError(
            f"request_timeout_seconds phải trong range (0, 300], "
            f"nhận được: {timeout_sec}"
        )
    return timeout_sec


def _validate_retry_count(retries: int) -> int:
    """Validate retry count (phải trong range 1-10)."""
    if retries < 1 or retries > 10:
        raise ValueError(
            f"ingestion_max_retries phải trong range [1, 10], "
            f"nhận được: {retries}"
        )
    return retries


def _validate_backoff(backoff_sec: int) -> int:
    """Validate backoff delay (phải >= 1)."""
    if backoff_sec < 1:
        raise ValueError(
            f"ingestion_backoff_seconds phải >= 1, "
            f"nhận được: {backoff_sec}"
        )
    return backoff_sec


def _validate_max_pages(max_pages: int) -> int:
    """Validate max_pages (phải trong range 1-200)."""
    if max_pages < 1 or max_pages > 200:
        raise ValueError(
            f"max_pages phải trong range [1, 200], "
            f"nhận được: {max_pages}"
        )
    return max_pages


def _validate_pages_per_batch(pages_per_batch: int) -> int:
    """Validate pages_per_batch (phải trong range 1-50)."""
    if pages_per_batch < 1 or pages_per_batch > 50:
        raise ValueError(
            f"pages_per_batch phải trong range [1, 50], "
            f"nhận được: {pages_per_batch}"
        )
    return pages_per_batch


def _validate_batch_delay(batch_delay: float) -> float:
    """Validate batch_delay_seconds (phải trong range 0.5-60)."""
    if batch_delay < 0.5 or batch_delay > 60:
        raise ValueError(
            f"batch_delay_seconds phải trong range [0.5, 60], "
            f"nhận được: {batch_delay}"
        )
    return batch_delay


def _validate_semaphore_size(sem_size: int) -> int:
    """Validate semaphore_size (phải trong range 1-10)."""
    if sem_size < 1 or sem_size > 10:
        raise ValueError(
            f"semaphore_size phải trong range [1, 10], "
            f"nhận được: {sem_size}"
        )
    return sem_size


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def _ensure_dagster_home(path_str: str) -> str:
    """Ensure DAGSTER_HOME path tồn tại, tạo nếu cần thiết."""
    if not path_str or not isinstance(path_str, str):
        raise ValueError("DAGSTER_HOME không được để trống")

    target = Path(path_str)
    if not target.is_absolute():
        target = PROJECT_ROOT / target

    if not target.exists():
        try:
            target.mkdir(parents=True, exist_ok=True, mode=0o755)
            logger = logging.getLogger(__name__)
            logger.info(f"Created DAGSTER_HOME: {target}")
        except Exception as e:
            raise ValueError(
                f"Không tạo được DAGSTER_HOME '{path_str}': {e}"
            ) from e

    return str(target)


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
        "AZURE_CONTAINER": (("storage", "azure_container"), str),
        "AZURE_STORAGE_ACCOUNT": (("storage", "azure_storage_account"), str),
        "AZURE_STORAGE_KEY": (("storage", "azure_storage_key"), str),
        "AZURE_ENDPOINT": (("storage", "azure_endpoint"), str),
        "RAW_PREFIX": (("storage", "raw_prefix"), str),
        "BRONZE_PREFIX": (("storage", "bronze_prefix"), str),
        "SILVER_PREFIX": (("storage", "silver_prefix"), str),
        "GOLD_PREFIX": (("storage", "gold_prefix"), str),
        "CDC_STATE_PREFIX": (("storage", "cdc_state_prefix"), str),
        "DATA_SOURCE_URL": (("ingestion", "data_source_url"), str),
        "REQUEST_TIMEOUT_SECONDS": (("ingestion", "request_timeout_seconds"), int),
        "INGESTION_MAX_RETRIES": (("ingestion", "ingestion_max_retries"), int),
        "INGESTION_BACKOFF_SECONDS": (("ingestion", "ingestion_backoff_seconds"), int),
        "MAX_PAGES": (("ingestion", "max_pages"), int),
        "PAGES_PER_BATCH": (("ingestion", "pages_per_batch"), int),
        "BATCH_DELAY_SECONDS": (("ingestion", "batch_delay_seconds"), float),
        "SEMAPHORE_SIZE": (("ingestion", "semaphore_size"), int),
        "CDC_ID_FIELD": (("cdc", "id_field"), str),
        "CDC_STATE_FILENAME": (("cdc", "state_filename"), str),
        "CDC_HASH_ALGORITHM": (("cdc", "hash_algorithm"), str),
        "CDC_HASH_FIELDS": (("cdc", "hash_fields"), lambda x: [i.strip() for i in x.split(",") if i.strip()]),
        "SMTP_HOST": (("alerts", "email", "smtp_host"), str),
        "SMTP_PORT": (("alerts", "email", "smtp_port"), int),
        "SMTP_SENDER": (("alerts", "email", "sender"), str),
        "SMTP_RECIPIENT": (("alerts", "email", "recipient"), str),
        "SMTP_PASSWORD": (("alerts", "email", "smtp_password"), str),
    }

    overrides: dict[str, Any] = {}
    for env_name, (path, parser) in mapping.items():
        raw = os.getenv(env_name)
        if raw is None:
            continue
        if env_name == "AZURE_ENDPOINT" and raw.strip() == "":
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

    dagster_home_raw = _required(runtime_cfg, "dagster_home", "runtime")
    dagster_home_validated = _ensure_dagster_home(str(dagster_home_raw))

    runtime = RuntimeSettings(
        project_name=str(_required(runtime_cfg, "project_name", "runtime")),
        profile=profile,
        dagster_home=dagster_home_validated,
        superset_secret_key=str(_required(runtime_cfg, "superset_secret_key", "runtime")),
    )

    log_settings = LoggingSettings(
        level=str(logging_cfg.get("level", "INFO")).upper(),
        fmt=str(logging_cfg.get("fmt", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")),
    )

    endpoint_raw = storage_cfg.get("azure_endpoint")
    if isinstance(endpoint_raw, str):
        endpoint_raw = endpoint_raw.strip()
    endpoint = None if endpoint_raw in {"", None} else str(endpoint_raw)

    storage = StorageSettings(
        azure_container=str(_required(storage_cfg, "azure_container", "storage")),
        azure_storage_account=str(_required(storage_cfg, "azure_storage_account", "storage")),
        azure_storage_key=str(_required(storage_cfg, "azure_storage_key", "storage")),
        azure_endpoint=endpoint,
        raw_prefix=storage_cfg.get("raw_prefix", "raw/real_estate"),
        bronze_prefix=storage_cfg.get("bronze_prefix", "bronze/real_estate"),
        silver_prefix=storage_cfg.get("silver_prefix", "silver/real_estate"),
        gold_prefix=storage_cfg.get("gold_prefix", "gold/real_estate"),
        cdc_state_prefix=storage_cfg.get("cdc_state_prefix", "state/cdc"),
    )

    ingestion = IngestionSettings(
        data_source_url=str(_required(ingestion_cfg, "data_source_url", "ingestion")),
        request_timeout_seconds=int(ingestion_cfg.get("request_timeout_seconds", 30)),
        ingestion_max_retries=int(ingestion_cfg.get("ingestion_max_retries", 3)),
        ingestion_backoff_seconds=int(ingestion_cfg.get("ingestion_backoff_seconds", 2)),
        max_pages=int(ingestion_cfg.get("max_pages", 10)),
        pages_per_batch=int(ingestion_cfg.get("pages_per_batch", 5)),
        batch_delay_seconds=float(ingestion_cfg.get("batch_delay_seconds", 2.0)),
        semaphore_size=int(ingestion_cfg.get("semaphore_size", 3)),
    )

    mdm_raw = config.get("mdm", {})
    entity_res = mdm_raw.get("entity_resolution", {})
    
    mdm = MdmSettings(
        fuzzy_match_threshold=float(entity_res.get("fuzzy_match_threshold", 0.85)),
        match_columns=list(entity_res.get("match_columns", ["title", "price", "area_sqm", "district"])),
        city_mapping=dict(mdm_raw.get("city_mapping", {})),
        district_mapping=dict(mdm_raw.get("district_mapping", {})),
        property_type_mapping=dict(mdm_raw.get("property_type_mapping", {}))
    )

    cdc_raw = config.get("cdc", {})
    cdc = CdcSettings(
        id_field=str(cdc_raw.get("id_field", "property_id")),
        hash_fields=list(cdc_raw.get("hash_fields", ["property_id", "title", "price", "area_sqm"])),
        state_filename=str(cdc_raw.get("state_filename", "fingerprints.json")),
        hash_algorithm=str(cdc_raw.get("hash_algorithm", "md5"))
    )

    alerts_raw = config.get("alerts", {}).get("email", {})
    alerts = AlertSettings(
        email_enabled=bool(alerts_raw.get("enabled", False)),
        smtp_host=str(alerts_raw.get("smtp_host", "smtp.gmail.com")),
        smtp_port=int(alerts_raw.get("smtp_port", 587)),
        sender_email=str(alerts_raw.get("sender", "")),
        recipient_email=str(alerts_raw.get("recipient", "")),
        smtp_password=str(os.getenv("SMTP_PASSWORD", ""))
    )

    return Settings(runtime=runtime, logging=log_settings, storage=storage, ingestion=ingestion, mdm=mdm, cdc=cdc, alerts=alerts)


def load_settings(profile: str | None = None, config_dir: str | None = None) -> Settings:
    """Nạp settings theo chuẩn config-driven và validate fail-fast."""

    load_dotenv(PROJECT_ROOT / ".env", override=False)

    selected_profile = profile or os.getenv("APP_PROFILE", "local")
    raw_config_dir = config_dir or os.getenv("CONFIG_DIR", "pipelines/config")
    resolved_dir = Path(raw_config_dir)
    if not resolved_dir.is_absolute():
        resolved_dir = PROJECT_ROOT / resolved_dir

    base_cfg = _load_yaml(resolved_dir / "base.yaml")
    profile_cfg = _load_yaml(resolved_dir / f"{selected_profile}.yaml")
    try:
        mdm_cfg = _load_yaml(resolved_dir / "mdm_rules.yaml")
    except FileNotFoundError:
        mdm_cfg = {}
        
    try:
        alerts_cfg = _load_yaml(resolved_dir / "alerts.yaml")
    except FileNotFoundError:
        alerts_cfg = {}
        
    merged = _deep_merge(base_cfg, profile_cfg)
    merged = _deep_merge(merged, mdm_cfg)
    merged = _deep_merge(merged, alerts_cfg)
    merged = _deep_merge(merged, _load_env_overrides())

    return _build_settings(merged, selected_profile)

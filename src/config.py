"""Contract cấu hình ứng dụng theo hướng config-driven.

Nguồn cấu hình được nạp theo thứ tự ưu tiên:
1) base.yaml
2) profile yaml (ví dụ: local.azurite.yaml)
3) environment variables
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
    profile: str # Ví dụ: local.azurite, prod.azure
    dagster_home: str # Đường dẫn tuyệt đối hoặc tương đối (từ PROJECT_ROOT) đến thư mục DAGSTER_HOME để lưu trữ logs, run history, v.v.
    superset_secret_key: str # Chuỗi bí mật dùng cho Superset session và CSRF protection (có thể random một giá trị đủ dài khi chạy local)


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    """Logging settings dùng xuyên suốt các modules."""

    level: str # Ví dụ: DEBUG (ghi chi tiết nhất), INFO (ghi thông tin chung, trạng thái hệ thống), WARNING (ghi cảnh báo), ERROR (ghi lỗi), CRITICAL (lỗi nghiem trọng sẽ không bị lọc bỏ)
    fmt: str # Ví dụ: "%(asctime)s | %(levelname)s | %(name)s | %(message)s" (thời gian | cấp độ log | tên logger | nội dung log) - có thể tùy chỉnh để thêm thread id, module name, v.v. theo nhu cầu observability của dự án.


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


@dataclass(frozen=True, slots=True)
class IngestionSettings:
    """Ingestion settings cho retry, timeout, batching, multi-category và data source endpoint."""

    data_source_url: str
    request_timeout_seconds: int
    ingestion_max_retries: int
    ingestion_backoff_seconds: int # Số giây chờ giữa các lần retry, sẽ tăng theo cấp số nhân (exponential backoff) để tránh quá tải hệ thống nguồn khi có sự cố tạm thời.
    max_pages: int # Tổng số trang cần cào (mỗi trang 20 records), default 10 cho MVP, 50 cho scaling (1000 records)
    pages_per_batch: int # Số trang cào trước khi tạm dừng (anti-ban), default 5
    batch_delay_seconds: float # Số giây chờ giữa các batch để tránh bị chặn, default 2.0
    semaphore_size: int # Số request đồng thời tối đa (anti-ban), default 3 để an toàn khi scale
    categories: list[int] # Danh sách category IDs cần cào (VD: [1000, 1001, 1002]), default [1000] (chỉ bất động sán)
    pages_per_category: int # Số trang cào/category (nếu multi-category), default = max_pages / len(categories)


@dataclass(frozen=True, slots=True)
class Settings:
    """Settings tổng hợp cho toàn bộ pipeline."""

    runtime: RuntimeSettings
    logging: LoggingSettings
    storage: StorageSettings
    ingestion: IngestionSettings

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
        
        TODO: Trong tương lai hỗ trợ SAS token + Managed Identity.
        """
        if self.storage.azure_endpoint:
            # Azurite mode (local development)
            return (
                f"DefaultEndpointsProtocol=http;"
                f"AccountName={self.storage.azure_storage_account};"
                f"AccountKey={self.storage.azure_storage_key};"
                f"BlobEndpoint={self.storage.azure_endpoint};"
            )
        else:
            # Azure Cloud mode (production)
            return (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={self.storage.azure_storage_account};"
                f"AccountKey={self.storage.azure_storage_key};"
                f"EndpointSuffix=core.windows.net"
            )

def _parse_bool(value: str) -> bool: 
    """Parse chuỗi boolean từ environment variables."""
    return value.strip().lower() in {"1", "true", "yes", "on"} # Hỗ trợ nhiều cách biểu diễn True phổ biến trong env vars (1, true, yes, on) để linh hoạt hơn khi cấu hình qua CLI hoặc CI/CD pipelines.


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


def _validate_categories(categories: list[int]) -> list[int]:
    """Validate categories list (không được rỗng, mỗi category trong range [1000, 1099])."""
    if not isinstance(categories, list) or len(categories) == 0:
        raise ValueError(
            f"categories phải là non-empty list, "
            f"nhận được: {categories}"
        )
    for cat_id in categories:
        if not isinstance(cat_id, int) or cat_id < 1000 or cat_id > 1099:
            raise ValueError(
                f"category ID phải trong range [1000, 1099], "
                f"nhận được: {cat_id}"
            )
    return categories


def _validate_pages_per_category(pages: int) -> int:
    """Validate pages_per_category (phải trong range 1-100)."""
    if pages < 1 or pages > 100:
        raise ValueError(
            f"pages_per_category phải trong range [1, 100], "
            f"nhận được: {pages}"
        )
    return pages


def _ensure_dagster_home(path_str: str) -> str:
    """Ensure DAGSTER_HOME path tồn tại, tạo nếu cần thiết.
    
    Trả về đường dẫn đã validate (tuyệt đối hoặc tương đối từ PROJECT_ROOT).
    """
    if not path_str or not isinstance(path_str, str):
        raise ValueError("DAGSTER_HOME không được để trống")
    
    target = Path(path_str)
    if not target.is_absolute():
        target = PROJECT_ROOT / target
    
    if not target.exists():
        try:
            target.mkdir(parents=True, exist_ok=True, mode=0o755) # dùng mode 755 để đảm bảo thư mục có quyền đọc/ghi cho owner và đọc/execute cho group và others, tránh lỗi permission khi Dagster cố gắng ghi logs hoặc run history vào đó.
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
            merged[key] = _deep_merge(merged[key], value) # Đệ quy merge dict con nếu cả base và override đều có dict ở key này
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
        "CATEGORIES": (("ingestion", "categories"), lambda x: [int(c.strip()) for c in x.split(",")]),
        "PAGES_PER_CATEGORY": (("ingestion", "pages_per_category"), int),
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

    logging = LoggingSettings(
        level=str(logging_cfg.get("level", "INFO")).upper(),
        fmt=str(logging_cfg.get("fmt", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")),
    )

    endpoint_raw = storage_cfg.get("azure_endpoint")
    if isinstance(endpoint_raw, str):
        endpoint_raw = endpoint_raw.strip()
    endpoint = None if endpoint_raw in {"", None} else str(endpoint_raw)
    
    raw_prefix = _validate_path_prefix(
        storage_cfg.get("raw_prefix", "raw/real_estate"), 
        "raw_prefix"
    )
    bronze_prefix = _validate_path_prefix(
        storage_cfg.get("bronze_prefix", "bronze/real_estate"), 
        "bronze_prefix"
    )
    silver_prefix = _validate_path_prefix(
        storage_cfg.get("silver_prefix", "silver/real_estate"), 
        "silver_prefix"
    )
    gold_prefix = _validate_path_prefix(
        storage_cfg.get("gold_prefix", "gold/real_estate"), 
        "gold_prefix"
    )
    cdc_state_prefix = _validate_path_prefix(
        storage_cfg.get("cdc_state_prefix", "state/cdc"), 
        "cdc_state_prefix"
    )
    
    storage = StorageSettings(
        azure_container=str(_required(storage_cfg, "azure_container", "storage")),
        azure_storage_account=str(_required(storage_cfg, "azure_storage_account", "storage")),
        azure_storage_key=str(_required(storage_cfg, "azure_storage_key", "storage")),
        azure_endpoint=endpoint,
        raw_prefix=raw_prefix,
        bronze_prefix=bronze_prefix,
        silver_prefix=silver_prefix,
        gold_prefix=gold_prefix,
        cdc_state_prefix=cdc_state_prefix,
    )

    timeout_validated = _validate_timeout(
        int(ingestion_cfg.get("request_timeout_seconds", 30))
    )
    retries_validated = _validate_retry_count(
        int(ingestion_cfg.get("ingestion_max_retries", 3))
    )
    backoff_validated = _validate_backoff(
        int(ingestion_cfg.get("ingestion_backoff_seconds", 2))
    )
    max_pages_validated = _validate_max_pages(
        int(ingestion_cfg.get("max_pages", 10))
    )
    pages_per_batch_validated = _validate_pages_per_batch(
        int(ingestion_cfg.get("pages_per_batch", 5))
    )
    batch_delay_validated = _validate_batch_delay(
        float(ingestion_cfg.get("batch_delay_seconds", 2.0))
    )
    semaphore_size_validated = _validate_semaphore_size(
        int(ingestion_cfg.get("semaphore_size", 3))
    )
    categories_validated = _validate_categories(
        ingestion_cfg.get("categories", [1000])  # Default: only real estate (1000)
    )
    # Calculate pages_per_category: total pages / num categories
    pages_per_category_default = max(1, max_pages_validated // len(categories_validated))
    pages_per_category_validated = _validate_pages_per_category(
        int(ingestion_cfg.get("pages_per_category", pages_per_category_default))
    )
    
    ingestion = IngestionSettings(
        data_source_url=str(_required(ingestion_cfg, "data_source_url", "ingestion")),
        request_timeout_seconds=timeout_validated,
        ingestion_max_retries=retries_validated,
        ingestion_backoff_seconds=backoff_validated,
        max_pages=max_pages_validated,
        pages_per_batch=pages_per_batch_validated,
        batch_delay_seconds=batch_delay_validated,
        semaphore_size=semaphore_size_validated,
        categories=categories_validated,
        pages_per_category=pages_per_category_validated,
    )

    return Settings(runtime=runtime, logging=logging, storage=storage, ingestion=ingestion)


def load_settings(profile: str | None = None, config_dir: str | None = None) -> Settings:
    """Nạp settings theo chuẩn config-driven và validate fail-fast."""

    load_dotenv(PROJECT_ROOT / ".env", override=False)

    selected_profile = profile or os.getenv("APP_PROFILE", "local.azurite")
    raw_config_dir = config_dir or os.getenv("CONFIG_DIR", "pipelines/config")
    resolved_dir = Path(raw_config_dir)
    if not resolved_dir.is_absolute():
        resolved_dir = PROJECT_ROOT / resolved_dir

    base_cfg = _load_yaml(resolved_dir / "base.yaml")
    profile_cfg = _load_yaml(resolved_dir / f"{selected_profile}.yaml")
    merged = _deep_merge(base_cfg, profile_cfg)
    merged = _deep_merge(merged, _load_env_overrides())

    return _build_settings(merged, selected_profile)

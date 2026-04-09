"""Khai báo Dagster resources.

Cung cấp settings, storage client và đặc biệt là SparkSession.
"""
from __future__ import annotations

from src.config import Settings, load_settings
from src.logging_config import setup_logging
from src.storage.azure_client import AzureStorageClient

def build_settings_resource(profile: str | None = None, config_dir: str | None = None) -> Settings:
    settings = load_settings(profile=profile, config_dir=config_dir)
    setup_logging(level=settings.logging.level)
    return settings

def build_storage_resource(profile: str | None = None, config_dir: str | None = None) -> AzureStorageClient:
    settings = build_settings_resource(profile=profile, config_dir=config_dir)
    return AzureStorageClient(settings)

def build_spark_resource(settings: Settings) -> Any:
    """Tạo SparkSession duy nhất, tối ưu RAM để bảo vệ máy local."""
    from pyspark.sql import SparkSession
    
    # 2GB RAM limit de test an toan tren may ca nhan
    builder = SparkSession.builder.appName(settings.runtime.project_name) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.6") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Connect to Azurite/Azure
    acc_name = settings.azure_storage_account
    acc_key = settings.azure_storage_key
    if settings.azure_endpoint:
        builder = builder.config(f"fs.azure.account.key.{acc_name}.dfs.core.windows.net", acc_key)
        # Bổ sung custom config cho hadoop-azure chạy với Azurite nếu cần
        # builder = builder.config("fs.azure.abfs.endpoint.suffix", ...)
        
    return builder.getOrCreate()

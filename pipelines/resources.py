"""Khai báo Dagster resources.

Cung cấp settings, storage client và đặc biệt là SparkSession.
"""
from __future__ import annotations

from typing import Any
from dagster import resource

from src.config import Settings, load_settings
from src.logging_config import setup_logging
from src.storage.azure_client import AzureStorageClient


@resource
def settings_resource(context) -> Settings:
    """Cấp phát tài nguyên cấu hình tổng thể cho pipeline (nạp tự động từ .env và yaml)."""
    settings = load_settings()
    setup_logging(level=settings.logging.level)
    return settings


@resource(required_resource_keys={"settings"})
def storage_resource(context) -> AzureStorageClient:
    """Cấp phát Azure Datalake/Azurite Storage Client."""
    return AzureStorageClient(context.resources.settings)


def build_spark_resource(settings: Settings) -> Any:
    """Tạo SparkSession duy nhất, tối ưu RAM để bảo vệ máy local."""
    from pyspark.sql import SparkSession
    
    # 1GB RAM limit de test an toan tren may ca nhan, tranh JVM bi kill (JAVA_GATEWAY_EXITED) do vuot qua RAM cua Docker
    builder = SparkSession.builder.appName(settings.runtime.project_name) \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
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


@resource(required_resource_keys={"settings"})
def spark_resource(context) -> Any:
    """Cấp phát SparkSession để xử lý bronze/silver/gold."""
    return build_spark_resource(context.resources.settings)

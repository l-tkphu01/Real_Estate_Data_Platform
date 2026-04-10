"""Entrypoint Dagster Definitions được load bởi workspace.yaml."""

from dagster import Definitions
from pipelines.jobs import ingestion_job, processing_job


defs = Definitions(
    jobs=[ingestion_job, processing_job],
)

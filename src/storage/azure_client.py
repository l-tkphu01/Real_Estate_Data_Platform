"""Azure Storage client abstraction."""

from typing import Any
from src.config import Settings

class AzureStorageClient:
    """Unified storage client interface cho Azure Data Lake / Azurite."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def get_connection_string(self) -> str:
        return self.settings.azure_connection_string()

    def put_json(self, key: str, payload: Any) -> None:
        raise NotImplementedError("Implement in build phase")

    def put_parquet(self, key: str, records: list[dict[str, Any]]) -> None:
        raise NotImplementedError("Implement in build phase")

    def list_keys(self, prefix: str) -> list[str]:
        raise NotImplementedError("Implement in build phase")

"""Khai báo Dagster resources.

Resources cung cấp shared dependencies như settings, logging và storage client
cho từng op.
"""


def build_settings_resource():
    """Trả về settings resource cho Dagster ops.

    TODO:
    - Khởi tạo từ environment variables.
    - Expose immutable config object.
    """

    raise NotImplementedError("Implement in build phase")


def build_storage_resource():
    """Trả về S3 storage client resource cho Dagster ops."""

    raise NotImplementedError("Implement in build phase")

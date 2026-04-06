"""Ops cho processing stage.

Luồng kỳ vọng:
1) validate records
2) clean và normalize
3) build silver và gold datasets
"""


def op_validate_records():
    """Chạy basic data quality checks và tách invalid records."""

    raise NotImplementedError("Implement in build phase")


def op_transform_silver():
    """Transform validated records sang silver schema."""

    raise NotImplementedError("Implement in build phase")


def op_transform_gold():
    """Tạo analytics-ready aggregates cho gold layer."""

    raise NotImplementedError("Implement in build phase")

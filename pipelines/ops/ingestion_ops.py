"""Ops cho ingestion stage.

Luồng kỳ vọng:
1) fetch source records
2) normalize payload
3) ghi immutable raw snapshot
"""


def op_fetch_source_data():
    """Fetch records từ source với retry và error handling."""

    raise NotImplementedError("Implement in build phase")


def op_store_raw_snapshot():
    """Lưu fetched records vào raw object storage."""

    raise NotImplementedError("Implement in build phase")

"""Ops cho CDC stage.

Luồng kỳ vọng:
1) load fingerprints trước đó
2) compute fingerprints hiện tại
3) chỉ giữ records mới/updated
"""


def op_detect_changes():
    """Lọc incoming records thành changed-only set cho idempotent loads."""

    raise NotImplementedError("Implement in build phase")


def op_persist_cdc_state():
    """Lưu updated CDC fingerprints sau khi processing thành công."""

    raise NotImplementedError("Implement in build phase")

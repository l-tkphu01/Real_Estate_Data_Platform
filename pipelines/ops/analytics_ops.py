"""Ops cho analytics và downstream delivery.

Luồng kỳ vọng:
1) materialize BI datasets
2) tùy chọn trigger Papermill notebook run
"""


def op_build_analytics_tables():
    """Build BI datasets để Superset tiêu thụ."""

    raise NotImplementedError("Implement in build phase")


def op_run_notebook_report():
    """Tùy chọn chạy parameterized notebook qua Papermill."""

    raise NotImplementedError("Implement in build phase")

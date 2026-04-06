"""Contract Dagster jobs cho end-to-end real estate pipeline."""


def build_ingestion_job():
    """Định nghĩa job cho fetch -> raw storage -> cập nhật CDC state."""

    raise NotImplementedError("Implement in build phase")


def build_processing_job():
    """Định nghĩa job cho clean/validate -> silver/gold writes."""

    raise NotImplementedError("Implement in build phase")

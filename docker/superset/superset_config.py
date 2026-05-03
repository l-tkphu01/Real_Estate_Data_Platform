"""Placeholder cấu hình Superset local cho dự án này."""

# File này được giữ tối giản ở giai đoạn hiện tại.
# Thêm SQLALCHEMY_DATABASE_URI và security hardening trước khi dùng production.

FEATURE_FLAGS = {
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
}

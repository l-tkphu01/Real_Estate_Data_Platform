# Scripts

Thư mục này chứa script hỗ trợ bootstrap và vận hành local.

Gợi ý script nên thêm trong giai đoạn build:
- `bootstrap.ps1`: tạo virtual environment và cài dependency.
- `run_pipeline_local.ps1`: chạy pipeline local qua Dagster.
- `smoke_test.ps1`: kiểm tra nhanh health endpoint của Azurite, Dagster, Superset.

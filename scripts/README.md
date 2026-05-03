# Scripts

Thư mục này chứa script hỗ trợ bootstrap và vận hành local.

Gợi ý script nên thêm trong giai đoạn build:
- `bootstrap.ps1`: tạo virtual environment và cài dependency.
- `run_pipeline_local.ps1`: chạy pipeline local qua Dagster.
- `smoke_test.ps1`: kiểm tra nhanh health endpoint của Azurite, Dagster, Superset.


# Lệnh xem danh sách lỗi (City, District) đang bị kẹt trong bảng Quarantine và số lượng bản ghi tương ứng 

docker exec -it real-estate-dagster-web python scripts/view_quarantine.py
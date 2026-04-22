# Đánh Giá Trước Khi Build (No-Code Review)

## 1) Rủi ro và tối ưu ở tầng Ingestion
- Rủi ro: API/web nguồn không ổn định làm mất dữ liệu theo đợt.
- Tối ưu: thêm retry + timeout + phân loại lỗi (network, HTTP, schema).
- Tối ưu: luôn lưu raw snapshot trước mọi bước transform.
- Tối ưu: kiểm tra contract dữ liệu nguồn (field bắt buộc, kiểu số, thời gian).

## 2) Rủi ro và tối ưu ở tầng Storage
- Rủi ro: phụ thuộc chặt vào endpoint cục bộ, khó chuyển cloud.
- Tối ưu: dùng một abstraction interface và Spark resource config cho cả Azurite (local) và Azure Data Lake Gen2.
- Tối ưu: đặt key có partition theo thời gian (`dt=YYYY-MM-DD/hr=HH`), tận dụng cấu trúc thư mục thật của ADLS.

## 3) Rủi ro và tối ưu ở tầng CDC
- Rủi ro: full reload mỗi lần chạy gây tốn tài nguyên và dễ trùng dữ liệu.
- Tối ưu: CDC dựa trên fingerprint ổn định (`property_id + price + listed_at`).
- Tối ưu: lưu CDC state trên object storage để orchestration stateless.

## 4) Rủi ro và tối ưu ở tầng Processing
- Rủi ro: dữ liệu chất lượng thấp làm sai dashboard downstream.
- Tối ưu: validate bắt buộc trước khi transform.
- Tối ưu: tách record lỗi vào vùng quarantine để audit/debug.
- Tối ưu: bảo đảm transform idempotent (cùng input -> cùng output).

## 5) Rủi ro và tối ưu ở tầng Lakehouse
- Rủi ro: ghi không transaction dẫn tới bảng không nhất quán. Xử lý dữ liệu in-memory gây tràn RAM máy.
- Tối ưu: dùng `pyspark` và `delta-spark` merge/upsert theo business key. Spark có cơ chế Distributed out-of-core.
- Tối ưu: schema evolution chỉ bật khi có kiểm soát thay đổi.

## 6) Rủi ro và tối ưu về Orchestration/Monitoring
- Rủi ro: thiếu quan sát pipeline health và SLA.
- Tối ưu: log có cấu trúc theo op (run_id, op_name, record_count).
- Tối ưu: cấu hình retry + cảnh báo cho Dagster runs.

## 7) Rủi ro và tối ưu ở tầng Analytics/Visualization
- Rủi ro: dashboard đọc nhầm dữ liệu chưa curate.
- Tối ưu: chỉ expose gold dataset cho Superset.
- Tối ưu: version hóa metric definition để báo cáo lặp lại nhất quán.

## 8) Cổng kiểm tra sẵn sàng production
- Bắt buộc: có kiểm thử idempotency và kịch bản replay-safe.
- Bắt buộc: không hardcode endpoint/credentials trong code.
- Bắt buộc: có checklist migrate local -> cloud và đã test.

# kết nối database
- Superset connect: duckdb:////app/data/superset.db

-> Nạp bảng Delta Lake vào Superset (Bước quan trọng nhất) Vì dữ liệu của bạn lưu dạng File (Delta Lake) chứ không phải MySQL/Postgres truyền thống, nên bạn không thể chọn bảng từ danh sách thả xuống được. Bạn phải nạp nó qua SQL Lab:

Trên thanh menu ngang, chọn SQL Lab -> SQL Editor.
Chọn Database là cái DuckDB bạn vừa tạo.
Ở khung soạn thảo, để đọc bảng Fact, bạn gõ đoạn mã này:
# Đánh Giá Trước Khi Build (No-Code Review)

## 1) Rủi ro và tối ưu ở tầng Ingestion
- Rủi ro: API/web nguồn không ổn định làm mất dữ liệu theo đợt.
- Tối ưu: thêm retry + timeout + phân loại lỗi (network, HTTP, schema).
- Tối ưu: luôn lưu raw snapshot trước mọi bước transform.
- Tối ưu: kiểm tra contract dữ liệu nguồn (field bắt buộc, kiểu số, thời gian).

## 2) Rủi ro và tối ưu ở tầng Storage
- Rủi ro: phụ thuộc chặt vào endpoint MinIO, khó chuyển cloud.
- Tối ưu: dùng một abstraction interface cho cả MinIO và AWS S3.
- Tối ưu: đặt key có partition theo thời gian (`dt=YYYY-MM-DD/hr=HH`).

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
- Rủi ro: ghi không transaction dẫn tới bảng không nhất quán.
- Tối ưu: dùng Delta merge/upsert theo business key.
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

# Pipeline Configurations ⚙️

Thư mục `pipelines/config/` bao gồm toàn bộ các tệp mô phỏng theo **kiến trúc Config-Driven**, cho phép bạn kiểm soát hành vi của cả pipeline mà không cần phải can thiệp trực tiếp vào mã nguồn Python.

## 🗂️ Danh sách Configuration Files (Sơ đồ chức năng)

Để quản lý dễ dàng hơn, cấu hình được phân rã thành các tệp cụ thể như sau:

| File | Vai Trò (Role) | Chức năng chính |
| :--- | :--- | :--- |
| `base.yaml` | **Cấu hình Gốc (Core)** | Là xương sống của ứng dụng. Chứa các thiết lập nền tảng, thiết lập thư mục Storage, cấu hình CDC (Change Data Capture), cấu hình Crawler (Ingestion). Các chỉ số này luôn được nạp ở mọi lúc. |
| `local.yaml` | **Profile Local** | Chuyên dùng cho môi trường giả lập (Azurite). Nó sẽ bọc (override) các endpoint và tham số phù hợp với resource của môi trường docker-compose ở máy local. |
| `local.azure.yaml`| **Profile Cloud Test** | Dành để trỏ pipeline ở máy tính chạy thử trực tiếp vào Storage Account trên Azure Cloud. |
| `mdm_rules.yaml` | **Từ Điển Tập Trung** | Chứa logic chuẩn hóa dữ liệu MDM (Entity Resolution, mapping chuẩn các địa danh, tên dự án). Tách tuyệt đối business logic rườm rà ra khỏi Python. |
| `run_config_local.yaml`| **Run Variables** | Blueprint cấu hình Dagster lúc chạy (Launch config overrides). |
| `alerts.yaml` | **Alerts/Monitoring** | Cấu hình cho cảnh báo đường mòn SLA, tích hợp Slack/Email (Blueprint). |
| `schedules.yaml` | **Lập Lịch** | Lịch trình các cron job kích hoạt hệ thống tự động chạy theo ngày/giờ (Blueprint). |

## ⚙️ Cơ chế nạp cấu hình (Loading Priority)

Hệ thống (*xem class Settings trong src/config.py*) sẽ nạp từng cấu hình lên theo thứ tự ghi đè như sau:
1. `base.yaml` (Ưu tiên thấp nhất - Fallback defaults)
2. `.yaml` profile file (`local.yaml`, v.v... dựa theo `APP_PROFILE`)
3. `mdm_rules.yaml` (Nạp song song từ điển mdm)
4. Tệp môi trường thuần `.env` (Ưu tiên cao nhất — Bí mật, Credentials, Keys)

---
> 💡 **Best Practice:** Để đổi hash algorithm CDC, hãy mở `base.yaml` phần `cdc: hash_algorithm`. Không nên sửa logic thẳng trong `src/cdc/fingerprint.py`.
# Pipeline Config

Thư mục này chứa các cấu hình cho orchestration, schedule và run settings.

## Danh sách file hiện tại
- `base.yaml`: cấu hình nền dùng chung cho mọi môi trường.
- `local.yaml`: override cho local chạy với Azurite emulator.
- `local.azure.yaml`: override cho local chạy với Azure ADLS Gen2.
- `mdm_rules.yaml`: quy tắc Master Data Management (entity resolution, city mapping).
- `run_config_local.yaml`: blueprint run config cho Dagster local.
- `schedules.yaml`: blueprint cron schedules.
- `alerts.yaml`: blueprint alerts/SLA (Service Level Agreement).

## Thứ tự ưu tiên khi nạp config
1. `base.yaml`
2. file profile (ví dụ `local.yaml`)
3. environment variables trong `.env` hoặc OS env

## Cách chọn profile
Trong file `.env`:
- `APP_PROFILE=local` để chạy Azurite local (default).
- `APP_PROFILE=local.azure` để dùng Azure Cloud.

Có thể override thư mục config bằng:
- `CONFIG_DIR=pipelines/config`

## Lưu ý quan trọng
- Không hardcode endpoint/credentials trong code.
- Secrets phải nằm trong env, không commit vào git.
- Chỉ thay profile/env khi chuyển Azurite → Azure, không sửa business logic.
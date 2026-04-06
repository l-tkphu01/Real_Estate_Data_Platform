# Pipeline Config

Thư mục này chứa các cấu hình cho orchestration, schedule và run settings.

## Danh sách file hiện tại
- `base.yaml`: cấu hình nền dùng chung cho mọi môi trường.
- `local.minio.yaml`: override cho local chạy với MinIO.
- `local.aws.yaml`: override cho local chạy với AWS S3.
- `run_config_local.yaml`: blueprint run config cho Dagster local.
- `schedules.yaml`: blueprint cron schedules.
- `alerts.yaml`: blueprint alerts/SLA.

## Thứ tự ưu tiên khi nạp config
1. `base.yaml`
2. file profile (ví dụ `local.minio.yaml`)
3. environment variables trong `.env` hoặc OS env

## Cách chọn profile
Trong file `.env`:
- `APP_PROFILE=local.minio` để chạy MinIO local.
- `APP_PROFILE=local.aws` để dùng AWS S3.

Có thể override thư mục config bằng:
- `CONFIG_DIR=pipelines/config`

## Lưu ý quan trọng
- Không hardcode endpoint/credentials trong code.
- Secrets phải nằm trong env, không commit vào git.
- Chỉ thay profile/env khi chuyển MinIO -> AWS, không sửa business logic.
    
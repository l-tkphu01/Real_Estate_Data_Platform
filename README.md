# Real Estate Data Platform - End-to-End Data Pipeline

## 1) Mục tiêu dự án

Dự án này hướng tới một pipeline dữ liệu bất động sản hoàn chỉnh theo chuỗi:

Ingestion -> Raw Storage -> CDC -> Processing -> Lakehouse -> Analytics -> Visualization

Mục tiêu thiết kế:
- Định hướng production nhưng vẫn thân thiện với người mới.
- Thiết kế mô-đun (modular), idempotent, có khả năng mở rộng theo khối lượng dữ liệu.
- Chạy local bằng Azurite (Azure Storage Emulator), sau đó chuyển sang Azure Data Lake Storage Gen2 (ADLS Gen2) mà không đổi logic cốt lõi.

## 2) Trạng thái hiện tại (giai đoạn thiết kế, chưa code logic)

Theo yêu cầu hiện tại, repository đang ở giai đoạn blueprint:
- Đã có đầy đủ cấu trúc thư mục.
- Các file Python là skeleton, mô tả rõ chức năng từng file/hàm.
- Chưa triển khai business logic để bạn có thể build tuần tự từ đầu.

## 3) Đánh giá trước khi build: các điểm cần tối ưu

Xem chi tiết tại file `docs/pre_build_assessment.md`.

Tóm tắt nhanh:
- Ingestion: cần retry, timeout, phân loại lỗi, kiểm tra contract dữ liệu nguồn.
- Storage: cần abstraction Azure Blob/ADLS Gen2 để tránh hardcode Azurite.
- CDC: bắt buộc lưu fingerprint state để tránh full reload.
- Processing: cần validation, tách record lỗi sang vùng quarantine. Xử lý chính bằng PySpark (thay vì Pandas) để tránh sập RAM.
- Lakehouse: cần Delta merge/upsert bằng delta-spark để bảo đảm ACID và replay-safe.
- Orchestration: cần logging có cấu trúc, theo dõi SLA, retry chính sách rõ ràng.

## 4) Kiến trúc tổng thể

### 4.1 Luồng nghiệp vụ

1. Ingestion
- Lấy dữ liệu từ API/web.
- Có retry + xử lý lỗi cơ bản.

2. Raw Storage
- Lưu payload gốc (JSON/Parquet) vào Azurite/ADLS Gen2.
- Lưu theo snapshot bất biến để audit và replay.

3. CDC
- Tính fingerprint (ví dụ: property_id + price + listed_at).
- Chỉ giữ record mới hoặc thay đổi.

4. Processing
- Validate dữ liệu bắt buộc.
- Làm sạch, chuẩn hóa field.
- Tạo dữ liệu sẵn sàng cho analytics.

5. Lakehouse
- Upsert vào bronze/silver bằng Delta Lake.
- Tạo gold dataset phục vụ BI.

6. Orchestration
- Dùng Dagster để định nghĩa jobs/ops và dependency.
- Gắn logging, retry, và quan sát run metadata.

7. Analytics
- Tạo các bộ dữ liệu tổng hợp cho BI.
- Có thể chạy notebook qua Papermill (tùy chọn).

8. Visualization
- Dùng Superset làm dashboard giá và so sánh khu vực.

### 4.2 Stack local
- Azurite: object storage giả lập Azure Blob Storage.
- Dagster: orchestration + monitoring, khởi tạo PySpark Session.
- Apache Spark (PySpark): xử lý dữ liệu lớn in-memory & distributed, có config giới hạn RAM (e.g. 2GB) khi chạy local.
- Superset: BI và dashboard.

## 5) Cấu trúc thư mục dự án

```text
Real Estate Data Platform – End-to-End Data Pipeline/
|-- .env.example
|-- .gitignore
|-- docker-compose.yml
|-- requirements.txt
|-- workspace.yaml
|-- README.md
|-- data/
|-- docker/
|   |-- dagster/
|   |   `-- Dockerfile
|   `-- superset/
|       `-- superset_config.py
|-- docs/
|   `-- pre_build_assessment.md
|-- notebooks/
|   `-- README.md
|-- pipelines/
|   |-- __init__.py
|   |-- definitions.py
|   |-- jobs.py
|   |-- resources.py
|   |-- config/
|   |   `-- README.md
|   `-- ops/
|       |-- __init__.py
|       |-- ingestion_ops.py
|       |-- cdc_ops.py
|       |-- processing_ops.py
|       `-- analytics_ops.py
|-- scripts/
|   `-- README.md
`-- src/
    |-- __init__.py
    |-- config.py
    |-- logging_config.py
    |-- models/
    |   `-- property.py
    |-- scraper/
    |   |-- __init__.py
    |   |-- client.py
    |   `-- normalizer.py
    |-- storage/
    |   |-- __init__.py
    |   |-- azure_client.py
    |   `-- raw_storage.py
    |-- cdc/
    |   |-- __init__.py
    |   |-- fingerprint.py
    |   `-- state_store.py
    |-- processing/
    |   |-- __init__.py
    |   |-- validation.py
    |   |-- cleaning.py
    |   `-- transform.py
    |-- lakehouse/
    |   |-- __init__.py
    |   `-- delta_writer.py
    `-- analytics/
        |-- __init__.py
        `-- aggregations.py
```

## 6) Vai trò từng file Python (để nắm luồng)

### 6.1 Nhóm core
- `src/config.py`: contract cấu hình môi trường.
- `src/logging_config.py`: chuẩn hóa logging cho toàn bộ pipeline.
- `src/models/property.py`: schema chuẩn của record bất động sản.

### 6.2 Nhóm ingestion + raw
- `src/scraper/client.py`: lấy dữ liệu nguồn, retry, xử lý lỗi.
- `src/scraper/normalizer.py`: chuẩn hóa payload về schema chung.
- `src/storage/azure_client.py`: abstraction đọc/ghi thư mục (Azurite hoặc ADLS).
- `src/storage/raw_storage.py`: tạo key partition và lưu snapshot raw.

### 6.3 Nhóm CDC
- `src/cdc/fingerprint.py`: tính fingerprint, phát hiện thay đổi.
- `src/cdc/state_store.py`: lưu/đọc state fingerprint giữa các lần chạy.

### 6.4 Nhóm processing + lakehouse
- `src/processing/validation.py`: kiểm tra chất lượng dữ liệu.
- `src/processing/cleaning.py`: làm sạch và chuẩn hóa giá trị.
- `src/processing/transform.py`: tạo dữ liệu silver/gold.
- `src/lakehouse/delta_writer.py`: ghi/upsert Delta Lake.

### 6.5 Nhóm analytics
- `src/analytics/aggregations.py`: tính toán dataset tổng hợp phục vụ BI.

### 6.6 Nhóm orchestration
- `pipelines/resources.py`: khai báo resource dùng chung cho các op.
- `pipelines/ops/*.py`: từng bước xử lý theo stage.
- `pipelines/jobs.py`: ghép op thành pipeline jobs.
- `pipelines/definitions.py`: entry point để Dagster load toàn bộ.

## 7) Hướng dẫn triển khai từng bước từ đầu

### Bước 0 - Chuẩn bị môi trường

Yêu cầu:
- Docker + Docker Compose
- Python 3.11
- Git

Mục tiêu:
- Máy local đủ công cụ để chạy stack.

### Bước 1 - Khởi tạo cấu hình

1. Tạo file `.env` tAzurite local:
- `AZURE_STORAGE_ACCOUNT=devstoreaccount1`
- `AZURE_STORAGE_KEY=Eby8vdM02xNO...`
- `AZURE_ENDPOINT=http://azurite:10000/devstoreaccount1`
3. Chọn profile config-driven:
- `APP_PROFILE=local.azurite` cho Azurite local.
- `APP_PROFILE=local.azure` cho Azure Data LakeO local.
- `APP_PROFILE=local.aws` cho AWS S3.
- `CONFIG_DIR=pipelines/config` để chỉ thư mục profile.

Definition of Done:
- Tất cả biến bắt buộc đã có giá trị.

### Bước 2 - Khởi động hạ tầng local

Chạy:
```bash
docker compose up -d --build
```

Kiểm tra:
- Azurite Container Logs
- Dagster UI: http://localhost:3000
- Superset: http://localhost:8088

Definition of Done:
- 3 service đều ở trạng thái healthy.

### Bước 3 - Triển khai ingestion layer

Thực hiện tại:
- `src/scraper/client.py`
- `src/scraper/normalizer.py`
- `src/storage/raw_storage.py`

Bắt buộc có:
- Retry exponential backoff.
- Timeout + exception handling.
- Lưu snapshot raw bất biến.

Kiểm thử tối thiểu:
- Lỗi mạng tạm thời vẫn retry đúng số lần.
- Payload sai định dạng không làm sập pipeline.

### Bước 4 - Triển khai CDC

Thực hiện tại:
- `src/cdc/fingerprint.py`
- `src/cdc/state_store.py`

Bắt buộc có:
- Fingerprint ổn định.
- Chỉ phát sinh record mới/thay đổi.
- Lưu state sau mỗi run thành công.

Kiểm thử tối thiểu:
- Chạy lại cùng input không tạo dữ liệu mới.
- Đổi `price` thì record được nhận diện updated.

### Bước 5 - Triển khai processing + validation

Thực hiện tại:
- `src/processing/validation.py`
- `src/processing/cleaning.py`
- `src/processing/transform.py`

Bắt buộc có:
- Required-field checks.
- Numeric constraints.
- Chuẩn hóa kiểu dữ liệu và naming.
- Idempotent transformation.

Kiểm thử tối thiểu:
- Cùng input chạy nhiều lần vẫn cho output giống nhau.

### Bước 6 - Triển khai Delta lakehouse

Thực hiện tại:
- `src/lakehouse/delta_writer.py`

Bắt buộc có:
- Merge/upsert cho bronze và silver.
- Overwrite có kiểm soát cho gold.
- Chính sách schema evolution rõ ràng.

Kiểm thử tối thiểu:
- Re-run cùng batch không tạo duplicate.

### Bước 7 - Triển khai Dagster orchestration

Thực hiện tại:
- `pipelines/resources.py`
- `pipelines/ops/*.py`
- `pipelines/jobs.py`
- `pipelines/definitions.py`

Bắt buộc có:
- Dependency rõ ràng giữa các op.
- Logging có context từng op.
- Retry policy cho các bước gọi external.

Kiểm thử tối thiểu:
- Trigger được một run thành công từ Dagster UI.
- Có thể rerun an toàn.

### Bước 8 - Triển khai analytics + notebooks (tùy chọn)

Thực hiện tại:
- `src/analytics/aggregations.py`
- `notebooks/` (chạy qua Papermill nếu cần)

Bắt buộc có:
- Dataset xu hướng giá theo thời gian.
- Dataset so sánh theo thành phố/quận.

### Bước 9 - Xây dashboard Superset

Bắt buộc có:
- Dashboard xu hướng giá.
- Dashboard so sánh khu vực.
- Bộ lọc theo city, district, khoảng thời gian.

### Bước 10 - Hardening trước production

Bắt buộc có:
- Alert khi pipeline fail.
- Quản lý secrets an toàn (không dùng default credentials).
- Data retention policy cho raw/bronze/silver/gold.
- Theo dõi chi phí và hiệu năng.

## 8) Cấu hình mẫu `.env`

Lấy `.env.example` làm chuẩn.

Với local Azurite:
```env
AZURE_ENDPOINT=http://azurite:10000/devstoreaccount1
AZURE_STORAGE_ACCOUNT=devstoreaccount1
AZURE_STORAGE_KEY=Eby8vd...
AZURE_CONTAINER=real-estate-platform
```

## 9) Chuyển Azurite sang Azure Data Lake (không đổi core logic)

Chỉ cần đổi biến môi trường:
- Đặt `APP_PROFILE=local.azure`.
- Để trống `AZURE_ENDPOINT=`.
- Đặt `AZURE_STORAGE_ACCOUNT` và `AZURE_STORAGE_KEY` theo Azure Tenant của bạn.
- Đặt `AZURE_CONTAINER` theo container thực tế.
- Cloud Authentication qua Connection String và adlfs/hadoop-azure.

Nguyên tắc:
- Toàn bộ module chỉ gọi qua interface storage abstraction.
- Không hardcode endpoint ở nơi khác ngoài config.

## 10) Lý do thiết kế (design decisions)

- Dùng CDC fingerprint để giảm chi phí, tránh duplicate.
- Tách raw/bronze/silver/gold để dễ audit, replay, truy vết.
- Dùng Dagster để quản trị dependency và quan sát vận hành.
- Dùng Superset để dashboard nhanh, phù hợp bài toán BI.

## 11) Hướng cải tiến tương lai

- Thêm contract testing cho API nguồn.
- Thêm framework data quality (ví dụ Great Expectations).
- Thêm unit/integration tests và CI/CD.
- Thêm lineage metadata (OpenLineage).
- Thêm data catalog và ownership model.

## 12) Bộ câu hỏi phỏng vấn mẫu

1. Vì sao cần raw immutable layer?
- Để audit, replay và debug mà không mất dữ liệu gốc.

2. CDC fingerprint giải quyết vấn đề gì?
- Tránh full reload, giảm tài nguyên, giữ idempotency.

3. Làm sao migrate Azurite sang Azure Data Lake mà không đổi code lõi?
- Dùng storage abstraction, chỉ thay biến môi trường. SparkSession cũng được config tự động lấy credentials Cloud thay vì endpoint devstore.

4. Vì sao cần tách bronze/silver/gold?
- Mỗi layer có mục tiêu khác nhau: ingest, curate, serve BI.

5. Idempotency quan trọng như thế nào?
- Có thể rerun sau lỗi mà không làm sai dữ liệu hay KPI.

6. Nếu source API thay đổi schema đột ngột thì xử lý sao?
- Thêm validation và route bản ghi lỗi vào quarantine.

## 13) Kế hoạch tiếp theo đề xuất

Theo đúng thứ tự để hạn chế rủi ro:
1. Hoàn thiện `src/config.py`, `pipelines/resources.py` (Mở SparkSession RAM nhỏ) và `src/storage/azure_client.py` trước.
2. Hoàn thiện ingestion + raw snapshot.
3. Hoàn thiện CDC + kiểm thử idempotency.
4. Hoàn thiện processing/lakehouse.
5. Nối vào Dagster jobs và Superset dashboards.

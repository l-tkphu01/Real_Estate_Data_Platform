# Kịch Bản Tích Hợp Azure Databricks với Dagster

Tài liệu này cung cấp kịch bản từng bước để chuyển đổi pipeline xử lý dữ liệu (PySpark) từ việc chạy trực tiếp trên Container App (bị giới hạn RAM) sang việc chạy trên **Azure Databricks Cluster** mạnh mẽ, do Dagster điều phối.

## 1. Kiến Trúc Hoạt Động (Architecture)

**Hiện tại:**
`Dagster Container (1GB RAM)` ---> Tự chạy `PySpark` nội bộ ---> **Lỗi Out Of Memory (OOM)**

**Mục tiêu (Với Databricks):**
`Dagster Container` ---> *Gửi lệnh chạy Job (API)* ---> `Databricks Cluster` ---> *Đọc/Ghi dữ liệu vào Data Lake* ---> Báo cáo kết quả về `Dagster`.

Dagster lúc này chỉ đóng vai trò là "Nhạc trưởng" (Orchestrator) ra lệnh, còn Databricks là "Công nhân" (Compute) thực thi xử lý dữ liệu nặng.

---

## 2. Các Bước Chuẩn Bị Trên Azure Databricks

Bạn cần lấy 3 thông tin quan trọng từ không gian làm việc (Workspace) Databricks của bạn:

1.  **Workspace URL (Host):**
    *   Mở Databricks Workspace của bạn.
    *   Copy URL trên thanh địa chỉ (ví dụ: `https://adb-123456789.azuredatabricks.net/`).
2.  **Personal Access Token (PAT):**
    *   Vào Databricks, click vào biểu tượng User của bạn ở góc phải trên cùng -> **Settings** -> **Developer** -> **Access tokens**.
    *   Tạo một Token mới và **copy lại ngay lập tức** (bạn sẽ không thể xem lại mã này).
3.  **Cluster ID:**
    *   Vào mục **Compute** -> Chọn Cluster bạn đã tạo (ví dụ: `realestate-cluster`).
    *   Mở tab **Configuration** -> **Advanced options** -> **Tags** -> Tìm `Cluster ID` (ví dụ: `0429-123456-abcdefg`).

---

## 3. Khai Báo Biến Môi Trường (Environment Variables)

Thêm các thông tin vừa lấy vào file `.env.cloud` và `.env` local của bạn:

```env
# ==============================
# Databricks Configuration
# ==============================
DATABRICKS_HOST=https://adb-123456789.azuredatabricks.net/
DATABRICKS_TOKEN=dapi...mã_token_của_bạn...
DATABRICKS_CLUSTER_ID=0429-123456-abcdefg
```

---

## 4. Cập Nhật Code Pipeline

### Bước 4.1: Thêm thư viện cần thiết
Mở file `requirements.txt` và thêm thư viện tích hợp của Dagster:

```text
dagster-databricks
databricks-sdk
```

### Bước 4.2: Đóng gói Code PySpark thành Script độc lập
Databricks cần chạy một file Python cụ thể. Thay vì code trực tiếp trong file `ops.py` của Dagster, bạn tách logic Spark (làm sạch, transform Bronze/Silver/Gold) ra thành các file script riêng rẽ (ví dụ: `scripts/process_bronze.py`, `scripts/process_silver.py`).

*Lưu ý: Các script này cần được upload lên không gian làm việc của Databricks (Databricks Workspace/Repos) hoặc lưu trữ trên Data Lake để Databricks có thể đọc và thực thi.*

### Bước 4.3: Định nghĩa Resource Databricks trong Dagster
Trong file `pipelines/resources.py` hoặc tạo mới file `pipelines/databricks_config.py`:

```python
from dagster import EnvVar
from dagster_databricks import databricks_client

databricks_api_client = databricks_client.configured(
    {
        "host": EnvVar("DATABRICKS_HOST"),
        "token": EnvVar("DATABRICKS_TOKEN"),
    }
)
```

### Bước 4.4: Sửa đổi Ops để gọi Databricks
Thay vì cấp phát `spark_resource` và chạy `.config()...`, bạn cấu hình lại các Ops (`op_process_bronze`, `op_process_silver`) để dùng thư viện `create_databricks_run_now_op` hoặc gửi payload gọi Databricks API:

```python
from dagster import op
from dagster_databricks import create_databricks_run_now_op

# Cách 1: Chạy một Job đã được định nghĩa sẵn trên Databricks
run_databricks_job_op = create_databricks_run_now_op(
    databricks_job_id=123456, # ID của job trên Databricks
    databricks_resource_key="databricks_client"
)

# Cách 2: Submit một đoạn mã PySpark mới (chạy trên Cluster có sẵn)
@op(required_resource_keys={"databricks_client"})
def submit_databricks_task(context):
    client = context.resources.databricks_client.api_client
    
    # Định nghĩa cấu hình tác vụ gửi sang Databricks
    run_conf = {
        "run_name": "Dagster_Bronze_Processing",
        "existing_cluster_id": os.environ["DATABRICKS_CLUSTER_ID"],
        "spark_python_task": {
            # Đường dẫn file script trên Databricks workspace
            "python_file": "/Workspace/Users/your_email/process_bronze.py", 
            "parameters": [os.environ["RAW_PREFIX"], os.environ["BRONZE_PREFIX"]]
        }
    }
    
    # Kích hoạt chạy và chờ kết quả
    run_id = client.submit_run(**run_conf)
    client.wait_for_run_to_complete(run_id)
    context.log.info(f"Hoàn thành tác vụ Databricks với Run ID: {run_id}")
```

---

## 5. Cấp Quyền Cho Databricks Đọc Azure Data Lake

Cluster Databricks cần có quyền truy cập vào Data Lake (tương tự như Dagster). 
Trong cấu hình **Advanced Options** -> **Spark Config** của Databricks Cluster, thêm các dòng sau:

```properties
fs.azure.account.key.strealestatedatalake.dfs.core.windows.net <YOUR_AZURE_STORAGE_KEY>
```
*(Lưu ý: Trong thực tế, Storage Key nên được lưu trong Databricks Secret Scope thay vì hardcode).*

---

## 6. Triển Khai (Deployment)

1. Cập nhật `requirements.txt`.
2. Sửa code `ops.py` và `resources.py`.
3. Đẩy file script PySpark lên Databricks Workspace (hoặc cấu hình Git Repos trên Databricks).
4. Khởi tạo lại Docker image và Push lên Azure Container Registry.
5. Tạo lại Revision trên Container App.

Với kịch bản này, Dagster chỉ cần **~100MB RAM** để theo dõi tiến độ, còn toàn bộ phần dữ liệu nặng được giao cho Databricks Cluster có **hàng chục GB RAM** xử lý.

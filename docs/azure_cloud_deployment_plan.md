# Kịch Bản 2: Triển Khai Kiến Trúc Cloud-Native Lên Azure

Kịch bản này là chuẩn mực của một **Modern Data Stack** thực thụ trong doanh nghiệp (Lakehouse Architecture). Nó tách biệt hoàn toàn 3 tầng: Lưu trữ (Storage) - Xử lý (Compute) - Điều phối (Orchestration/Serving), giúp tối ưu hóa chi phí và hiệu suất.

Việc chọn kiến trúc này làm đồ án tốt nghiệp sẽ giúp bạn đạt điểm tối đa ở phần "Thiết kế Hệ Thống" (System Architecture).

---

## 1. Bản Đồ Chuyển Đổi (Local to Cloud Mapping)

Hệ thống bạn đang chạy ở Local (trên Docker) sẽ được "ánh xạ" lên các dịch vụ tương ứng của Azure như sau:

| Thành phần Local | Dịch vụ Azure Tương ứng | Vai trò trong hệ thống |
| :--- | :--- | :--- |
| Thư mục `data/lakehouse/` | **Azure Data Lake Storage Gen2 (ADLS)** | Chứa toàn bộ Data (Bronze, Silver, Warehouse) dưới dạng File. Cơ cấu thư mục y hệt ổ cứng máy tính. Chi phí cực rẻ. |
| PySpark (`src/processing/`) | **Azure Databricks** | Cỗ máy Compute để xử lý Big Data. Đọc data từ ADLS, tính toán, và ghi lại vào ADLS. |
| PostgreSQL (Metadata DB) | **Azure Database for PostgreSQL Flexible Server** | Chứa thông tin Metadata cho Dagster chạy và lưu cấu hình Superset. |
| Dagster UI & Daemon | **Azure Container Apps (ACA)** | Môi trường Serverless để chạy giao diện web của Dagster và lên lịch chạy Job. |
| Apache Superset | **Azure Container Apps (ACA)** | Chạy giao diện phân tích, kết nối với Databricks (qua JDBC) hoặc truy vấn trực tiếp ADLS để vẽ biểu đồ. |
| Python Crawler | **Azure Functions / Container Apps** | Chạy script cào dữ liệu từ Chợ Tốt và đẩy file thô vào ADLS tầng Bronze. |

---

## 2. Lộ Trình Triển Khai Từng Bước (Step-by-Step)

### Bước 1: Xây dựng tầng Lưu trữ (Storage Layer)
1. Lên Azure Portal, tạo một **Storage Account**.
2. **Cực kỳ quan trọng:** Phải tick chọn ô *Enable hierarchical namespace* để nâng cấp nó thành **ADLS Gen2**. Điều này giúp Storage của bạn có cơ trúc Thư mục/File (Folder/File) rõ ràng hệt như ổ cứng máy tính, rất tốt cho Big Data.
3. Tạo 1 Container tên là `datalake`, bên trong tạo các thư mục: `bronze`, `silver`, `warehouse`, `quarantine`.

### Bước 2: Xây dựng tầng Xử lý (Compute Layer)
1. Tạo một **Azure Databricks Workspace**.
2. Bên trong Databricks, tạo một **Cluster** nhỏ (Single Node, Standard_DS3_v2) để tiết kiệm tiền.
3. **Cấu hình Sinh Tồn:** Bật tính năng **Auto-termination** sau 10 hoặc 15 phút. (Nghĩa là nếu Pipeline chạy xong, sau 10 phút không ai dùng nó sẽ tự động tắt để KHÔNG tốn tiền).
4. Viết script để "Mount" (gắn) cái thư mục ADLS Gen2 ở Bước 1 vào Databricks. Lúc này code PySpark của bạn có thể đọc/ghi thẳng vào Data Lake.

### Bước 3: Xây dựng tầng Quản trị (Metadata Layer)
1. Tạo một **Azure Database for PostgreSQL Flexible Server**.
2. Chọn tier `Burstable B1ms` (Cực kỳ rẻ, chỉ tầm 15$/tháng).
3. Tạo 2 database riêng biệt: `dagster_db` và `superset_db`.

### Bước 4: Xây dựng tầng Điều phối & Hiển thị (Orchestration & Serving)
1. Build file Docker image của Dagster và Superset, đẩy lên **Azure Container Registry (ACR)**.
2. Dùng **Azure Container Apps (ACA)** kéo image đó về chạy. Cấu hình biến môi trường kết nối tới Database ở Bước 3.
3. Ưu điểm của ACA là Serverless, nó tự động scale theo nhu cầu sử dụng, có Free Grant (lượng sử dụng miễn phí) hàng tháng.

---

## 3. Chiến Lược "Sinh Tồn" Với Ngân Sách Sinh Viên (100$)

Để đảm bảo 100$ đủ cho 3 tháng bảo vệ đồ án, bạn **BẮT BUỘC** phải tuân thủ các nguyên tắc sau:

1. **Databricks là con dao hai lưỡi:** Nó xử lý dữ liệu nhanh nhất thế giới nhưng cũng đốt tiền rất nhanh. **Luôn luôn bật Auto-terminate**. Job Dagster mỗi ngày chỉ cần bật Databricks lên chạy 5 phút rồi tắt, tốn chưa tới $0.1/ngày.
2. **Ngừng chạy Superset/Dagster khi đi ngủ:** Azure Container Apps cho phép bạn thu hẹp (scale down) về 0 khi không dùng. Hãy tắt nó khi không test code.
3. **Tạo Budget Alert (Báo động Đỏ):**
   - Vào `Cost Management + Billing` trên Azure.
   - Tạo Budget giới hạn: $30/tháng.
   - Cài đặt cảnh báo: Gửi email cho bạn ngay lập tức nếu xài tới 50% ($15) hoặc 90% ($27). Bằng cách này, bạn sẽ không bao giờ bị "cháy ví" trong im lặng.

## 4. Tại sao hội đồng sẽ đánh giá rất cao kiến trúc này?

*   **Tách biệt Storage và Compute:** Đây là tinh hoa của Data Engineering hiện đại. Dữ liệu (ADLS) lưu trữ rẻ mạt và vĩnh viễn. Khi cần tính toán mới mướn máy (Databricks) xử lý vài phút rồi trả lại. Tối ưu chi phí tuyệt đối.
*   **Trực quan:** Tổ chức thư mục giống hệt máy tính. Không phải nhét tất cả vào Database đen ngòm.
*   **Scalable (Mở rộng dễ dàng):** Nếu sau này dữ liệu tăng lên 100 triệu dòng, bạn chỉ cần tăng cấu hình Databricks (Compute) lên mà không cần động chạm hay chuyển dời dữ liệu (Storage).

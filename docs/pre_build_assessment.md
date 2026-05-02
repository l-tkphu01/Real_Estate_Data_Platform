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
SELECT * FROM read_delta('/app/data/lakehouse/silver/fact_listings');
---
Giai thích về các bảng dim/fact trong Data Warehouse:
- Dim_location: lưu thông tin địa lý để phân tích về dữ liệu (vidu: xem số lượng tin đăng bất động sản theo từng tỉnh/thành phố hoặc quận/huyện)
  + location_key: là khóa chính số nguyên tự tăng, định danh duy nhất cho mỗi cặp (tỉnh, quận) mục đích để khi liên kết với bảng Fact (bảng chứa dữ liệu giao dịch) thì tra cứu nhanh và tiết kiệm bộ nhớ so với dùng chuỗi 'Hồ Chí Minh-Quận 1'
    * có nghĩa: nếu gặp 3 tin đều là Hồ chí minh-quận 1 --> đánh số 1
    * nếu gặp 1 tin là Hà nội-quận 1 --> đánh số 2
    * vậy sẽ chỉ có 2 dòng dữ liệu trong bảng dim_location, không phụ thuộc vào số lượng tin đăng
  + city: tên tỉnh/thành phố (ví dụ: 'Hồ Chí Minh')
  + district: tên quận/huyện (ví dụ: 'Quận 1')
  + city_normalized: tên tỉnh/thành phố đã được chuẩn hóa (loại bỏ khoảng trắng thừa, viết hoa chữ cái đầu, ví dụ: 'Ho Chi Minh' -> 'Hồ Chí Minh') giúp đảm bảo tính nhất quán dữ liệu khi phân tích.
  + region_code: mã khu vực (ví dụ: 'SGN' cho Hồ Chí Minh) giúp phân loại nhanh các khu vực lớn để phân tích theo vùng miền.
  -> tại sao lại nhét: region_code vào bảng dim_location? --> vì đây là khái niệm Hierarchical Dimension (chiều phân cấp) theo Quận -> Tỉnh -> Vùng miền -> Quốc gia. (để dễ dàng phân tích lượng bất động sản theo từng vùng miền chẳng hạn thay vì phân tích từng tỉnh/thành phố riêng lẻ)
  -> ngoài ra còn có tính năng chiều phân cấp: 
     *Drill-down (Khoan sâu) trên Superset: 
     khi sếp thấy cột miền nam doanh thu cao nhất thì click đúp vào cột "miền nam".
     --> biểu đồ tự động vỡ ra thành các cột tỉnh/thành: hồ chí minh(40ty), hà nội(20ty), đà nẵng(10ty) 
     --> nếu click vào hồ chí minh --> biểu đồ tiếp tục vỡ ra thành các quận/huyện: quận 1(10ty), quận 2(10ty), quận 3(10ty), quận 4(10ty),...     
     * Drill-up (Thu gọn) trên Superset: 
     tương tự ngược lại với drill-down, khi sếp muốn xem lại tổng quan toàn miền thì click vào nút drill-up 
     --> biểu đồ tự động thu gọn lại thành các cột tỉnh/thành: hồ chí minh(40ty), hà nội(20ty), đà nẵng(10ty) 
     --> nếu click vào nút drill-up lần nữa --> biểu đồ tự động thu gọn lại thành 1 cột: miền nam(70ty)  

     * Nếu trường hợp: biểu đồ vỡ ra quá nhiều tỉnh/thành, quận/ huyện --> chỉ hiển thị top 10 (hoặc top 5) và các tỉnh còn lại gom chung lại là "other" 
     * hoặc có thể làm theo phương án này: 
     Trải nghiệm của Sếp: Sếp click vào "Miền Nam" trên biểu đồ tròn -> Superset mượt mà chuyển sang hiển thị một cái biểu đồ Cột xếp hạng từ cao xuống thấp 63 tỉnh. Nhìn vào biết ngay HCM đứng top 1, cột dài nhất. Rất trực quan!
     Tuyệt đối tránh: Biểu đồ Donut quá nhiều mảnh (Pie Chart) khi có >10 danh mục sẽ trở thành "bánh donut rác", khiến mắt người nhìn phải đảo loạn để so sánh tỷ lệ. Trong trường hợp này, Bar Chart (cột) là VUA của Drill-down.
- dim_property_type: là bảng chiều trong mô hình Star Schema, đóng vai trò như một "từ điển chuẩn hóa" chuyên dùng để phân loại và lưu trữ các loại hình bất động sản.
  + property_type_key: khóa chính số nguyên tự tăng, định danh duy nhất cho mỗi loại hình bất động sản 
  + property_type_name: tên gọi chính thức, chuẩn hóa của loại hình bất động sản (ví dụ: 'Nhà phố', 'Căn hộ', 'Đất nền') giúp đảm bảo tính nhất quán dữ liệu khi phân tích.
  + property_type_group: nhóm danh mục lớn chứa loại hình bất động sản, giúp phân loại các loại hình tương tự nhau lại với nhau (ví dụ: 'Nhà ở', 'Đất ở', 'Bất động sản thương mại') UNIQUE đảm bảo kh có 2 cột trùng tên trong từ điển để tránh nhầm lẫn 
  + mapping_source: cột tàng hình mà MDM tạo ra ở bước clean. Nó lưu lại xem loại BĐS này được phân loại dựa vào đâu: "tự tin dùng Regex MDM" (do khớp từ khóa trong title) hay "vơt đáy bằng Category chợ tốt" (do MDM bó tay phải xài lại category gốc của bên bán, ngoài ra category gốc của tin bán đối chiếu với MDM (AI regrex) --> cho ra kết quả cuối cùng).
  --> trả lời cho các bài toán: 
  1. bài toán xu hướng thị trường vĩ mô: dùng cột property_type_group 
  + "Hiện tại dòng tiền trên thị trường bất động sản đang đổ về đâu? Nhu cầu mua để ở (Nhóm Residential), mua để kinh doanh (Nhóm Commercial), hay mua để đầu cơ chờ lên giá (Nhóm Land) đang chiếm ưu thế?"
  2. Bài toán chiến lược sản phẩm (Dùng cột property_type_name)
  + Giám đốc kinh doanh (CSO) sẽ hỏi: "Trong phân khúc Căn hộ, loại hình nào đang có thanh khoản tốt nhất (bán nhanh nhất)?"
  + (Ví dụ thực tế từ Data): Trích xuất từ bảng fact_listing, chúng ta thấy thời gian rao bán trung bình (avg_listing_age_days) của nhóm "Căn hộ Studio" lên tới 45 ngày (bán rất chậm, ế ẩm). Trong khi đó, nhóm "Căn hộ 3PN" chỉ mất trung bình 12 ngày là biến mất khỏi web (bán cực nhanh).
  + Kết luận: Sản phẩm "Căn hộ 3PN" đang khan hiếm và cực kỳ dễ chốt sale -> Chiến lược: Chỉ đạo team Sales tập trung săn lùng nguồn hàng 3PN, và dồn ngân sách Marketing đẩy mạnh hiển thị loại hình này để tăng doanh thu.
  3. Bài toán quản trị dữ liệu (Data Governance): Dùng cột mapping_source
  + "Chất lượng dữ liệu của chúng ta đến từ đâu?" 
  + "Có phải team MDM đang làm rất tốt việc chuẩn hóa dữ liệu (tỷ lệ match cao), hay là do chúng ta đang phải phụ thuộc quá nhiều vào dữ liệu thô từ bên ngoài (mapping_source = 'Chợ Tốt External')?"
  --> nhờ đó ta đánh giá được hiệu quả của team MDM (Master Data Management - Quản trị dữ liệu chủ), giúp tìm ra điểm yếu trong quy trình chuẩn hóa và cải thiện nó (VD: tìm ra từ khóa mới cho regex để lần sau không phải dùng "Chợ Tốt External" nhiều nữa)

  + UNIQUE: vì cột này dùng để JOIN với bảng fact, nó phải là DUY NHẤT. Cụ thể: 1 loại BĐS (ví dụ: "Căn hộ chung cư") chỉ được định nghĩa duy nhất 1 lần, dù có xuất hiện bao nhiêu lần trong dữ liệu thô. Nếu có 2 dòng trùng tên, bảng sẽ lỗi, giúp ta đảm bảo tính toàn vẹn của mô hình (sạch không có dữ liệu rác). 

  VD: Bảng dims fact_listing (bảng chứa dữ liệu giao dịch) thì KHÔNG có cột: 
  - city: 'Hồ Chí Minh' (vì nó sẽ JOIN key với dim_location)
  - property_type_name: 'Căn hộ chung cư' (vì nó sẽ JOIN key với dim_property_type)
  - posted_date, listed_date (vì đã join với dim_time)
  -> Điều này giúp bảng Fact cực kỳ nhỏ, nhẹ, tối ưu cho việc truy vấn.
- dim_price_segment: Trả lời cho câu hỏi --> phân khúc nào đang bán chạy nhất, phân khúc nào tồn kho nhiều nhất?
  + price_segment_key: Surrogate Key để JOIN với bảng Fact. Không có gì đặc biệt, y hệt như location_key hay property_type_key mình đã giải thích trước đó.
  + segment_name: tên viết tắt nội bộ của team phân tích để dễ gọi.
    * affordable  →  tên gọi trong CODE
    * mid         →  tên gọi trong CODE
    * luxury      →  tên gọi trong CODE
  + price_floor_vnd: giá dưới mức của phân khúc
  + price_ceiling_vnd: giá trên mức của phân khúc (NULL là trên mức)
  + segment_label_vi: tên gọi hiển thị
  --> sử dụng SCD Type 2
  + valid_from: ngày hiệu lực của phân khúc
  + valid_to: ngày hết hiệu lực của phân khúc (NULL là phân khúc đang có hiệu lực)
  + is_current: đánh dấu phân khúc đang có hiệu lực
  --> nguồn dữ liệu giá nguồn: Tham khảo từ Báo cáo thị trường BĐS Quý 1/2024 của Batdongsan.com.vn và Khung phân hạng nhà ở của Bộ Xây Dựng / VARS (Hội Môi giới BĐS Việt Nam). Mức giá <4 tỷ hiện nay được coi là mốc khởi điểm (Affordable) để tiếp cận nhà ở (đặc biệt khi tệp dữ liệu bao gồm cả Nhà phố, Đất nền tại các Đô thị lớn).
  
  LƯU Ý: KHI GIÁ CẢ THỊ TRƯỜNG THAY ĐỔI, CẦN CẬP NHẬT BẢNG DIM_PRICE_SEGMENT ĐỂ PHẢN ÁNH ĐÚNG THỰC TẾ CỦA THỊ TRƯỜNG BẰNG CÁCH VIẾT SQL THỦ CÔNG
  -- 1. Bạn tự viết lệnh này để báo cho hệ thống biết luật cũ đã hết hạn
  UPDATE dim_price_segment 
  SET valid_to = '2025-12-31', is_current = FALSE 
  WHERE valid_to IS NULL;

  -- 2. Bạn tự viết lệnh này để bơm luật mới vào
  INSERT INTO dim_price_segment (...) VALUES (5, 'affordable', 0, 6 tỷ, '2026-01-01', NULL, TRUE);
  --> VIỆC NÀY CHỈ LÀM MỘT LẦN TRONG ĐỜI
  câu hỏi đặt ra: nếu quá nhiều bất động sản với nhiều mức giá khác nhau cào về với cái ngưỡng hardcode thì biểu đồ sẽ bị lệch
  --> giải quyết bằng cách sử dụng phương pháp lọc cụ thể: căn hộ chung cư or nhà phố ,.... sau đó xem tổng quan bên trong.

  cách tối ưu nhất: (cách này sẽ áp dụng cho tương lai, vì phải thay đổi, test rất nhiều).

  Bước 1: Tính ngưỡng (Hybrid Algorithm)
   → Percentile từ dữ liệu thực tế ra con số p25, p75...
   → Business Team review: "p25 = 5.2 tỷ, tao chốt xuống 5 tỷ cho tròn"
   → Ra quyết định: ngưỡng mới là 5 tỷ

  Bước 2: Cập nhật ngưỡng (SCD Type 2 + PostgreSQL)
    → Mở pgAdmin, UPDATE dòng cũ: valid_to = hôm nay, is_current = FALSE
    → INSERT dòng mới: floor = 0, ceiling = 5 tỷ, valid_from = ngày mai
    → Pipeline tự đọc ngưỡng mới lần chạy tiếp theo

  Kết quả:
    → Ngưỡng chính xác với thị trường (nhờ Hybrid)
    → Lịch sử được bảo vệ (nhờ SCD Type 2)
    → Không cần sửa code Python (nhờ PostgreSQL) 

---
# lệnh xem dữ liệu trong quarantine
docker exec -it real-estate-dagster-web python scripts/view_quarantine.py 
---

# lệnh kết nối vào kho: 
docker login acrrealestateimages.azurecr.io

# Đóng gói code Dagster và Superset thành 2 kiện hàng (Docker Image) và gửi lên kho.
# 1. Đóng gói và đẩy Dagster
# Lệnh 1: Gói hàng (Build)

docker build --provenance=false -t acrrealestateimages.azurecr.io/dagster:latest -f docker/dagster/Dockerfile .

# Lệnh 2: Gửi lên mây (Push)
docker push acrrealestateimages.azurecr.io/dagster:latest

# 2. Đóng gói và đẩy Superset


# Lệnh 3: Gói hàng (Build)
docker build -t acrrealestateimages.azurecr.io/superset:latest -f docker/superset/Dockerfile .

# Lệnh 4: Gửi lên mây (Push)
docker push acrrealestateimages.azurecr.io/superset:latest

# Kết Nối Power BI Desktop → Azure Data Lake Storage Gen2

## Cách kết nối (Chỉ cần 2 lần Get Data):

### Lần 1: Load toàn bộ Warehouse (7 bảng)
# 1. Power BI Desktop → Get Data → Azure → Azure Data Lake Storage Gen2
# 2. Nhập URL thư mục gốc warehouse:
#    https://strealestatedatalake.dfs.core.windows.net/datalake/warehouse/
# 3. Data View: chọn File System View → OK
# 4. Authentication: Account Key → paste key từ Azure Portal
#    (Azure Portal → Storage Account → strealestatedatalake → Access keys → copy Key1)
# 5. Navigator hiện 7 folder con → chọn tất cả → Transform Data
# 6. Trong Power Query, đặt tên lại từng query:
#    - fact_listing, fact_market_snapshot
#    - dim_location, dim_property_type, dim_price_segment, dim_area_segment, dim_time

### Lần 2: Load Silver layer (cho chart AI Performance)
# 1. Get Data → Azure Data Lake Storage Gen2
# 2. Nhập URL:
#    https://strealestatedatalake.dfs.core.windows.net/datalake/silver/
# 3. Load → đặt tên: silver_listings

## Danh sách URL từng bảng (nếu cần load riêng lẻ):
# https://strealestatedatalake.dfs.core.windows.net/datalake/warehouse/fact_listing/
# https://strealestatedatalake.dfs.core.windows.net/datalake/warehouse/dim_location/
# https://strealestatedatalake.dfs.core.windows.net/datalake/warehouse/dim_property_type/
# https://strealestatedatalake.dfs.core.windows.net/datalake/warehouse/dim_price_segment/
# https://strealestatedatalake.dfs.core.windows.net/datalake/warehouse/dim_area_segment/
# https://strealestatedatalake.dfs.core.windows.net/datalake/warehouse/dim_time/
# https://strealestatedatalake.dfs.core.windows.net/datalake/warehouse/fact_market_snapshot/
# https://strealestatedatalake.dfs.core.windows.net/datalake/silver/real_estate/

## Kiểm tra kết nối thành công:
# 1. Panel "Data" bên phải → thấy danh sách bảng với các cột bên trong = OK
# 2. Tab "Model" (icon sơ đồ bên trái) → thấy các bảng dạng hộp = OK
# 3. Home → Transform Data → thấy preview dữ liệu từng bảng = OK

--- 
# Tính năng cài đặt hẹn giờ refresh (tự làm tươi dữ liệu) trên Power BI Service

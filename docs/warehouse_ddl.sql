-- ═══════════════════════════════════════════════════════════════════════════════
-- DDL Tham Chiếu: Star Schema Data Warehouse
-- Dự án: Real Estate Data Platform (Bất Động Sản Chợ Tốt)
-- 
-- LƯU Ý: File này chỉ mang tính tham chiếu (reference).
-- Thực tế, các bảng được tạo dưới dạng Delta Lake tables bằng PySpark.
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────
-- 1. DIMENSION TABLES
-- ─────────────────────────────────────────────────────────────────

-- dim_location: Chiều Địa Lý (Tỉnh/TP + Quận/Huyện)
CREATE TABLE IF NOT EXISTS dim_location (
    location_key    INT          PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    district        VARCHAR(100) NOT NULL,
    city_normalized VARCHAR(100) NOT NULL,
    district_normalized VARCHAR(100) NOT NULL,
    region_code     VARCHAR(10)  DEFAULT NULL,
    UNIQUE (city_normalized, district_normalized)
);

-- dim_property_type: Chiều Loại Bất Động Sản
CREATE TABLE IF NOT EXISTS dim_property_type (
    property_type_key   INT          PRIMARY KEY,
    property_type_name  VARCHAR(100) NOT NULL UNIQUE,
    property_type_group VARCHAR(50)  DEFAULT 'Residential',
    mapping_source      VARCHAR(50)  DEFAULT 'Unknown'
);

-- Seed từ mdm_rules.yaml
INSERT INTO dim_property_type (property_type_key, property_type_name, property_type_group, mapping_source) VALUES
(1, 'Căn hộ chung cư',        'Residential', 'MDM Regex'),
(2, 'Nhà đất',                'Residential', 'MDM Regex'),
(3, 'Khác (Không xác định)',  'Unknown',     'Fallback');


-- dim_price_segment: Chiều Phân Khúc Giá (Áp dụng SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_price_segment (
    price_segment_key INT            PRIMARY KEY,
    segment_name      VARCHAR(30)    NOT NULL, -- Không còn UNIQUE nữa vì 1 phân khúc có thể có nhiều dòng lịch sử
    price_floor_vnd   DECIMAL(15,2)  NOT NULL,
    price_ceiling_vnd DECIMAL(15,2)  DEFAULT NULL,
    segment_label_vi  VARCHAR(50)    NOT NULL,
    valid_from        DATE           NOT NULL,
    valid_to          DATE           DEFAULT NULL,
    is_current        BOOLEAN        NOT NULL DEFAULT TRUE
);

-- Seed từ logic trong transform.py
INSERT INTO dim_price_segment (price_segment_key, segment_name, price_floor_vnd, price_ceiling_vnd, segment_label_vi, valid_from, valid_to, is_current) VALUES
(1, 'affordable', 0.00,              3999999999.99, 'Bình dân (< 4 tỷ)',   '2020-01-01', NULL, TRUE),
(2, 'mid',        4000000000.00,     7999999999.99, 'Trung cấp (4-8 tỷ)',  '2020-01-01', NULL, TRUE),
(3, 'upper_mid',  8000000000.00,    11999999999.99, 'Trung cao (8-12 tỷ)', '2020-01-01', NULL, TRUE),
(4, 'luxury',    12000000000.00,            NULL,   'Cao cấp (≥ 12 tỷ)',   '2020-01-01', NULL, TRUE);


-- dim_time: Chiều Thời Gian (Pre-populated 2024-2028)
CREATE TABLE IF NOT EXISTS dim_time (
    time_key     INT         PRIMARY KEY,  -- Format: YYYYMMDD
    full_date    DATE        NOT NULL UNIQUE, -- ngày, định dạng YYYY-MM-DD
    day_of_week  SMALLINT    NOT NULL,      -- 1(Mon) - 7(Sun)
    day_name     VARCHAR(15) NOT NULL,      -- Tên ngày trong tuần (Thứ 2 - Chủ Nhật)
    week_of_year SMALLINT    NOT NULL, -- Số thứ tự tuần trong năm
    month        SMALLINT    NOT NULL, -- Số thứ tự tháng trong năm
    month_name   VARCHAR(15) NOT NULL,      -- Tên tháng trong năm (Tiếng Việt)
    quarter      SMALLINT    NOT NULL, -- Số thứ tự quý trong năm
    year         SMALLINT    NOT NULL, -- Năm
    is_weekend   BOOLEAN     NOT NULL DEFAULT FALSE 
);


-- dim_area_segment: Chiều Phân Khúc Diện Tích
CREATE TABLE IF NOT EXISTS dim_area_segment (
    area_segment_key  INT           PRIMARY KEY,
    segment_name      VARCHAR(30)   NOT NULL UNIQUE,
    area_floor_sqm    DECIMAL(8,2)  NOT NULL,
    area_ceiling_sqm  DECIMAL(8,2)  DEFAULT NULL,
    segment_label_vi  VARCHAR(50)   NOT NULL
);

-- Seed từ logic trong transform.py
INSERT INTO dim_area_segment (area_segment_key, segment_name, area_floor_sqm, area_ceiling_sqm, segment_label_vi) VALUES
(1, 'compact',    0.00,    49.99,  'Nhỏ gọn (< 50 m²)'),
(2, 'standard',   50.00,   89.99,  'Tiêu chuẩn (50-90 m²)'),
(3, 'spacious',   90.00,  129.99,  'Rộng rãi (90-130 m²)'),
(4, 'villa_like', 130.00,   NULL,  'Biệt thự (≥ 130 m²)');


-- ─────────────────────────────────────────────────────────────────
-- 2. FACT TABLES
-- ─────────────────────────────────────────────────────────────────

-- fact_listing: Bảng Sự Kiện Tin Đăng (Grain: 1 property_id = 1 row)
CREATE TABLE IF NOT EXISTS fact_listing (
    listing_key        BIGINT        PRIMARY KEY, -- 2.1 tỷ (int), 9.2 tỷ tỷ (Bigint).
    property_id        VARCHAR(50)   NOT NULL UNIQUE, -- cột CDC --> theo dõi thay đổi, không trùng lặp
    location_key       INT           NOT NULL REFERENCES dim_location(location_key),
    property_type_key  INT           NOT NULL REFERENCES dim_property_type(property_type_key),
    price_segment_key  INT           NOT NULL REFERENCES dim_price_segment(price_segment_key),
    area_segment_key   INT           NOT NULL REFERENCES dim_area_segment(area_segment_key),
    posted_time_key    INT           NOT NULL REFERENCES dim_time(time_key),
    ingested_time_key  INT           NOT NULL REFERENCES dim_time(time_key),
    title              VARCHAR(500)  DEFAULT NULL,
    price_vnd          DECIMAL(15,2) NOT NULL,
    price_billion_vnd  DECIMAL(8,3)  NOT NULL,
    area_sqm           DECIMAL(8,2)  NOT NULL,
    price_per_sqm      DECIMAL(15,2) DEFAULT NULL,
    bedrooms           SMALLINT      DEFAULT 0,
    listing_age_days   INT           DEFAULT 0, -- tin đã đăng bao nhiêu ngày --> phát hiện hàng tồn kho lâu
    created_at         TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- dim_time: xuất hiện 2 lần (posted, ingested) --> đây là kĩ thuật Role-Playing Dimension , cùng một bảng tg nhưng đóng 2 vai trò.
--> 1. Tháng 3 có bao nhiêu tin được đăng lên ? (dùng posted_time_key)
--> 2. Tháng 3 có bao nhiêu tin được nạp vào hệ thống ? (dùng ingested_time_key)
--> Tại sao không dùng property_id 
    --> lí do 1.
    --> property_id phụ thuộc vào nguồn dữ liệu bên ngoài (nguồn kh đáng tin cậy), là mã do "Chợ tốt" tự đặt ra và có thể thay đổi bất cứ lúc nào
    --> Chợ Tốt đặt lại số (Reset ID) sau khi nâng cấp hệ thống?
    --> Họ xóa tin rồi đăng lại, tạo ra property_id mới cho cùng 1 căn nhà?
    --> Nếu dùng property_id làm Primary Key, toàn bộ hệ thống phụ thuộc vào quyết định của bên thứ 3 (Chợ Tốt). Rất nguy hiểm!
    --> lí do 2: Hiệu suất truy vấn (Performance)
    --> Lý do 3: Tính ổn định của mô hình (Model Stability)
    --> listing_key là con số do hệ thống của bạn tự sinh ra, không ai có thể can thiệp từ bên ngoài. Dù Chợ Tốt có thay đổi format property_id từ "12345" sang "CT-2026-12345", bảng Fact của bạn vẫn hoàn toàn bình thường.
    --> Lý do 4: Mở rộng đa nguồn (Multi-Source)
    --> Tương lai bạn muốn cào thêm Batdongsan.com.vn. Cả 2 nguồn đều có property_id riêng. Với Surrogate Key:

    -- chuẩn kiến trúc: Kimball — cha đẻ của phương pháp luận Data Warehouse — quy định rõ: "Fact Table phải luôn dùng Surrogate Key, không bao giờ dùng Natural Key làm Primary Key."
-- 3. Đánh chỉ mục (Index) cho bảng fact_listing (Tăng tốc độ truy vấn)
-- Bảng "mục lục" mà Database tự xây sẵn để tìm kiếm nhanh hơn.
CREATE INDEX IF NOT EXISTS idx_fact_listing_location ON fact_listing(location_key);
CREATE INDEX IF NOT EXISTS idx_fact_listing_posted   ON fact_listing(posted_time_key);
CREATE INDEX IF NOT EXISTS idx_fact_listing_segment  ON fact_listing(price_segment_key);
CREATE INDEX IF NOT EXISTS idx_fact_listing_area     ON fact_listing(area_segment_key);
CREATE INDEX IF NOT EXISTS idx_fact_listing_ptype    ON fact_listing(property_type_key);


-- fact_market_snapshot: Bảng Sự Kiện Tổng Hợp Thị Trường
-- (Grain: 1 khu vực + 1 thời điểm snapshot = 1 row)
-- Có nghĩa: là một ảnh chụp nhanh thị trường tại một khu vực vào 1 ngày cụ thể.
-- Chiến lược: MERGE on (location_key, snapshot_time_key)
--   + Cùng ngày: update snapshot (tránh duplicate)
--   + Ngày mới: append snapshot mới (tích lũy lịch sử)
CREATE TABLE IF NOT EXISTS fact_market_snapshot (
    snapshot_key          BIGINT        PRIMARY KEY,
    location_key          INT           NOT NULL REFERENCES dim_location(location_key),
    snapshot_time_key     INT           NOT NULL REFERENCES dim_time(time_key),
    listing_count         INT           NOT NULL DEFAULT 0, -- Hôm nay quận 1 có bao nhiêu tin đang rao
    avg_price             DECIMAL(15,2) DEFAULT NULL, -- Giá nhà trung bình ở Q1 là bao nhiêu?
    median_price          DECIMAL(15,2) DEFAULT NULL, -- Giá nào là giá ở giữa nhất?" (Ít bị méo bởi nhà 100 tỷ hơn avg)
    p90_price             DECIMAL(15,2) DEFAULT NULL, -- 90% nhà ở Q1 có giá dưới bao nhiêu?
    avg_area_sqm          DECIMAL(8,2)  DEFAULT NULL, 
    avg_price_per_sqm     DECIMAL(15,2) DEFAULT NULL,
    p90_price_per_sqm     DECIMAL(15,2) DEFAULT NULL,
    max_price             DECIMAL(15,2) DEFAULT NULL,
    min_price             DECIMAL(15,2) DEFAULT NULL,
    avg_listing_age_days  DECIMAL(6,2)  DEFAULT NULL,
    luxury_listing_ratio  DECIMAL(5,4)  DEFAULT NULL,
    snapshot_at           TIMESTAMP     NOT NULL,
    UNIQUE (location_key, snapshot_time_key)
);

-- Sắp xếp từ rẻ → đắt:
-- Căn 1:   2 tỷ
-- Căn 2:   3 tỷ
-- Căn 3:   4 tỷ
-- Căn 4:   5 tỷ
-- Căn 5:   5 tỷ
-- Căn 6:   6 tỷ
-- Căn 7:   7 tỷ
-- Căn 8:   8 tỷ
-- Căn 9:   9 tỷ
-- Căn 10: 500 tỷ  ← Penthouse siêu sang
-- Giá trung bình (AVG): (2+3+4+5+5+6+7+8+9+500) / 10 = 55.9 tỷ
-- Giá trung vị (MEDIAN): 5.5 tỷ (ngay tại chỗ "xẻ đôi")
-- Sếp nhìn vào và hiểu lầm: "Ồ, giá nhà Q1 trung bình 55 tỷ à? Đắt thế!"
-- Nhưng thực tế 9 trong 10 căn chỉ có giá 2-9 tỷ! avg_price bị 1 căn penthouse 500 tỷ kéo lên hoàn toàn mất đi giá trị tham chiếu. Đây gọi là Outlier Distortion.
-- nên để tính median thì phải sắp xếp từ thấp lên cao hoặc cao xuống thấp trước.
-- p90_price: giải quyết bài toán --> "Ngưỡng giá nào là ranh giới giữa BĐS bình thường và BĐS thực sự đắt (Cao cấp) tại khu vực này?"
CREATE INDEX IF NOT EXISTS idx_snapshot_location ON fact_market_snapshot(location_key);
CREATE INDEX IF NOT EXISTS idx_snapshot_time     ON fact_market_snapshot(snapshot_time_key);

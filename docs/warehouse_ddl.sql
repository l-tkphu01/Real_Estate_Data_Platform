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
    region_code     VARCHAR(10)  DEFAULT NULL,
    UNIQUE (city, district)
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


-- dim_price_segment: Chiều Phân Khúc Giá
CREATE TABLE IF NOT EXISTS dim_price_segment (
    price_segment_key INT            PRIMARY KEY,
    segment_name      VARCHAR(30)    NOT NULL UNIQUE,
    price_floor_vnd   DECIMAL(15,2)  NOT NULL,
    price_ceiling_vnd DECIMAL(15,2)  DEFAULT NULL,
    segment_label_vi  VARCHAR(50)    NOT NULL
);

-- Seed từ logic trong transform.py
INSERT INTO dim_price_segment (price_segment_key, segment_name, price_floor_vnd, price_ceiling_vnd, segment_label_vi) VALUES
(1, 'affordable', 0.00,              3999999999.99, 'Bình dân (< 4 tỷ)'),
(2, 'mid',        4000000000.00,     7999999999.99, 'Trung cấp (4-8 tỷ)'),
(3, 'upper_mid',  8000000000.00,    11999999999.99, 'Trung cao (8-12 tỷ)'),
(4, 'luxury',    12000000000.00,            NULL,   'Cao cấp (≥ 12 tỷ)');


-- dim_time: Chiều Thời Gian (Pre-populated 2024-2028)
CREATE TABLE IF NOT EXISTS dim_time (
    time_key     INT         PRIMARY KEY,  -- Format: YYYYMMDD
    full_date    DATE        NOT NULL UNIQUE,
    day_of_week  SMALLINT    NOT NULL,      -- 1(Mon) - 7(Sun)
    day_name     VARCHAR(15) NOT NULL,      -- Tiếng Việt
    week_of_year SMALLINT    NOT NULL,
    month        SMALLINT    NOT NULL,
    month_name   VARCHAR(15) NOT NULL,      -- Tiếng Việt
    quarter      SMALLINT    NOT NULL,
    year         SMALLINT    NOT NULL,
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
    listing_key        BIGINT        PRIMARY KEY,
    property_id        VARCHAR(50)   NOT NULL UNIQUE,
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
    listing_age_days   INT           DEFAULT 0,
    created_at         TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_listing_location ON fact_listing(location_key);
CREATE INDEX IF NOT EXISTS idx_fact_listing_posted   ON fact_listing(posted_time_key);
CREATE INDEX IF NOT EXISTS idx_fact_listing_segment  ON fact_listing(price_segment_key);
CREATE INDEX IF NOT EXISTS idx_fact_listing_area     ON fact_listing(area_segment_key);
CREATE INDEX IF NOT EXISTS idx_fact_listing_ptype    ON fact_listing(property_type_key);


-- fact_market_snapshot: Bảng Sự Kiện Tổng Hợp Thị Trường
-- (Grain: 1 khu vực + 1 thời điểm snapshot = 1 row)
-- Chiến lược: MERGE on (location_key, snapshot_time_key)
--   + Cùng ngày: update snapshot (tránh duplicate)
--   + Ngày mới: append snapshot mới (tích lũy lịch sử)
CREATE TABLE IF NOT EXISTS fact_market_snapshot (
    snapshot_key          BIGINT        PRIMARY KEY,
    location_key          INT           NOT NULL REFERENCES dim_location(location_key),
    snapshot_time_key     INT           NOT NULL REFERENCES dim_time(time_key),
    listing_count         INT           NOT NULL DEFAULT 0,
    avg_price             DECIMAL(15,2) DEFAULT NULL,
    median_price          DECIMAL(15,2) DEFAULT NULL,
    p90_price             DECIMAL(15,2) DEFAULT NULL,
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

CREATE INDEX IF NOT EXISTS idx_snapshot_location ON fact_market_snapshot(location_key);
CREATE INDEX IF NOT EXISTS idx_snapshot_time     ON fact_market_snapshot(snapshot_time_key);

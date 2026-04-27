"""Contract xây dựng các bảng Dimension cho Star Schema bằng PySpark.

Mỗi hàm nhận Silver/Gold DataFrame và trả về Dimension DataFrame sẵn sàng ghi Delta.
Sử dụng row_number() over Window để tạo surrogate key tự tăng an toàn trên mọi môi trường.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# dim_location — Chiều Địa Lý
# ═══════════════════════════════════════════════════════════════════════════════

def build_dim_location(
    silver_df: DataFrame,
    mdm_settings: Any = None,
) -> DataFrame:
    """Trích xuất distinct (city, district) từ Silver và tạo surrogate key.
    
    Args:
        silver_df: DataFrame Silver layer đã qua MDM cleaning.
        mdm_settings: Settings MDM chứa city_mapping (optional, dùng cho city_normalized).
    
    Returns:
        DataFrame với schema: location_key, city, district, city_normalized, region_code
    """
    # 1. Lấy danh sách unique (city, district)
    location_df = silver_df.select("city", "district").distinct()
    
    # 2. Tạo city_normalized và district_normalized — nếu đã qua MDM thì city/district đã chuẩn rồi
    location_df = location_df.withColumn("city_normalized", F.col("city")) \
                             .withColumn("district_normalized", F.col("district"))
    
    # 3. Map region_code từ city_normalized
    # Lưu ý: Region Code ở đây chính là Mã vùng miền (VD: HCM là Miền Nam, HN là Miền Bắc)
    # Lý do hardcode cứng: Vì miền của 10 tỉnh thành này là cố định, không thay đổi theo thời gian
    # và không thể lấy từ metadata (không có trong file config của bạn)
    region_map = {
        "Hồ Chí Minh": "HCM",
        "Hà Nội": "HN",
        "Đà Nẵng": "DN",
        "Cần Thơ": "CT",
        "Hải Phòng": "HP",
        "Bình Dương": "BD",
        "Đồng Nai": "DNA",
        "Long An": "LA",
        "Bà Rịa - Vũng Tàu": "BRVT",
        "Khánh Hòa": "KH",
        # Đây chính là phần bạn cần phải cập nhật thêm nếu sau này có tỉnh thành mới
    }
    
    # Tạo map expression từ dict (VD: HỒ CHÍ MINH -> HCM, HÀ NỘI -> HN, ...)
    from itertools import chain 
    if region_map:
        map_expr = F.create_map([F.lit(x) for x in chain(*region_map.items())])
        location_df = location_df.withColumn(
            "region_code",
            F.coalesce(map_expr.getItem(F.col("city_normalized")), F.lit("OTHER"))
            # Nếu city_normalized không có trong map -> gán là "OTHER"
        )
    else:
        location_df = location_df.withColumn("region_code", F.lit("OTHER")) 
    
    # 4. Tạo surrogate key (bắt đầu từ 1) an toàn bằng Window Function
    window_spec = Window.orderBy(F.lit(1))
    location_df = location_df.withColumn(
        "location_key",
        F.row_number().over(window_spec)
    )
    
    # 5. Reorder columns
    return location_df.select(
        "location_key", "city", "district", "city_normalized", "district_normalized", "region_code"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# dim_property_type — Chiều Loại BĐS
# ═══════════════════════════════════════════════════════════════════════════════

def build_dim_property_type(silver_df: DataFrame) -> DataFrame:
    """Trích xuất distinct property_type từ Silver và tạo bảng chiều.
    
    Args:
        silver_df: DataFrame Silver layer đã có cột property_type sau MDM.
    
    Returns:
        DataFrame với schema: property_type_key, property_type_name, 
                               property_type_group, mapping_source
    """
    # 1. Lấy danh sách unique property_type
    type_df = silver_df.select("property_type").distinct()
    
    # 2. Phân loại property_type_group dựa trên tên
    residential_keywords = [
        "căn hộ", "chung cư", "nhà", "biệt thự", "phòng trọ", 
        "nhà trọ", "penthouse", "condotel", "apartment"
    ]
    commercial_keywords = ["văn phòng", "mặt bằng", "cửa hàng", "shop", "kiot"]
    land_keywords = ["đất", "nền", "trang trại", "vườn"]
    
    residential_pattern = "(" + "|".join(residential_keywords) + ")"
    commercial_pattern = "(" + "|".join(commercial_keywords) + ")"
    land_pattern = "(" + "|".join(land_keywords) + ")"
    
    group_expr = (
        F.when(F.lower(F.col("property_type")).rlike(commercial_pattern), "Commercial")
        .when(F.lower(F.col("property_type")).rlike(land_pattern), "Land")
        .when(F.lower(F.col("property_type")).rlike(residential_pattern), "Residential")
        .otherwise("Unknown") # Nếu không rơi vào các nhóm trên, gán là "Unknown"
    )
    
    type_df = type_df.withColumn("property_type_group", group_expr)
    
    # 3. Đánh dấu mapping_source
    type_df = type_df.withColumn(
        "mapping_source",
        F.when(
            F.col("property_type") == "Khác (Không xác định)", "Fallback"
        ).otherwise("MDM Regex")
    )
    
    # 4. Surrogate key an toàn bằng Window Function
    window_spec = Window.orderBy(F.lit(1))
    type_df = type_df.withColumn(
        "property_type_key",
        F.row_number().over(window_spec)
    )
    
    return type_df.select(
        "property_type_key",
        F.col("property_type").alias("property_type_name"),
        "property_type_group",
        "mapping_source",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# dim_price_segment — Chiều Phân Khúc Giá (Seed tĩnh)
# ═══════════════════════════════════════════════════════════════════════════════

def build_dim_price_segment(spark: SparkSession) -> DataFrame:
    """Tạo bảng chiều phân khúc giá từ business rules cố định.
    
    Các ngưỡng tương ứng với logic trong transform.py:
        - affordable: < 4 tỷ
        - mid: 4-8 tỷ
        - upper_mid: 8-12 tỷ
        - luxury: >= 12 tỷ
    """
    seed_data = [
        (1, "affordable", Decimal("0"),               Decimal("3999999999.99"), "Bình dân (< 4 tỷ)",    date(2020, 1, 1), None, True),
        (2, "mid",        Decimal("4000000000.00"),    Decimal("7999999999.99"), "Trung cấp (4-8 tỷ)",   date(2020, 1, 1), None, True),
        (3, "upper_mid",  Decimal("8000000000.00"),    Decimal("11999999999.99"), "Trung cao (8-12 tỷ)",  date(2020, 1, 1), None, True),
        (4, "luxury",     Decimal("12000000000.00"),   None,                     "Cao cấp (≥ 12 tỷ)",    date(2020, 1, 1), None, True),
    ]
    
    from src.warehouse.schema import DIM_PRICE_SEGMENT_SCHEMA
    return spark.createDataFrame(seed_data, schema=DIM_PRICE_SEGMENT_SCHEMA)


# ═══════════════════════════════════════════════════════════════════════════════
# dim_area_segment — Chiều Phân Khúc Diện Tích (Seed tĩnh)
# ═══════════════════════════════════════════════════════════════════════════════

def build_dim_area_segment(spark: SparkSession) -> DataFrame:
    """Tạo bảng chiều phân khúc diện tích từ business rules cố định.
    
    Các ngưỡng tương ứng với logic trong transform.py:
        - compact: < 50 m²
        - standard: 50-90 m²
        - spacious: 90-130 m²
        - villa_like: >= 130 m²
    """
    seed_data = [
        (1, "compact",    Decimal("0"),      Decimal("49.99"),   "Nhỏ gọn (< 50 m²)"),
        (2, "standard",   Decimal("50.00"),  Decimal("89.99"),   "Tiêu chuẩn (50-90 m²)"),
        (3, "spacious",   Decimal("90.00"),  Decimal("129.99"),  "Rộng rãi (90-130 m²)"),
        (4, "villa_like", Decimal("130.00"), None,               "Biệt thự (≥ 130 m²)"),
    ]
    
    from src.warehouse.schema import DIM_AREA_SEGMENT_SCHEMA
    return spark.createDataFrame(seed_data, schema=DIM_AREA_SEGMENT_SCHEMA)


# ═══════════════════════════════════════════════════════════════════════════════
# dim_time — Chiều Thời Gian (Pre-populated)
# ═══════════════════════════════════════════════════════════════════════════════

_DAY_NAMES_VI = {
    0: "Thứ Hai",
    1: "Thứ Ba",
    2: "Thứ Tư",
    3: "Thứ Năm",
    4: "Thứ Sáu",
    5: "Thứ Bảy",
    6: "Chủ Nhật",
}

_MONTH_NAMES_VI = {
    1: "Tháng 1", 2: "Tháng 2", 3: "Tháng 3", 4: "Tháng 4",
    5: "Tháng 5", 6: "Tháng 6", 7: "Tháng 7", 8: "Tháng 8",
    9: "Tháng 9", 10: "Tháng 10", 11: "Tháng 11", 12: "Tháng 12",
}


def build_dim_time(
    spark: SparkSession,
    start_date: date = date(2024, 1, 1),
    end_date: date = date(2028, 12, 31),
) -> DataFrame:
    """Sinh bảng chiều thời gian cho phạm vi ngày cho trước.
    
    Args:
        spark: SparkSession instance.
        start_date: Ngày bắt đầu (mặc định 2024-01-01).
        end_date: Ngày kết thúc (mặc định 2028-12-31).
    
    Returns:
        DataFrame dim_time với đầy đủ thuộc tính calendar.
    """
    rows = []
    current = start_date
    while current <= end_date:
        weekday_idx = current.weekday()  # 0=Mon, 6=Sun
        iso_week = current.isocalendar()[1]
        
        rows.append((
            int(current.strftime("%Y%m%d")),  # time_key: YYYYMMDD
            current,                           # full_date
            weekday_idx + 1,                   # day_of_week: 1(Mon)-7(Sun)
            _DAY_NAMES_VI[weekday_idx],        # day_name
            iso_week,                          # week_of_year
            current.month,                     # month
            _MONTH_NAMES_VI[current.month],    # month_name
            (current.month - 1) // 3 + 1,     # quarter
            current.year,                      # year
            weekday_idx >= 5,                  # is_weekend
        ))
        current += timedelta(days=1)
    
    from src.warehouse.schema import DIM_TIME_SCHEMA
    logger.info(f"Đã sinh dim_time: {len(rows)} ngày ({start_date} → {end_date})")
    return spark.createDataFrame(rows, schema=DIM_TIME_SCHEMA)

"""Schema definitions cho Star Schema Data Warehouse.

Định nghĩa StructType cho mỗi bảng Dim/Fact,
đảm bảo tính nhất quán khi tạo và ghi Delta tables.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ═══════════════════════════════════════════════════════════════════════════════
# DIMENSION SCHEMAS
# ═══════════════════════════════════════════════════════════════════════════════

DIM_LOCATION_SCHEMA = StructType([
    StructField("location_key", IntegerType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("district", StringType(), nullable=False),
    StructField("city_normalized", StringType(), nullable=False),
    StructField("district_normalized", StringType(), nullable=False),
    StructField("region_code", StringType(), nullable=True),
])

DIM_PROPERTY_TYPE_SCHEMA = StructType([
    StructField("property_type_key", IntegerType(), nullable=False),
    StructField("property_type_name", StringType(), nullable=False),
    StructField("property_type_group", StringType(), nullable=True),
    StructField("mapping_source", StringType(), nullable=True),
])

DIM_PRICE_SEGMENT_SCHEMA = StructType([
    StructField("price_segment_key", IntegerType(), nullable=False),
    StructField("segment_name", StringType(), nullable=False),
    StructField("price_floor_vnd", DecimalType(15, 2), nullable=False),
    StructField("price_ceiling_vnd", DecimalType(15, 2), nullable=True),
    StructField("segment_label_vi", StringType(), nullable=False),
    StructField("valid_from", DateType(), nullable=False),
    StructField("valid_to", DateType(), nullable=True),
    StructField("is_current", BooleanType(), nullable=False),
])

DIM_TIME_SCHEMA = StructType([
    StructField("time_key", IntegerType(), nullable=False),
    StructField("full_date", DateType(), nullable=False),
    StructField("day_of_week", ShortType(), nullable=False),
    StructField("day_name", StringType(), nullable=False),
    StructField("week_of_year", ShortType(), nullable=False),
    StructField("month", ShortType(), nullable=False),
    StructField("month_name", StringType(), nullable=False),
    StructField("quarter", ShortType(), nullable=False),
    StructField("year", ShortType(), nullable=False),
    StructField("is_weekend", BooleanType(), nullable=False),
])

DIM_AREA_SEGMENT_SCHEMA = StructType([
    StructField("area_segment_key", IntegerType(), nullable=False),
    StructField("segment_name", StringType(), nullable=False),
    StructField("area_floor_sqm", DecimalType(8, 2), nullable=False),
    StructField("area_ceiling_sqm", DecimalType(8, 2), nullable=True),
    StructField("segment_label_vi", StringType(), nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════════════
# FACT SCHEMAS
# ═══════════════════════════════════════════════════════════════════════════════

FACT_LISTING_SCHEMA = StructType([
    StructField("listing_key", LongType(), nullable=False),
    StructField("property_id", StringType(), nullable=False),
    StructField("location_key", IntegerType(), nullable=False),
    StructField("property_type_key", IntegerType(), nullable=False),
    StructField("price_segment_key", IntegerType(), nullable=False),
    StructField("area_segment_key", IntegerType(), nullable=False),
    StructField("posted_time_key", IntegerType(), nullable=False),
    StructField("ingested_time_key", IntegerType(), nullable=False),
    StructField("title", StringType(), nullable=True),
    StructField("price_vnd", DecimalType(15, 2), nullable=False),
    StructField("price_billion_vnd", DecimalType(8, 3), nullable=False),
    StructField("area_sqm", DecimalType(8, 2), nullable=False),
    StructField("price_per_sqm", DecimalType(15, 2), nullable=True),
    StructField("bedrooms", ShortType(), nullable=True),
    StructField("listing_age_days", IntegerType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
])

FACT_MARKET_SNAPSHOT_SCHEMA = StructType([
    StructField("snapshot_key", LongType(), nullable=False),
    StructField("location_key", IntegerType(), nullable=False),
    StructField("snapshot_time_key", IntegerType(), nullable=False),
    StructField("listing_count", IntegerType(), nullable=False),
    StructField("avg_price", DecimalType(15, 2), nullable=True),
    StructField("median_price", DecimalType(15, 2), nullable=True),
    StructField("p90_price", DecimalType(15, 2), nullable=True),
    StructField("avg_area_sqm", DecimalType(8, 2), nullable=True),
    StructField("avg_price_per_sqm", DecimalType(15, 2), nullable=True),
    StructField("p90_price_per_sqm", DecimalType(15, 2), nullable=True),
    StructField("max_price", DecimalType(15, 2), nullable=True),
    StructField("min_price", DecimalType(15, 2), nullable=True),
    StructField("avg_listing_age_days", DecimalType(6, 2), nullable=True),
    StructField("luxury_listing_ratio", DecimalType(5, 4), nullable=True),
    StructField("snapshot_at", TimestampType(), nullable=False),
])

"""Contract xây dựng các bảng Fact cho Star Schema bằng PySpark.

Thực hiện FK lookup từ Dimension tables, tạo surrogate key,
và output Fact DataFrame sẵn sàng ghi Delta.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# fact_listing — Bảng Sự Kiện Tin Đăng
# ═══════════════════════════════════════════════════════════════════════════════

def build_fact_listing(
    silver_df: DataFrame,
    dim_location_df: DataFrame,
    dim_property_type_df: DataFrame,
    dim_price_segment_df: DataFrame,
    dim_area_segment_df: DataFrame,
) -> DataFrame:
    """Transform Silver records thành fact_listing bằng cách join với các Dimension.
    
    Grain: 1 property_id = 1 row (deduplicated).
    
    Args:
        silver_df: DataFrame Silver layer (đã transform, có price_segment, property_type...).
        dim_location_df: Dimension Location đã có location_key.
        dim_property_type_df: Dimension Property Type đã có property_type_key.
        dim_price_segment_df: Dimension Price Segment đã có price_segment_key.
        dim_area_segment_df: Dimension Area Segment đã có area_segment_key.
    
    Returns:
        DataFrame fact_listing với tất cả FK lookups.
    """
    now_ts = F.current_timestamp()
    
    # 1. Chuẩn bị Silver: parse posted_at thành date → time_key
    fact_df = silver_df \
        .withColumn("_posted_at_clean", 
                    F.regexp_replace(F.col("posted_at"), "Z$", "+00:00")) \
        .withColumn("_posted_dt", F.to_timestamp(F.col("_posted_at_clean"))) \
        .withColumn("posted_time_key",
                    F.date_format(F.col("_posted_dt"), "yyyyMMdd").cast("int")) \
        .withColumn("_ingested_at_clean",
                    F.regexp_replace(F.col("ingested_at"), "Z$", "+00:00")) \
        .withColumn("_ingested_dt", F.to_timestamp(F.col("_ingested_at_clean"))) \
        .withColumn("ingested_time_key",
                    F.date_format(F.col("_ingested_dt"), "yyyyMMdd").cast("int"))
    
    # 2. FK Lookup: dim_location (join on city + district)
    fact_df = fact_df.join(
        dim_location_df.select("location_key", "city", "district"),
        on=["city", "district"],
        how="left",
    )
    
    # 3. FK Lookup: dim_property_type (join on property_type)
    fact_df = fact_df.join(
        dim_property_type_df.select(
            "property_type_key",
            F.col("property_type_name").alias("_pt_name"),
        ),
        on=fact_df["property_type"] == dim_property_type_df["property_type_name"],
        how="left",
    ).drop("_pt_name")
    
    # 4. FK Lookup: dim_price_segment (join on price_segment)
    fact_df = fact_df.join(
        dim_price_segment_df.select(
            "price_segment_key",
            F.col("segment_name").alias("_seg_name"),
        ),
        on=fact_df["price_segment"] == dim_price_segment_df["segment_name"],
        how="left",
    ).drop("_seg_name")
    
    # 5. FK Lookup: dim_area_segment (join on area_segment)
    fact_df = fact_df.join(
        dim_area_segment_df.select(
            "area_segment_key",
            F.col("segment_name").alias("_area_seg_name"),
        ),
        on=fact_df["area_segment"] == dim_area_segment_df["segment_name"],
        how="left",
    ).drop("_area_seg_name")
    
    # 6. Xử lý NULL FK (fallback = -1 cho unknown dimension)
    fact_df = fact_df \
        .withColumn("location_key", F.coalesce(F.col("location_key"), F.lit(-1))) \
        .withColumn("property_type_key", F.coalesce(F.col("property_type_key"), F.lit(-1))) \
        .withColumn("price_segment_key", F.coalesce(F.col("price_segment_key"), F.lit(-1))) \
        .withColumn("area_segment_key", F.coalesce(F.col("area_segment_key"), F.lit(-1))) \
        .withColumn("posted_time_key", F.coalesce(F.col("posted_time_key"), F.lit(19000101))) \
        .withColumn("ingested_time_key", F.coalesce(F.col("ingested_time_key"), F.lit(19000101)))
    
    # 7. Tạo surrogate key + timestamps
    fact_df = fact_df \
        .withColumn("listing_key", F.monotonically_increasing_id() + 1) \
        .withColumn("created_at", now_ts) \
        .withColumn("updated_at", now_ts)
    
    # 8. Select final columns (area_segment_key thay thế degenerate area_segment)
    return fact_df.select(
        F.col("listing_key").cast("long"),
        "property_id",
        F.col("location_key").cast("int"),
        F.col("property_type_key").cast("int"),
        F.col("price_segment_key").cast("int"),
        F.col("area_segment_key").cast("int"),
        F.col("posted_time_key").cast("int"),
        F.col("ingested_time_key").cast("int"),
        "title",
        F.col("price").cast("decimal(15,2)").alias("price_vnd"),
        F.col("price_billion_vnd").cast("decimal(8,3)"),
        F.col("area_sqm").cast("decimal(8,2)"),
        F.col("price_per_sqm").cast("decimal(15,2)"),
        F.col("bedrooms").cast("short"),
        F.col("listing_age_days").cast("int"),
        "created_at",
        "updated_at",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# fact_market_snapshot — Bảng Sự Kiện Tổng Hợp Thị Trường
# ═══════════════════════════════════════════════════════════════════════════════

def build_fact_market_snapshot(
    gold_df: DataFrame,
    dim_location_df: DataFrame,
) -> DataFrame:
    """Transform Gold aggregate thành fact_market_snapshot bằng FK lookup.
    
    Grain: 1 khu vực (city + district) + 1 thời điểm snapshot = 1 row.
    
    Args:
        gold_df: DataFrame Gold layer (aggregated metrics by city + district).
        dim_location_df: Dimension Location đã có location_key.
    
    Returns:
        DataFrame fact_market_snapshot.
    """
    # 1. FK Lookup: dim_location
    snapshot_df = gold_df.join(
        dim_location_df.select("location_key", "city", "district"),
        on=["city", "district"],
        how="left",
    )
    
    # 2. Tạo snapshot_time_key từ snapshot_at
    snapshot_df = snapshot_df \
        .withColumn("_snapshot_dt", F.to_timestamp(F.col("snapshot_at"))) \
        .withColumn("snapshot_time_key",
                    F.date_format(F.col("_snapshot_dt"), "yyyyMMdd").cast("int")) \
        .withColumn("snapshot_at_ts", F.col("_snapshot_dt"))
    
    # 3. Xử lý NULL FK
    snapshot_df = snapshot_df \
        .withColumn("location_key", F.coalesce(F.col("location_key"), F.lit(-1))) \
        .withColumn("snapshot_time_key", F.coalesce(F.col("snapshot_time_key"), F.lit(19000101)))
    
    # 4. Surrogate key
    snapshot_df = snapshot_df.withColumn(
        "snapshot_key", F.monotonically_increasing_id() + 1
    )
    
    # 5. Select final columns
    return snapshot_df.select(
        F.col("snapshot_key").cast("long"),
        F.col("location_key").cast("int"),
        F.col("snapshot_time_key").cast("int"),
        F.col("listing_count").cast("int"),
        F.col("avg_price").cast("decimal(15,2)"),
        F.col("median_price").cast("decimal(15,2)"),
        F.col("p90_price").cast("decimal(15,2)"),
        F.col("avg_area_sqm").cast("decimal(8,2)"),
        F.col("avg_price_per_sqm").cast("decimal(15,2)"),
        F.col("p90_price_per_sqm").cast("decimal(15,2)"),
        F.col("max_price").cast("decimal(15,2)"),
        F.col("min_price").cast("decimal(15,2)"),
        F.col("avg_listing_age_days").cast("decimal(6,2)"),
        F.col("luxury_listing_ratio").cast("decimal(5,4)"),
        F.col("snapshot_at_ts").alias("snapshot_at"),
    )

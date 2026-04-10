"""Contract transformation cho analytics-ready schema sử dụng PySpark DataFrames."""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def transform_for_silver(df: DataFrame) -> DataFrame:
    """Ánh xạ cleaned DataFrame (Bronze) sang silver layer schema với Feature Engineering."""
    
    # Group theo phân khúc giá
    price_segment_expr = F.when(F.col("price") < 4_000_000_000, "affordable") \
                          .when(F.col("price") < 8_000_000_000, "mid") \
                          .when(F.col("price") < 12_000_000_000, "upper_mid") \
                          .otherwise("luxury")
                          
    # Group theo phân khúc diện tích                   
    area_segment_expr = F.when(F.col("area_sqm") < 50, "compact") \
                         .when(F.col("area_sqm") < 90, "standard") \
                         .when(F.col("area_sqm") < 130, "spacious") \
                         .otherwise("villa_like")

    now_utc = F.current_timestamp()
    
    silver_df = df \
        .withColumn("price_billion_vnd", F.round(F.col("price") / 1_000_000_000, 3)) \
        .withColumn("price_per_sqm", F.round(F.col("price") / F.col("area_sqm"), 2)) \
        .withColumn("price_segment", price_segment_expr) \
        .withColumn("area_segment", area_segment_expr) \
        .withColumn("_posted_at_raw", F.regexp_replace(F.col("posted_at"), "Z$", "+00:00")) \
        .withColumn("posted_dt", F.to_timestamp(F.col("_posted_at_raw"))) \
        .withColumn("posted_date", F.to_date(F.col("posted_dt")).cast("string")) \
        .withColumn("listing_age_days", F.greatest(F.lit(0), F.datediff(now_utc, F.col("posted_dt")))) \
        .withColumn("ingested_at", F.date_format(now_utc, "yyyy-MM-dd'T'HH:mm:ss")) \
        .drop("posted_dt", "_posted_at_raw")

    return silver_df


def transform_for_gold(df: DataFrame) -> DataFrame:
    """Tổng hợp phân tích Group By (Silver -> Gold) bằng PySpark Distributed Grouping."""
    
    snapshot_at = F.current_timestamp()

    # Nhóm dữ liệu Group By và Aggregate
    gold_df = df.groupBy("city", "district").agg(
        F.count("*").alias("listing_count"),
        F.round(F.sum("price") / F.count("*"), 2).alias("avg_price"),
        F.round(F.sum("area_sqm") / F.count("*"), 2).alias("avg_area_sqm"),
        F.round(F.sum("price_per_sqm") / F.count("*"), 2).alias("avg_price_per_sqm"),
        F.round(F.expr("percentile_approx(price, 0.5)"), 2).alias("median_price"),
        F.round(F.expr("percentile_approx(price, 0.9)"), 2).alias("p90_price"),
        F.round(F.expr("percentile_approx(price_per_sqm, 0.9)"), 2).alias("p90_price_per_sqm"),
        F.round(F.max("price"), 2).alias("max_price"),
        F.round(F.min("price"), 2).alias("min_price"),
        F.round(F.sum("listing_age_days") / F.count("*"), 2).alias("avg_listing_age_days"),
        F.round(
            F.sum(F.when(F.col("price_segment") == "luxury", 1).otherwise(0)) / F.count("*"), 
            4
        ).alias("luxury_listing_ratio"),
    ).withColumn("snapshot_at", F.date_format(snapshot_at, "yyyy-MM-dd'T'HH:mm:ss"))

    return gold_df

"""Contract cleaning để xử lý nulls, text normalization và typing bằng PySpark."""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F


def clean_records(df: DataFrame) -> DataFrame:
    """Clean và chuẩn hóa records trước khi nạp vào curated layers bằng song song PySpark."""
    
    # 1. Text normalization: Xóa khoảng trắng thừa
    # 2. Xử lý TimeZone: Chuyển chuỗi Z sang UTC Timestamp
    clean_df = df \
        .withColumn("title", F.trim(F.regexp_replace(F.col("title"), r'\s+', ' '))) \
        .withColumn("city", F.trim(F.regexp_replace(F.col("city"), r'\s+', ' '))) \
        .withColumn("district", F.trim(F.regexp_replace(F.col("district"), r'\s+', ' '))) \
        .withColumn("_posted_at_raw", 
                    F.regexp_replace(F.trim(F.col("posted_at")), "Z$", "+00:00")) \
        .withColumn("_posted_at_dt", F.to_timestamp(F.col("_posted_at_raw")))

    # 3. Chuẩn hóa kiểu dữ liệu
    clean_df = clean_df \
        .withColumn("price", F.round(F.col("price").cast("double"), 2)) \
        .withColumn("area_sqm", F.round(F.col("area_sqm").cast("double"), 2)) \
        .withColumn("bedrooms", F.greatest(F.lit(0), F.col("bedrooms").cast("int")))

    # 4. Giữ bản ghi mới nhất theo property_id để giảm trùng lặp (Deduplication)
    # Tương tự việc dùng dictionary trong Python nhưng chạy bằng Distributed Window Function
    window_spec = Window.partitionBy("property_id") \
                        .orderBy(F.col("_posted_at_dt").desc_nulls_last())
    
    dedup_df = clean_df \
        .withColumn("_rn", F.row_number().over(window_spec)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn", "_posted_at_dt", "_posted_at_raw")
        
    return dedup_df

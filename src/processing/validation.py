"""Data quality checks cho mandatory fields và value ranges bằng PySpark."""

from typing import Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def validate_records(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Tách PySpark DataFrame thành tập valid và invalid.
    Sử dụng Spark Column expressions để kiểm tra song song dữ liệu lớn.
    """
    required_fields = ["property_id", "city", "district", "price", "area_sqm", "bedrooms", "posted_at"]

    # 1. Null/Empty checks (Missing Required Fields)
    null_cond = F.lit(False)
    for col_name in required_fields:
        null_cond = null_cond | F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")
        
    df_eval = df.withColumn("_is_missing", null_cond)
    
    # 2. Type và Numeric checks
    df_eval = df_eval \
        .withColumn("_price_num", F.col("price").cast("double")) \
        .withColumn("_area_num", F.col("area_sqm").cast("double")) \
        .withColumn("_bed_num", F.col("bedrooms").cast("int"))

    cast_fail_cond = F.col("_price_num").isNull() | F.col("_area_num").isNull() | F.col("_bed_num").isNull()
    numeric_fail_cond = (F.col("_price_num") <= 0) | (F.col("_area_num") <= 0) | (F.col("_bed_num") < 0)
    
    # 3. Gán cờ báo lỗi _dq_reason
    df_eval = df_eval.withColumn("_dq_reason", 
            F.when(F.col("_is_missing"), "missing_required_fields")
             .when(cast_fail_cond, "type_cast_failed")
             .when(numeric_fail_cond, "numeric_constraint_failed")
             .otherwise(F.lit(None))
        )
        
    # 4. Tách rẽ nhánh 2 tập Dataframe
    valid_df = df_eval.filter(F.col("_dq_reason").isNull()) \
        .drop("_is_missing", "_price_num", "_area_num", "_bed_num", "_dq_reason")
    
    invalid_df = df_eval.filter(F.col("_dq_reason").isNotNull()) \
        .drop("_is_missing", "_price_num", "_area_num", "_bed_num")
    
    return valid_df, invalid_df

"""Xem các bản ghi có listing_type = UNKNOWN trong Silver Delta."""
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("view_unknown") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

silver_path = "/opt/dagster/app/data/lakehouse/silver/real_estate"

if not os.path.exists(silver_path):
    print("Silver Delta chưa tồn tại!")
else:
    df = spark.read.format("delta").load(silver_path)
    
    if "listing_type" not in df.columns:
        print("Cột listing_type chưa có trong Silver")
    else:
        total = df.count()
        unknown_df = df.filter(df.listing_type == "UNKNOWN")
        unknown_count = unknown_df.count()
        
        print(f"\n📊 TỔNG QUAN LISTING TYPE:")
        df.groupBy("listing_type").count().orderBy("count", ascending=False).show()
        
        print(f"\n❓ UNKNOWN: {unknown_count}/{total} ({round(unknown_count/total*100, 1)}%)")
        print(f"\n🔍 MẪU 20 TIN UNKNOWN (để label thủ công):")
        unknown_df.select("title", "property_type", "city", "district", "listing_type") \
            .show(20, truncate=80)

spark.stop()

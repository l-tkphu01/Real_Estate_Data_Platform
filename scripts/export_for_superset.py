"""Export Quarantine data ra CSV để import vào Superset."""
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("export_quarantine") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Export Quarantine
quarantine_path = "/opt/dagster/app/data/lakehouse/quarantine"
output_csv = "/opt/dagster/app/data/exports/quarantine.csv"

if os.path.exists(quarantine_path):
    df = spark.read.format("delta").load(quarantine_path)
    count = df.count()
    print(f"📦 Quarantine: {count} bản ghi")
    
    os.makedirs("/opt/dagster/app/data/exports", exist_ok=True)
    df.toPandas().to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"Exported → {output_csv}")
else:
    print("Quarantine chưa tồn tại")

# Export Silver (UNKNOWN listing_type)
silver_path = "/opt/dagster/app/data/lakehouse/silver/real_estate"
output_unknown = "/opt/dagster/app/data/exports/unknown_listing_type.csv"

if os.path.exists(silver_path):
    df = spark.read.format("delta").load(silver_path)
    if "listing_type" in df.columns:
        unknown_df = df.filter((df.listing_type == "UNKNOWN") | (df.listing_type.isNull()))
        count = unknown_df.count()
        print(f"❓ Unknown listing_type: {count} bản ghi")
        unknown_df.toPandas().to_csv(output_unknown, index=False, encoding="utf-8-sig")
        print(f"Exported → {output_unknown}")

spark.stop()

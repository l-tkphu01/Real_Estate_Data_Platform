import os
import sys
from pathlib import Path

# Cấu hình môi trường cho PySpark
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["APP_PROFILE"] = "local"
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.config import load_settings
from pipelines.resources import build_spark_resource

def main():
    settings = load_settings()
    spark = build_spark_resource(settings)
    spark.sparkContext.setLogLevel("ERROR")
    
    quarantine_path = str(PROJECT_ROOT / "data" / "lakehouse" / "quarantine")
    
    print(f"\nĐang đọc bảng Quarantine tại: {quarantine_path}\n")
    try:
        df = spark.read.format("delta").load(quarantine_path)
        print(f"Tổng số bản ghi đang bị kẹt: {df.count()}\n")
        
        print("DANH SÁCH CÁC QUẬN/HUYỆN ĐANG CHỜ BẠN KHAI BÁO VÀO YAML:")
        # Lấy ra danh sách các cặp (City, District) duy nhất bị lỗi để dễ copy
        df.select("city", "district") \
          .distinct() \
          .orderBy("city", "district") \
          .show(50, truncate=False)
          
    except Exception as e:
        print(f"Lỗi đọc bảng (có thể bảng chưa được tạo): {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()

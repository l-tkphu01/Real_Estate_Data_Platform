import os
import sys
from pathlib import Path

# Thêm thư mục gốc vào PYTHONPATH để có thể import các modules từ src/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Đặt môi trường giả lập cho local testing
os.environ["APP_PROFILE"] = "local"
os.environ["AZURE_ENDPOINT"] = ""
# Dùng tên lệnh 'python' thay vì đường dẫn tuyệt đối để tránh lỗi ký tự 'Đ' trên Windows
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[2] pyspark-shell"

from pyspark.sql import SparkSession
from src.config import load_settings
from src.processing.cleaning import clean_records

def main():
    print("Dang khoi tao Spark Session de test MDM...")
    spark = SparkSession.builder \
        .appName("TestMDM_Mapping") \
        .master("local[2]") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    print("\nNap cau hinh tu mdm_rules.yaml thong qua src/config.py...")
    settings = load_settings()
    
    print("\nCau hinh MDM dang ap dung:")
    for standard, aliases in settings.mdm.city_mapping.items():
        print(f" -> Ten chuan: {ascii(standard)} | Bao gom cac bien the: {ascii(aliases)}")

    print("\nDang tao du lieu gia lap (Messy Data)...")
    mock_data = [
        # Truong hop 1: Regex bat duoc (Chu "Can ho"), se uu tien MDM, lot "Can ho chung cu"
        {"property_id": "1", "title": "Ban can ho gia re", "source_category": "Mua ban", "city": "HCM", "district": "Q1", "price": 100, "area_sqm": 50, "bedrooms": "1", "posted_at": "2026-04-18T10:17:27Z"},
        # Truong hop 2: Regex bat duoc ("Biet thu"), source_category bi sai, he thong se uu tien MDM ("Nha dat")
        {"property_id": "2", "title": "Biet thu cao cap", "source_category": "Dat nong nghiep", "city": "Sài Gòn", "district": "Bình Thạnh", "price": 200, "area_sqm": 70, "bedrooms": "2", "posted_at": "2026-04-18T10:00:00Z"},
        # Truong hop 3: Regex THAT BAI (Chu "nha mat pho" bi thieu tieng Viet trong rules), => VOT bang Source Category la "Kiot Thuong Mai"
        {"property_id": "3", "title": "Kiot doi dien benh vien", "source_category": "Kiot Thuong Mai", "city": "SG", "district": "Q3", "price": 300, "area_sqm": 120, "bedrooms": "3", "posted_at": "2026-04-18T09:00:00Z"},
    ]
    
    df = spark.createDataFrame(mock_data)
    
    print("Du lieu TRUOC KHI xu ly MDM:")
    df.select("property_id", "title", "source_category", "city").show(truncate=False)

    print("\nDang chay ham `clean_records` (Data Quality + MDM)...")
    cleaned_df = clean_records(df, settings.mdm)

    print("Du lieu SAU KHI xu ly MDM va TRICH XUAT THONG TIN:")
    cleaned_df.select("property_id", "city", "district", "property_type", "mapping_status").show(truncate=False)
    
    print("Kiem tra ket qua:")
    print("- ID 1, 3: Phải map ra 'Hồ Chí Minh' và 'Quận 1'/'Quận 3', status: MAPPED")
    print("- ID 2: 'Bình Thạnh' chưa có trong mapping rules -> status: UNMAPPED")
    
    spark.stop()

if __name__ == "__main__":
    main()

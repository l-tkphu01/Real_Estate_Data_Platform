import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["APP_PROFILE"] = "local"

from pipelines.resources import build_spark_resource
from src.config import load_settings
from src.processing.cleaning import clean_records
from delta.tables import DeltaTable

def main():
    settings = load_settings()
    spark = build_spark_resource(settings)
    spark.sparkContext.setLogLevel("ERROR")
    
    quarantine_path = str(PROJECT_ROOT / "data" / "lakehouse" / "quarantine")
    print(f"Reading Quarantine: {quarantine_path}")
    
    df_quarantine = spark.read.format("delta").load(quarantine_path)
    print(f"Total quarantine records: {df_quarantine.count()}")
    
    # Run cleaning logic
    cleaned_df = clean_records(df_quarantine, getattr(settings, "mdm", None))
    print(f"After clean_records dedup: {cleaned_df.count()}")
    
    if "mapping_status" in cleaned_df.columns:
        df_success = cleaned_df.filter(cleaned_df["mapping_status"] == "MAPPED")
        df_failed = cleaned_df.filter(cleaned_df["mapping_status"] == "UNMAPPED")
        print(f"Success: {df_success.count()}")
        print(f"Failed: {df_failed.count()}")
        
        print("Mapping source column exists?", "mapping_source" in cleaned_df.columns)
    else:
        print("mapping_status not found!")

if __name__ == "__main__":
    main()

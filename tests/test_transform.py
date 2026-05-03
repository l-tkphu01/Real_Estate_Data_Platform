import pytest
from src.processing.transform import transform_for_silver, transform_for_gold
from pyspark.sql.types import DoubleType

def test_transform_for_silver(spark):
    records = [
        {
            "property_id": "1",
            "title": "Nhà đẹp trung tâm",
            "city": "HCMC",
            "district": "Q1",
            "price": 5_000_000_000.0,
            "area_sqm": 50.0,
            "bedrooms": 2,
            "posted_at": "2024-01-01T00:00:00Z",
            # Giả lập data đã được dọn từ bước cleaning bằng Spark
            "_posted_at_dt": "2024-01-01 00:00:00",
            "_posted_at_raw": "2024-01-01T00:00:00+00:00"
        }
    ]
    
    df = spark.createDataFrame(records)
    silver_df = transform_for_silver(df)
    
    silver_list = silver_df.collect()
    assert len(silver_list) == 1
    row = silver_list[0].asDict()
    
    # Kiểm tra các logic tính toán
    assert row["price_billion_vnd"] == 5.0
    assert row["price_segment"] == "mid"
    assert row["area_segment"] == "standard"
    assert row["price_per_sqm"] == 100000000.0  # 5,000,000,000 / 50
    
    # Kiểm tra metadata datetime
    assert "ingested_at" in row
    assert "listing_age_days" in row
    assert row["posted_date"] == "2024-01-01"

def test_transform_for_gold(spark):
    silver_records = [
        {
            "city": "HCMC",
            "district": "Q1",
            "price": 5_000_000_000.0,
            "area_sqm": 50.0,
            "price_per_sqm": 100_000_000.0,
            "price_segment": "mid",
            "listing_age_days": 10
        },
        {
            "city": "HCMC",
            "district": "Q1",
            "price": 15_000_000_000.0,
            "area_sqm": 100.0,
            "price_per_sqm": 150_000_000.0,
            "price_segment": "luxury",
            "listing_age_days": 20
        }
    ]
    
    df = spark.createDataFrame(silver_records)
    gold_df = transform_for_gold(df)
    
    gold_list = gold_df.collect()
    assert len(gold_list) == 1
    row = gold_list[0].asDict()
    
    assert row["city"] == "HCMC"
    assert row["district"] == "Q1"
    assert row["listing_count"] == 2
    
    # Tính tỉ lệ luxury
    assert row["luxury_listing_ratio"] == 0.5  # 1 trên 2 listing
    
    # Avg indicators
    assert row["avg_price"] == 10_000_000_000.0
    assert row["avg_area_sqm"] == 75.0
    
    # Min/Max indicators
    assert row["max_price"] == 15_000_000_000.0
    assert row["min_price"] == 5_000_000_000.0
    
    assert "snapshot_at" in row

import pytest
from src.processing.transform import transform_for_silver, transform_for_gold

def test_transform_for_silver():
    records = [
        {
            "property_id": "1",
            "title": "Nhà đẹp trung tâm",
            "city": "HCMC",
            "district": "Q1",
            "price": 5000000000,  # 5 Tỷ -> Segment: mid
            "area_sqm": 50,       # 50m2 -> Segment: standard
            "bedrooms": 2,
            "posted_at": "2024-01-01T00:00:00Z"
        }
    ]
    
    silver = transform_for_silver(records)
    assert len(silver) == 1
    row = silver[0]
    
    # Kiểm tra các logic tính toán
    assert row["price_billion_vnd"] == 5.0
    assert row["price_segment"] == "mid"
    assert row["area_segment"] == "standard"
    assert row["price_per_sqm"] == 100000000.0  # 5,000,000,000 / 50
    assert row["title"] == "Nhà đẹp trung tâm"
    
    # Kiểm tra metadata datetime
    assert "ingested_at" in row
    assert "listing_age_days" in row
    assert row["posted_date"] == "2024-01-01"

def test_transform_for_gold():
    silver_records = [
        {
            "property_id": "1",
            "city": "HCMC",
            "district": "Q1",
            "price": 5_000_000_000,
            "area_sqm": 50,
            "price_per_sqm": 100_000_000,
            "price_segment": "mid",
            "listing_age_days": 10
        },
        {
            "property_id": "2",
            "city": "HCMC",
            "district": "Q1",
            "price": 15_000_000_000,
            "area_sqm": 100,
            "price_per_sqm": 150_000_000,
            "price_segment": "luxury",
            "listing_age_days": 20
        }
    ]
    
    gold = transform_for_gold(silver_records)
    assert len(gold) == 1
    row = gold[0]
    
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

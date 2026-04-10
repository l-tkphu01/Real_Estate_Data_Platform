import pytest
from src.processing.validation import validate_records

def test_validate_records_valid(spark):
    records = [
        {
            "property_id": "1",
            "city": "HCMC",
            "district": "Q1",
            "price": 5000000000.0,
            "area_sqm": 50.0,
            "bedrooms": 2,
            "posted_at": "2024-01-01T00:00:00Z"
        }
    ]
    df = spark.createDataFrame(records)
    valid_df, invalid_df = validate_records(df)
    
    valid_list = valid_df.collect()
    invalid_list = invalid_df.collect()
    
    assert len(valid_list) == 1
    assert len(invalid_list) == 0

def test_validate_records_missing_fields(spark):
    records = [
        {
            "property_id": "2",
            "city": "HCMC",
            "district": "", # District rỗng -> missing
            "price": None,
            "area_sqm": None,
            "bedrooms": None,
            "posted_at": None
        }
    ]
    df = spark.createDataFrame(records, schema="property_id string, city string, district string, price double, area_sqm double, bedrooms int, posted_at string")
    valid_df, invalid_df = validate_records(df)
    
    invalid_list = invalid_df.collect()
    assert len(invalid_list) == 1
    assert invalid_list[0]["_dq_reason"] == "missing_required_fields"

def test_validate_records_invalid_types(spark):
    # Testing cast failed
    records = [
        {
            "property_id": "3",
            "city": "HCMC",
            "district": "Q1",
            "price": "không_phải_số",
            "area_sqm": "bảy_chục",
            "bedrooms": 2,
            "posted_at": "2024-10-10T10:00:00Z"
        }
    ]
    df = spark.createDataFrame(records)
    valid_df, invalid_df = validate_records(df)
    
    invalid_list = invalid_df.collect()
    assert len(invalid_list) == 1
    assert invalid_list[0]["_dq_reason"] == "type_cast_failed"

def test_validate_records_negative_values(spark):
    records = [
        {
            "property_id": "4",
            "city": "HCMC",
            "district": "Q1",
            "price": -1000.0,
            "area_sqm": -5.0,
            "bedrooms": -1,
            "posted_at": "2024-10-10T10:00:00Z"
        }
    ]
    df = spark.createDataFrame(records)
    valid_df, invalid_df = validate_records(df)
    
    invalid_list = invalid_df.collect()
    assert len(invalid_list) == 1
    assert invalid_list[0]["_dq_reason"] == "numeric_constraint_failed"

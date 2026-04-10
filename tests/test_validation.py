import pytest
from src.processing.validation import validate_records

def test_validate_records_valid():
    records = [
        {
            "property_id": "1",
            "city": "HCMC",
            "district": "Q1",
            "price": 5000000000,
            "area_sqm": 50,
            "bedrooms": 2,
            "posted_at": "2024-01-01T00:00:00Z"
        }
    ]
    valid, invalid = validate_records(records)
    assert len(valid) == 1
    assert len(invalid) == 0

def test_validate_records_missing_fields():
    records = [
        {
            "property_id": "2",
            "city": "HCMC"
            # Thiếu district, price, area_sqm...
        }
    ]
    valid, invalid = validate_records(records)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["_dq_reason"] == "missing_required_fields"

def test_validate_records_invalid_types():
    records = [
        {
            "property_id": "3",
            "city": "HCMC",
            "district": "Q1",
            "price": "không_phải_số",
            "area_sqm": 50,
            "bedrooms": 2,
            "posted_at": "2024-10-10T10:00:00Z"
        }
    ]
    valid, invalid = validate_records(records)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["_dq_reason"] == "type_cast_failed"

def test_validate_records_negative_values():
    records = [
        {
            "property_id": "4",
            "city": "HCMC",
            "district": "Q1",
            "price": -1000,
            "area_sqm": -5,
            "bedrooms": -1,
            "posted_at": "2024-10-10T10:00:00Z"
        }
    ]
    valid, invalid = validate_records(records)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["_dq_reason"] == "numeric_constraint_failed"

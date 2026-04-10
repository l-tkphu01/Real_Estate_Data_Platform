"""Contract transformation cho analytics-ready schema."""

from datetime import datetime, timezone
from statistics import median
from typing import Any


def _price_segment(price: float) -> str:
    if price < 4_000_000_000:
        return "affordable"
    if price < 8_000_000_000:
        return "mid"
    if price < 12_000_000_000:
        return "upper_mid"
    return "luxury"


def _area_segment(area_sqm: float) -> str:
    if area_sqm < 50:
        return "compact"
    if area_sqm < 90:
        return "standard"
    if area_sqm < 130:
        return "spacious"
    return "villa_like"


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = max(0, min(len(sorted_values) - 1, int(round((len(sorted_values) - 1) * p))))
    return sorted_values[idx]


def _parse_timestamp(value: str, fallback: datetime) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return fallback

    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def transform_for_silver(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ánh xạ cleaned records sang silver layer schema."""
    silver_records: list[dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)
    now_utc_naive = now_utc.replace(tzinfo=None)
    for record in records:
        area_sqm = float(record["area_sqm"])
        price = float(record["price"])
        posted_at = str(record["posted_at"])
        posted_dt = _parse_timestamp(posted_at, fallback=now_utc_naive)
        posted_date = posted_dt.date().isoformat()
        age_days = max(0, int((now_utc_naive - posted_dt).days))

        silver_records.append(
            {
                "property_id": str(record["property_id"]),
                "title": str(record.get("title", "")),
                "city": str(record["city"]),
                "district": str(record["district"]),
                "price": price,
                "price_billion_vnd": round(price / 1_000_000_000, 3),
                "area_sqm": area_sqm,
                "bedrooms": int(record["bedrooms"]),
                "price_per_sqm": round(price / area_sqm, 2),
                "price_segment": _price_segment(price),
                "area_segment": _area_segment(area_sqm),
                "listing_age_days": age_days,
                "posted_at": posted_at,
                "posted_date": posted_date,
                "ingested_at": now_utc.isoformat(timespec="seconds"),
            }
        )
    return silver_records


def transform_for_gold(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Tổng hợp silver records thành BI-ready gold datasets."""
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        key = (str(record["city"]), str(record["district"]))
        if key not in grouped:
            grouped[key] = {
                "city": key[0],
                "district": key[1],
                "listing_count": 0,
                "sum_price": 0.0,
                "sum_area_sqm": 0.0,
                "sum_price_per_sqm": 0.0,
                "sum_listing_age_days": 0,
                "max_price": 0.0,
                "min_price": float("inf"),
                "prices": [],
                "prices_per_sqm": [],
                "luxury_count": 0,
            }

        price = float(record["price"])
        area_sqm = float(record["area_sqm"])
        price_per_sqm = float(record["price_per_sqm"])
        listing_age_days = int(record.get("listing_age_days", 0))
        bucket = grouped[key]

        bucket["listing_count"] += 1
        bucket["sum_price"] += price
        bucket["sum_area_sqm"] += area_sqm
        bucket["sum_price_per_sqm"] += price_per_sqm
        bucket["sum_listing_age_days"] += listing_age_days
        bucket["max_price"] = max(bucket["max_price"], price)
        bucket["min_price"] = min(bucket["min_price"], price)
        bucket["prices"].append(price)
        bucket["prices_per_sqm"].append(price_per_sqm)
        if str(record.get("price_segment", "")) == "luxury":
            bucket["luxury_count"] += 1

    snapshot_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    gold_records: list[dict[str, Any]] = []
    for value in grouped.values():
        count = value["listing_count"]
        gold_records.append(
            {
                "city": value["city"],
                "district": value["district"],
                "listing_count": count,
                "avg_price": round(value["sum_price"] / count, 2),
                "avg_area_sqm": round(value["sum_area_sqm"] / count, 2),
                "avg_price_per_sqm": round(value["sum_price_per_sqm"] / count, 2),
                "median_price": round(median(value["prices"]), 2),
                "p90_price": round(_percentile(value["prices"], 0.9), 2),
                "p90_price_per_sqm": round(_percentile(value["prices_per_sqm"], 0.9), 2),
                "max_price": round(value["max_price"], 2),
                "min_price": round(value["min_price"], 2),
                "avg_listing_age_days": round(value["sum_listing_age_days"] / count, 2),
                "luxury_listing_ratio": round(value["luxury_count"] / count, 4),
                "snapshot_at": snapshot_at,
            }
        )

    return sorted(gold_records, key=lambda item: (item["city"], item["district"]))

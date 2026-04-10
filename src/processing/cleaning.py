"""Contract cleaning để xử lý nulls, text normalization và typing."""

from datetime import datetime, timezone
from typing import Any


def _normalize_space(value: str) -> str:
    return " ".join(value.split())


def _parse_timestamp(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return datetime.min

    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def clean_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Clean và chuẩn hóa records trước khi nạp vào curated layers."""
    # Giữ bản ghi mới nhất theo property_id để giảm trùng dữ liệu.
    dedup: dict[str, dict[str, Any]] = {}
    for record in records:
        property_id = str(record["property_id"]).strip()
        posted_at_raw = str(record["posted_at"]).strip()
        posted_at_dt = _parse_timestamp(posted_at_raw)

        current = dedup.get(property_id)
        if current is not None:
            current_dt = current.get("_posted_at_dt", datetime.min)
            if posted_at_dt <= current_dt:
                continue

        dedup[property_id] = {
            "property_id": property_id,
            "title": _normalize_space(str(record.get("title", "")).strip()),
            "city": _normalize_space(str(record["city"]).strip()),
            "district": _normalize_space(str(record["district"]).strip()),
            "price": round(float(record["price"]), 2),
            "area_sqm": round(float(record["area_sqm"]), 2),
            "bedrooms": max(0, int(record["bedrooms"])),
            "posted_at": posted_at_raw,
            "_posted_at_dt": posted_at_dt,
        }

    cleaned: list[dict[str, Any]] = []
    for item in dedup.values():
        item.pop("_posted_at_dt", None)
        cleaned.append(item)

    cleaned.sort(key=lambda row: row["posted_at"], reverse=True)
    return cleaned

"""Kiểm tra data sẵn sàng cho Superset Dashboard."""
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

layers = {
    "Bronze": "/app/data/lakehouse/bronze/real_estate/",
    "Silver": "/app/data/lakehouse/silver/real_estate/",
    "Gold": "/app/data/lakehouse/gold/real_estate/",
    "Quarantine": "/app/data/lakehouse/quarantine/",
    "Fact Listing": "/app/data/lakehouse/warehouse/fact_listing/",
    "Dim Location": "/app/data/lakehouse/warehouse/dim_location/",
    "Dim Property Type": "/app/data/lakehouse/warehouse/dim_property_type/",
    "Dim Time": "/app/data/lakehouse/warehouse/dim_time/",
    "Dim Price Segment": "/app/data/lakehouse/warehouse/dim_price_segment/",
    "Dim Area Segment": "/app/data/lakehouse/warehouse/dim_area_segment/",
    "Fact Market Snapshot": "/app/data/lakehouse/warehouse/fact_market_snapshot/",
}

print("=" * 60)
print("📊 KIỂM TRA DATA SẴN SÀNG CHO SUPERSET")
print("=" * 60)

for name, path in layers.items():
    try:
        r = con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()
        cols = con.execute(f"SELECT * FROM delta_scan('{path}') LIMIT 0").description
        status = "" if r[0] > 0 else "EMPTY"
        print(f"  {status} {name:25s}: {r[0]:>6,} rows | {len(cols):>3} cols")
    except Exception as e:
        print(f"  {name:25s}: {str(e)[:50]}")

print("=" * 60)
con.close()

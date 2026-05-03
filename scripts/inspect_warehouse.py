"""Xem schema tất cả bảng trong Warehouse để thiết kế Dashboard."""
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

tables = {
    "fact_listing": "/app/data/lakehouse/warehouse/fact_listing/",
    "dim_location": "/app/data/lakehouse/warehouse/dim_location/",
    "dim_property_type": "/app/data/lakehouse/warehouse/dim_property_type/",
    "dim_time": "/app/data/lakehouse/warehouse/dim_time/",
    "dim_price_segment": "/app/data/lakehouse/warehouse/dim_price_segment/",
    "dim_area_segment": "/app/data/lakehouse/warehouse/dim_area_segment/",
    "fact_market_snapshot": "/app/data/lakehouse/warehouse/fact_market_snapshot/",
    "silver": "/app/data/lakehouse/silver/real_estate/",
    "gold": "/app/data/lakehouse/gold/real_estate/",
}

for name, path in tables.items():
    try:
        df = con.execute(f"SELECT * FROM delta_scan('{path}') LIMIT 0").description
        count = con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()[0]
        print(f"=== {name} ({count} rows) ===")
        for col_info in df:
            print(f"  {col_info[0]}: {col_info[1]}")
        print()
    except Exception as e:
        print(f"=== {name}: ERROR {e} ===")
        print()

# Sample data from key tables
print("=== SAMPLE: fact_listing (5 rows) ===")
rows = con.execute("SELECT * FROM delta_scan('/app/data/lakehouse/warehouse/fact_listing/') LIMIT 5").fetchall()
cols = [d[0] for d in con.execute("SELECT * FROM delta_scan('/app/data/lakehouse/warehouse/fact_listing/') LIMIT 0").description]
print("  Columns:", cols)
for row in rows:
    print(" ", row)

print()
print("=== SAMPLE: gold (5 rows) ===")
rows = con.execute("SELECT * FROM delta_scan('/app/data/lakehouse/gold/real_estate/') LIMIT 5").fetchall()
cols = [d[0] for d in con.execute("SELECT * FROM delta_scan('/app/data/lakehouse/gold/real_estate/') LIMIT 0").description]
print("  Columns:", cols)
for row in rows:
    print(" ", row)

con.close()

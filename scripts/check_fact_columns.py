"""Create the hierarchical drill-down Treemap chart via Superset Python API."""
from superset.app import create_app
app = create_app()

with app.app_context():
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard
    from superset.connectors.sqla.models import SqlaTable, TableColumn, SqlMetric
    from superset import db
    import json

    # =============================================
    # STEP 1: Create the Dataset
    # =============================================
    dataset_name = "drilldown_hierarchy"
    
    # Check if dataset already exists
    existing = db.session.query(SqlaTable).filter_by(table_name=dataset_name).first()
    if existing:
        print(f"Dataset '{dataset_name}' already exists (id={existing.id}), deleting...")
        db.session.delete(existing)
        db.session.commit()

    # Get the DuckDB database connection
    from superset.models.core import Database
    database = db.session.query(Database).first()
    print(f"Using database: {database.database_name} (id={database.id})")

    sql_query = """SELECT
    loc.region_code AS vung_mien,
    loc.city AS thanh_pho,
    loc.district AS quan_huyen,
    pt.property_type_name AS loai_hinh,
    CASE
        WHEN s.listing_type = 'BAN' THEN 'Ban'
        WHEN s.listing_type = 'CHO_THUE' THEN 'Cho Thue'
        WHEN s.listing_type = 'SANG_NHUONG' THEN 'Sang Nhuong'
        ELSE 'Khac'
    END AS loai_giao_dich,
    COUNT(*) AS so_tin
FROM delta_scan('/app/data/lakehouse/warehouse/fact_listing/') f
JOIN delta_scan('/app/data/lakehouse/warehouse/dim_location/') loc
    ON f.location_key = loc.location_key
JOIN delta_scan('/app/data/lakehouse/warehouse/dim_property_type/') pt
    ON f.property_type_key = pt.property_type_key
JOIN delta_scan('/app/data/lakehouse/silver/real_estate/') s
    ON f.property_id = s.property_id
WHERE s.listing_type IS NOT NULL
GROUP BY loc.region_code, loc.city, loc.district, pt.property_type_name, s.listing_type
ORDER BY so_tin DESC"""

    dataset = SqlaTable(
        table_name=dataset_name,
        database_id=database.id,
        sql=sql_query,
        is_sqllab_view=True,
    )
    db.session.add(dataset)
    db.session.flush()  # Get ID
    
    # Add columns
    cols_info = [
        ("vung_mien", "STRING", True, True),
        ("thanh_pho", "STRING", True, True),
        ("quan_huyen", "STRING", True, True),
        ("loai_hinh", "STRING", True, True),
        ("loai_giao_dich", "STRING", True, True),
        ("so_tin", "NUMBER", True, False),
    ]
    for col_name, col_type, filterable, groupby in cols_info:
        col = TableColumn(
            column_name=col_name,
            type=col_type,
            filterable=filterable,
            groupby=groupby,
            table_id=dataset.id,
        )
        db.session.add(col)
    
    # Add metric
    metric = SqlMetric(
        metric_name="sum_so_tin",
        expression="SUM(so_tin)",
        table_id=dataset.id,
    )
    db.session.add(metric)
    db.session.commit()
    print(f"Dataset '{dataset_name}' created with id={dataset.id}")

    # =============================================
    # STEP 2: Create the Treemap Chart
    # =============================================
    chart_params = {
        "datasource": f"{dataset.id}__table",
        "viz_type": "treemap_v2",
        "slice_id": None,
        "groupby": ["vung_mien", "thanh_pho", "quan_huyen", "loai_hinh", "loai_giao_dich"],
        "metrics": [{
            "aggregate": "SUM",
            "column": {
                "column_name": "so_tin",
                "type": "NUMBER",
            },
            "expressionType": "SIMPLE",
            "label": "SUM(so_tin)",
        }],
        "row_limit": 10000,
        "color_scheme": "supersetColors",
        "treemap_ratio": 1.618033988749895,
        "show_upper_labels": True,
        "show_labels": True,
        "label_type": "key",
        "number_format": "SMART_NUMBER",
        "date_format": "smart_date",
        "dashboards": [1],
        "emit_filter": True,
    }

    chart = Slice(
        slice_name="🔍 Drill-Down Phân Cấp BĐS (Vùng→Tỉnh→Quận→Loại→Giao dịch)",
        datasource_id=dataset.id,
        datasource_type="table",
        viz_type="treemap_v2",
        params=json.dumps(chart_params),
    )
    db.session.add(chart)
    db.session.commit()
    print(f"Treemap chart created with id={chart.id}")

    # =============================================
    # STEP 3: Add chart to dashboard
    # =============================================
    dash = db.session.query(Dashboard).first()
    if dash:
        current_slices = list(dash.slices)
        if chart not in current_slices:
            current_slices.append(chart)
            dash.slices = current_slices
            db.session.commit()
            print(f"Chart added to dashboard '{dash.dashboard_title}'")
    
    print("\n=== ALL DONE! ===")
    print("Refresh your Dashboard (F5) to see the new Treemap chart!")

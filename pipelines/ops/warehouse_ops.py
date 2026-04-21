"""Dagster Ops cho Star Schema Warehouse ETL.

Luồng kỳ vọng:
1) Đọc Silver + Gold Delta tables đã có sẵn
2) Build các bảng Dimension (dim_location, dim_property_type, dim_price_segment, dim_time)
3) Build các bảng Fact (fact_listing, fact_market_snapshot)
4) Ghi tất cả vào warehouse layer (data/lakehouse/warehouse/)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dagster import op

from src.warehouse.dim_builders import (
    build_dim_area_segment,
    build_dim_location,
    build_dim_price_segment,
    build_dim_property_type,
    build_dim_time,
)
from src.warehouse.fact_builders import (
    build_fact_listing,
    build_fact_market_snapshot,
)
from src.warehouse.delta_io import (
    read_dimension,
    write_dimension,
    write_fact,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _silver_delta_path(settings: Any) -> str:
    """Lấy đường dẫn Silver Delta table dựa trên profile."""
    if "local" in settings.runtime.profile:
        return str(PROJECT_ROOT / "data" / "lakehouse" / settings.storage.silver_prefix)
    return (
        f"abfs://{settings.storage.azure_container}"
        f"@{settings.azure_storage_account}.dfs.core.windows.net"
        f"/{settings.storage.silver_prefix}"
    )


def _gold_delta_path(settings: Any) -> str:
    """Lấy đường dẫn Gold Delta table dựa trên profile."""
    if "local" in settings.runtime.profile:
        return str(PROJECT_ROOT / "data" / "lakehouse" / settings.storage.gold_prefix)
    return (
        f"abfs://{settings.storage.azure_container}"
        f"@{settings.azure_storage_account}.dfs.core.windows.net"
        f"/{settings.storage.gold_prefix}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: Đọc dữ liệu Silver + Gold từ Lakehouse
# ═══════════════════════════════════════════════════════════════════════════════

@op(required_resource_keys={"settings", "spark"})
def op_read_silver_for_warehouse(context) -> list[dict[str, Any]]:
    """Đọc Silver Delta table hiện có để làm nguồn cho Dimension + Fact."""
    spark = context.resources.spark
    settings = context.resources.settings
    silver_path = _silver_delta_path(settings)

    try:
        silver_df = spark.read.format("delta").load(silver_path)
        count = silver_df.count()
        context.log.info(f"📖 Đã đọc Silver Delta: {count} records từ {silver_path}")
        return [row.asDict() for row in silver_df.collect()]
    except Exception as e:
        context.log.error(f"❌ Không đọc được Silver Delta tại {silver_path}: {e}")
        return []


@op(required_resource_keys={"settings", "spark"})
def op_read_gold_for_warehouse(context) -> list[dict[str, Any]]:
    """Đọc Gold Delta table hiện có để làm nguồn cho fact_market_snapshot."""
    spark = context.resources.spark
    settings = context.resources.settings
    gold_path = _gold_delta_path(settings)

    try:
        gold_df = spark.read.format("delta").load(gold_path)
        count = gold_df.count()
        context.log.info(f"📖 Đã đọc Gold Delta: {count} records từ {gold_path}")
        return [row.asDict() for row in gold_df.collect()]
    except Exception as e:
        context.log.error(f"❌ Không đọc được Gold Delta tại {gold_path}: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: Build + Write Dimension Tables
# ═══════════════════════════════════════════════════════════════════════════════

@op(required_resource_keys={"settings", "spark"})
def op_build_dimensions(context, silver_records: list[dict[str, Any]]) -> dict[str, str]:
    """Xây dựng và ghi tất cả 4 Dimension tables từ Silver data.
    
    Returns:
        Dict mapping tên bảng → đường dẫn Delta đã ghi.
    """
    spark = context.resources.spark
    settings = context.resources.settings
    paths: dict[str, str] = {}

    if not silver_records:
        context.log.warning("⚠️ Silver records rỗng, bỏ qua build dimensions.")
        return paths

    silver_df = spark.createDataFrame(silver_records)

    # --- dim_location ---
    context.log.info("🏗️ Building dim_location...")
    dim_loc = build_dim_location(silver_df, getattr(settings, "mdm", None))
    paths["dim_location"] = write_dimension(spark, dim_loc, "dim_location", settings)
    loc_count = dim_loc.count()
    context.log.info(f"   ✅ dim_location: {loc_count} khu vực")

    # --- dim_property_type ---
    context.log.info("🏗️ Building dim_property_type...")
    dim_pt = build_dim_property_type(silver_df)
    paths["dim_property_type"] = write_dimension(spark, dim_pt, "dim_property_type", settings)
    pt_count = dim_pt.count()
    context.log.info(f"   ✅ dim_property_type: {pt_count} loại BĐS")

    # --- dim_price_segment ---
    context.log.info("🏗️ Building dim_price_segment...")
    dim_ps = build_dim_price_segment(spark)
    paths["dim_price_segment"] = write_dimension(spark, dim_ps, "dim_price_segment", settings)
    context.log.info(f"   ✅ dim_price_segment: 4 phân khúc giá")

    # --- dim_area_segment ---
    context.log.info("🏗️ Building dim_area_segment...")
    dim_as = build_dim_area_segment(spark)
    paths["dim_area_segment"] = write_dimension(spark, dim_as, "dim_area_segment", settings)
    context.log.info(f"   ✅ dim_area_segment: 4 phân khúc diện tích")

    # --- dim_time ---
    context.log.info("🏗️ Building dim_time...")
    dim_time = build_dim_time(spark)
    paths["dim_time"] = write_dimension(spark, dim_time, "dim_time", settings)
    time_count = dim_time.count()
    context.log.info(f"   ✅ dim_time: {time_count} ngày (2024-2028)")

    context.log.info(f"🎉 Hoàn thành build {len(paths)} Dimension tables!")
    return paths


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: Build + Write Fact Tables
# ═══════════════════════════════════════════════════════════════════════════════

@op(required_resource_keys={"settings", "spark"})
def op_build_fact_listing(
    context,
    silver_records: list[dict[str, Any]],
    dim_paths: dict[str, str],
) -> str:
    """Xây dựng và ghi fact_listing từ Silver + Dimension tables.
    
    Returns:
        Đường dẫn Delta của fact_listing.
    """
    spark = context.resources.spark
    settings = context.resources.settings

    if not silver_records or not dim_paths:
        context.log.warning("⚠️ Thiếu dữ liệu nguồn, bỏ qua fact_listing.")
        return "empty"

    silver_df = spark.createDataFrame(silver_records)

    # Đọc lại Dimension tables từ Delta
    dim_loc = read_dimension(spark, "dim_location", settings)
    dim_pt = read_dimension(spark, "dim_property_type", settings)
    dim_ps = read_dimension(spark, "dim_price_segment", settings)
    dim_as = read_dimension(spark, "dim_area_segment", settings)

    if dim_loc is None or dim_pt is None or dim_ps is None or dim_as is None:
        context.log.error("❌ Không đọc được Dimension tables. Chạy op_build_dimensions trước.")
        return "error"

    # Build fact
    context.log.info("🏗️ Building fact_listing...")
    fact_df = build_fact_listing(silver_df, dim_loc, dim_pt, dim_ps, dim_as)
    
    fact_path = write_fact(
        spark, fact_df, "fact_listing", settings,
        key_columns=["property_id"],
    )
    
    fact_count = fact_df.count()
    context.log.info(f"🎉 fact_listing: {fact_count} tin đăng được ghi vào {fact_path}")
    return fact_path


@op(required_resource_keys={"settings", "spark"})
def op_build_fact_market_snapshot(
    context,
    gold_records: list[dict[str, Any]],
    dim_paths: dict[str, str],
) -> str:
    """Xây dựng và ghi fact_market_snapshot từ Gold + dim_location.
    
    Returns:
        Đường dẫn Delta của fact_market_snapshot.
    """
    spark = context.resources.spark
    settings = context.resources.settings

    if not gold_records or not dim_paths:
        context.log.warning("⚠️ Thiếu dữ liệu nguồn, bỏ qua fact_market_snapshot.")
        return "empty"

    gold_df = spark.createDataFrame(gold_records)

    # Đọc dim_location
    dim_loc = read_dimension(spark, "dim_location", settings)
    if dim_loc is None:
        context.log.error("❌ Không đọc được dim_location. Chạy op_build_dimensions trước.")
        return "error"

    # Build fact
    context.log.info("🏗️ Building fact_market_snapshot...")
    snapshot_df = build_fact_market_snapshot(gold_df, dim_loc)
    
    # APPEND mode: tích lũy lịch sử snapshot qua các lần chạy
    # MERGE on (location_key, snapshot_time_key) để tránh duplicate cùng ngày
    snapshot_path = write_fact(
        spark, snapshot_df, "fact_market_snapshot", settings,
        key_columns=["location_key", "snapshot_time_key"],
    )
    
    snapshot_count = snapshot_df.count()
    context.log.info(
        f"🎉 fact_market_snapshot: {snapshot_count} khu vực được ghi vào {snapshot_path}"
    )
    return snapshot_path

"""Contract cleaning để xử lý nulls, text normalization và typing bằng PySpark."""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F


from typing import Any

def clean_records(df: DataFrame, mdm_settings: Any = None) -> DataFrame:
    """Clean và chuẩn hóa records trước khi nạp vào curated layers bằng song song PySpark."""
    
    # 1. Text normalization: Xóa khoảng trắng thừa
    # 2. Xử lý TimeZone: Chuyển chuỗi Z sang UTC Timestamp
    clean_df = df \
        .withColumn("title", F.trim(F.regexp_replace(F.col("title"), r'\s+', ' '))) \
        .withColumn("city", F.trim(F.regexp_replace(F.col("city"), r'\s+', ' '))) \
        .withColumn("district", F.trim(F.regexp_replace(F.col("district"), r'\s+', ' '))) \
        .withColumn("_posted_at_raw", 
                    F.regexp_replace(F.trim(F.col("posted_at")), "Z$", "+00:00")) \
        .withColumn("_posted_at_dt", F.to_timestamp(F.col("_posted_at_raw")))
    
    # 3. Chuẩn hóa bằng Master Data Management (MDM - Reference Data)
    if mdm_settings and getattr(mdm_settings, "city_mapping", None):
        from itertools import chain
        reversed_map = {}
        # VD: Sài Gòn -> Hồ Chí Minh, SG -> Hồ Chí Minh
        for standard_name, aliases in mdm_settings.city_mapping.items():
            reversed_map[standard_name.lower()] = standard_name
            for alias in aliases:
                reversed_map[alias.lower()] = standard_name
                
        # Tạo Map Expression trong PySpark
        map_expr = F.create_map([F.lit(x) for x in chain(*reversed_map.items())])
        
        # Override cột city nếu map được tên chuẩn, nếu không thì giữ nguyên tên cũ
        clean_df = clean_df.withColumn(
            "city",
            F.coalesce(map_expr.getItem(F.lower(F.col("city"))), F.col("city"))
        )

    # 4. Chuẩn hóa kiểu dữ liệu số học
    clean_df = clean_df \
        .withColumn("price", F.round(F.col("price").cast("double"), 2)) \
        .withColumn("area_sqm", F.round(F.col("area_sqm").cast("double"), 2)) \
        .withColumn("bedrooms", F.greatest(F.lit(0), F.col("bedrooms").cast("int")))

    # 5. Information Extraction (Khai phá dữ liệu): Phân loại Bất Động Sản từ Tiêu đề
    if mdm_settings and getattr(mdm_settings, "property_type_mapping", None):
        # Mặc định gọi là "Khác" nếu không bóc tách được
        type_expr = F.lit("Khác (Không xác định)")
        
        # Duyệt qua bộ từ khóa trong mdm_rules.yaml
        for standard_type, keywords in mdm_settings.property_type_mapping.items():
            if keywords:
                # Ép regex chuẩn Tiếng Việt bằng cách hạ chữ hoa thành chữ thường cả Title và Keyword
                lowercase_keywords = [k.lower() for k in keywords]
                regex_pattern = "(" + "|".join(lowercase_keywords) + ")"
                
                # Cắm biểu thức If-Else (When-Otherwise) chồng lên nhau
                type_expr = F.when(
                    F.lower(F.col("title")).rlike(regex_pattern), standard_type
                ).otherwise(type_expr)
                
        # --- THUẬT TOÁN HYBRID FALLBACK ---
        # B1: Tạm ghi kết quả quét Regex vào cột _mdm_category
        clean_df = clean_df.withColumn("_mdm_category", type_expr)
        
        # B2: Đề phòng Data truyền vào chưa có cột source_category thì tự độn rỗng
        if "source_category" not in clean_df.columns:
            clean_df = clean_df.withColumn("source_category", F.lit("Khác"))
            
        # B3: Luật gộp 2 cột - Nếu MDM thất bại ("Khác (Không xác định)") -> Vớt bằng source_category
        fallback_expr = F.when(
            F.col("_mdm_category") != "Khác (Không xác định)", F.col("_mdm_category")
        ).otherwise(
            F.when(
                (F.col("source_category").isNotNull()) & (F.col("source_category") != "Khác") & (F.col("source_category") != ""), 
                F.col("source_category")
            ).otherwise(F.col("_mdm_category"))
        )
        
        # Tạo thêm cột tàng hình để báo cáo hiệu suất (Nguồn gốc mapping)
        mapping_source_expr = F.when(
            F.col("_mdm_category") != "Khác (Không xác định)", F.lit("Tự tin dùng Regex MDM")
        ).otherwise(F.lit("Vớt đáy bằng Category Chợ Tốt"))
        
        # B4: Ghi đè vào cột chuẩn property_type và xóa cột tạm
        clean_df = clean_df.withColumn("property_type", fallback_expr)\
                           .withColumn("mapping_source", mapping_source_expr)\
                           .drop("_mdm_category")

    # 4. Giữ bản ghi mới nhất theo property_id để giảm trùng lặp (Deduplication)
    # Tương tự việc dùng dictionary trong Python nhưng chạy bằng Distributed Window Function
    window_spec = Window.partitionBy("property_id") \
                        .orderBy(F.col("_posted_at_dt").desc_nulls_last())
    
    dedup_df = clean_df \
        .withColumn("_rn", F.row_number().over(window_spec)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn", "_posted_at_dt", "_posted_at_raw")
        
    return dedup_df

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
    if mdm_settings:
        from itertools import chain
        
        # --- 3A. Chuẩn hóa City ---
        if getattr(mdm_settings, "city_mapping", None):
            reversed_map_city = {}
            for standard_name, aliases in mdm_settings.city_mapping.items():
                reversed_map_city[standard_name.lower()] = standard_name
                for alias in aliases:
                    reversed_map_city[alias.lower()] = standard_name
            map_expr_city = F.create_map([F.lit(x) for x in chain(*reversed_map_city.items())])
            
            clean_df = clean_df.withColumn(
                "_city_mapped", map_expr_city.getItem(F.lower(F.col("city")))
            )
        else:
            clean_df = clean_df.withColumn("_city_mapped", F.lit(None))
            
        # --- 3B. Chuẩn hóa District ---
        if getattr(mdm_settings, "district_mapping", None):
            reversed_map_dist = {}
            for standard_name, aliases in mdm_settings.district_mapping.items():
                reversed_map_dist[standard_name.lower()] = standard_name
                for alias in aliases:
                    reversed_map_dist[alias.lower()] = standard_name
            map_expr_dist = F.create_map([F.lit(x) for x in chain(*reversed_map_dist.items())])
            
            clean_df = clean_df.withColumn(
                "_district_mapped", map_expr_dist.getItem(F.lower(F.col("district")))
            )
        else:
            clean_df = clean_df.withColumn("_district_mapped", F.lit(None))
            
        # --- 3C. Phân loại MAPPED / UNMAPPED cho Quarantine ---
        clean_df = clean_df.withColumn(
            "mapping_status",
            F.when(F.col("_city_mapped").isNotNull() & F.col("_district_mapped").isNotNull(), F.lit("MAPPED"))
            .otherwise(F.lit("UNMAPPED"))
        )
        
        # Ghi đè vào cột chính (Nếu lỗi UNMAPPED thì vẫn giữ nguyên chữ gốc khách nhập để xem xét)
        clean_df = clean_df.withColumn("city", F.coalesce("_city_mapped", "city")) \
                           .withColumn("district", F.coalesce("_district_mapped", "district")) \
                           .drop("_city_mapped", "_district_mapped")    # 4. Chuẩn hóa kiểu dữ liệu số học
    clean_df = clean_df \
        .withColumn("price", F.round(F.col("price").cast("double"), 2)) \
        .withColumn("area_sqm", F.round(F.col("area_sqm").cast("double"), 2)) \
        .withColumn("bedrooms", F.greatest(F.lit(0), F.col("bedrooms").cast("int")))

    # 5. Information Extraction (Khai phá dữ liệu): Phân loại Bất Động Sản từ Tiêu đề
    if mdm_settings and getattr(mdm_settings, "property_type_mapping", None):
        # Đề phòng Data truyền vào chưa có cột source_category thì tự độn rỗng
        if "source_category" not in clean_df.columns:
            clean_df = clean_df.withColumn("source_category", F.lit("Khác"))

        # Mặc định gọi là "Khác" nếu không bóc tách được
        type_expr_title = F.lit("Khác (Không xác định)")
        type_expr_source = F.lit("Khác (Không xác định)")
        
        # Duyệt qua bộ từ khóa trong mdm_rules.yaml
        for standard_type, keywords in mdm_settings.property_type_mapping.items():
            if keywords:
                # Ép regex chuẩn Tiếng Việt bằng cách hạ chữ hoa thành chữ thường cả Title và Keyword
                lowercase_keywords = [k.lower() for k in keywords]
                regex_pattern = "(" + "|".join(lowercase_keywords) + ")"
                
                # Cắm biểu thức If-Else (When-Otherwise) chồng lên nhau cho TITLE
                type_expr_title = F.when(
                    F.lower(F.col("title")).rlike(regex_pattern), standard_type
                ).otherwise(type_expr_title)
                
                # Cắm biểu thức If-Else cho SOURCE_CATEGORY
                type_expr_source = F.when(
                    F.lower(F.col("source_category")).rlike(regex_pattern), standard_type
                ).otherwise(type_expr_source)
                
        # --- THUẬT TOÁN HYBRID FALLBACK V3 (Regex → ML → Quarantine) ---
        # B1: Tạm ghi kết quả quét Regex vào các cột
        clean_df = clean_df.withColumn("_mdm_category_title", type_expr_title)\
                           .withColumn("_mdm_category_source", type_expr_source)
            
        # B2: Luật gộp - Ưu tiên Title -> tới Source (đã qua MDM) -> tới Source gốc
        fallback_expr = F.when(
            F.col("_mdm_category_title") != "Khác (Không xác định)", F.col("_mdm_category_title")
        ).otherwise(
            F.when(
                F.col("_mdm_category_source") != "Khác (Không xác định)", F.col("_mdm_category_source")
            ).otherwise(
                F.when(
                    (F.col("source_category").isNotNull()) & (F.col("source_category") != "Khác") & (F.col("source_category") != ""), 
                    F.col("source_category")
                ).otherwise(F.lit("Khác (Không xác định)"))
            )
        )
        
        # B3: Tạo thêm cột tàng hình để báo cáo hiệu suất (Nguồn gốc mapping)
        mapping_source_expr = F.when(
            F.col("_mdm_category_title") != "Khác (Không xác định)", F.lit("Regex (Title)")
        ).otherwise(
            F.when(
                F.col("_mdm_category_source") != "Khác (Không xác định)", F.lit("Regex (Category)")
            ).otherwise(F.lit("Chờ ML dự đoán"))
        )
        
        # B4: Ghi kết quả Regex vào cột chính
        clean_df = clean_df.withColumn("property_type", fallback_expr)\
                           .withColumn("mapping_source", mapping_source_expr)\
                           .drop("_mdm_category_title", "_mdm_category_source")

        # ═══════════════════════════════════════════════════════════════
        # B5: LAYER 2 — ML MODEL FALLBACK (Chỉ cho các bản ghi Regex bó tay)
        # ═══════════════════════════════════════════════════════════════
        try:
            from src.processing.ml_classifier import predict_batch, CONFIDENCE_THRESHOLD

            # Tách: bản ghi đã phân loại (Regex OK) vs chưa phân loại (cần ML)
            df_mapped = clean_df.filter(F.col("property_type") != "Khác (Không xác định)")
            df_unmapped = clean_df.filter(F.col("property_type") == "Khác (Không xác định)")

            unmapped_count = df_unmapped.count()
            print(f"🤖 ML Fallback: {unmapped_count} bản ghi cần AI dự đoán")

            if unmapped_count > 0:
                unmapped_pd = df_unmapped.toPandas()
                records = unmapped_pd.to_dict("records")
                predictions = predict_batch(records)

                for i, pred in enumerate(predictions):
                    pt = pred.get("property_type_ml")
                    pt_conf = pred.get("property_type_confidence", 0.0)

                    if pt and pt_conf >= CONFIDENCE_THRESHOLD:
                        unmapped_pd.at[unmapped_pd.index[i], "property_type"] = pt
                        unmapped_pd.at[unmapped_pd.index[i], "mapping_source"] = f"ML Model ({pt_conf:.0%})"
                    else:
                        unmapped_pd.at[unmapped_pd.index[i], "property_type"] = "Khác (Không xác định)"
                        unmapped_pd.at[unmapped_pd.index[i], "mapping_source"] = "Quarantine (ML < 70%)"

                spark = clean_df.sparkSession
                df_ml_result = spark.createDataFrame(unmapped_pd, schema=df_unmapped.schema)
                clean_df = df_mapped.unionByName(df_ml_result)

                ml_resolved = sum(1 for p in predictions 
                                  if p.get("property_type_ml") and p.get("property_type_confidence", 0) >= CONFIDENCE_THRESHOLD)
                print(f"[SUCCESS] ML Property Type: phân loại thêm {ml_resolved}/{unmapped_count} bản ghi")
            else:
                print("[SUCCESS] Regex đã phân loại 100% property_type — Không cần gọi ML")

        except ImportError:
            print("[WARN] Không tìm thấy ml_classifier hoặc thiếu thư viện. Bỏ qua ML fallback.")
        except Exception as e:
            import traceback
            print(f"[ERROR] ML property_type lỗi: {e}")
            traceback.print_exc()

    # ═══════════════════════════════════════════════════════════════
    # 6. LISTING TYPE — Phân loại BÁN / CHO THUÊ / SANG NHƯỢNG (ML)
    # ═══════════════════════════════════════════════════════════════
    # Listing type không có Regex layer → ML dự đoán 100% bản ghi
    try:
        from src.processing.ml_classifier import predict_batch, CONFIDENCE_THRESHOLD

        print("[START] Đang chạy ML Listing Type...")
        all_pd = clean_df.toPandas()
        records = all_pd.to_dict("records")
        predictions = predict_batch(records)

        listing_types = []
        for pred in predictions:
            lt = pred.get("listing_type_ml")
            lt_conf = pred.get("listing_type_confidence", 0.0)
            if lt and lt_conf >= CONFIDENCE_THRESHOLD:
                listing_types.append(lt)
            else:
                listing_types.append("UNKNOWN")

        all_pd["listing_type"] = listing_types
        spark = clean_df.sparkSession
        clean_df = spark.createDataFrame(all_pd)

        from collections import Counter
        lt_counts = Counter(listing_types)
        print(f"[DONE] Listing Type phân loại xong: {dict(lt_counts)}")

    except ImportError:
        print("[WARN] Thiếu ml_classifier. Gán listing_type = UNKNOWN.")
        clean_df = clean_df.withColumn("listing_type", F.lit("UNKNOWN"))
    except Exception as e:
        import traceback
        print(f"[ERROR] Listing type lỗi: {e}")
        traceback.print_exc()
        clean_df = clean_df.withColumn("listing_type", F.lit("UNKNOWN"))

    # 4. Giữ bản ghi mới nhất theo property_id để giảm trùng lặp (Deduplication)
    # Tương tự việc dùng dictionary trong Python nhưng chạy bằng Distributed Window Function
    window_spec = Window.partitionBy("property_id") \
                        .orderBy(F.col("_posted_at_dt").desc_nulls_last())
    
    dedup_df = clean_df \
        .withColumn("_rn", F.row_number().over(window_spec)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn", "_posted_at_dt", "_posted_at_raw")
        
    return dedup_df

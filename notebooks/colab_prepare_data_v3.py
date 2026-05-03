# ==============================================================================
# NOTEBOOK 1 (COLAB v3.1 – FINAL EDITION): CHUẨN BỊ DATA & FEATURE ENGINEERING
# ==============================================================================
# Bản hoàn chỉnh cuối cùng – Đã vá toàn bộ lỗ hổng từ 3 vòng review
# ==============================================================================

import pandas as pd
import numpy as np
import re
try:
    from google.colab import files as colab_files
except ImportError:
    colab_files = None

print("Đã load thư viện (Final Edition v3.1)!")

# ══════════════════════════════════════════════════════════════
# BƯỚC 1: UPLOAD DATA (Hỗ trợ: Raw JSON, Parquet, CSV — nhiều file cùng lúc)
# ══════════════════════════════════════════════════════════════
import json

# Bảng ánh xạ: Tên cột gốc từ API Chợ Tốt → Tên cột chuẩn Pipeline
COLUMN_MAP = {
    "list_id": "property_id",
    "subject": "title",
    "region_name": "city",
    "area_name": "district",
    "price": "price",
    "size": "area_sqm",
    "rooms": "bedrooms",
    "category_name": "source_category",
    "cg_name": "source_category",
}

def read_one_file(fname):
    """Đọc 1 file (JSON / Parquet / CSV) và trả về DataFrame chuẩn."""
    if fname.endswith('.json'):
        with open(fname, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        # JSON có thể là: list of records, hoặc {"ads": [...]}
        if isinstance(raw, dict):
            raw = raw.get("ads", raw.get("data", raw.get("results", [raw])))
        if isinstance(raw, dict):
            raw = [raw]
        tmp = pd.DataFrame(raw)
    elif fname.endswith('.parquet'):
        tmp = pd.read_parquet(fname)
    else:
        tmp = pd.read_csv(fname)

    # Tự động map tên cột API sang tên cột Pipeline (nếu cần)
    rename = {k: v for k, v in COLUMN_MAP.items() if k in tmp.columns}
    if rename:
        tmp = tmp.rename(columns=rename)
    return tmp

try:
    print("📂 CHỌN FILE DATA (JSON raw / Parquet / CSV — chọn NHIỀU file cùng lúc):")
    uploaded = colab_files.upload()

    all_dfs = []
    for fname in uploaded.keys():
        try:
            tmp = read_one_file(fname)
            all_dfs.append(tmp)
            print(f"  {fname}: {len(tmp)} bản ghi")
        except Exception as e:
            print(f"  {fname}: Lỗi - {e}")

    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\n📦 TỔNG CỘNG: {len(df)} bản ghi từ {len(all_dfs)} file.")

except Exception as e:
    print(f"Chạy giả lập: {e}")
    df = pd.DataFrame([
        {"property_id":"1","title":"Bán nhà mặt tiền Lê Văn Sỹ Q3 5x20m SHR","source_category":"Nhà ở","price":8.5e9,"area_sqm":100,"bedrooms":4,"city":"Tp Hồ Chí Minh","district":"Quận 3"},
        {"property_id":"2","title":"Phòng trọ mới xây gần ĐH Hutech có ban công","source_category":"Phòng trọ","price":3.5e6,"area_sqm":20,"bedrooms":np.nan,"city":"Tp Hồ Chí Minh","district":"Quận Bình Thạnh"},
        {"property_id":"3","title":"Sang quán cafe đông khách 2 mặt tiền","source_category":"Văn phòng, Mặt bằng kinh doanh","price":2.5e8,"area_sqm":80,"bedrooms":0,"city":"Tp Hồ Chí Minh","district":"Quận Gò Vấp"},
    ])

# ══════════════════════════════════════════════════════════════
# BƯỚC 2: XỬ LÝ MISSING VALUE THÔNG MINH
# ══════════════════════════════════════════════════════════════
df["title"] = df["title"].fillna("").astype(str).str.lower().str.strip()
df["source_category"] = df["source_category"].fillna("").astype(str).str.lower().str.strip()
df["city"] = df.get("city", pd.Series(dtype=str)).fillna("Unknown").astype(str)
df["district"] = df.get("district", pd.Series(dtype=str)).fillna("Unknown").astype(str)
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df["area_sqm"] = pd.to_numeric(df["area_sqm"], errors="coerce")
df["bedrooms"] = pd.to_numeric(df["bedrooms"], errors="coerce").fillna(-1)

# ══════════════════════════════════════════════════════════════
# BƯỚC 3: LỌC SPAM + TRÙNG LẶP NÂNG CAO (Stronger Dedup)
# ══════════════════════════════════════════════════════════════
before_count = len(df)

# 3A: Lọc title quá ngắn (spam/rác)
df = df[df["title"].str.len() > 10]

# 3B: Lọc trùng lặp MẠNH hơn (hash 50 ký tự đầu + giá)
# Bắt cả trường hợp người dùng copy tin cũ, sửa vài chữ cuối
df["_title_hash"] = df["title"].str[:50]
df = df.drop_duplicates(subset=["_title_hash", "price"])
df = df.drop(columns=["_title_hash"])

# 3C: Lọc bản ghi thiếu CẢ giá LẪN diện tích (không cứu được)
df = df[~((df["price"].isna() | (df["price"] == 0)) & (df["area_sqm"].isna() | (df["area_sqm"] == 0)))]

print(f"🧹 Lọc Spam/Trùng: {before_count} → {len(df)} (Loại {before_count - len(df)} rác)")

# ══════════════════════════════════════════════════════════════
# BƯỚC 4: LỌC OUTLIER BẰNG IQR THEO TỪNG THÀNH PHỐ
# ══════════════════════════════════════════════════════════════
def filter_outlier_by_iqr(group, column="price", multiplier=3):
    valid = group[group[column].notna() & (group[column] > 0)]
    if len(valid) < 10:
        return group
    Q1 = valid[column].quantile(0.25)
    Q3 = valid[column].quantile(0.75)
    IQR = Q3 - Q1
    upper = Q3 + multiplier * IQR
    return group[(group[column] <= upper) | (group[column].isna()) | (group[column] == 0)]

before_iqr = len(df)
df = df.groupby("city", group_keys=False).apply(filter_outlier_by_iqr)
print(f"🧹 Lọc Outlier (IQR/City): {before_iqr} → {len(df)}")

# ══════════════════════════════════════════════════════════════
# BƯỚC 5: GÁN NHÃN LISTING_TYPE + CONFIDENCE SCORE
# ══════════════════════════════════════════════════════════════
# ĐÃ XÓA HOÀN TOÀN Price Guard hard rule (bug nghiêm trọng)
# Thay bằng Confidence Score để lọc data train chất lượng cao

def get_listing_type_with_confidence(row):
    cat, title = row["source_category"], row["title"]

    # Tầng 1: source_category (Tin cậy nhất - Người dùng tự chọn)
    if re.search(r"cho thuê|thuê|phòng trọ|nhà trọ|ký túc", cat):
        return "CHO_THUE", 0.95
    if re.search(r"mua bán|bán", cat):
        return "BAN", 0.95

    # Tầng 2: title (Tin cậy trung bình)
    if re.search(r"sang nhượng|sang quán|sang lại|sang gấp", title):
        return "SANG_NHUONG", 0.85
    if re.search(r"cho thuê|thuê|phòng trọ|nhà trọ|ở ghép", title):
        return "CHO_THUE", 0.80
    if re.search(r"cần bán|bán gấp|bán nhà|bán lỗ", title):
        return "BAN", 0.80

    # Tầng 3: KHÔNG ĐOÁN MÒ — Trả về UNKNOWN với confidence thấp
    # (Thay vì dùng Price Guard sai lầm)
    return "UNKNOWN", 0.30

results = df.apply(get_listing_type_with_confidence, axis=1, result_type="expand")
df["listing_type"] = results[0]
df["listing_confidence"] = results[1]

print(f"\n🏷️ LISTING TYPE (trước khi lọc):")
print(df["listing_type"].value_counts().to_string())
print(f"Bản ghi UNKNOWN (không chắc chắn): {(df['listing_type'] == 'UNKNOWN').sum()}")

# ══════════════════════════════════════════════════════════════
# BƯỚC 6: GÁN NHÃN PROPERTY_TYPE (Context Filter + Priority)
# ══════════════════════════════════════════════════════════════
# Dùng hàm thay vì dict/list để xử lý Context Filter (edge cases)

def get_property_type_with_confidence(row):
    cat = row["source_category"]
    title = row["title"]
    text = cat + " " + title

    # PRIORITY 1: Sang nhượng/Mặt bằng (Cụ thể nhất → Kiểm tra trước)
    if re.search(r"sang quán|sang nhượng|sang lại|sang gấp", text):
        return "Sang nhượng/Mặt bằng", 0.90
    if re.search(r"văn phòng|mặt bằng|shop\s?house|kiot", text):
        return "Sang nhượng/Mặt bằng", 0.85

    # PRIORITY 2: Phòng trọ
    if re.search(r"phòng trọ|nhà trọ|phòng gác lửng|ký túc xá|sleepbox|phòng cho thuê|ở ghép", text):
        return "Phòng trọ/Nhà trọ", 0.90

    # PRIORITY 3: Biệt thự
    if re.search(r"biệt thự|villa", text):
        return "Biệt thự/Villa", 0.90

    # PRIORITY 4: Căn hộ
    if re.search(r"chung cư|căn hộ|condotel|apartment|chcc|penthouse|officetel", text):
        return "Căn hộ chung cư", 0.90

    # PRIORITY 5: Nhà phố (Kiểm tra TRƯỚC Đất vì "nhà có đất" → Nhà, không phải Đất)
    if re.search(r"nhà phố|nhà hẻm|nhà riêng|nhà liền kề|nhà nguyên căn|nhà ở|nhà \d+ tầng|nhà lầu|nhà cấp", text):
        return "Nhà phố/Nhà riêng", 0.85

    # PRIORITY 6 (CUỐI CÙNG): Đất nền — CHỈ khi KHÔNG match bất kỳ class nào ở trên
    # Context Filter: "mặt bằng đất" đã bị bắt ở Priority 1 → không lọt xuống đây
    if re.search(r"\bđất\b|đất nền|đất dự án|đất thổ cư|đất nông nghiệp|lô góc", text):
        return "Đất nền/Đất dự án", 0.80

    return "UNMAPPED", 0.0

results_pt = df.apply(get_property_type_with_confidence, axis=1, result_type="expand")
df["property_type"] = results_pt[0]
df["property_confidence"] = results_pt[1]

# ══════════════════════════════════════════════════════════════
# BƯỚC 7: FEATURE ENGINEERING BẬC CAO
# ══════════════════════════════════════════════════════════════

# --- Numeric Features ---
df["price_double"] = df["price"].fillna(0)
df["area_double"] = df["area_sqm"].fillna(0)
df["bedrooms_int"] = df["bedrooms"]
df["log_price"] = np.log1p(df["price_double"])
df["title_length"] = df["title"].str.len()

# --- Interaction Features ---
df["price_per_sqm"] = np.where(df["area_double"] > 0, df["price_double"] / df["area_double"], 0)
df["price_per_bedroom"] = np.where(df["bedrooms_int"] > 0, df["price_double"] / df["bedrooms_int"], 0)

# --- Soft Price Signal (Thay thế Price Guard cứng bằng Feature mềm cho AI tự học) ---
df["price_hint_rent"] = (df["price_double"].between(1, 500_000_000)).astype(int)

# --- BĐS Domain Features ---
df["has_mat_tien"] = df["title"].str.contains(r"mặt tiền|mt\b|mặt phố", na=False).astype(int)
df["has_so_do"]    = df["title"].str.contains(r"sổ đỏ|sổ hồng|shr\b|có sổ|sổ riêng", na=False).astype(int)
df["has_hxh"]      = df["title"].str.contains(r"hẻm xe hơi|hxh\b|hẻm ô tô|ô tô vào", na=False).astype(int)
df["has_view"]     = df["title"].str.contains(r"view", na=False).astype(int)
df["has_noi_that"] = df["title"].str.contains(r"nội thất|full đồ|full nt", na=False).astype(int)
df["is_hem"]       = df["title"].str.contains(r"\bhẻm\b", na=False).astype(int)

# --- Missing Flags ---
df["is_missing_bedroom"] = (df["bedrooms_int"] == -1).astype(int)
df["is_missing_price"]   = ((df["price_double"] == 0) | df["price"].isna()).astype(int)

# --- Interaction Feature bổ sung ---
df["area_per_bedroom"] = np.where(df["bedrooms_int"] > 0, df["area_double"] / df["bedrooms_int"], 0)

# LƯU Ý: KHÔNG tính district_avg_price ở đây!
# Vì nếu tính trên toàn bộ dataset → Data Leakage (test data rò rỉ vào train)
# Feature này sẽ được tính BÊN TRONG Notebook 2 SAU KHI split train/test

# ══════════════════════════════════════════════════════════════
# BƯỚC 8: LỌC CHỈ GIỮ DATA CHẤT LƯỢNG CAO (Confidence > 0.7)
# ══════════════════════════════════════════════════════════════
out_cols = [
    "property_id", "title", "source_category", "city", "district",
    "price_double", "area_double", "bedrooms_int",
    "price_per_sqm", "price_per_bedroom", "area_per_bedroom",
    "log_price", "title_length",
    "price_hint_rent",
    "has_mat_tien", "has_so_do", "has_hxh", "has_view", "has_noi_that", "is_hem",
    "is_missing_bedroom", "is_missing_price",
    "listing_type", "listing_confidence",
    "property_type", "property_confidence"
]

# Lọc: Chỉ giữ bản ghi có nhãn rõ ràng (confidence cao)
df_train = df[
    (df["property_type"] != "UNMAPPED") &
    (df["listing_type"] != "UNKNOWN") &
    (df["listing_confidence"] >= 0.7) &
    (df["property_confidence"] >= 0.7)
][out_cols]

output_file = "training_data_final.csv"
df_train.to_csv(output_file, index=False)

# ══════════════════════════════════════════════════════════════
# BƯỚC 9: BÁO CÁO TỔNG KẾT
# ══════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("📊 BÁO CÁO CHUẨN BỊ DỮ LIỆU v3.1 (FINAL EDITION)")
print("="*65)

print(f"\n📦 Tổng bản ghi gốc (Bronze):      {before_count}")
print(f"🧹 Sau lọc Spam/Trùng/Outlier:      {len(df)}")
print(f"Bản ghi CHẤT LƯỢNG CAO (Train):   {len(df_train)}")
print(f"Bản ghi bị loại (UNKNOWN/LOW):    {len(df) - len(df_train)}")

print(f"\n🏷️ PROPERTY TYPE:")
print(df_train["property_type"].value_counts().to_string())

print(f"\n💰 LISTING TYPE:")
print(df_train["listing_type"].value_counts().to_string())

print(f"\n📊 CONFIDENCE TRUNG BÌNH:")
print(f"   Listing:  {df_train['listing_confidence'].mean():.2f}")
print(f"   Property: {df_train['property_confidence'].mean():.2f}")

try:
    print(f"\n⏳ Đang tải '{output_file}' về máy...")
    colab_files.download(output_file)
except: pass

print("\n👉 BƯỚC TIẾP: Chạy file 'colab_compare_models_v3.py' để train model!")
print("   (File model vẫn dùng colab_compare_models_v3.py, chỉ đổi tên file load)")

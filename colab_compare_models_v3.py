# ==============================================================================
# NOTEBOOK 2 (COLAB v3.1 – FINAL EDITION): HUẤN LUYỆN & SO SÁNH 4 MÔ HÌNH AI
# ==============================================================================
# Bản hoàn chỉnh cuối cùng – Tương thích dữ liệu Final Edition
# ==============================================================================

# !pip install xgboost tabulate

import pandas as pd
import numpy as np
import time
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, f1_score, classification_report, confusion_matrix
from tabulate import tabulate
import warnings
warnings.filterwarnings("ignore")
try:
    from google.colab import files as colab_files
except ImportError:
    colab_files = None

print("✅ Thư viện ML đã sẵn sàng (Final Edition v3.1)!")

# ══════════════════════════════════════════════════════════════
# BƯỚC 1: LOAD DỮ LIỆU ĐÃ CHUẨN BỊ
# ══════════════════════════════════════════════════════════════
file_name = "training_data_final.csv"
try:
    df = pd.read_csv(file_name)
    print(f"📦 Đã load {len(df)} bản ghi từ '{file_name}'.")
except FileNotFoundError:
    try:
        print("📂 Chọn file training_data_final.csv để upload:")
        uploaded = colab_files.upload()
        file_name = list(uploaded.keys())[0]
        df = pd.read_csv(file_name)
        print(f"📦 Đã load {len(df)} bản ghi.")
    except Exception as e:
        print(f"⚠️ Dùng data giả lập: {e}")
        rows = []
        for i in range(200):
            rows.append({
                "title": f"bán nhà mặt tiền quận {i%10} sổ hồng riêng hẻm xe hơi view đẹp",
                "source_category": ["nhà ở","đất","phòng trọ","mặt bằng"][i%4],
                "city": ["HCM","HN","DN"][i%3], "district": f"Q{i%10}",
                "price_double": np.random.choice([0, 3e6, 5e8, 2e9, 8e9]),
                "area_double": np.random.uniform(15, 300),
                "bedrooms_int": np.random.choice([-1, 0, 1, 2, 3, 4, 5]),
                "price_per_sqm": np.random.uniform(1e5, 1e8),
                "price_per_bedroom": np.random.uniform(0, 3e9),
                "log_price": np.random.uniform(10, 24),
                "title_length": np.random.randint(15, 80),
                "has_mat_tien": np.random.randint(0,2), "has_so_do": np.random.randint(0,2),
                "has_hxh": np.random.randint(0,2), "has_view": np.random.randint(0,2),
                "has_noi_that": np.random.randint(0,2), "is_hem": np.random.randint(0,2),
                "is_missing_bedroom": np.random.randint(0,2), "is_missing_price": np.random.randint(0,2),
                "listing_type": ["BAN","CHO_THUE","SANG_NHUONG"][i%3],
                "property_type": ["Nhà phố/Nhà riêng","Căn hộ chung cư","Đất nền/Đất dự án","Phòng trọ/Nhà trọ","Sang nhượng/Mặt bằng","Biệt thự/Villa"][i%6]
            })
        df = pd.DataFrame(rows)

# ══════════════════════════════════════════════════════════════
# BƯỚC 2: ĐỊNH NGHĨA FEATURE GROUPS
# ══════════════════════════════════════════════════════════════
TEXT_FEATURES = ["title", "source_category"]
NUMERIC_FEATURES = ["log_price", "area_double", "price_per_sqm", "price_per_bedroom",
                    "area_per_bedroom", "bedrooms_int", "title_length"]
FLAG_FEATURES = ["has_mat_tien", "has_so_do", "has_hxh", "has_view",
                 "has_noi_that", "is_hem", "is_missing_bedroom", "is_missing_price",
                 "price_hint_rent"]
LOCATION_FEATURES = ["city"]  # 👈 Chỉ giữ city (63 cột). Bỏ district để tránh nổ RAM
# district_avg_price sẽ được tính AN TOÀN sau khi split (chống Data Leakage)
LOCATION_VALUE_COLS = ["district_avg_price"]  # Tính trong hàm train

ALL_FEATURES = TEXT_FEATURES + NUMERIC_FEATURES + FLAG_FEATURES + LOCATION_FEATURES

# ══════════════════════════════════════════════════════════════
# BƯỚC 3: PIPELINE TIỀN XỬ LÝ (ColumnTransformer)
# ══════════════════════════════════════════════════════════════
# 🚨 CHỐNG DATA LEAKAGE: Tính district_avg_price CHỈ trên TRAIN SET
def add_location_value_features(X_train, X_test):
    """Tính giá trung bình theo quận CHỈ từ train, rồi map sang test."""
    valid = X_train[X_train["price_double"] > 0]
    district_map = valid.groupby("district")["price_double"].mean()

    X_train = X_train.copy()
    X_test = X_test.copy()
    X_train["district_avg_price"] = X_train["district"].map(district_map).fillna(0)
    X_test["district_avg_price"] = X_test["district"].map(district_map).fillna(0)

    return X_train, X_test

# Features cho Pipeline (không chứa district_avg_price vì nó được thêm sau)
PIPELINE_NUMERIC = NUMERIC_FEATURES + LOCATION_VALUE_COLS

preprocessor = ColumnTransformer([
    ("title_tfidf", TfidfVectorizer(
        max_features=2000,
        ngram_range=(1,2),
        min_df=2,
        sublinear_tf=True
    ), "title"),
    ("cat_tfidf", TfidfVectorizer(max_features=50), "source_category"),
    ("numeric", StandardScaler(), PIPELINE_NUMERIC),
    ("flags", "passthrough", FLAG_FEATURES),
    ("location", OneHotEncoder(handle_unknown="ignore", sparse_output=False), LOCATION_FEATURES),
], remainder="drop")

# ══════════════════════════════════════════════════════════════
# BƯỚC 4: HÀM HUẤN LUYỆN & SO SÁNH CHUNG
# ══════════════════════════════════════════════════════════════
FEATURES_FOR_MODEL = TEXT_FEATURES + PIPELINE_NUMERIC + FLAG_FEATURES + LOCATION_FEATURES

def train_and_compare(df_full, y_encoded, label_encoder, task_name):
    """Train 4 mô hình (chống Data Leakage), so sánh, và trả về model tốt nhất."""

    # Split trước
    X_train_raw, X_test_raw, y_train, y_test = train_test_split(
        df_full, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
    )

    # Tính district_avg_price CHỈ từ train (Chống Data Leakage!)
    X_train_raw, X_test_raw = add_location_value_features(X_train_raw, X_test_raw)

    # Chọn cột cho model
    X_train = X_train_raw[FEATURES_FOR_MODEL]
    X_test = X_test_raw[FEATURES_FOR_MODEL]

    models = {
        "Logistic Regression": Pipeline([
            ("prep", preprocessor),
            ("clf", LogisticRegression(max_iter=1000, multi_class="multinomial",
                                      class_weight="balanced", random_state=42))
        ]),
        "Random Forest": Pipeline([
            ("prep", preprocessor),
            ("clf", RandomForestClassifier(n_estimators=100, class_weight="balanced",
                                          random_state=42, n_jobs=-1))
        ]),
        "XGBoost": Pipeline([
            ("prep", preprocessor),
            ("clf", XGBClassifier(n_estimators=100, max_depth=6, learning_rate=0.1,
                                 random_state=42, eval_metric="mlogloss"))
        ]),
        "Voting Ensemble": Pipeline([
            ("prep", preprocessor),
            ("clf", VotingClassifier(
                estimators=[
                    ("lr", LogisticRegression(max_iter=1000, multi_class="multinomial",
                                             class_weight="balanced", random_state=42)),
                    ("rf", RandomForestClassifier(n_estimators=100, class_weight="balanced",
                                                 random_state=42)),
                    ("xgb", XGBClassifier(n_estimators=100, random_state=42, eval_metric="mlogloss"))
                ],
                voting="soft"
            ))
        ])
    }

    results = []
    best_acc = 0
    best_model = None
    best_name = ""

    print(f"\n🚀 HUẤN LUYỆN [{task_name}] – 4 mô hình...\n")

    for name, model in models.items():
        t0 = time.time()
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        elapsed = time.time() - t0

        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average="weighted")

        results.append([name, f"{acc*100:.2f}%", f"{f1:.4f}", f"{elapsed:.2f}s"])
        print(f"  ✅ {name}: Accuracy={acc*100:.1f}%, F1={f1:.4f} ({elapsed:.2f}s)")

        if acc > best_acc:
            best_acc = acc
            best_model = model
            best_name = name

    # Bảng xếp hạng
    print(f"\n{'='*75}")
    print(f"🏆 BẢNG XẾP HẠNG [{task_name}]")
    print(f"{'='*75}")
    print(tabulate(results, headers=["Thuật Toán", "Accuracy", "F1-Score", "Thời gian"], tablefmt="github"))

    # Báo cáo chi tiết
    y_pred_best = best_model.predict(X_test)
    target_names = label_encoder.inverse_transform(sorted(set(y_test)))
    print(f"\n📊 CLASSIFICATION REPORT – [{best_name}]:")
    print(classification_report(y_test, y_pred_best, target_names=target_names))

    # Confusion Matrix (Nhìn rõ model hay nhầm class nào với class nào)
    print(f"📊 CONFUSION MATRIX – [{best_name}]:")
    cm = confusion_matrix(y_test, y_pred_best)
    cm_df = pd.DataFrame(cm, index=target_names, columns=target_names)
    print(cm_df.to_string())

    # Cross Validation 5-fold
    print(f"\n🔄 CROSS VALIDATION 5-FOLD [{best_name}]:")
    cv_scores = cross_val_score(best_model, X_train, y_train, cv=5, scoring="accuracy")
    print(f"   Scores:  {[f'{s:.3f}' for s in cv_scores]}")
    print(f"   Mean:    {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
    print(f"   → Model {'ỔN ĐỊNH ✅' if cv_scores.std() < 0.05 else 'CẦN CẢI THIỆN ⚠️'}")

    return best_model, best_name, best_acc

# ══════════════════════════════════════════════════════════════
# BƯỚC 5A: TRAIN MODEL – PROPERTY TYPE (6 lớp)
# ══════════════════════════════════════════════════════════════
print("="*75)
print("📘 BÀI TOÁN A: PHÂN LOẠI LOẠI HÌNH BĐS (Property Type)")
print("="*75)

le_property = LabelEncoder()
y_property = le_property.fit_transform(df["property_type"])

# Truyền cả DataFrame (bao gồm district) để hàm tự tính district_avg_price
best_prop_model, best_prop_name, best_prop_acc = train_and_compare(
    df, y_property, le_property, "PROPERTY TYPE"
)

# ══════════════════════════════════════════════════════════════
# BƯỚC 5B: TRAIN MODEL – LISTING TYPE (BÁN / CHO THUÊ / SANG NHƯỢNG)
# ══════════════════════════════════════════════════════════════
print("\n" + "="*75)
print("📙 BÀI TOÁN B: PHÂN LOẠI LOẠI TIN (Listing Type)")
print("="*75)

le_listing = LabelEncoder()
y_listing = le_listing.fit_transform(df["listing_type"])

best_list_model, best_list_name, best_list_acc = train_and_compare(
    df, y_listing, le_listing, "LISTING TYPE"
)

# ══════════════════════════════════════════════════════════════
# BƯỚC 6: XUẤT MODEL BUNDLE (Model + Encoder + Feature List)
# ══════════════════════════════════════════════════════════════
import joblib

# Bundle = Model + Encoder + Feature List (Production-ready)
property_bundle = {
    "model": best_prop_model,
    "encoder": le_property,
    "features": ALL_FEATURES,
    "version": "v3.1_final"
}
listing_bundle = {
    "model": best_list_model,
    "encoder": le_listing,
    "features": ALL_FEATURES,
    "version": "v3.1_final"
}

joblib.dump(property_bundle, "property_type_bundle.pkl")
joblib.dump(listing_bundle, "listing_type_bundle.pkl")

print("\n" + "="*75)
print("🎉 TỔNG KẾT HUẤN LUYỆN v3.1 (Final Edition)")
print("="*75)
print(f"  🏠 Property Type Model: {best_prop_name} → Accuracy: {best_prop_acc*100:.1f}%")
print(f"  💰 Listing Type Model:  {best_list_name} → Accuracy: {best_list_acc*100:.1f}%")
print(f"  📦 Files đã xuất (Bundle = Model + Encoder + Feature List):")
print(f"     - property_type_bundle.pkl")
print(f"     - listing_type_bundle.pkl")
print(f"  📝 Features được lưu cùng model: {len(ALL_FEATURES)} features")

# Tải xuống tự động
try:
    for f in ["property_type_bundle.pkl", "listing_type_bundle.pkl"]:
        colab_files.download(f)
    print("  ⬇️  Đã tải toàn bộ files về máy!")
except: pass

print("\n✅ HOÀN THÀNH! Đưa 2 file .pkl bundle này lên Azure Data Lake để Pipeline Dagster xài! 🚀")

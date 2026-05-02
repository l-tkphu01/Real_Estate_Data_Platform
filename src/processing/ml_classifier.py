"""ML Classifier: Load model .pkl đã train và dự đoán property_type & listing_type.

Module này đóng vai trò "Bộ não AI" của Pipeline.
Được gọi bởi cleaning.py khi Regex không phân loại được (Fallback Layer 2).

Flow: Regex (Layer 1) → ML Model (Layer 2) → Quarantine (Layer 3)
"""

import logging
import os
import re
from functools import lru_cache # @lru_cache: viết tắt của least recently used cache. Dùng để lưu kết quả của hàm, tránh gọi lại nhiều lần.
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__) 
# Đường dẫn mặc định tới file .pkl bundle (tương đối từ thư mục gốc dự án)
DEFAULT_MODEL_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "models"
)

CONFIDENCE_THRESHOLD = 0.70  # Dưới ngưỡng này → đẩy vào Quarantine


@lru_cache(maxsize=1)
def _load_bundle(bundle_path: str) -> dict[str, Any]:
    """Load model bundle từ file .pkl (cached để không load lại nhiều lần)."""
    import joblib
    bundle = joblib.load(bundle_path)
    logger.info(f"✅ Đã load ML model bundle: {bundle_path} (v{bundle.get('version', '?')})")
    return bundle


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Tạo các Feature cần thiết từ raw data (giống Notebook 1).

    Hàm này đảm bảo dữ liệu đầu vào có đủ các cột mà model cần.
    """
    result = df.copy()

    # Text features (giữ nguyên)
    result["title"] = result["title"].fillna("").astype(str).str.lower().str.strip()
    result["source_category"] = result["source_category"].fillna("").astype(str).str.lower().str.strip()
    result["city"] = result["city"].fillna("Unknown").astype(str)
    result["district"] = result["district"].fillna("Unknown").astype(str)

    # Numeric features — handle tên cột từ PySpark (price) vs Colab (price_double)
    price_col = "price_double" if "price_double" in result.columns else "price"
    area_col = "area_double" if "area_double" in result.columns else "area_sqm"
    bed_col = "bedrooms_int" if "bedrooms_int" in result.columns else "bedrooms"

    result["price_double"] = pd.to_numeric(result[price_col], errors="coerce").fillna(0)
    result["area_double"] = pd.to_numeric(result[area_col], errors="coerce").fillna(0)
    result["bedrooms_int"] = pd.to_numeric(result[bed_col], errors="coerce").fillna(-1)
    result["log_price"] = np.log1p(result["price_double"])
    result["title_length"] = result["title"].str.len()

    # Interaction features
    result["price_per_sqm"] = np.where(result["area_double"] > 0, result["price_double"] / result["area_double"], 0)
    result["price_per_bedroom"] = np.where(result["bedrooms_int"] > 0, result["price_double"] / result["bedrooms_int"], 0)
    result["area_per_bedroom"] = np.where(result["bedrooms_int"] > 0, result["area_double"] / result["bedrooms_int"], 0)

    # Soft price signal (tạo tín hiệu gợi ý giá, giúp AI nhận diện nhanh loại hình thuê/cho thuê dựa trên giá tiền)
    result["price_hint_rent"] = result["price_double"].between(1, 500_000_000).astype(int)

    # BĐS domain features
    result["has_mat_tien"] = result["title"].str.contains(r"mặt tiền|mt\b|mặt phố", na=False).astype(int)
    result["has_so_do"] = result["title"].str.contains(r"sổ đỏ|sổ hồng|shr\b|có sổ|sổ riêng", na=False).astype(int)
    result["has_hxh"] = result["title"].str.contains(r"hẻm xe hơi|hxh\b|hẻm ô tô|ô tô vào", na=False).astype(int)
    result["has_view"] = result["title"].str.contains(r"view", na=False).astype(int)
    result["has_noi_that"] = result["title"].str.contains(r"nội thất|full đồ|full nt", na=False).astype(int)
    result["is_hem"] = result["title"].str.contains(r"\bhẻm\b", na=False).astype(int)

    # Missing flags
    result["is_missing_bedroom"] = (result["bedrooms_int"] == -1).astype(int)
    result["is_missing_price"] = (result["price_double"] == 0).astype(int)

    # Location value — luôn tạo cột, dù chỉ có 1 bản ghi
    result["district_avg_price"] = 0.0
    valid = result[result["price_double"] > 0]
    if len(valid) > 0:
        district_map = valid.groupby("district")["price_double"].mean()
        result["district_avg_price"] = result["district"].map(district_map).fillna(0)

    return result


def predict_batch(
    records: list[dict],
    model_dir: str | None = None,
) -> list[dict]:
    """Dự đoán property_type và listing_type cho một batch records.

    Args:
        records: List các dict (mỗi dict là 1 bản ghi BĐS).
        model_dir: Thư mục chứa file .pkl bundle.

    Returns:
        List các dict, mỗi dict chứa:
            - property_type_ml: Nhãn dự đoán
            - property_type_confidence: Độ tự tin (0.0 → 1.0)
            - listing_type_ml: Nhãn dự đoán
            - listing_type_confidence: Độ tự tin (0.0 → 1.0)
    """
    if not records:
        return []

    model_dir = model_dir or DEFAULT_MODEL_DIR

    # Load bundles
    prop_path = os.path.join(model_dir, "property_type_bundle.pkl")
    list_path = os.path.join(model_dir, "listing_type_bundle.pkl")

    if not os.path.exists(prop_path) or not os.path.exists(list_path):
        logger.warning(f"⚠️ Không tìm thấy model .pkl tại {model_dir}. Bỏ qua ML inference.")
        return [{"property_type_ml": None, "listing_type_ml": None} for _ in records]

    prop_bundle = _load_bundle(prop_path)
    list_bundle = _load_bundle(list_path)

    prop_model = prop_bundle["model"]
    prop_encoder = prop_bundle["encoder"]
    list_model = list_bundle["model"]
    list_encoder = list_bundle["encoder"]
    feature_cols = list(prop_bundle["features"])

    # ColumnTransformer bên trong model cần district_avg_price 
    # (nằm trong PIPELINE_NUMERIC) dù feature list không ghi ra
    if "district_avg_price" not in feature_cols:
        feature_cols.append("district_avg_price")

    # Build features
    df = pd.DataFrame(records)
    df = _build_features(df)

    # Đảm bảo có đủ cột (nếu thiếu thì điền 0)
    for col in feature_cols:
        if col not in df.columns:
            df[col] = 0

    X = df[feature_cols]

    # Tách preprocessor và raw model từ Pipeline để handle feature padding
    # Pipeline structure: [('preprocessor', ColumnTransformer), ('model', XGBClassifier/RF)]
    def _safe_predict(pipeline, encoder, X_row, label_name):
        """Predict an toàn: tách preprocessor + pad features nếu XGBoost đòi hỏi."""
        try:
            pred = pipeline.predict(X_row)[0]
            proba = pipeline.predict_proba(X_row)[0]
            conf = float(max(proba))
            return encoder.inverse_transform([pred])[0], round(conf, 3)
        except ValueError as e:
            if "Feature shape mismatch" in str(e) or "feature" in str(e).lower():
                # XGBoost strict check — tách preprocessor và pad thủ công
                try:
                    from scipy.sparse import issparse, hstack as sp_hstack, csr_matrix
                    preprocessor = pipeline[0]  # ColumnTransformer
                    raw_model = pipeline[1]      # XGBClassifier
                    
                    X_transformed = preprocessor.transform(X_row)
                    expected = raw_model.n_features_in_
                    
                    if issparse(X_transformed):
                        actual = X_transformed.shape[1]
                        if actual < expected:
                            padding = csr_matrix((X_transformed.shape[0], expected - actual))
                            X_transformed = sp_hstack([X_transformed, padding])
                    else:
                        actual = X_transformed.shape[1]
                        if actual < expected:
                            padding = np.zeros((X_transformed.shape[0], expected - actual))
                            X_transformed = np.hstack([X_transformed, padding])
                    
                    pred = raw_model.predict(X_transformed)[0]
                    proba = raw_model.predict_proba(X_transformed)[0]
                    conf = float(max(proba))
                    return encoder.inverse_transform([pred])[0], round(conf, 3)
                except Exception as inner_e:
                    logger.warning(f"ML {label_name} pad lỗi: {inner_e}")
                    return None, 0.0
            else:
                raise

    results = []
    for i in range(len(X)):
        row = X.iloc[[i]]
        result = {}

        try:
            label, conf = _safe_predict(prop_model, prop_encoder, row, "property_type")
            result["property_type_ml"] = label
            result["property_type_confidence"] = conf
        except Exception as e:
            logger.warning(f"ML predict property_type lỗi: {e}")
            result["property_type_ml"] = None
            result["property_type_confidence"] = 0.0

        try:
            label, conf = _safe_predict(list_model, list_encoder, row, "listing_type")
            result["listing_type_ml"] = label
            result["listing_type_confidence"] = conf
        except Exception as e:
            logger.warning(f"ML predict listing_type lỗi: {e}")
            result["listing_type_ml"] = None
            result["listing_type_confidence"] = 0.0

        results.append(result)

    return results

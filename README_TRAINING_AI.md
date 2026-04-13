# 🤖 README_TRAINING_AI.md - Hướng Dẫn Training AI Models

> Hướng dẫn chuyển từ **Multi-Category Big Data Processing** sang **Category-Specific AI Model Training**.

---

## 📊 Architecture Overview

### Phase 1: Production - Multi-Category Big Data Processing (Current)

```
┌──────────────────────────────────────────────────┐
│ CONFIG: pipelines/config/local.scale.yaml         │
│ categories: [1000, 1001, 1002]                    │
└──────────────────────────────────────────────────┘
                        ↓
        ┌───────────────┼───────────────┐
        ↓               ↓               ↓
    ┌─────────┐   ┌──────────┐   ┌────────────┐
    │ BĐS     │   │ Xe Cộ    │   │ Điện Thoại │
    │ (1000)  │   │ (1001)   │   │ (1002)     │
    └─────────┘   └──────────┘   └────────────┘
        ↓               ↓               ↓
┌──────────────────────────────────────────────────┐
│ Azurite Storage:                                  │
│ ├─ raw/ (Mixed từ 3 categories)                   │
│ ├─ silver/ (Deduplicated, mixed)                  │
│ └─ gold/ (Aggregated, mixed)                      │
└──────────────────────────────────────────────────┘
```

**Tóm tắt:** Lấy dữ liệu từ tất cả 3 categories cùng lúc, tối ưu cho Big Data processing (speed + volume).

---

### Phase 2: Training - Category-Specific Model Training (Fallback)

```
┌────────────────────────────────────────────────────┐
│ STEP 1: PROPERTY MODEL TRAINING                    │
├────────────────────────────────────────────────────┤
│ CONFIG: pipelines/config/local.property.yaml       │
│ categories: [1000] ← Comment local.scale.yaml      │
└────────────────────────────────────────────────────┘
                ↓
        ┌──────────────┐
        │ BĐS (1000)   │ × 1000 records
        └──────────────┘
                ↓
        Azurite: /training/property/property_*.json
        Local: data/training/property/
                ↓
    ┌─────────────────────────────────────┐
    │ Train Model: XGBoost/LightGBM        │
    │ - Feature: area, location, rooms    │
    │ - Target: price                     │
    │ - Output: property_price_model.pkl  │
    └─────────────────────────────────────┘

┌────────────────────────────────────────────────────┐
│ STEP 2: VEHICLE MODEL TRAINING                     │
├────────────────────────────────────────────────────┤
│ CONFIG: pipelines/config/local.vehicle.yaml        │
│ categories: [1001] ← Uncomment, run again          │
└────────────────────────────────────────────────────┘
                ↓
        ┌──────────────┐
        │ Xe (1001)    │ × 1000 records
        └──────────────┘
                ↓
        Azurite: /training/vehicle/vehicle_*.json
        Local: data/training/vehicle/
                ↓
    ┌─────────────────────────────────────┐
    │ Train Model: XGBoost/LightGBM        │
    │ - Feature: brand, km, year, engine  │
    │ - Target: price                     │
    │ - Output: vehicle_price_model.pkl   │
    └─────────────────────────────────────┘

┌────────────────────────────────────────────────────┐
│ STEP 3: ELECTRONICS MODEL TRAINING                 │
├────────────────────────────────────────────────────┤
│ CONFIG: pipelines/config/local.electronics.yaml    │
│ categories: [1002] ← Uncomment, run final time     │
└────────────────────────────────────────────────────┘
                ↓
        ┌──────────────┐
        │ Phone (1002) │ × 1000 records
        └──────────────┘
                ↓
        Azurite: /training/electronics/electronics_*.json
        Local: data/training/electronics/
                ↓
    ┌──────────────────────────────────┐
    │ Train Model: XGBoost/LightGBM     │
    │ - Feature: brand, storage, cpu    │
    │ - Target: price                   │
    │ - Output: electronics_price_model │
    └──────────────────────────────────┘
```

---

## 🚀 Step-by-Step Guide

### **Before Training (Prerequisite)**

Đảm bảo bạn đã hoàn thành Phase 05:
```bash
# 1. Bash script để kiểm tra config hiện tại
echo $APP_PROFILE  # Should show: local.scale (multi-category mode)

# 2. Kiểm tra container Azurite chạy chưa
docker ps | grep azurite

# 3. Kiểm tra raw/ folder đã có dữ liệu chưa
# Nếu chưa, run: python -m dagster dev --config pipelines/config/local.scale.yaml
```

---

### **Training for Property (BĐS)**

#### Step 1: Switch Config
```bash
# File: .env hoặc terminal
export APP_PROFILE=local.property
# Hoặc hardcode trong pipelines/jobs.py:
# load_config_from_file("pipelines/config/local.property.yaml")
```

#### Step 2: Run Pipeline (Export Training Data)
```bash
python -m dagster dev --config pipelines/config/local.property.yaml

# Chờ 1-2 phút cho pipeline hoàn thành
# Output sẽ lưu vào:
# - Azurite: /training/property/property_20260413_*.json
# - Local: data/training/property/property_20260413_*.json
```

#### Step 3: Train Model
```bash
# Uncomment export logic ở ingestion_ops.py
python scripts/train_property_model.py

# Model sẽ được save vào:
# - models/property/property_price_model.pkl
# - models/property/property_model_metrics.json
```

#### Step 4: Validate Model
```bash
python scripts/evaluate_property_model.py

# Output:
# R² Score: 0.85
# MAE: ₫50,000,000
# RMSE: ₫75,000,000
```

---

### **Training for Vehicle (Xe Cộ)**

#### Step 1: Switch Config
```bash
# Switch to vehicle config
export APP_PROFILE=local.vehicle
```

#### Step 2: Run Pipeline (Export Training Data)
```bash
python -m dagster dev --config pipelines/config/local.vehicle.yaml
# Chờ hoàn thành
```

#### Step 3: Train Model
```bash
python scripts/train_vehicle_model.py

# Model sẽ được save vào:
# - models/vehicle/vehicle_price_model.pkl
# - models/vehicle/vehicle_model_metrics.json
```

---

### **Training for Electronics (Điện Thoại)**

#### Step 1: Switch Config
```bash
export APP_PROFILE=local.electronics
```

#### Step 2: Run Pipeline (Export Training Data)
```bash
python -m dagster dev --config pipelines/config/local.electronics.yaml
# Chờ hoàn thành
```

#### Step 3: Train Model
```bash
python scripts/train_electronics_model.py

# Model sẽ được save vào:
# - models/electronics/electronics_price_model.pkl
# - models/electronics/electronics_model_metrics.json
```

---

## 📁 File Structure After Training

```
Real Estate Data Platform/
├── data/
│   ├── raw/                          # Production raw data (mixed categories)
│   ├── training/
│   │   ├── property/
│   │   │   ├── README.md
│   │   │   ├── property_20260413_001.json  (1000 records)
│   │   │   └── property_20260413_002.json
│   │   ├── vehicle/
│   │   │   ├── README.md
│   │   │   ├── vehicle_20260413_001.json   (1000 records)
│   │   │   └── vehicle_20260413_002.json
│   │   └── electronics/
│   │       ├── README.md
│   │       ├── electronics_20260413_001.json (1000 records)
│   │       └── electronics_20260413_002.json
│   └── models/                       # ← Trained ML models (TODO)
│       ├── property/
│       │   ├── property_price_model.pkl
│       │   └── property_model_metrics.json
│       ├── vehicle/
│       │   ├── vehicle_price_model.pkl
│       │   └── vehicle_model_metrics.json
│       └── electronics/
│           ├── electronics_price_model.pkl
│           └── electronics_model_metrics.json
│
├── pipelines/config/
│   ├── base.yaml              # Default
│   ├── local.scale.yaml       # Multi-category (Phase 05 - Current)
│   ├── local.property.yaml    # Single: BĐS only
│   ├── local.vehicle.yaml     # Single: Xe only
│   └── local.electronics.yaml # Single: Điện Thoại only
│
└── README_TRAINING_AI.md      # ← You are here
```

---

## 🔄 Config Working Flow

### Development (Current)
```bash
# local.scale.yaml (Multi-Category Big Data)
export APP_PROFILE=local.scale
python -m dagster dev

# Output: Raw data từ 3 categories
# → Azurite: raw/ (mixed)
# → Pipeline: ingestion + processing + save gold/
```

### Training Phase (After Development)
```bash
# Switch to training mode - chạy 3 lần
export APP_PROFILE=local.property && python -m dagster dev  # Run 1
export APP_PROFILE=local.vehicle && python -m dagster dev   # Run 2
export APP_PROFILE=local.electronics && python -m dagster dev  # Run 3

# Output: Training data từ từng category
# → Azurite: training/property/, training/vehicle/, training/electronics/
# → Local: data/training/property/, data/training/vehicle/, data/training/electronics/
```

---

## ⚙️ Commented Export Logic (Fallback)

Location: `pipelines/ops/ingestion_ops.py`

```python
# TO ACTIVATE EXPORT:
# 1. Uncomment function: op_export_training_data()
# 2. Add to job in jobs.py:
#    export_task = op_export_training_data(normalized_data)
# 3. Run with single-category config
# 4. Data exported to: /training/{category}/{category}_*.json

# LOGIC:
# - Detect category from settings.ingestion.categories
# - If len(categories) > 1: SKIP (production mode)
# - If len(categories) == 1: EXPORT to /training/{category_prefix}/
```

---

## 🎯 Next Steps (Roadmap)

### Phase 07: AI Model Development
- [ ] Create `scripts/train_property_model.py`
- [ ] Create `scripts/train_vehicle_model.py`
- [ ] Create `scripts/train_electronics_model.py`
- [ ] Save models to `data/models/`

### Phase 08: Model Serving
- [ ] Create REST API: `/predict/property`
- [ ] Create REST API: `/predict/vehicle`
- [ ] Create REST API: `/predict/electronics`
- [ ] Deploy to Docker/Kubernetes

### Phase 09: Monitoring & Retraining
- [ ] Monitor model performance
- [ ] Retrain models monthly/quarterly
- [ ] Version control models (MLflow)

---

## 📚 Reference

- **Config Files**: `pipelines/config/local.*.yaml`
- **Export Logic**: `pipelines/ops/ingestion_ops.py` (commented)
- **Training Data**: `data/training/{property,vehicle,electronics}/`
- **Azurite Structure**: 
  - Production: `raw/`, `silver/`, `gold/`
  - Training: `training/property/`, `training/vehicle/`, `training/electronics/`

---

## 🔗 Related Documentation

- [Multi-Category Big Data Pipeline](../../docs/pre_build_assessment.md)
- [Configuration Guide](../../pipelines/config/README.md)
- [Dagster Ops Documentation](../../pipelines/ops/)

---

**Last Updated:** April 13, 2026
**Environments:** local.scale (multi) + local.property/vehicle/electronics (training)
**Status:** ✅ Fallback infrastructure ready

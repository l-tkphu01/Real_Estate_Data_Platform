# Xe Cộ Training Data

Thư mục này chứa dữ liệu huấn luyện cho **AI model dự báo giá xe cộ**.

## Cước Chạy

```bash
# 1. Chuyển sang single-category mode
export APP_PROFILE=local.vehicle

# 2. Chạy pipeline Dagster
python -m dagster dev

# 3. Dữ liệu sẽ được lưu vào:
# - Azurite: /training/vehicle/vehicle_*.json (nếu export logic uncommented)
# - Local: data/training/vehicle/vehicle_*.json
```

## Dữ Liệu

- **Records**: ~1000 (50 trang × 20 records/trang)
- **Category**: 1001 (Xe Cộ)
- **Features**: Hãng xe, mô hình, km chạy, năm sản xuất, giá, v.v.

## Training Model

Xem [README_TRAINING_AI.md](../../README_TRAINING_AI.md) để tìm hiểu cách training model từ dữ liệu này.

## Files

- `vehicle_*.json` - Raw training data exported từ pipeline

# Bất Động Sản Training Data

Thư mục này chứa dữ liệu huấn luyện cho **AI model dự báo giá nhà đất**.

## Bước Chạy

```bash
# 1. Chuyển sang single-category mode
export APP_PROFILE=local.property

# 2. Chạy pipeline Dagster
python -m dagster dev

# 3. Dữ liệu sẽ được lưu vào:
# - Azurite: /training/property/property_*.json (nếu export logic uncommented)
# - Local: data/training/property/property_*.json
```

## Dữ Liệu

- **Records**: ~1000 (50 trang × 20 records/trang)
- **Category**: 1000 (Bất Động Sán)
- **Features**: Diện tích, vị trí, số phòng, giá, v.v.

## Training Model

Xem [README_TRAINING_AI.md](../../README_TRAINING_AI.md) để tìm hiểu cách training model từ dữ liệu này.

## Files

- `property_*.json` - Raw training data exported từ pipeline

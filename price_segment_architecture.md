# Kiến trúc Quản lý Phân khúc Giá BĐS (Enterprise-Grade)
## Real Estate Data Platform — Price Segment Management

> **Mục tiêu:** Mô tả chi tiết phương án kết hợp 3 kỹ thuật (Hybrid Algorithm + SCD Type 2 + PostgreSQL) để quản lý ngưỡng phân khúc giá một cách chính xác, tự động hóa tối đa, và không làm mất lịch sử dữ liệu.

---

## 1. Bối cảnh & Vấn đề

### Vấn đề V1 (Hiện tại — Hardcode trong Python)
```python
# dim_builders.py — PHÈN: Sếp muốn đổi ngưỡng → Phải mở code Python sửa tay!
price_segment_expr = F.when(F.col("price") < 4_000_000_000, "affordable") \
                      .when(F.col("price") < 8_000_000_000, "mid")
```

**3 điểm yếu chết người:**
1. **Ngưỡng cứng không bám thị trường:** Lạm phát 2026 nhưng vẫn dùng ngưỡng 2020.
2. **Mất lịch sử khi sửa (SCD Type 1 hành vi):** Sửa code → toàn bộ báo cáo năm cũ bị sai.
3. **Rủi ro kỹ thuật:** Developer sửa nhầm dòng → Pipeline nổ tung.

---

## 2. Giải pháp: Kết hợp 3 kỹ thuật

```
FULL ENTERPRISE COMBO
──────────────────────────────────────────────────────
Hybrid Algorithm  →  SCD Type 2  →  PostgreSQL Config
(Tính ngưỡng)       (Lưu lịch sử)  (Quản lý Business Rules)
```

---

## 3. Chi tiết từng kỹ thuật

### Kỹ thuật 1 — Hybrid Algorithm (Tính ngưỡng thông minh)

Hệ thống tự **tính toán ngưỡng gợi ý** từ dữ liệu thực tế bằng Percentile, rồi trình con người phê duyệt (không tự động cập nhật).

```python
# analytics/price_threshold_advisor.py
def compute_suggested_thresholds(silver_df, property_type_filter=None):
    if property_type_filter:
        df = silver_df.filter(F.col("property_type_group") == property_type_filter)
    else:
        df = silver_df

    quantiles = df.approxQuantile("price", [0.25, 0.50, 0.75, 0.95], 0.05)
    p25, p50, p75, p95 = quantiles

    return {
        "affordable_ceiling": round(p25 / 1e9, 1) * 1e9,
        "mid_ceiling":        round(p75 / 1e9, 1) * 1e9,
        "upper_mid_ceiling":  round(p95 / 1e9, 1) * 1e9,
        "computed_at":        datetime.now().isoformat(),
        "data_points":        df.count(),
    }
```

**Báo cáo gợi ý gửi Business Team qua Email/Slack mỗi quý:**
```
BÁO CÁO NGƯỠNG GIÁ THÁNG 01/2026
Dữ liệu: 45.230 tin đăng (Căn hộ chung cư)

Ngưỡng HIỆN TẠI:  Bình dân < 4 tỷ
Ngưỡng GỢI Ý:     Bình dân < 5.2 tỷ (p25 = 5.18 tỷ)
Chênh lệch:       +1.2 tỷ (+30%) → Thị trường đang lạm phát!

→ Vui lòng xem xét và phê duyệt:
  [APPROVE] http://internal-dashboard/price-rules/review
  [REJECT]  http://internal-dashboard/price-rules/review
```

---

### Kỹ thuật 2 — SCD Type 2 (Bảo toàn lịch sử tuyệt đối)

Bảng `dim_price_segment` được thêm 3 cột quản lý vòng đời:

```sql
CREATE TABLE biz_rules.price_segments (
    id           SERIAL PRIMARY KEY,
    segment_name VARCHAR(30)   NOT NULL,
    floor_vnd    DECIMAL(15,2) NOT NULL,
    ceiling_vnd  DECIMAL(15,2) DEFAULT NULL,
    label_vi     VARCHAR(50)   NOT NULL,
    valid_from   DATE          NOT NULL,
    valid_to     DATE          DEFAULT NULL,  -- NULL = Đang hiệu lực
    is_current   BOOLEAN       NOT NULL DEFAULT TRUE,
    approved_by  VARCHAR(100),               -- Người phê duyệt
    approved_at  TIMESTAMP                   -- Thời điểm phê duyệt
);
```

**Trạng thái bảng SAU KHI Business Team bấm APPROVE ngưỡng mới (01/2026):**

| id | segment_name | ceiling | valid_from | valid_to | is_current |
|----|---|---|---|---|---|
| 1 | affordable | 4 tỷ | 2020-01-01 | 2025-12-31 | FALSE ❌ |
| 2 | mid | 8 tỷ | 2020-01-01 | 2025-12-31 | FALSE ❌ |
| 3 | upper_mid | 12 tỷ | 2020-01-01 | 2025-12-31 | FALSE ❌ |
| 4 | luxury | NULL | 2020-01-01 | 2025-12-31 | FALSE ❌ |
| **5** | **affordable** | **6 tỷ** | **2026-01-01** | **NULL** | **TRUE ✅** |
| **6** | **mid** | **10 tỷ** | **2026-01-01** | **NULL** | **TRUE ✅** |
| **7** | **upper_mid** | **15 tỷ** | **2026-01-01** | **NULL** | **TRUE ✅** |
| **8** | **luxury** | **NULL** | **2026-01-01** | **NULL** | **TRUE ✅** |

> **Nguyên tắc vàng SCD Type 2:** KHÔNG BAO GIỜ DELETE hay UPDATE con số cũ. Chỉ INSERT dòng mới!

---

### Kỹ thuật 3 — PostgreSQL Config Store (Xóa bỏ hardcode)

PySpark đọc luật trực tiếp từ PostgreSQL, không còn bất kỳ con số nào trong code Python:

```python
# src/warehouse/dim_builders.py — KHÔNG CÒN HARDCODE!
def build_dim_price_segment(spark: SparkSession, pg_jdbc_url: str) -> DataFrame:
    return (
        spark.read
        .format("jdbc")
        .option("url", pg_jdbc_url)
        .option("dbtable", "biz_rules.price_segments")
        .option("driver", "org.postgresql.Driver")
        .load()
        .filter(F.col("is_current") == True)
    )
```

**Logic JOIN trong Fact Builder (đọc đúng luật theo ngày đăng tin):**
```python
# src/warehouse/fact_builders.py
fact_df = silver_df.join(
    dim_price_segment,
    on=(
        # Điều kiện 1: Giá nhà lọt vào khe giữa Sàn và Trần
        (F.col("price") >= F.col("floor_vnd")) &
        (F.col("price") < F.col("ceiling_vnd") | F.col("ceiling_vnd").isNull()) &
        # Điều kiện 2: Ngày đăng tin nằm trong Hạn hiệu lực của luật
        (F.col("posted_date") >= F.col("valid_from")) &
        (F.col("valid_to").isNull() | (F.col("posted_date") <= F.col("valid_to")))
    ),
    how="left"
)
```

---

## 4. Luồng vận hành đầy đủ (End-to-End Flow)

```
[HÀNG QUÝ — Tự động bởi Dagster Sensor]
   Pipeline phân tích Percentile từ Silver Layer
             │
             ▼
   Gửi báo cáo gợi ý → Email/Slack cho Business Team
             │
             ▼
[MỖI LẦN THỊ TRƯỜNG BIẾN ĐỘNG — Con người]
   Business Team đọc báo cáo
             │
      ┌──────┴──────┐
   APPROVE       REJECT
      │              │
      ▼              ▼
   Hệ thống tự động:     Giữ nguyên
   1. UPDATE is_current = FALSE cho dòng cũ
   2. INSERT dòng mới với is_current = TRUE
   3. Ghi log: approved_by, approved_at
      │
      ▼
[HÀNG NGÀY — Tự động bởi Dagster]
   Bot cào dữ liệu BĐS từ Chợ Tốt
             │
             ▼
   PySpark đọc luật hiện hành từ PostgreSQL
   (Tự động lấy dòng is_current = TRUE)
             │
             ▼
   JOIN tin đăng với luật theo (giá + ngày đăng)
             │
             ▼
   Ghi fact_listing vào Delta Lake
             │
             ▼
   Superset hiển thị Dashboard cập nhật
```

---

## 5. So sánh 3 phiên bản kiến trúc

| Tiêu chí | V1 — Đồ án (Hiện tại) | V2 — Startup | V3 — Enterprise (Full Combo) |
|---|---|---|---|
| Khi đổi ngưỡng giá | Sửa code Python, commit, deploy | UPDATE SQL trong pgAdmin | Bấm APPROVE trên web |
| Ngưỡng bám thị trường? | ❌ Cứng mãi | ❌ Nhớ thì cập nhật | ✅ Hệ thống tự gợi ý hàng quý |
| Lịch sử được bảo vệ? | ❌ Mất khi sửa code | ✅ SCD Type 2 | ✅ SCD Type 2 |
| Cần mở code Python? | ✅ Bắt buộc | ❌ Không | ❌ Không |
| Rủi ro sửa nhầm code | 🔴 Cao | 🟡 Thấp | 🟢 Không có |
| Audit Trail (ai sửa gì lúc mấy giờ?) | ❌ Không | ⚠️ Git log | ✅ `approved_by`, `approved_at` |

---

## 6. Roadmap triển khai

### Phase 1 — MVP (Đồ án) ✅ HOÀN THÀNH
- [x] Hardcode ngưỡng 4-8-12 tỷ trong `dim_builders.py`
- [x] Cấu trúc SCD Type 2 trong DDL (`valid_from`, `valid_to`, `is_current`)
- [x] Tài liệu hóa Business Rules và nguồn trích dẫn

### Phase 2 — PostgreSQL Config Store
- [ ] Tạo schema `biz_rules` trong PostgreSQL (Docker container)
- [ ] Di chuyển seed data từ Python sang PostgreSQL
- [ ] Cập nhật `dim_builders.py` đọc từ JDBC

### Phase 3 — Hybrid Algorithm + Semi-Automation
- [ ] Viết module `analytics/price_threshold_advisor.py`
- [ ] Tích hợp báo cáo hàng quý qua Dagster Sensor + Email Alert
- [ ] Xây dựng trang Approve/Reject đơn giản (Streamlit hoặc Metabase)

---

## 7. Nguồn tham khảo Business Rules

| Nguồn | Tổ chức | Năm | Ghi chú |
|---|---|---|---|
| Báo cáo thị trường BĐS | Batdongsan.com.vn | Q1/2024 | Bộ lọc giá < 4 tỷ = Vừa túi tiền |
| Khung phân hạng nhà ở | Bộ Xây Dựng / VARS | 2023 | Dưới 25-30 triệu/m² = Bình dân |
| Báo cáo thị trường | CBRE Vietnam | 2024 | Phân khúc cao cấp >50 triệu/m² |

> **Lưu ý:** Các ngưỡng tính theo **Tổng giá trị tài sản (Total Asset Value)**, phù hợp với đặc thù dữ liệu hỗn hợp (Nhà phố, Đất nền, Căn hộ — diện tích rất khác nhau). Khi phân tích chi tiết, luôn **Filter theo Loại hình BĐS trước** để tránh lệch phân phối.

"""Ingestion client với contract retry và error handling."""

import logging
import requests
from typing import Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.config import Settings

logger = logging.getLogger(__name__)

def fetch_raw_records(settings: Settings) -> list[dict[str, Any]]:
    """Lấy raw property records từ external source (API).

    Sử dụng Tenacity để tự động Retry HTTP requests.
    Nếu cấu hình URL là example.com, sẽ mock data để Pipeline có thể hoạt động end-to-end.
    """
    url = settings.ingestion.data_source_url
    timeout = settings.ingestion.request_timeout_seconds

    @retry(
        stop=stop_after_attempt(settings.ingestion.ingestion_max_retries),
        wait=wait_exponential(multiplier=settings.ingestion.ingestion_backoff_seconds, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True
    )
    def _fetch() -> list[dict[str, Any]]:
        logger.info(f"Đang tiến hành cào dữ liệu từ URL: {url}")
        
        # MOCK DATA TRONG GIAI ĐOẠN DEV (nếu chưa có API thật)
        if "example.com" in url:
            logger.warning(f"URL đang là {url}. Tiến hành khởi tạo dữ liệu giả lập (Mock Data).")
            return _generate_mock_data()

        # Thực thi HTTP Request thật sự
        response = requests.get(url, timeout=timeout)
        response.raise_for_status() 
        return response.json()

    try:
        data = _fetch()
        logger.info(f"Thành công lấy {len(data)} bản ghi bất động sản.")
        return data
    except Exception as e:
        logger.error(f"Lấy dữ liệu thất bại sau nhiều lần thử: {e}")
        return []

def _generate_mock_data() -> list[dict[str, Any]]:
    """Hàm phụ: Sinh dữ liệu mẫu bất động sản."""
    import uuid
    import random
    from datetime import datetime, timedelta
    
    cities = ["Hà Nội", "Hồ Chí Minh", "Đà Nẵng"]
    districts = {
        "Hà Nội": ["Cầu Giấy", "Nam Từ Liêm", "Tây Hồ", "Đống Đa"], 
        "Hồ Chí Minh": ["Quận 1", "Quận 2", "Bình Thạnh", "Gò Vấp"], 
        "Đà Nẵng": ["Hải Châu", "Sơn Trà", "Ngũ Hành Sơn"]
    }
                 
    data = []
    for _ in range(25): # Mô phỏng cào được 25 tin nhà đất
        city = random.choice(cities)
        data.append({
            "id": str(uuid.uuid4()),
            "title": f"Bán nhà nguyên căn đẹp tại {city}",
            "city": city,
            "district": random.choice(districts[city]),
            "price_vnd": random.randint(2000, 15000) * 1000000,  # Giá từ 2 tỷ -> 15 tỷ
            "area": random.randint(30, 150),                     # 30m2 -> 150m2
            "bedrooms": random.randint(1, 5),
            "created_at": (datetime.now() - timedelta(days=random.randint(0, 5))).isoformat()
        })
    return data


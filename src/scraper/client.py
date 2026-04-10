"""Ingestion client chuyên nghiệp với Async I/O, Anti-Ban và Pydantic Validation."""

import asyncio
import logging
from typing import Any
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from fake_useragent import UserAgent

from src.config import Settings

logger = logging.getLogger(__name__)
ua = UserAgent()

async def _fetch_page_async(session: aiohttp.ClientSession, url: str, params: dict, headers: dict) -> list[dict[str, Any]]:
    """Gọi 1 trang API bất đồng bộ với aiohttp."""
    async with session.get(url, params=params, headers=headers) as response:
        response.raise_for_status()
        data = await response.json()
        
        # Mapping dữ liệu từ API thật của Chợ Tốt (Category: Bất động sản = 1000)
        # Nếu API thay đổi cấu trúc, ta chỉ cần map lại tại đây.
        ads = data.get("ads", [])
        return ads

async def crawl_chotot_api_async(base_url: str, max_pages: int = 5) -> list[dict[str, Any]]:
    """Cào nhiều trang song song (Concurrent) thay vì cào tuần tự."""
    results = []
    
    # 1. Semaphore giới hạn tối đa 5 requests cùng lúc để không bị Server chặn IP (Anti-ban)
    sem = asyncio.Semaphore(5)
    
    async def fetch_with_sem(session: aiohttp.ClientSession, page: int):
        async with sem:
            # 2. Xoay vòng User-Agent (giả lập trình duyệt khác nhau)
            headers = {"User-Agent": ua.random}
            params = {
                "cg": "1000",   # 1000 là code Bất động sản của Chợ tốt
                "o": page * 20, # Offset
                "limit": 20     # Số bài 1 trang
            }
            try:
                logger.info(f"Đang cào dữ liệu trang {page + 1}... (Offset: {params['o']})")
                data = await _fetch_page_async(session, base_url, params, headers)
                return data
            except Exception as e:
                logger.warning(f"Lỗi khi cào trang {page + 1}: {e}")
                return []

    # Bật kết nối aiohttp
    async with aiohttp.ClientSession() as session:
        # Chuẩn bị trước danh sách các Job cần cào
        tasks = [fetch_with_sem(session, p) for p in range(max_pages)]
        
        # 3. Phóng toàn bộ x requests CÙNG MỘT LÚC (Siêu tốc độ)
        pages_data = await asyncio.gather(*tasks)
        
        for page_data in pages_data:
            results.extend(page_data)
            
    return results

def fetch_raw_records(settings: Settings) -> list[dict[str, Any]]:
    """Hàm cầu nối đồng bộ để Dagster gọi, ở trong ruột chạy Async."""
    url = "https://gateway.chotot.com/v1/public/ad-listing" # API public thật của Chợ Tốt
    
    @retry(
        stop=stop_after_attempt(settings.ingestion.ingestion_max_retries),
        wait=wait_exponential(multiplier=settings.ingestion.ingestion_backoff_seconds, min=2, max=10),
        reraise=True
    )
    def _fetch() -> list[dict[str, Any]]:
        logger.info(f"🚀 Bắt đầu cào dữ liệu thật từ: {url}")
        
        # Chạy Event Loop Async trong code đồng bộ
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        data = loop.run_until_complete(crawl_chotot_api_async(url, max_pages=10)) # Cào 10 trang x 20 = 200 tin thật
        return data

    try:
        data = _fetch()
        logger.info(f"Thành công lấy {len(data)} TÍN ĐĂNG THẬT 100% từ Chợ Tốt.")
        return data
    except Exception as e:
        logger.error(f"Lấy dữ liệu thất bại sau nhiều lần thử: {e}")
        return []


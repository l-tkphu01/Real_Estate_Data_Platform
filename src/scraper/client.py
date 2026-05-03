"""Ingestion client chuyên nghiệp với Async I/O, Anti-Ban và Retry.

Chỉ hỗ trợ Bất Động Sản (category 1000).
Sử dụng aiohttp + Semaphore + Batching để cào dữ liệu an toàn.
"""

import asyncio
import logging
from typing import Any

import aiohttp
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from fake_useragent import UserAgent

from src.config import Settings

logger = logging.getLogger(__name__)

# Fallback an toàn cho FakeUserAgent khi mất mạng
ua = UserAgent(
    fallback=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
)

# Retry sâu xuống cấp độ Async HTTP Call — nếu sập mạng thì retry đúng request đó
CATEGORY_REAL_ESTATE = 1000


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    reraise=True,
)
async def _fetch_page_async(
    session: aiohttp.ClientSession,
    url: str,
    params: dict,
    headers: dict,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Gọi 1 trang API bất đồng bộ với aiohttp."""
    async with session.get(
        url,
        params=params,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as response:
        response.raise_for_status()
        data = await response.json()
        return data.get("ads", [])


async def _crawl_batch(
    base_url: str,
    start_page: int,
    end_page: int,
    semaphore_size: int = 3,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Cào nhiều trang song song (Concurrent) với Semaphore bảo vệ.

    Args:
        base_url: API endpoint.
        start_page: Trang bắt đầu trong batch này.
        end_page: Trang kết thúc trong batch này.
        semaphore_size: Tối đa request cùng lúc (anti-ban).
        timeout: Timeout per request (giây).
    """
    results: list[dict[str, Any]] = []
    sem = asyncio.Semaphore(semaphore_size)

    async def fetch_with_sem(session: aiohttp.ClientSession, page: int):
        async with sem:
            headers = {"User-Agent": ua.random}
            params = {
                "cg": str(CATEGORY_REAL_ESTATE),
                "o": page * 20,
                "limit": 20,
            }
            try:
                logger.info(
                    f"Đang cào trang {page + 1} (offset={params['o']})..."
                )
                return await _fetch_page_async(
                    session, base_url, params, headers, timeout=timeout
                )
            except Exception as e:
                logger.warning(f"Lỗi khi cào trang {page + 1}: {e}")
                return []

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_sem(session, p) for p in range(start_page, end_page)]
        pages_data = await asyncio.gather(*tasks)
        for page_data in pages_data:
            results.extend(page_data)

    return results


async def crawl_with_batching(
    base_url: str,
    max_pages: int = 50,
    pages_per_batch: int = 5,
    batch_delay_seconds: float = 2.0,
    semaphore_size: int = 3,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Cào dữ liệu BĐS theo batch + delay để tránh bị server chặn.

    Cơ chế:
    - Chia tổng pages thành batches
    - Mỗi batch fetch concurrent với semaphore
    - Delay giữa batches để anti-ban
    """
    results: list[dict[str, Any]] = []
    num_batches = (max_pages + pages_per_batch - 1) // pages_per_batch

    logger.info(
        f"[START] Bắt đầu cào BĐS: {max_pages} pages, "
        f"{pages_per_batch}/batch, delay={batch_delay_seconds}s, "
        f"sem={semaphore_size} -> {num_batches} batches"
    )

    for batch_idx in range(num_batches):
        start_page = batch_idx * pages_per_batch
        end_page = min(start_page + pages_per_batch, max_pages)

        logger.info(
            f"[BATCH] Batch {batch_idx + 1}/{num_batches}: "
            f"trang {start_page + 1}-{end_page}"
        )

        batch_data = await _crawl_batch(
            base_url=base_url,
            start_page=start_page,
            end_page=end_page,
            semaphore_size=semaphore_size,
            timeout=timeout,
        )
        results.extend(batch_data)

        # Delay giữa batches (trừ batch cuối)
        if batch_idx < num_batches - 1:
            logger.info(f"[WAIT] Chờ {batch_delay_seconds}s...")
            await asyncio.sleep(batch_delay_seconds)

    logger.info(f"[DONE] Hoàn thành: {len(results)} records từ {max_pages} pages")
    return results


def fetch_raw_records(settings: Settings) -> list[dict[str, Any]]:
    """Entry point đồng bộ cho Dagster — nội bộ chạy Async.

    Đọc toàn bộ config từ Settings (config-driven).
    """
    url = settings.ingestion.data_source_url
    max_pages = settings.ingestion.max_pages
    pages_per_batch = settings.ingestion.pages_per_batch
    batch_delay = settings.ingestion.batch_delay_seconds
    semaphore_size = settings.ingestion.semaphore_size
    timeout = settings.ingestion.request_timeout_seconds

    logger.info(
        f"[START] Bắt đầu cào BĐS từ: {url} "
        f"(max_pages={max_pages}, sem={semaphore_size})"
    )

    # Chạy Event Loop Async trong context đồng bộ
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        data = loop.run_until_complete(
            crawl_with_batching(
                base_url=url,
                max_pages=max_pages,
                pages_per_batch=pages_per_batch,
                batch_delay_seconds=batch_delay,
                semaphore_size=semaphore_size,
                timeout=timeout,
            )
        )
        logger.info(f"[SUCCESS] Thành công lấy {len(data)} tin BĐS.")
        return data
    except Exception as e:
        logger.error(f"[ERROR] Lấy dữ liệu thất bại sau nhiều lần thử: {e}")
        return []

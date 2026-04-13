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

async def _fetch_page_async(session: aiohttp.ClientSession, url: str, params: dict, headers: dict, timeout: int = 30) -> list[dict[str, Any]]: # Đối tượng phiên làm việc HTTP của aiohttp, URL endpoint, tham số truy vấn và header (chứa User-Agent) để gọi API. (Quản lí kết nối, cookie, timeout, tái sử dụng socket hiệu quả hơn so với requests đồng bộ, Truyền vào để khỏi phải tạo lại mỗi lần gọi API)
    """Gọi 1 trang API bất đồng bộ với aiohttp."""
    async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as response: # Gọi xong nó sẽ đóng đúng cách
        response.raise_for_status() # Nếu API trả về lỗi HTTP (4xx, 5xx) sẽ ném ra exception để Tenacity bắt và retry lại
        data = await response.json() # Đọc nội dung phản hồi và parse JSON thành dict Python. await vì nó là I/O-bound operation, giúp không block event loop trong khi chờ đợi dữ liệu từ server.
        
        # Mapping dữ liệu từ API thật của Chợ Tốt (Category: Bất động sản = 1000)
        # Nếu API thay đổi cấu trúc, ta chỉ cần map lại tại đây.
        ads = data.get("ads", [])
        return ads

async def crawl_chotot_api_async(base_url: str, max_pages: int = 5, semaphore_size: int = 3, category_id: int = 1000, timeout: int = 30) -> list[dict[str, Any]]:
    """Cào nhiều trang song song (Concurrent) thay vì cào tuần tự.
    
    Default values:
    - max_pages=5: Cào 5 trang/batch (nhỏ, an toàn)
    - semaphore_size=3: Tối đa 3 request cùng lúc (an toàn, tránh ban IP)
    - category_id=1000: Default category là Bất động sản, có thể override cho Xe, Điện thoại, v.v.
    
    Nếu quên pass tham số, vẫn dùng default an toàn thay vì default cũ (sem=5 nguy hiểm)
    """
    results = []
    
    # 1. Semaphore giới hạn tối đa request cùng lúc để không bị Server chặn IP (Anti-ban)
    sem = asyncio.Semaphore(semaphore_size) # Giới hạn số lượng request đồng thời để tránh bị server chặn IP. Nếu có quá nhiều request cùng lúc, server có thể coi đó là hành vi đáng ngờ và chặn IP của bạn. Semaphore giúp kiểm soát số lượng request đang chạy cùng lúc, đảm bảo rằng chỉ có tối đa N request được thực hiện đồng thời. Khi một request hoàn thành, nó sẽ giải phóng semaphore, cho phép request tiếp theo bắt đầu. Điều này giúp giảm nguy cơ bị server chặn và vẫn duy trì hiệu suất cao khi cào dữ liệu từ API.
    
    async def fetch_with_sem(session: aiohttp.ClientSession, page: int):
        async with sem:
            # 2. Xoay vòng User-Agent (giả lập trình duyệt khác nhau)
            headers = {"User-Agent": ua.random}
            params = {
                "cg": str(category_id),  # Category ID động (1000=BĐS, 1001=Xe, 1002=Điện thoại, v.v.)
                "o": page * 20, # Offset tính theo số bài (20 bài/trang), page bắt đầu từ 0, page là mình tự định nghĩa
                "limit": 20     # Số bài 1 trang
            }
            try:
                logger.info(f"Đang cào dữ liệu trang {page + 1} (category={category_id})... (Offset: {params['o']})")
                data = await _fetch_page_async(session, base_url, params, headers, timeout=timeout)
                return data
            except Exception as e:
                logger.warning(f"Lỗi khi cào trang {page + 1} (category={category_id}): {e}")
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


async def crawl_multiple_categories(
    base_url: str,
    categories: list[int],
    pages_per_category: int = 5,
    pages_per_batch: int = 5,
    batch_delay_seconds: float = 2.0,
    semaphore_size: int = 3,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Cào dữ liệu từ nhiều danh mục với batching + delay để tránh bị chặn.
    
    Cơ chế:
    - Lặp từng danh mục (category)
    - Trong mỗi danh mục, cào pages_per_category trang theo batch
    - Delay giữa các batch AND giữa các danh mục để an toàn
    - Aggregate kết quả từ tất cả danh mục
    
    VD: categories=[1000, 1001, 1002], pages_per_category=5, pages_per_batch=5, delay=2s
    → Batch 1 (cat 1000): 5 pages
    → Delay 2s
    → Batch 2 (cat 1001): 5 pages  
    → Delay 2s
    → Batch 3 (cat 1002): 5 pages
    → Total: 3 categories × 5 pages = 15 pages, ~10 giây tổng
    """
    results = []
    total_categories = len(categories)
    
    logger.info(
        f"🚀 Bắt đầu cào multi-category: {total_categories} danh mục, "
        f"{pages_per_category} pages/category, batching={pages_per_batch}, delay={batch_delay_seconds}s"
    )
    
    for cat_idx, category in enumerate(categories):
        logger.info(
            f"📂 Danh mục {cat_idx + 1}/{total_categories}: "
            f"cg={category}, {pages_per_category} pages"
        )
        
        # Cào dữ liệu từ category này theo batch
        category_data = await crawl_chotot_api_with_batching(
            base_url=base_url,
            max_pages=pages_per_category,
            pages_per_batch=pages_per_batch,
            batch_delay_seconds=batch_delay_seconds,
            semaphore_size=semaphore_size,
            category_id=category,  # Truyền category vào để override hardcoded "1000"
            timeout=timeout,
        )
        results.extend(category_data)
        
        # Delay giữa các danh mục (trừ danh mục cuối) để an toàn
        if cat_idx < total_categories - 1:
            logger.info(
                f"⏸️  Chờ {batch_delay_seconds}s trước danh mục tiếp theo..."
            )
            await asyncio.sleep(batch_delay_seconds)
    
    logger.info(
        f"✅ Hoàn thành: {len(results)} records từ {total_categories} danh mục "
        f"({len(results) // total_categories} records/danh mục trung bình)"
    )
    return results


async def crawl_chotot_api_with_batching(
    base_url: str,
    max_pages: int = 50,
    pages_per_batch: int = 5,
    batch_delay_seconds: float = 2.0,
    semaphore_size: int = 3,
    category_id: int = 1000,  # Default: Bất động sán, có thể override cho Xe, Điện thoại, v.v.
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Cào dữ liệu theo batch + delay để tránh bị server chặn khi scale up.
    
    Cơ chế:
    - Chia tổng số pages thành các batch (VD: 50 pages = 10 batch x 5 pages/batch)
    - Trong mỗi batch, fetch concurrent với semaphore giới hạn tối đa semaphore_size request
    - Giữa các batch, delay batch_delay_seconds để server và ban chặn IP
    - Mỗi batch có log riêng để monitoring progress
    - Hỗ trợ multi-category thông qua tham số category_id
    
    VD: max_pages=50, pages_per_batch=5, batch_delay=2s → 10 batch, ~20 giây tổng
    """
    results = []
    num_batches = (max_pages + pages_per_batch - 1) // pages_per_batch  # Làm tròn lên
    
    logger.info(
        f"🚀 Bắt đầu cào theo batch: {max_pages} pages, "
        f"{pages_per_batch} pages/batch, delay={batch_delay_seconds}s, "
        f"sem_size={semaphore_size}, category={category_id} → {num_batches} batches"
    )
    
    for batch_idx in range(num_batches):
        start_page = batch_idx * pages_per_batch
        end_page = min(start_page + pages_per_batch, max_pages)
        
        logger.info(
            f"📦 Batch {batch_idx + 1}/{num_batches}: "
            f"cào trang {start_page + 1}-{end_page} "
            f"({end_page - start_page} pages)"
        )
        
        # Fetch concurrent trong batch này
        batch_data = await crawl_chotot_api_async(
            base_url=base_url,
            max_pages=end_page - start_page,
            semaphore_size=semaphore_size,
            category_id=category_id,
            timeout=timeout,
        )
        results.extend(batch_data)
        
        # Delay giữa các batch (trừ batch cuối) để tránh bị chặn
        if batch_idx < num_batches - 1:
            logger.info(
                f"⏸️  Chờ {batch_delay_seconds}s trước batch tiếp theo..."
            )
            await asyncio.sleep(batch_delay_seconds)
    
    logger.info(f"✅ Hoàn thành: {len(results)} records từ {max_pages} pages")
    return results

def fetch_raw_records(settings: Settings) -> list[dict[str, Any]]:
    """Hàm cầu nối đồng bộ để Dagster gọi, ở trong ruột chạy Async.
    
    Hỗ trợ single-category (cách cũ) và multi-category (cách mới).
    API endpoint, batch config, retry logic được quản lý từ settings.
    """
    url = settings.ingestion.data_source_url  # Config-driven URL
    max_pages = settings.ingestion.max_pages
    pages_per_batch = settings.ingestion.pages_per_batch
    batch_delay = settings.ingestion.batch_delay_seconds
    semaphore_size = settings.ingestion.semaphore_size
    categories = settings.ingestion.categories
    pages_per_category = settings.ingestion.pages_per_category
    timeout = settings.ingestion.request_timeout_seconds
    
    @retry(
        stop=stop_after_attempt(settings.ingestion.ingestion_max_retries),
        wait=wait_exponential(multiplier=settings.ingestion.ingestion_backoff_seconds, min=2, max=10),
        reraise=True
    )
    def _fetch() -> list[dict[str, Any]]:
        logger.info(
            f"🚀 Bắt đầu cào dữ liệu từ: {url} "
            f"(categories={categories}, max_pages={max_pages}, sem_size={semaphore_size})"
        )
        
        # Chạy Event Loop Async trong code đồng bộ
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Nếu có nhiều categories, sử dụng multi-category fetcher
        if len(categories) > 1:
            logger.info(f"📂 Sử dụng multi-category fetcher cho {len(categories)} danh mục")
            data = loop.run_until_complete(
                crawl_multiple_categories(
                    base_url=url,
                    categories=categories,
                    pages_per_category=pages_per_category,
                    pages_per_batch=pages_per_batch,
                    batch_delay_seconds=batch_delay,
                    semaphore_size=semaphore_size,
                    timeout=timeout,
                )
            )
        else:
            # Nếu 1 category, dùng cách cũ để tương thích ngược
            logger.info(f"📂 Sử dụng single-category fetcher cho category={categories[0]}")
            data = loop.run_until_complete(
                crawl_chotot_api_with_batching(
                    base_url=url,
                    max_pages=max_pages,
                    pages_per_batch=pages_per_batch,
                    batch_delay_seconds=batch_delay,
                    semaphore_size=semaphore_size,
                    category_id=categories[0],
                    timeout=timeout,
                )
            )
        return data

    try:
        data = _fetch()
        logger.info(f"✅ Thành công lấy {len(data)} TIN từ cấu hình.")
        return data
    except Exception as e:
        logger.error(f"❌ Lấy dữ liệu thất bại sau nhiều lần thử: {e}")
        return []



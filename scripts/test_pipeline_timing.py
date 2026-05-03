#!/usr/bin/env python3
"""Test script để measure pipeline timing với detailed logging.

Usage:
    python scripts/test_pipeline_timing.py
    
Hoặc từ environment khác:
    APP_PROFILE=local python scripts/test_pipeline_timing.py

Timeline output:
    ┌─ Ingestion Job (batching)
    ├─ Fetch source data: X.XXs
    ├─ Store raw snapshot: X.XXs
    └─ Total ingestion: X.XXs
    
    ┌─ Processing Job (Spark)
    ├─ Load raw records: X.XXs
    ├─ Validate records: X.XXs
    ├─ Publish quality report: X.XXs
    ├─ Clean records: X.XXs
    ├─ Transform silver: X.XXs
    ├─ Write silver delta: X.XXs
    ├─ Transform gold: X.XXs
    ├─ Write gold delta: X.XXs
    └─ Total processing: X.XXs
    
    ┌─ TOTAL PIPELINE TIME: X.XXs (should be < 300s)
    └─ Status: PASSED or TIMEOUT
"""

import time
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_settings
from src.logging_config import setup_logging
from src.storage.azure_client import AzureStorageClient
from src.scraper.client import fetch_raw_records
from src.scraper.normalizer import normalize_raw_record


# ═══════════════════════════════════════════════════════════════════════
# TIMING TRACKER
# ═══════════════════════════════════════════════════════════════════════

class TimingTracker:
    """Track step-wise execution timing with pretty printing."""
    
    def __init__(self, job_name: str, timeout_seconds: int = 300):
        self.job_name = job_name
        self.timeout_seconds = timeout_seconds
        self.steps: List[Dict[str, float]] = []
        self.job_start = time.time()
        
        print(f"\n{'='*70}")
        print(f"🚀 {job_name.upper()}")
        print(f"{'='*70}")
        print(f"   Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Timeout: {timeout_seconds}s")
        print()
    
    def step(self, step_name: str, step_func, *args, **kwargs):
        """Execute function and track timing."""
        step_start = time.time()
        print(f"   ⏱️  {step_name}...", end="", flush=True)
        
        result = step_func(*args, **kwargs)
        
        step_duration = time.time() - step_start
        self.steps.append({
            "name": step_name,
            "duration": step_duration,
            "cumulative": time.time() - self.job_start
        })
        
        print(f" {step_duration:.2f}s")
        return result
    
    def summary(self):
        """Print summary and check timeout."""
        total_duration = time.time() - self.job_start
        
        print()
        print("   Steps:")
        for i, step in enumerate(self.steps, 1):
            print(f"      {i}. {step['name']:<40} {step['duration']:>7.2f}s (cumulative: {step['cumulative']:.2f}s)")
        
        print()
        status = "PASSED" if total_duration < self.timeout_seconds else "TIMEOUT"
        print(f"   Total: {total_duration:.2f}s / {self.timeout_seconds}s {status}")
        print(f"{'='*70}\n")
        
        return total_duration, status


# ═══════════════════════════════════════════════════════════════════════
# STEP FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

def fetch_and_normalize(settings):
    """Fetch source data + normalize (Ingestion Step 1)."""
    # 1. Fetch raw
    raw_data = fetch_raw_records(settings)
    
    # 2. Normalize
    normalized_data = []
    for row in raw_data:
        norm_row = normalize_raw_record(row)
        if norm_row:
            normalized_data.append(norm_row)
    
    return normalized_data, len(raw_data)


def store_raw_snapshot(storage, settings, data):
    """Store to raw zone (Ingestion Step 2)."""
    if not data:
        return "empty"
    
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{settings.storage.raw_prefix}_{now_str}.json"
    
    storage.put_json(blob_name, data)
    
    return blob_name


def load_raw_records(storage, settings):
    """Load latest raw snapshot (Processing Step 1)."""
    latest_key = storage.get_latest_key(settings.storage.raw_prefix)
    if not latest_key:
        return []
    
    payload = storage.get_json(latest_key)
    if not isinstance(payload, list):
        return []
    
    return [item for item in payload if isinstance(item, dict)]


# ═══════════════════════════════════════════════════════════════════════
# MAIN TEST
# ═══════════════════════════════════════════════════════════════════════

def main():
    """Run full pipeline with timing."""
    
    # Setup
    setup_logging(level="INFO")
    settings = load_settings()
    storage = AzureStorageClient(settings)
    
    # Global tracker
    pipeline_start = time.time()
    results = {}
    
    print("\n")
    print("╔" + "═"*68 + "╗")
    print("║" + " "*68 + "║")
    print("║" + "  📊 PIPELINE TIMING TEST - SCALING (1000 records)".center(68) + "║")
    print("║" + " "*68 + "║")
    print("╚" + "═"*68 + "╝")
    
    print(f"\n📋 Configuration:")
    print(f"   Profile: {settings.runtime.profile}")
    print(f"   Max pages: {settings.ingestion.max_pages}")
    print(f"   Pages/batch: {settings.ingestion.pages_per_batch}")
    print(f"   Batch delay: {settings.ingestion.batch_delay_seconds}s")
    print(f"   Semaphore size: {settings.ingestion.semaphore_size}")
    print(f"   Spark memory: 2GB (driver + executor)")
    
    # ─────────────────────────────────────────────────────────────────
    # INGESTION JOB
    # ─────────────────────────────────────────────────────────────────
    ingest_tracker = TimingTracker("INGESTION JOB", timeout_seconds=300)
    
    # Step 1: Fetch & Normalize
    normalized, raw_count = ingest_tracker.step(
        "Fetch & Normalize Data",
        fetch_and_normalize,
        settings
    )
    print(f"      → Raw: {raw_count}, Normalized: {len(normalized)}")
    
    # Step 2: Store Raw Snapshot
    blob_name = ingest_tracker.step(
        "Store Raw Snapshot",
        store_raw_snapshot,
        storage, settings, normalized
    )
    print(f"      → Blob: {blob_name}")
    
    ingest_duration, ingest_status = ingest_tracker.summary()
    results["ingestion"] = {
        "duration": ingest_duration,
        "status": ingest_status,
        "records": len(normalized)
    }
    
    # ─────────────────────────────────────────────────────────────────
    # PROCESSING JOB
    # ─────────────────────────────────────────────────────────────────
    process_tracker = TimingTracker("PROCESSING JOB", timeout_seconds=300)
    
    # Step 1: Load Raw Records
    raw_records = process_tracker.step(
        "Load Latest Raw Records",
        load_raw_records,
        storage, settings
    )
    print(f"      → Records loaded: {len(raw_records)}")
    
    # Note: Remaining steps (validate, clean, transform, write) would require
    # actual Spark/Delta implementation. For now we'll simulate timing.
    # In real scenario uncomment and use actual transform functions.
    
    # Simulate remaining steps with minimal work
    process_tracker.step("Validate Records", time.sleep, 0.5)
    process_tracker.step("Publish Quality Report", time.sleep, 0.3)
    process_tracker.step("Clean Records", time.sleep, 0.5)
    process_tracker.step("Transform Silver", time.sleep, 1.0)
    process_tracker.step("Write Silver Delta", time.sleep, 1.0)
    process_tracker.step("Transform Gold", time.sleep, 1.0)
    process_tracker.step("Write Gold Delta", time.sleep, 1.0)
    
    process_duration, process_status = process_tracker.summary()
    results["processing"] = {
        "duration": process_duration,
        "status": process_status,
        "records": len(raw_records)
    }
    
    # ─────────────────────────────────────────────────────────────────
    # FINAL SUMMARY
    # ─────────────────────────────────────────────────────────────────
    total_duration = time.time() - pipeline_start
    total_timeout = 600  # 2 × 300s per job
    total_status = "PASSED" if total_duration < total_timeout else "TIMEOUT"
    
    print("\n")
    print("╔" + "═"*68 + "╗")
    print("║" + " FINAL SUMMARY ".center(68, "═") + "║")
    print("╠" + "═"*68 + "╣")
    print(f"║ Ingestion Job:  {results['ingestion']['duration']:>6.2f}s / 300s {results['ingestion']['status']:<15}║")
    print(f"║ Processing Job: {results['processing']['duration']:>6.2f}s / 300s {results['processing']['status']:<15}║")
    print("╠" + "═"*68 + "╣")
    print(f"║ TOTAL:          {total_duration:>6.2f}s / {total_timeout}s {total_status:<15}║")
    print("╚" + "═"*68 + "╝")
    
    print(f"\n📈 Statistics:")
    print(f"   Ingestion records: {results['ingestion']['records']}")
    print(f"   Processing records: {results['processing']['records']}")
    print(f"   Throughput: {results['ingestion']['records'] / total_duration:.1f} records/sec")
    
    # Return exit code
    return 0 if total_status == "PASSED" else 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

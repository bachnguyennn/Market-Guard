import argparse
import json
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from azure.core import MatchConditions
from azure.core.exceptions import AzureError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from .dashboard.azure_loader import read_snapshot
from .dashboard_publisher import (
    ACCOUNT_URL,
    CONTAINER,
    MANIFEST_NAME,
    export_dashboard,
    publish_exports,
)


def select_recent_blobs(
    container, start: datetime, end: datetime, *,
    max_scan: int = 50_000, max_batches: int = 10_000,
    max_bytes: int = 128 * 1024 * 1024,
) -> list:
    """Select a bounded ingestion-time window without silently truncating it."""
    if min(max_scan, max_batches, max_bytes) <= 0 or start >= end:
        raise ValueError("Source limits must be positive and the time window ordered")
    selected = []
    total_bytes = 0
    for scanned, blob in enumerate(container.list_blobs(name_starts_with="coinbase/"), 1):
        if scanned > max_scan:
            raise ValueError("Bronze listing exceeds the scan limit; no snapshot was published")
        if not blob.name.endswith(".jsonl") or not start <= blob.last_modified <= end:
            continue
        if blob.size <= 0 or not blob.etag:
            raise ValueError("Bronze metadata contains an empty or unversioned batch")
        selected.append(blob)
        total_bytes += blob.size
        if len(selected) > max_batches or total_bytes > max_bytes:
            raise ValueError("Bronze selection exceeds download limits; no snapshot was published")
    if not selected:
        raise ValueError("No Bronze batches exist in the requested window; keeping the existing snapshot")
    return sorted(selected, key=lambda blob: blob.name)


def download_bronze(container, blobs: list, destination: Path) -> None:
    """Download immutable versions; failed or changed blobs abort the refresh."""
    destination.mkdir(parents=True, exist_ok=False)

    def download(item):
        index, blob = item
        path = destination / f"batch_{index:06d}.jsonl"
        temporary = path.with_suffix(".part")
        try:
            with temporary.open("wb") as output:
                container.get_blob_client(blob.name).download_blob(
                    offset=0, length=blob.size + 1, etag=blob.etag,
                    match_condition=MatchConditions.IfNotModified,
                    max_concurrency=1,
                ).readinto(output)
            if temporary.stat().st_size != blob.size:
                raise ValueError("A Bronze download did not match its listed byte count")
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(download, enumerate(blobs)))


def calculate_analytics(bronze: Path, output: Path) -> dict:
    from pyspark.sql import SparkSession

    from .run_pipeline import run_batch

    spark = (
        SparkSession.builder.appName("MarketGuard-AnalyticsRefresh")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    try:
        return run_batch(spark, bronze, output)
    finally:
        spark.stop()


def refresh_analytics(
    service, work_root: Path, *, lookback_hours: int = 6, publish: bool = False,
    now: datetime | None = None, max_scan: int = 50_000,
    max_batches: int = 10_000, max_bytes: int = 128 * 1024 * 1024,
) -> dict:
    """Rebuild recent analytics; Azure writes require an explicit publish opt-in."""
    if not 1 <= lookback_hours <= 24:
        raise ValueError("Lookback must be between 1 and 24 hours")
    end = now or datetime.now(UTC)
    start = end - timedelta(hours=lookback_hours)
    bronze_container = service.get_container_client("bronze")
    blobs = select_recent_blobs(
        bronze_container, start, end,
        max_scan=max_scan, max_batches=max_batches, max_bytes=max_bytes,
    )
    work_root.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix="run_", dir=work_root))
    download_bronze(bronze_container, blobs, run_dir / "bronze")
    pipeline = calculate_analytics(run_dir / "bronze", run_dir / "analytics")
    files, snapshot = export_dashboard(run_dir / "analytics", run_dir / "export")
    dashboard_container = service.get_container_client(CONTAINER)
    if dashboard_container.get_blob_client(MANIFEST_NAME).exists():
        current_gold, _, _ = read_snapshot(dashboard_container)
        if pd.Timestamp(snapshot["latest_market_minute"]) < current_gold["minute_start"].max():
            raise ValueError("New analytics are older than the published snapshot; refusing replacement")
    if publish:
        publish_exports(service, files)
        saved_gold, saved_anomalies, _ = read_snapshot(dashboard_container)
        if (
            len(saved_gold) != snapshot["gold_rows"]
            or len(saved_anomalies) != snapshot["anomaly_rows"]
            or int(saved_gold["trade_count"].sum()) != pipeline["silver_count"]
            or int(saved_anomalies["is_anomaly"].sum()) != snapshot["alert_count"]
        ):
            raise RuntimeError("Published dashboard counts failed verification")
    return {
        "completed_at": datetime.now(UTC).isoformat(),
        "published": publish,
        "source_window_start": start.isoformat(),
        "source_window_end": end.isoformat(),
        "source_batches": len(blobs),
        "source_bytes": sum(blob.size for blob in blobs),
        **{key: value for key, value in pipeline.items() if key != "outputs"},
        **snapshot,
    }


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Refresh recent Azure Bronze analytics; defaults to a read-only dry run.")
    parser.add_argument("--lookback-hours", type=int, choices=range(1, 25), default=6)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--work-dir", type=Path, default=root / "data/analytics_refresh")
    parser.add_argument("--report", type=Path, default=root / "data/analytics-refresh-summary.json")
    args = parser.parse_args()
    try:
        with DefaultAzureCredential() as credential, BlobServiceClient(
            account_url=ACCOUNT_URL, credential=credential,
            connection_timeout=10, read_timeout=30, retry_total=2,
        ) as service:
            summary = refresh_analytics(
                service, args.work_dir, lookback_hours=args.lookback_hours, publish=args.publish,
            )
    except AzureError as error:
        parser.exit(1, f"Azure refresh failed ({type(error).__name__}). Check OIDC/Azure sign-in and container data roles.\n")
    except ValueError as error:
        parser.exit(1, f"Analytics validation failed: {error}\n")
    except RuntimeError as error:
        parser.exit(1, f"Analytics refresh failed ({type(error).__name__}); no successful refresh report was written.\n")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

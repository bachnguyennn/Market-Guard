import argparse
from datetime import UTC, datetime
from hashlib import sha256
import json
from pathlib import Path
from urllib.parse import urlsplit

from azure.core.exceptions import AzureError, ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings

from .dashboard.data_loader import load_anomaly_data, load_gold_data

ACCOUNT_URL = "https://stmarketguard3b4d59fa.blob.core.windows.net"
CONTAINER = "dashboard"
BLOB_NAMES = ("latest/gold.parquet", "latest/anomalies.parquet")
MANIFEST_NAME = "latest/snapshot.json"
GOLD_FIELDS = ["product_id", "minute_start", "trade_count", "base_volume", "notional_volume", "vwap"]
ANOMALY_FIELDS = [
    *GOLD_FIELDS, "missing_minutes", "is_data_gap", "segment_id", "volume_zscore",
    "price_return", "return_zscore", "is_volume_spike", "is_statistical_price_jump",
    "is_anomaly", "anomaly_type",
]


def export_dashboard(input_dir: Path, export_dir: Path) -> tuple[dict[str, Path], dict]:
    """Validate both analytics tables before exporting compact single-file snapshots."""
    input_dir = input_dir.resolve()
    export_dir = export_dir.resolve()
    if input_dir.is_relative_to(export_dir) or export_dir.is_relative_to(input_dir):
        raise ValueError("Analytics input and export directories must not overlap")
    gold = load_gold_data(str(input_dir / "gold"))
    anomalies = load_anomaly_data(str(input_dir / "anomalies"))
    if gold.empty:
        raise ValueError("Gold is empty; refusing to publish an empty dashboard")
    if not set(GOLD_FIELDS).issubset(gold.columns):
        raise ValueError("Gold must contain all six market metric columns")
    keys = ["product_id", "minute_start"]
    if gold[keys].isna().any().any() or anomalies[keys].isna().any().any():
        raise ValueError("Dashboard product/minute keys must not contain null values")
    if gold.duplicated(keys).any() or anomalies.duplicated(keys).any():
        raise ValueError("Dashboard product/minute keys must be unique")
    if not gold[keys].equals(anomalies[keys]):
        raise ValueError("Gold and anomaly snapshots must have the same product/minute keys")
    snapshots = {
        BLOB_NAMES[0]: gold[GOLD_FIELDS],
        BLOB_NAMES[1]: anomalies[[field for field in ANOMALY_FIELDS if field in anomalies.columns]],
    }
    payloads = {name: frame.to_parquet(index=False, engine="pyarrow") for name, frame in snapshots.items()}
    export_dir.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, payload in payloads.items():
        path = export_dir / Path(name).name
        temporary = path.with_suffix(".parquet.tmp")
        temporary.write_bytes(payload)
        temporary.replace(path)
        files[name] = path
    return files, {
        "gold_rows": len(gold), "anomaly_rows": len(anomalies),
        "alert_count": int(anomalies["is_anomaly"].sum()),
        "latest_market_minute": gold["minute_start"].max().isoformat(),
    }


def publish_exports(service: BlobServiceClient, files: dict[str, Path]) -> list[dict]:
    """Publish and verify both blobs, then commit their integrity manifest last."""
    if set(files) != set(BLOB_NAMES):
        raise ValueError("Only the two expected dashboard snapshot blobs may be published")
    payloads = {name: files[name].read_bytes() for name in BLOB_NAMES}
    container = service.get_container_client(CONTAINER)
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    if container.get_container_properties()["public_access"] is not None:
        raise ValueError("The dashboard container must be private; refusing to upload")
    published = []
    for name, payload in payloads.items():
        blob = container.get_blob_client(name)
        blob.upload_blob(
            payload, overwrite=True, validate_content=True,
            content_settings=ContentSettings(content_type="application/vnd.apache.parquet"),
        )
        if blob.download_blob().readall() != payload:
            raise RuntimeError(f"Uploaded snapshot failed read-back verification: {name}")
        published.append({"blob": name, "bytes": len(payload), "sha256": sha256(payload).hexdigest()})
    manifest = json.dumps({
        "version": 1, "published_at": datetime.now(UTC).isoformat(), "blobs": published,
    }, sort_keys=True).encode("utf-8")
    manifest_blob = container.get_blob_client(MANIFEST_NAME)
    manifest_blob.upload_blob(
        manifest, overwrite=True, validate_content=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    if manifest_blob.download_blob().readall() != manifest:
        raise RuntimeError("Uploaded snapshot manifest failed read-back verification")
    return published


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Export analytics and publish private Azure dashboard snapshots.")
    parser.add_argument("--input-dir", type=Path, default=project_root / "data/batch_output")
    parser.add_argument("--export-dir", type=Path, default=project_root / "data/dashboard_export")
    parser.add_argument("--account-url", default=ACCOUNT_URL)
    parser.add_argument("--export-only", action="store_true")
    args = parser.parse_args()
    address = urlsplit(args.account_url)
    if (
        address.scheme != "https"
        or not address.hostname
        or not address.hostname.endswith(".blob.core.windows.net")
        or address.path not in ("", "/")
        or address.query or address.fragment or address.username or address.password
    ):
        parser.error("--account-url must be an HTTPS account endpoint without credentials or a SAS query")
    files, summary = export_dashboard(args.input_dir, args.export_dir)
    if args.export_only:
        print(json.dumps({**summary, "published": False}, indent=2))
        return
    try:
        with DefaultAzureCredential() as credential, BlobServiceClient(
            account_url=args.account_url, credential=credential,
            connection_timeout=10, read_timeout=30, retry_total=2,
        ) as service:
            published = publish_exports(service, files)
    except AzureError as error:
        parser.exit(1, f"Azure publishing failed: {type(error).__name__} (HTTP {getattr(error, 'status_code', 'n/a')}). "
                    "Check Azure sign-in and Storage Blob Data Contributor access. "
                    "The two snapshot replacements are not an atomic transaction.\n")
    print(json.dumps({**summary, "published": True, "container": CONTAINER, "manifest": MANIFEST_NAME, "blobs": published}, indent=2))


if __name__ == "__main__":
    main()

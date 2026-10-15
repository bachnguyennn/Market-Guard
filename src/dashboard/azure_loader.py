import json
import re
from collections.abc import Mapping
from hashlib import sha256
from io import BytesIO
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import streamlit as st

from .data_loader import validate_anomaly_data, validate_gold_data

MANIFEST_NAME = "latest/snapshot.json"
BLOB_NAMES = ("latest/gold.parquet", "latest/anomalies.parquet")
MAX_PARQUET_BYTES = 32 * 1024 * 1024


class AzureSnapshotError(ValueError):
    """Safe, user-facing configuration or snapshot error without credentials."""


def _now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def validate_settings(settings) -> tuple[str, str, str]:
    if not isinstance(settings, Mapping):
        raise AzureSnapshotError("The Azure secrets section must be a TOML table.")
    values = [settings.get(field) for field in ("account_url", "container", "sas_token")]
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise AzureSnapshotError("Set account_url, container, and sas_token in the Azure secrets section.")
    account_url, container, token = [value.strip() for value in values]
    try:
        address = urlsplit(account_url)
        port = address.port
    except ValueError:
        raise AzureSnapshotError("The Azure account endpoint is invalid.") from None
    if (
        address.scheme != "https" or not address.hostname
        or not address.hostname.endswith(".blob.core.windows.net")
        or address.path not in ("", "/") or address.query or address.fragment
        or address.username or address.password or port not in (None, 443)
    ):
        raise AzureSnapshotError("Azure account_url must be a plain HTTPS Blob Storage account endpoint.")
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9]|-(?!-)){1,61}[a-z0-9]", container):
        raise AzureSnapshotError("The Azure container name is invalid.")
    token = token.removeprefix("?")
    fields = parse_qs(token)
    permissions = fields.get("sp", [""])[0]
    if (
        not fields.get("sig") or fields.get("sr") != ["c"]
        or "r" not in permissions or not set(permissions).issubset({"r", "l"})
        or fields.get("spr") != ["https"]
    ):
        raise AzureSnapshotError("Use an HTTPS-only, read-only container SAS, not an account key or a write-enabled SAS.")
    try:
        expiry = pd.Timestamp(fields.get("se", [""])[0])
        if pd.isna(expiry) or expiry.tzinfo is None:
            raise ValueError("Missing UTC expiry")
    except (ValueError, TypeError):
        raise AzureSnapshotError("The Azure SAS must have a valid timezone-aware expiry.") from None
    if expiry <= _now():
        raise AzureSnapshotError("The Azure read-only SAS has expired. Rotate it in Streamlit secrets.")
    return account_url.rstrip("/"), container, token


def _read_blob(container, name: str, limit: int) -> bytes:
    payload = container.get_blob_client(name).download_blob(offset=0, length=limit + 1).readall()
    if len(payload) > limit:
        raise AzureSnapshotError("Azure snapshot exceeds this dashboard's supported size.")
    return payload


def read_snapshot(container) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Read a matched pair; hashes reject torn or concurrent latest-blob updates."""
    try:
        manifest = json.loads(_read_blob(container, MANIFEST_NAME, 16_384))
        records = manifest["blobs"]
        if manifest["version"] != 1 or not isinstance(records, list) or len(records) != 2:
            raise ValueError("Invalid manifest")
        entries = {entry["blob"]: entry for entry in records}
        if set(entries) != set(BLOB_NAMES):
            raise ValueError("Unexpected blob paths")
        published_at = pd.Timestamp(manifest["published_at"])
        if pd.isna(published_at) or published_at.tzinfo is None:
            raise ValueError("Invalid publication timestamp")
        published_at = published_at.tz_convert("UTC")
        frames = []
        for name in BLOB_NAMES:
            entry = entries[name]
            size = entry["bytes"]
            if type(size) is not int or not 0 < size <= MAX_PARQUET_BYTES:
                raise ValueError("Invalid blob size")
            payload = _read_blob(container, name, size)
            if len(payload) != size or sha256(payload).hexdigest() != entry["sha256"]:
                raise AzureSnapshotError("Azure snapshots are incomplete or changing. Retry after publishing finishes.")
            frames.append(pd.read_parquet(BytesIO(payload), engine="pyarrow"))
        gold = validate_gold_data(frames[0])
        anomalies = validate_anomaly_data(frames[1])
        keys = ["product_id", "minute_start"]
        if any(frame[keys].isna().any().any() or frame.duplicated(keys).any() for frame in [gold, anomalies]):
            raise ValueError("Invalid snapshot keys")
        if not gold[keys].equals(anomalies[keys]):
            raise ValueError("Mismatched snapshot keys")
    except AzureSnapshotError:
        raise
    except (ValueError, KeyError, TypeError, OSError, OverflowError):
        raise AzureSnapshotError("Azure snapshot validation failed. Republish the analytics and manifest together.") from None
    return gold, anomalies, {"published_at": published_at, "downloaded_at": _now()}


@st.cache_data(ttl=60, max_entries=8, show_spinner=False)
def _cached_snapshot(account_url: str, container: str, credential_fingerprint: str, _sas_token: str):
    try:
        from azure.core.exceptions import AzureError
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        raise AzureSnapshotError("Azure support is missing. Redeploy with requirements-dashboard.txt.") from None
    try:
        with BlobServiceClient(
            account_url=account_url, credential=_sas_token,
            connection_timeout=5, read_timeout=10, retry_total=1,
        ) as service:
            return read_snapshot(service.get_container_client(container))
    except AzureError:
        raise AzureSnapshotError(
            "Unable to read Azure snapshots. Check the read-only SAS, expiry, container access, "
            "and that the publisher uploaded latest/snapshot.json. No fallback data is being shown."
        ) from None


def load_azure_snapshot(settings):
    account_url, container, token = validate_settings(settings)
    fingerprint = sha256(token.encode("utf-8")).hexdigest()
    return _cached_snapshot(account_url, container, fingerprint, _sas_token=token)


def clear_azure_cache() -> None:
    _cached_snapshot.clear()

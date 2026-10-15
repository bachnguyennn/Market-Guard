import json
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import urlencode

import pandas as pd
import pytest
from azure.core.exceptions import HttpResponseError
from marketguard.dashboard import azure_loader
from streamlit.testing.v1 import AppTest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def settings(expiry=None, **overrides):
    fields = {
        "sr": "c", "sp": "rl", "spr": "https", "sig": "unit-test-only",
        "se": (expiry or pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=1)).isoformat(),
    }
    fields.update(overrides)
    return {
        "account_url": "https://example.blob.core.windows.net", "container": "dashboard",
        "sas_token": urlencode(fields),
    }


@pytest.fixture
def snapshot():
    gold = pd.DataFrame({
        "product_id": ["BTC-USD", "ETH-USD"],
        "minute_start": pd.to_datetime(["2020-01-01T12:00:00Z"] * 2),
        "trade_count": [4, 3], "base_volume": [Decimal(4), Decimal(3)],
        "notional_volume": [Decimal(400), Decimal(60)], "vwap": [Decimal(100), Decimal(20)],
    })
    anomalies = gold.assign(
        is_anomaly=[False, True], anomaly_type=["NONE", "PRICE"],
        is_volume_spike=[False, False], is_statistical_price_jump=[False, True],
    )
    return gold, anomalies, {
        "published_at": pd.Timestamp("2026-10-14T21:35:00Z"),
        "downloaded_at": pd.Timestamp("2026-10-14T21:36:00Z"),
    }


@pytest.fixture
def payloads(snapshot):
    frames = snapshot[:2]
    blobs = {name: frame.to_parquet(index=False) for name, frame in zip(azure_loader.BLOB_NAMES, frames, strict=True)}
    manifest = {
        "version": 1, "published_at": "2026-10-14T22:35:00+01:00",
        "blobs": [{"blob": name, "bytes": len(payload), "sha256": sha256(payload).hexdigest()} for name, payload in blobs.items()],
    }
    blobs[azure_loader.MANIFEST_NAME] = json.dumps(manifest).encode()
    return blobs


def fake_container(payloads):
    container = Mock()

    def blob_client(name):
        blob = Mock()

        def download(offset, length):
            stream = Mock()
            stream.readall.return_value = payloads[name][offset:offset + length]
            return stream

        blob.download_blob.side_effect = download
        return blob

    container.get_blob_client.side_effect = blob_client
    return container


def test_reads_only_manifest_and_two_matched_parquet_blobs(payloads):
    container = fake_container(payloads)
    gold, anomalies, metadata = azure_loader.read_snapshot(container)
    assert int(gold["trade_count"].sum()) == 7
    assert int(anomalies["is_anomaly"].sum()) == 1
    assert gold["vwap"].tolist() == [Decimal(100), Decimal(20)]
    assert str(gold["minute_start"].dt.tz) == "UTC"
    assert metadata["published_at"] == pd.Timestamp("2026-10-14T21:35:00Z")
    assert [call.args[0] for call in container.get_blob_client.call_args_list] == [azure_loader.MANIFEST_NAME, *azure_loader.BLOB_NAMES]


@pytest.mark.parametrize("problem", ["hash", "missing_manifest", "bad_manifest", "unexpected_path", "too_large", "wrong_keys", "invalid_schema"])
def test_rejects_incomplete_corrupt_or_mismatched_snapshots(payloads, snapshot, problem):
    manifest = json.loads(payloads[azure_loader.MANIFEST_NAME])
    if problem == "hash":
        payloads[azure_loader.BLOB_NAMES[0]] += b"changed"
    elif problem == "missing_manifest":
        payloads.pop(azure_loader.MANIFEST_NAME)
    elif problem == "bad_manifest":
        payloads[azure_loader.MANIFEST_NAME] = b"not-json"
    elif problem == "unexpected_path":
        manifest["blobs"][0]["blob"] = "unrelated/private.parquet"
    elif problem == "too_large":
        manifest["blobs"][0]["bytes"] = azure_loader.MAX_PARQUET_BYTES + 1
    else:
        frame = snapshot[1 if problem == "wrong_keys" else 0].copy()
        if problem == "wrong_keys":
            frame.loc[0, "product_id"] = "OTHER-USD"
            entry = manifest["blobs"][1]
        else:
            frame = frame.drop(columns="vwap")
            entry = manifest["blobs"][0]
        payload = frame.to_parquet(index=False)
        payloads[entry["blob"]] = payload
        entry.update(bytes=len(payload), sha256=sha256(payload).hexdigest())
    if problem not in {"missing_manifest", "bad_manifest"}:
        payloads[azure_loader.MANIFEST_NAME] = json.dumps(manifest).encode()
    with pytest.raises(azure_loader.AzureSnapshotError):
        azure_loader.read_snapshot(fake_container(payloads))


@pytest.mark.parametrize("permissions", ["rw", "racwdl", "l", ""])
def test_rejects_write_enabled_or_non_read_sas(permissions):
    with pytest.raises(azure_loader.AzureSnapshotError, match="read-only"):
        azure_loader.validate_settings(settings(sp=permissions))


@pytest.mark.parametrize("invalid", [{}, {"account_url": "x"}, "not-a-table"])
def test_incomplete_secrets_are_rejected_without_exposing_values(invalid):
    with pytest.raises(azure_loader.AzureSnapshotError):
        azure_loader.validate_settings(invalid)


def test_rejects_expired_token_and_insecure_or_credential_bearing_endpoints():
    with pytest.raises(azure_loader.AzureSnapshotError, match="expired"):
        azure_loader.validate_settings(settings(expiry=pd.Timestamp("2020-01-01T00:00:00Z")))
    for endpoint in ["http://example.blob.core.windows.net", "https://other.example", "https://example.blob.core.windows.net?sig=private"]:
        with pytest.raises(azure_loader.AzureSnapshotError):
            azure_loader.validate_settings({**settings(), "account_url": endpoint})


@pytest.fixture
def sdk_mock(monkeypatch, snapshot):
    import azure.storage.blob

    azure_loader.clear_azure_cache()
    service = Mock()
    service.__enter__ = Mock(return_value=service)
    service.__exit__ = Mock(return_value=False)
    client = Mock(return_value=service)
    monkeypatch.setattr(azure.storage.blob, "BlobServiceClient", client)
    reader = Mock(return_value=snapshot)
    monkeypatch.setattr(azure_loader, "read_snapshot", reader)
    yield client, reader
    azure_loader.clear_azure_cache()


def test_cache_reuses_pair_and_refresh_or_token_rotation_invalidates_it(sdk_mock):
    client, reader = sdk_mock
    configuration = settings()
    azure_loader.load_azure_snapshot(configuration)
    azure_loader.load_azure_snapshot(configuration)
    assert reader.call_count == 1
    azure_loader.clear_azure_cache()
    azure_loader.load_azure_snapshot(configuration)
    assert reader.call_count == 2
    rotated = settings(sig="rotated-test-signature")
    azure_loader.load_azure_snapshot(rotated)
    assert reader.call_count == 3
    assert client.call_args.kwargs["credential"] == rotated["sas_token"]


def test_expired_credentials_cannot_reuse_a_cached_snapshot(sdk_mock, monkeypatch):
    _, reader = sdk_mock
    before = pd.Timestamp("2026-10-14T12:00:00Z")
    monkeypatch.setattr(azure_loader, "_now", lambda: before)
    configuration = settings(expiry=before + pd.Timedelta(minutes=1))
    azure_loader.load_azure_snapshot(configuration)
    monkeypatch.setattr(azure_loader, "_now", lambda: before + pd.Timedelta(minutes=2))
    with pytest.raises(azure_loader.AzureSnapshotError, match="expired"):
        azure_loader.load_azure_snapshot(configuration)
    assert reader.call_count == 1


def test_download_errors_are_redacted_and_not_cached(sdk_mock):
    _, reader = sdk_mock
    reader.side_effect = HttpResponseError("https://private.example?sig=DO-NOT-DISPLAY")
    for _attempt in range(2):
        with pytest.raises(azure_loader.AzureSnapshotError) as raised:
            azure_loader.load_azure_snapshot(settings())
        assert "DO-NOT-DISPLAY" not in str(raised.value)
    assert reader.call_count == 2


def test_configured_azure_ui_uses_cloud_frames_freshness_and_refresh(monkeypatch, snapshot):
    reader = Mock(return_value=snapshot)
    clear = Mock()
    monkeypatch.setattr(azure_loader, "load_azure_snapshot", reader)
    monkeypatch.setattr(azure_loader, "clear_azure_cache", clear)
    page = AppTest.from_file(str(PROJECT_ROOT / "streamlit_app.py"))
    page.secrets["azure"] = settings()
    page.run()
    assert not page.exception
    assert page.tabs[0].metric[0].value == "7"
    assert page.tabs[1].metric[0].value == "1"
    assert "Azure analytics snapshot" in page.info[0].value
    assert any("2020-01-01 12:00 UTC" in caption.value for caption in page.caption)
    assert any("over 30 minutes old" in warning.value for warning in page.warning)
    page.selectbox[0].select("ETH-USD").run()
    assert page.tabs[0].metric[0].value == "3"
    assert page.tabs[1].metric[0].value == "1"
    page.button[0].click().run()
    clear.assert_called_once_with()
    assert not page.sidebar.text_input


def test_configured_azure_failure_does_not_fall_back_to_synthetic_demo(monkeypatch):
    monkeypatch.setattr(azure_loader, "load_azure_snapshot", Mock(side_effect=azure_loader.AzureSnapshotError("Azure snapshots unavailable. Rotate read-only SAS.")))
    page = AppTest.from_file(str(PROJECT_ROOT / "streamlit_app.py"))
    page.secrets["azure"] = settings()
    page.run()
    assert not page.exception
    assert not page.metric
    assert page.error[0].value == "Azure snapshots unavailable. Rotate read-only SAS."
    assert not any("Synthetic demo snapshot" in notice.value for notice in page.info)


def test_partial_azure_secrets_show_configuration_error_instead_of_demo():
    page = AppTest.from_file(str(PROJECT_ROOT / "streamlit_app.py"))
    page.secrets["azure"] = {"container": "dashboard"}
    page.run()
    assert not page.exception
    assert not page.metric
    assert "Set account_url, container, and sas_token" in page.error[0].value

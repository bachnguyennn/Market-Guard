from decimal import Decimal
import json
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from marketguard.dashboard_publisher import BLOB_NAMES, CONTAINER, MANIFEST_NAME, export_dashboard, publish_exports


@pytest.fixture
def analytics(tmp_path):
    source = tmp_path / "analytics"
    source.mkdir()
    gold = pd.DataFrame({
        "product_id": ["BTC-USD", "ETH-USD"],
        "minute_start": pd.to_datetime(["2026-10-14T12:00:00Z"] * 2),
        "trade_count": [3, 1], "base_volume": [Decimal(2), Decimal(1)],
        "notional_volume": [Decimal(200), Decimal(20)],
        "vwap": [Decimal(100), Decimal(20)], "internal_note": ["not-for-publication"] * 2,
    })
    anomalies = gold.assign(is_anomaly=[False, True], anomaly_type=["NONE", "PRICE"])
    for name, data in [("gold", gold), ("anomalies", anomalies)]:
        (source / name).mkdir()
        data.to_parquet(source / name / "part.parquet", index=False)
    return source


def test_export_compacts_parts_preserves_decimals_and_filters_extra_fields(analytics, tmp_path):
    files, summary = export_dashboard(analytics, tmp_path / "export")
    assert set(files) == set(BLOB_NAMES)
    assert summary == {"gold_rows": 2, "anomaly_rows": 2, "alert_count": 1, "latest_market_minute": "2026-10-14T12:00:00+00:00"}
    for path in files.values():
        frame = pd.read_parquet(path)
        assert len(frame) == 2
        assert "internal_note" not in frame.columns
        assert isinstance(frame["vwap"].iloc[0], Decimal)
        assert str(frame["minute_start"].dt.tz) == "UTC"
    assert (analytics / "gold/part.parquet").exists()


@pytest.mark.parametrize("invalid_case", ["duplicate", "different_keys", "missing_metric", "empty"])
def test_invalid_analytics_are_rejected_before_export(analytics, tmp_path, invalid_case):
    path = analytics / ("anomalies" if invalid_case == "different_keys" else "gold") / "part.parquet"
    frame = pd.read_parquet(path)
    if invalid_case == "duplicate":
        frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
    elif invalid_case == "different_keys":
        frame.loc[0, "product_id"] = "OTHER-USD"
    elif invalid_case == "missing_metric":
        frame = frame.drop(columns="base_volume")
    else:
        frame = frame.iloc[:0]
    frame.to_parquet(path, index=False)
    with pytest.raises(ValueError):
        export_dashboard(analytics, tmp_path / "export")
    assert not (tmp_path / "export").exists()


def test_export_refuses_to_overwrite_input(analytics):
    with pytest.raises(ValueError, match="must not overlap"):
        export_dashboard(analytics, analytics / "gold")


@pytest.fixture
def azure_setup(analytics, tmp_path):
    files, _ = export_dashboard(analytics, tmp_path / "export")
    service = Mock()
    container = service.get_container_client.return_value
    container.get_container_properties.return_value = {"public_access": None}
    blobs = {name: Mock() for name in BLOB_NAMES}
    for name, blob in blobs.items():
        blob.download_blob.return_value.readall.return_value = files[name].read_bytes()
    manifest_blob = Mock()
    manifest_blob.upload_blob.side_effect = lambda payload, **kwargs: setattr(
        manifest_blob.download_blob.return_value.readall, "return_value", payload,
    )
    blobs[MANIFEST_NAME] = manifest_blob
    container.get_blob_client.side_effect = blobs.__getitem__
    return service, container, blobs, files


@pytest.mark.parametrize("already_exists", [False, True])
def test_publish_creates_or_reuses_private_container_and_verifies_both_uploads(azure_setup, already_exists):
    service, container, blobs, files = azure_setup
    if already_exists:
        container.create_container.side_effect = ResourceExistsError("already exists")
    result = publish_exports(service, files)
    service.get_container_client.assert_called_once_with(CONTAINER)
    container.create_container.assert_called_once_with()
    assert [item["blob"] for item in result] == list(BLOB_NAMES)
    for name in BLOB_NAMES:
        blob = blobs[name]
        arguments = blob.upload_blob.call_args
        assert arguments.args == (files[name].read_bytes(),)
        assert arguments.kwargs["overwrite"] is True
        assert arguments.kwargs["validate_content"] is True
        assert arguments.kwargs["content_settings"].content_type == "application/vnd.apache.parquet"
        blob.download_blob.assert_called_once_with()
    manifest = json.loads(blobs[MANIFEST_NAME].upload_blob.call_args.args[0])
    assert manifest["version"] == 1
    assert manifest["blobs"] == result
    assert all(len(item["sha256"]) == 64 for item in result)


@pytest.mark.parametrize("public_access", ["blob", "container"])
def test_existing_public_container_is_never_used(azure_setup, public_access):
    service, container, blobs, files = azure_setup
    container.create_container.side_effect = ResourceExistsError("already exists")
    container.get_container_properties.return_value = {"public_access": public_access}
    with pytest.raises(ValueError, match="must be private"):
        publish_exports(service, files)
    for blob in blobs.values():
        blob.upload_blob.assert_not_called()


def test_missing_second_file_prevents_all_azure_operations(azure_setup):
    service, _, _, files = azure_setup
    files[BLOB_NAMES[1]].unlink()
    with pytest.raises(FileNotFoundError):
        publish_exports(service, files)
    service.get_container_client.assert_not_called()


def test_publisher_refuses_unexpected_blob_names(azure_setup):
    service, _, _, files = azure_setup
    with pytest.raises(ValueError, match="Only the two expected"):
        publish_exports(service, {**files, "bronze/raw.parquet": Path("raw.parquet")})
    service.get_container_client.assert_not_called()


@pytest.mark.parametrize("failed_name", BLOB_NAMES)
def test_upload_failure_propagates_without_reporting_success(azure_setup, failed_name):
    service, _, blobs, files = azure_setup
    blobs[failed_name].upload_blob.side_effect = HttpResponseError("upload failed")
    with pytest.raises(HttpResponseError):
        publish_exports(service, files)
    if failed_name == BLOB_NAMES[0]:
        blobs[BLOB_NAMES[1]].upload_blob.assert_not_called()
    blobs[MANIFEST_NAME].upload_blob.assert_not_called()


def test_corrupt_readback_stops_publication(azure_setup):
    service, _, blobs, files = azure_setup
    blobs[BLOB_NAMES[0]].download_blob.return_value.readall.return_value = b"corrupt"
    with pytest.raises(RuntimeError, match="read-back verification"):
        publish_exports(service, files)
    blobs[BLOB_NAMES[1]].upload_blob.assert_not_called()
    blobs[MANIFEST_NAME].upload_blob.assert_not_called()


def test_manifest_failure_is_not_reported_as_a_complete_publication(azure_setup):
    service, _, blobs, files = azure_setup
    blobs[MANIFEST_NAME].upload_blob.side_effect = HttpResponseError("manifest upload failed")
    with pytest.raises(HttpResponseError):
        publish_exports(service, files)

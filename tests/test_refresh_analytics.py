import json
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from azure.core import MatchConditions
from azure.core.exceptions import HttpResponseError
from marketguard import refresh_analytics as refresh

NOW = datetime(2026, 10, 15, 12, tzinfo=UTC)


def batch(name="coinbase/batch.jsonl", modified=NOW, size=5):
    return SimpleNamespace(name=name, last_modified=modified, size=size, etag='"version-1"')


def test_selection_filters_ingestion_window_and_jsonl_names():
    recent = batch()
    boundary = batch("coinbase/start.jsonl", NOW - timedelta(hours=6))
    container = Mock()
    container.list_blobs.return_value = [
        recent, batch("coinbase/old.jsonl", NOW - timedelta(hours=7)),
        batch("coinbase/future.jsonl", NOW + timedelta(seconds=1)),
        batch("coinbase/notes.txt"), boundary,
    ]
    selected = refresh.select_recent_blobs(container, NOW - timedelta(hours=6), NOW)
    assert selected == [recent, boundary]
    container.list_blobs.assert_called_once_with(name_starts_with="coinbase/")


@pytest.mark.parametrize("limits", [
    {"max_scan": 1}, {"max_batches": 1}, {"max_bytes": 9},
])
def test_selection_never_silently_truncates_when_limit_is_exceeded(limits):
    container = Mock()
    container.list_blobs.return_value = [batch(), batch("coinbase/second.jsonl")]
    with pytest.raises(ValueError, match="limit"):
        refresh.select_recent_blobs(container, NOW - timedelta(hours=6), NOW, **limits)


def test_scan_limit_also_counts_old_batches():
    container = Mock()
    container.list_blobs.return_value = [batch(modified=NOW - timedelta(days=1))] * 3
    with pytest.raises(ValueError, match="scan limit"):
        refresh.select_recent_blobs(container, NOW - timedelta(hours=6), NOW, max_scan=2)


@pytest.mark.parametrize("size", [0, -1])
def test_empty_or_invalid_source_batches_are_rejected(size):
    container = Mock()
    container.list_blobs.return_value = [batch(size=size)]
    with pytest.raises(ValueError, match="empty or unversioned"):
        refresh.select_recent_blobs(container, NOW - timedelta(hours=6), NOW)


def test_no_recent_batches_preserves_the_existing_snapshot():
    container = Mock()
    container.list_blobs.return_value = []
    with pytest.raises(ValueError, match="keeping the existing snapshot"):
        refresh.select_recent_blobs(container, NOW - timedelta(hours=6), NOW)


def test_download_uses_etag_and_safe_local_names(tmp_path):
    container = Mock()
    container.get_blob_client.return_value.download_blob.return_value.readinto.side_effect = (
        lambda output: output.write(b"data\n")
    )
    source = batch("coinbase/../../untrusted.jsonl")
    destination = tmp_path / "bronze"
    refresh.download_bronze(container, [source], destination)
    assert (destination / "batch_000000.jsonl").read_bytes() == b"data\n"
    assert list(destination.glob("*.part")) == []
    container.get_blob_client.return_value.download_blob.assert_called_once_with(
        offset=0, length=6, etag=source.etag,
        match_condition=MatchConditions.IfNotModified, max_concurrency=1,
    )


@pytest.mark.parametrize("failure", ["changed", "short", "long"])
def test_changed_or_incomplete_downloads_abort_without_a_jsonl_file(tmp_path, failure):
    container = Mock()
    downloader = container.get_blob_client.return_value.download_blob
    if failure == "changed":
        downloader.side_effect = HttpResponseError("ETag changed")
        error_type = HttpResponseError
    else:
        downloader.return_value.readinto.side_effect = (
            lambda output: output.write(b"x" if failure == "short" else b"123456")
        )
        error_type = ValueError
    destination = tmp_path / "bronze"
    with pytest.raises(error_type):
        refresh.download_bronze(container, [batch()], destination)
    assert list(destination.iterdir()) == []


@pytest.fixture
def setup(monkeypatch, tmp_path):
    service = Mock()
    bronze = Mock()
    bronze.list_blobs.return_value = [batch()]
    dashboard = Mock()
    dashboard.get_blob_client.return_value.exists.return_value = True
    service.get_container_client.side_effect = {"bronze": bronze, "dashboard": dashboard}.__getitem__
    gold = pd.DataFrame({"minute_start": pd.to_datetime(["2026-10-15T12:00Z"]), "trade_count": [3]})
    anomalies = pd.DataFrame({"is_anomaly": [False]})
    mocks = {
        "download_bronze": Mock(),
        "calculate_analytics": Mock(return_value={
            "bronze_count": 4, "silver_count": 3, "excluded_count": 1,
            "gold_count": 1, "anomaly_count": 1, "alert_count": 0,
            "outputs": {"gold": "not-in-the-report"},
        }),
        "export_dashboard": Mock(return_value=({}, {
            "gold_rows": 1, "anomaly_rows": 1, "alert_count": 0,
            "latest_market_minute": "2026-10-15T12:00:00+00:00",
        })),
        "publish_exports": Mock(),
        "read_snapshot": Mock(return_value=(gold, anomalies, {})),
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(refresh, name, mock)
    return SimpleNamespace(service=service, bronze=bronze, dashboard=dashboard, mocks=mocks, root=tmp_path)


def test_default_dry_run_reads_but_never_uploads(setup):
    report = refresh.refresh_analytics(setup.service, setup.root, now=NOW)
    assert report["published"] is False
    assert report["source_batches"] == 1
    assert report["source_bytes"] == 5
    assert report["source_window_start"] == "2026-10-15T06:00:00+00:00"
    assert report["silver_count"] == 3
    assert "outputs" not in report
    setup.mocks["publish_exports"].assert_not_called()
    setup.mocks["download_bronze"].assert_called_once()
    setup.mocks["calculate_analytics"].assert_called_once()


def test_publish_happens_after_processing_and_is_read_back(setup):
    order = Mock()
    for name, mock in setup.mocks.items():
        order.attach_mock(mock, name)
    report = refresh.refresh_analytics(setup.service, setup.root, now=NOW, publish=True)
    assert report["published"] is True
    assert [call[0] for call in order.mock_calls] == [
        "download_bronze", "calculate_analytics", "export_dashboard",
        "read_snapshot", "publish_exports", "read_snapshot",
    ]
    setup.mocks["publish_exports"].assert_called_once_with(setup.service, {})


@pytest.mark.parametrize("stage", [
    "download_bronze", "calculate_analytics", "export_dashboard", "read_snapshot",
])
def test_failed_preparation_does_not_authorize_upload(setup, stage):
    setup.mocks[stage].side_effect = RuntimeError("failed")
    with pytest.raises(RuntimeError, match="failed"):
        refresh.refresh_analytics(setup.service, setup.root, now=NOW, publish=True)
    setup.mocks["publish_exports"].assert_not_called()


def test_old_market_data_cannot_replace_newer_snapshot(setup):
    newer = pd.DataFrame({"minute_start": pd.to_datetime(["2026-10-15T12:01Z"])})
    setup.mocks["read_snapshot"].return_value = (newer, pd.DataFrame(), {})
    with pytest.raises(ValueError, match="older than the published snapshot"):
        refresh.refresh_analytics(setup.service, setup.root, now=NOW, publish=True)
    setup.mocks["publish_exports"].assert_not_called()


def test_first_publication_without_a_manifest_is_supported(setup):
    setup.dashboard.get_blob_client.return_value.exists.return_value = False
    report = refresh.refresh_analytics(setup.service, setup.root, now=NOW, publish=True)
    assert report["published"] is True
    setup.mocks["read_snapshot"].assert_called_once_with(setup.dashboard)


def test_upload_failure_propagates_and_cannot_report_success(setup):
    setup.mocks["publish_exports"].side_effect = HttpResponseError("upload failed")
    with pytest.raises(HttpResponseError):
        refresh.refresh_analytics(setup.service, setup.root, now=NOW, publish=True)
    assert setup.mocks["read_snapshot"].call_count == 1


@pytest.mark.parametrize("field", ["trades", "gold", "anomalies", "alerts"])
def test_readback_mismatch_cannot_report_success(setup, field):
    gold, anomalies, metadata = setup.mocks["read_snapshot"].return_value
    if field == "trades":
        gold = gold.assign(trade_count=99)
    elif field == "gold":
        gold = gold.iloc[:0]
    elif field == "anomalies":
        anomalies = anomalies.iloc[:0]
    else:
        anomalies = anomalies.assign(is_anomaly=True)
    setup.mocks["read_snapshot"].side_effect = [
        setup.mocks["read_snapshot"].return_value, (gold, anomalies, metadata),
    ]
    with pytest.raises(RuntimeError, match="counts failed verification"):
        refresh.refresh_analytics(setup.service, setup.root, now=NOW, publish=True)


def test_each_run_has_a_separate_staging_directory(setup):
    refresh.refresh_analytics(setup.service, setup.root, now=NOW)
    refresh.refresh_analytics(setup.service, setup.root, now=NOW)
    paths = [call.args[2] for call in setup.mocks["download_bronze"].call_args_list]
    assert paths[0] != paths[1]
    assert all(path.parent.is_relative_to(setup.root) for path in paths)


@pytest.mark.parametrize("hours", [0, 25])
def test_invalid_lookback_is_rejected_before_any_azure_calls(setup, hours):
    with pytest.raises(ValueError, match="Lookback"):
        refresh.refresh_analytics(setup.service, setup.root, lookback_hours=hours)
    setup.service.get_container_client.assert_not_called()


@pytest.mark.parametrize("publish", [False, True])
def test_cli_only_publishes_on_opt_in_and_writes_the_summary(monkeypatch, tmp_path, publish):
    report = tmp_path / "summary.json"
    summary = {"published": publish, "silver_count": 3}
    runner = Mock(return_value=summary)
    monkeypatch.setattr(refresh, "DefaultAzureCredential", lambda: nullcontext(Mock()))
    monkeypatch.setattr(refresh, "BlobServiceClient", lambda **kwargs: nullcontext(Mock()))
    monkeypatch.setattr(refresh, "refresh_analytics", runner)
    monkeypatch.setattr("sys.argv", ["refresh", "--report", str(report)] + (["--publish"] if publish else []))
    refresh.main()
    assert json.loads(report.read_text()) == summary
    assert runner.call_args.kwargs["publish"] is publish
    assert runner.call_args.kwargs["lookback_hours"] == 6


def test_cli_redacts_azure_errors_and_never_writes_a_success_report(monkeypatch, tmp_path, capsys):
    report = tmp_path / "summary.json"
    runner = Mock(side_effect=HttpResponseError("sensitive-token-in-sdk-error"))
    monkeypatch.setattr(refresh, "DefaultAzureCredential", lambda: nullcontext(Mock()))
    monkeypatch.setattr(refresh, "BlobServiceClient", lambda **kwargs: nullcontext(Mock()))
    monkeypatch.setattr(refresh, "refresh_analytics", runner)
    monkeypatch.setattr("sys.argv", ["refresh", "--report", str(report)])
    with pytest.raises(SystemExit) as error:
        refresh.main()
    assert error.value.code == 1
    assert not report.exists()
    captured = capsys.readouterr()
    assert "sensitive-token" not in captured.err + captured.out
    assert "Check OIDC/Azure sign-in" in captured.err

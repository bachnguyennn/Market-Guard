import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import pipeline


def test_process_batch_uploads_complete_unique_batches(monkeypatch, tmp_path):
    trades = [
        {"trade_id": 1001},
        {"trade_id": 1002},
        {"trade_id": 1003},
    ]
    uploads = []

    def fake_upload(file_path, blob_name):
        records = [
            json.loads(line)
            for line in file_path.read_text(encoding="utf-8").splitlines()
        ]
        uploads.append((file_path, blob_name, records))

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pipeline, "upload_with_retry", fake_upload)

    asyncio.run(pipeline.process_batch(trades))
    asyncio.run(pipeline.process_batch(trades))

    assert len(uploads) == 2
    assert uploads[0][1] != uploads[1][1]
    assert uploads[0][2] == trades
    assert uploads[1][2] == trades
    assert not uploads[0][0].exists()
    assert not uploads[1][0].exists()


def test_process_batch_keeps_file_when_upload_fails(monkeypatch, tmp_path):
    trades = [{"trade_id": 1001}, {"trade_id": 1002}]

    def fake_upload(file_path, blob_name):
        raise OSError("Azure unavailable")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pipeline, "upload_with_retry", fake_upload)

    with pytest.raises(OSError, match="Azure unavailable"):
        asyncio.run(pipeline.process_batch(trades))

    pending_files = list((tmp_path / "data" / "pending").glob("*.jsonl"))
    assert len(pending_files) == 1
    assert len(pending_files[0].read_text(encoding="utf-8").splitlines()) == 2


def test_batches_are_named_by_utc_upload_hour(monkeypatch, tmp_path):
    from datetime import UTC, datetime

    from src.storage import bronze_hour_prefix

    names = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pipeline, "upload_with_retry", lambda path, name: names.append(name))
    before = datetime.now(UTC)
    asyncio.run(pipeline.process_batch([{"trade_id": 1}]))
    after = datetime.now(UTC)
    assert names[0].startswith((bronze_hour_prefix(before), bronze_hour_prefix(after)))
    assert names[0].endswith(".jsonl") and "/batch_" in names[0]


def test_hour_prefix_requires_timezone_and_uses_utc():
    from datetime import datetime, timedelta, timezone

    from src.storage import bronze_hour_prefix

    toronto = timezone(timedelta(hours=-5))
    assert bronze_hour_prefix(datetime(2026, 11, 4, 20, 30, tzinfo=toronto)) == "coinbase/2026/11/05/01/"
    with pytest.raises(ValueError):
        bronze_hour_prefix(datetime(2026, 11, 4, 20))


def test_quote_batches_upload_outside_the_trade_prefix(monkeypatch, tmp_path):
    uploads = []

    def fake_upload(file_path, blob_name):
        uploads.append(
            (blob_name, json.loads(file_path.read_text(encoding="utf-8")))
        )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pipeline, "upload_with_retry", fake_upload)

    asyncio.run(pipeline.process_quote_batch([{"best_bid": "100"}]))

    blob_name, record = uploads[0]
    assert blob_name.startswith("coinbase-quotes/")
    assert not blob_name.startswith("coinbase/")
    assert record == {"best_bid": "100"}
    assert not list((tmp_path / "data" / "pending_quotes").glob("*.jsonl"))


def test_recovery_keeps_trades_and_quotes_in_their_own_prefixes(monkeypatch, tmp_path):
    uploads = []

    def fake_upload(file_path, blob_name):
        uploads.append(blob_name)

    monkeypatch.chdir(tmp_path)
    for directory in ("pending", "pending_quotes"):
        pending = tmp_path / "data" / directory
        pending.mkdir(parents=True)
        (pending / "batch_a.jsonl").write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(pipeline, "upload_with_retry", fake_upload)

    asyncio.run(pipeline.recover_pending_batches())

    assert len(uploads) == 2
    assert uploads[0].startswith("coinbase/")
    assert uploads[1].startswith("coinbase-quotes/")
    assert not list((tmp_path / "data").rglob("*.jsonl"))

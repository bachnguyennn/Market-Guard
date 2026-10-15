import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import azure_storage
from storage import create_batch_file


def test_upload_succeeds_on_first_attempt(monkeypatch):
    calls = []

    def fake_upload(file_path, blob_name):
        calls.append((file_path, blob_name))

    monkeypatch.setattr(azure_storage, "upload_batch", fake_upload)

    azure_storage.upload_with_retry("trades.jsonl", "trades.jsonl")

    assert calls == [("trades.jsonl", "trades.jsonl")]


def test_upload_fails_twice_then_succeeds(monkeypatch):
    calls = []
    delays = []

    def fake_upload(file_path, blob_name):
        calls.append((file_path, blob_name))
        if len(calls) < 3:
            raise OSError("Simulated Azure failure")

    monkeypatch.setattr(azure_storage, "upload_batch", fake_upload)
    monkeypatch.setattr(azure_storage.time, "sleep", delays.append)

    azure_storage.upload_with_retry("trades.jsonl", "trades.jsonl")

    assert len(calls) == 3
    assert delays == [1, 2]


def test_upload_always_fails(monkeypatch):
    error = OSError("Simulated Azure failure")
    calls = []

    def fake_upload(file_path, blob_name):
        calls.append((file_path, blob_name))
        raise error

    monkeypatch.setattr(azure_storage, "upload_batch", fake_upload)
    monkeypatch.setattr(azure_storage.time, "sleep", lambda delay: None)

    with pytest.raises(OSError) as raised:
        azure_storage.upload_with_retry("trades.jsonl", "trades.jsonl")

    assert raised.value is error
    assert len(calls) == 3

def test_upload_success(monkeypatch):
    call_count = 0

    def fake_upload(file_path, blob_name):
        nonlocal call_count
        call_count += 1

    monkeypatch.setattr(azure_storage, "upload_batch", fake_upload)

    azure_storage.upload_with_retry("test.jsonl", "test-batch.jsonl")

    assert call_count == 1
def test_retry_eventually_succeeds(monkeypatch):
    attempts = 0
    delays = []

    def fake_upload(file_path, blob_name):
        nonlocal attempts
        attempts += 1

        if attempts < 3:
            raise OSError("Simulated Azure failure")

    def fake_sleep(seconds):
        delays.append(seconds)

    monkeypatch.setattr(azure_storage, "upload_batch", fake_upload)

    monkeypatch.setattr(azure_storage.time, "sleep", fake_sleep)

    azure_storage.upload_with_retry(
        "test.jsonl",
        "test-batch.jsonl",
        max_retries=3,
    )

    assert attempts == 3
    assert delays == [1, 2]


def test_upload_fails_after_max_retries(monkeypatch):
    attempts = 0

    def fake_upload(file_path, blob_name):
        nonlocal attempts
        attempts += 1
        raise OSError("Azure Storage unavailable")

    monkeypatch.setattr(azure_storage, "upload_batch", fake_upload)

    monkeypatch.setattr(azure_storage.time, "sleep", lambda seconds: None)

    with pytest.raises(OSError, match="Azure Storage unavailable"):
        azure_storage.upload_with_retry(
            "test.jsonl",
            "test-batch.jsonl",
            max_retries=3,
        )

    assert attempts == 3


def test_atomic_batch_creation(tmp_path):
    trades = [
        {"trade_id": 1001},
        {"trade_id": 1002},
        {"trade_id": 1003},
    ]

    batch_path = create_batch_file(trades, str(tmp_path))

    assert batch_path.exists()

    records = [
        json.loads(line)
        for line in batch_path.read_text(encoding="utf-8").splitlines()
    ]
    assert records == trades

    assert list(tmp_path.glob("*.tmp")) == []

    second_batch_path = create_batch_file(trades, str(tmp_path))
    assert second_batch_path.name != batch_path.name

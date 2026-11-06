import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import collector, pipeline

MATCH = {"type": "match", "trade_id": 1, "product_id": "BTC-USD"}
TICKER = {
    "type": "ticker",
    "product_id": "BTC-USD",
    "best_bid": "100.00",
    "best_bid_size": "1.5",
    "best_ask": "100.01",
    "best_ask_size": "2.5",
}
HEARTBEAT = {"type": "heartbeat", "product_id": "BTC-USD"}


class FakeWebSocket:
    def __init__(self, messages):
        self.messages = messages
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def send(self, message):
        self.sent.append(json.loads(message))

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in self.messages:
            yield json.dumps(message)


@pytest.fixture
def empty_buffers(monkeypatch):
    monkeypatch.setattr(collector, "buffer", [])
    monkeypatch.setattr(collector, "quote_buffer", [])


def run_collector(monkeypatch, messages):
    websocket = FakeWebSocket(messages)
    monkeypatch.setattr(collector.websockets, "connect", lambda _: websocket)

    async def run():
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(collector.collect_trades(task_group))

    asyncio.run(run())
    return websocket


def test_collector_subscribes_to_trades_and_quotes(monkeypatch, empty_buffers):
    websocket = run_collector(monkeypatch, [])

    assert websocket.sent[0]["channels"] == ["matches", "heartbeat", "ticker"]


def test_collector_separates_quotes_from_trades(monkeypatch, empty_buffers):
    run_collector(monkeypatch, [MATCH, TICKER, HEARTBEAT])

    assert collector.buffer == [MATCH]
    assert len(collector.quote_buffer) == 1
    quote = collector.quote_buffer[0]
    assert quote["best_bid"] == "100.00"
    assert quote["best_ask_size"] == "2.5"
    assert datetime.fromisoformat(quote["received_at"]).tzinfo is not None


def test_full_quote_buffer_is_handed_off(monkeypatch, empty_buffers):
    batches = []

    async def fake_process(batch):
        batches.append(batch)

    monkeypatch.setattr(collector, "QUOTE_BATCH_SIZE", 2)
    monkeypatch.setattr(collector, "process_quote_batch", fake_process)

    run_collector(monkeypatch, [TICKER, TICKER, TICKER])

    assert [len(batch) for batch in batches] == [2]
    assert len(collector.quote_buffer) == 1


def test_quote_upload_failure_does_not_stop_trade_collection(
    monkeypatch, tmp_path, empty_buffers
):
    def failing_upload(file_path, blob_name):
        raise OSError("Azure unavailable")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pipeline, "upload_with_retry", failing_upload)
    monkeypatch.setattr(collector, "QUOTE_BATCH_SIZE", 1)

    run_collector(monkeypatch, [TICKER, MATCH, TICKER, MATCH])

    assert collector.buffer == [MATCH, MATCH]
    pending = list((tmp_path / "data" / "pending_quotes").glob("*.jsonl"))
    assert len(pending) == 2

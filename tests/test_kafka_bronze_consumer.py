import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest

from src.kafka.bronze_consumer import TOPIC, run_consumer


def make_message(trade_id, partition, offset, payload=None):
    message = Mock()
    message.error.return_value = None
    message.value.return_value = (
        payload
        if payload is not None
        else json.dumps(
            {"trade_id": trade_id, "product_id": "BTC-USD"}
        ).encode("utf-8")
    )
    message.topic.return_value = TOPIC
    message.partition.return_value = partition
    message.offset.return_value = offset
    return message


def install_consumer(monkeypatch, messages):
    consumer = Mock()
    consumer.poll.side_effect = messages
    consumer.commit.return_value = []
    monkeypatch.setattr(
        "src.kafka.bronze_consumer.create_consumer",
        lambda: consumer,
    )
    return consumer


def test_successful_upload_commits_each_partition(monkeypatch):
    messages = [
        make_message(1001, partition=0, offset=4),
        make_message(1002, partition=1, offset=7),
    ]
    consumer = install_consumer(monkeypatch, messages)
    process_batch = AsyncMock()
    monkeypatch.setattr("src.kafka.bronze_consumer.process_batch", process_batch)

    asyncio.run(run_consumer(batch_size=2, max_batches=1, poll_timeout=0))

    process_batch.assert_awaited_once()
    committed = {
        (offset.topic, offset.partition): offset.offset
        for offset in consumer.commit.call_args.kwargs["offsets"]
    }
    assert committed == {(TOPIC, 0): 5, (TOPIC, 1): 8}
    assert consumer.commit.call_args.kwargs["asynchronous"] is False
    consumer.close.assert_called_once_with()


def test_upload_failure_does_not_commit(monkeypatch):
    consumer = install_consumer(
        monkeypatch,
        [make_message(1001, partition=0, offset=4)],
    )
    process_batch = AsyncMock(side_effect=RuntimeError("upload failed"))
    monkeypatch.setattr("src.kafka.bronze_consumer.process_batch", process_batch)

    with pytest.raises(RuntimeError, match="upload failed"):
        asyncio.run(run_consumer(batch_size=1, max_batches=1, poll_timeout=0))

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()


def test_malformed_json_is_not_committed(monkeypatch):
    consumer = install_consumer(
        monkeypatch,
        [make_message(1001, partition=0, offset=4, payload=b"not-json")],
    )
    process_batch = AsyncMock()
    monkeypatch.setattr("src.kafka.bronze_consumer.process_batch", process_batch)

    with pytest.raises(json.JSONDecodeError):
        asyncio.run(run_consumer(batch_size=1, max_batches=1, poll_timeout=0))

    process_batch.assert_not_awaited()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()


def test_crash_before_commit_leaves_offsets_replayable(monkeypatch):
    consumer = install_consumer(
        monkeypatch,
        [make_message(1001, partition=0, offset=4)],
    )
    monkeypatch.setattr(
        "src.kafka.bronze_consumer.process_batch",
        AsyncMock(side_effect=KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(run_consumer(batch_size=1, max_batches=1, poll_timeout=0))

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()

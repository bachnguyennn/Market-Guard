import json
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaException

from src.kafka.consumer import TOPIC, consume_trades


def test_consume_trades_decodes_prints_and_commits(monkeypatch, capsys):
    message = Mock()
    message.error.return_value = None
    message.value.return_value = json.dumps(
        {"trade_id": 1001, "product_id": "BTC-USD"}
    ).encode("utf-8")
    consumer = Mock()
    consumer.poll.return_value = message
    monkeypatch.setattr("src.kafka.consumer.create_consumer", lambda: consumer)

    consume_trades(max_messages=1, poll_timeout=0)

    consumer.subscribe.assert_called_once_with([TOPIC])
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)
    consumer.close.assert_called_once_with()
    assert "Consumed trade 1001 for BTC-USD" in capsys.readouterr().out


def test_consume_trades_does_not_commit_failed_processing(monkeypatch):
    message = Mock()
    message.error.return_value = None
    message.value.return_value = b"not-json"
    consumer = Mock()
    consumer.poll.return_value = message
    monkeypatch.setattr("src.kafka.consumer.create_consumer", lambda: consumer)

    with pytest.raises(json.JSONDecodeError):
        consume_trades(max_messages=1, poll_timeout=0)

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()


def test_consume_trades_raises_kafka_errors(monkeypatch):
    error = Mock()
    error.code.return_value = 123
    message = Mock()
    message.error.return_value = error
    consumer = Mock()
    consumer.poll.return_value = message
    monkeypatch.setattr("src.kafka.consumer.create_consumer", lambda: consumer)

    with pytest.raises(KafkaException):
        consume_trades(max_messages=1, poll_timeout=0)

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()

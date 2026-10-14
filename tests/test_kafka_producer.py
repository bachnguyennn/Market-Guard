from unittest.mock import Mock

from src.kafka.producer import TOPIC, main, publish_trade


def test_publish_trade_uses_product_as_key_and_serializes_json():
    producer = Mock()
    trade = {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": 123,
        "price": "60000.00",
        "size": "0.01",
    }

    publish_trade(producer, trade)

    producer.produce.assert_called_once_with(
        topic=TOPIC,
        key="BTC-USD",
        value=(
            '{"type": "match", "product_id": "BTC-USD", '
            '"trade_id": 123, "price": "60000.00", "size": "0.01"}'
        ),
    )
    producer.poll.assert_called_once_with(0)


def test_main_flushes_the_synthetic_trade(monkeypatch):
    producer = Mock()
    producer.flush.return_value = 0
    monkeypatch.setattr("src.kafka.producer.create_producer", lambda: producer)

    main()

    producer.produce.assert_called_once()
    assert producer.produce.call_args.kwargs["key"] == "BTC-USD"
    assert '"trade_id": 1001' in producer.produce.call_args.kwargs["value"]
    producer.flush.assert_called_once_with()

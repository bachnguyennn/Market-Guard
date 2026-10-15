from copy import deepcopy
from decimal import Decimal
from unittest.mock import Mock

import pytest
from marketguard.demo_publisher import publish_trades
from marketguard.demo_verifier import verify_gold_records


@pytest.fixture
def example_records():
    trades = [
        {"product_id": "BTC-USD", "time": "2026-10-14T12:00:00Z", "price": "100", "size": "1"},
        {"product_id": "BTC-USD", "time": "2026-10-14T12:00:10Z", "price": "110", "size": "2"},
        {"product_id": "ETH-USD", "time": "2026-10-14T12:00:00Z", "price": "20", "size": "1"},
        {"product_id": "BTC-USD", "time": "2026-10-14T12:01:00Z", "price": "120", "size": "1"},
    ]
    rows = [
        {"product_id": "BTC-USD", "minute_epoch": 1791979200, "trade_count": 2,
         "base_volume": Decimal(3), "notional_volume": Decimal(320), "vwap": Decimal("106.666667")},
        {"product_id": "ETH-USD", "minute_epoch": 1791979200, "trade_count": 1,
         "base_volume": Decimal(1), "notional_volume": Decimal(20), "vwap": Decimal(20)},
        {"product_id": "BTC-USD", "minute_epoch": 1791979260, "trade_count": 1,
         "base_volume": Decimal(1), "notional_volume": Decimal(120), "vwap": Decimal(120)},
    ]
    return trades, rows


def test_verifier_checks_decimal_aggregates_and_product_minute_groups(example_records):
    trades, rows = example_records
    verify_gold_records(trades, rows)


@pytest.mark.parametrize("field", ["trade_count", "base_volume", "notional_volume", "vwap"])
def test_verifier_rejects_corrupt_aggregates(example_records, field):
    trades, rows = deepcopy(example_records)
    rows[0][field] += 1
    with pytest.raises(ValueError, match=f"Incorrect Gold {field}"):
        verify_gold_records(trades, rows)


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), 106.666667, None])
def test_verifier_rejects_non_decimal_or_nonfinite_values(example_records, value):
    trades, rows = example_records
    rows[0]["vwap"] = value
    with pytest.raises(ValueError, match="Incorrect Gold vwap"):
        verify_gold_records(trades, rows)


def test_verifier_rejects_duplicate_or_missing_buckets(example_records):
    trades, rows = example_records
    with pytest.raises(ValueError, match="Duplicate Gold bucket"):
        verify_gold_records(trades, rows + [rows[0]])
    with pytest.raises(ValueError, match="buckets do not match"):
        verify_gold_records(trades, rows[1:])


def test_publisher_waits_for_delivery_and_tracks_both_partition_offsets():
    producer = Mock()
    producer.flush.return_value = 0

    def deliver(**kwargs):
        message = Mock()
        message.partition.return_value = int(kwargs["key"])
        message.offset.return_value = 7
        kwargs["on_delivery"](None, message)

    producer.produce.side_effect = deliver
    assert publish_trades(producer, "demo", [{"product_id": "0"}, {"product_id": "1"}]) == {0: 8, 1: 8}
    producer.flush.assert_called_once_with(30)


@pytest.mark.parametrize("delivery_error,remaining", [("broker unavailable", 0), (None, 1)])
def test_publisher_does_not_report_failed_deliveries_as_success(delivery_error, remaining):
    producer = Mock()
    producer.flush.return_value = remaining
    if delivery_error:
        producer.produce.side_effect = lambda **kwargs: kwargs["on_delivery"](delivery_error, None)
    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        publish_trades(producer, "demo", [{"product_id": "BTC-USD"}])

import json
import os
from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.spark.kafka_stream import parse_trade_messages


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .appName("MarketGuard-KafkaStream-Test")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_parse_trade_messages_decodes_binary_values_as_strings(spark):
    trade = {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": 1001,
        "price": "68000.00",
        "size": "0.15",
        "time": "2026-10-14T12:00:00Z",
    }
    kafka_df = spark.createDataFrame(
        [(json.dumps(trade).encode("utf-8"),)],
        "value binary",
    )

    parsed_df = parse_trade_messages(kafka_df)

    assert parsed_df.first().asDict() == {
        **trade,
        "trade_id": "1001",
    }
    assert all(isinstance(field.dataType, StringType) for field in parsed_df.schema)


def test_parse_trade_messages_keeps_nulls_for_invalid_or_missing_values(spark):
    kafka_df = spark.createDataFrame(
        [(b"not-json",), (None,), (b'{"type":"heartbeat"}',)],
        "value binary",
    )

    rows = parse_trade_messages(kafka_df).collect()

    assert len(rows) == 3
    assert all(value is None for value in rows[0])
    assert all(value is None for value in rows[1])
    assert rows[2].type == "heartbeat"
    assert rows[2].trade_id is None

import os
from datetime import datetime, timedelta
from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.spark.data_quality import assign_continuous_segments, detect_missing_minutes


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .appName("MarketGuard-DataQuality-Test")
        .master("local[1]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_detect_missing_minutes_isolated_by_product(spark):
    start = datetime(2026, 10, 14, 12, 0)
    gold_df = spark.createDataFrame(
        [
            ("BTC-USD", start),
            ("BTC-USD", start + timedelta(minutes=1)),
            ("BTC-USD", start + timedelta(minutes=3)),
            ("ETH-USD", start + timedelta(minutes=2)),
            ("ETH-USD", start + timedelta(minutes=5)),
        ],
        "product_id string, minute_start timestamp",
    )

    result = detect_missing_minutes(gold_df)
    rows = {
        (row.product_id, row.minute_start): row
        for row in result.collect()
    }

    assert result.count() == gold_df.count()

    first_btc = rows[("BTC-USD", start)]
    assert first_btc.previous_minute is None
    assert first_btc.seconds_since_previous is None
    assert first_btc.missing_minutes == 0
    assert first_btc.is_data_gap is False

    next_btc = rows[("BTC-USD", start + timedelta(minutes=1))]
    assert next_btc.seconds_since_previous == 60
    assert next_btc.missing_minutes == 0
    assert next_btc.is_data_gap is False

    btc_gap = rows[("BTC-USD", start + timedelta(minutes=3))]
    assert btc_gap.seconds_since_previous == 120
    assert btc_gap.missing_minutes == 1
    assert btc_gap.is_data_gap is True

    first_eth = rows[("ETH-USD", start + timedelta(minutes=2))]
    assert first_eth.missing_minutes == 0
    assert first_eth.is_data_gap is False

    eth_gap = rows[("ETH-USD", start + timedelta(minutes=5))]
    assert eth_gap.seconds_since_previous == 180
    assert eth_gap.missing_minutes == 2
    assert eth_gap.is_data_gap is True


def test_assign_continuous_segments_starts_a_segment_after_each_gap(spark):
    start = datetime(2026, 10, 14, 12, 0)
    gold_df = spark.createDataFrame(
        [
            ("BTC-USD", start),
            ("BTC-USD", start + timedelta(minutes=1)),
            ("BTC-USD", start + timedelta(minutes=4)),
            ("BTC-USD", start + timedelta(minutes=5)),
            ("ETH-USD", start + timedelta(minutes=2)),
        ],
        "product_id string, minute_start timestamp",
    )

    rows = assign_continuous_segments(detect_missing_minutes(gold_df)).collect()
    segments = {
        (row.product_id, row.minute_start): row.segment_id
        for row in rows
    }

    assert segments[("BTC-USD", start)] == 0
    assert segments[("BTC-USD", start + timedelta(minutes=1))] == 0
    assert segments[("BTC-USD", start + timedelta(minutes=4))] == 1
    assert segments[("BTC-USD", start + timedelta(minutes=5))] == 1
    assert segments[("ETH-USD", start + timedelta(minutes=2))] == 0

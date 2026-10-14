import os
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.spark.anomoly_detection import (
    calculate_price_returns,
    calculate_rolling_volatility,
    detect_price_jumps,
    detect_statistical_price_jumps,
    detect_volume_spikes,
)


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .appName("MarketGuard-AnomalyDetection-Test")
        .master("local[1]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_detect_volume_spikes_uses_product_history(spark):
    start = datetime(2026, 10, 14, 15, 30)
    rows = [
        ("BTC-USD", start + timedelta(minutes=index), Decimal(volume))
        for index, volume in enumerate(["100", "110", "90", "100", "100", "1000"])
    ]
    rows.extend(
        ("ETH-USD", start + timedelta(minutes=index), Decimal("200"))
        for index in range(6)
    )
    schema = StructType(
        [
            StructField("product_id", StringType(), nullable=False),
            StructField("minute_start", TimestampType(), nullable=False),
            StructField("notional_volume", DecimalType(20, 8), nullable=False),
        ]
    )

    result = detect_volume_spikes(
        spark.createDataFrame(rows, schema),
        lookback=5,
        threshold=3.0,
    )
    result.show(truncate=False)
    results = {
        (row.product_id, row.minute_start): row
        for row in result.collect()
    }

    btc_spike = results[("BTC-USD", start + timedelta(minutes=5))]
    assert btc_spike.history_count == 5
    assert btc_spike.volume_zscore > Decimal("3")
    assert btc_spike.is_volume_spike is True

    assert all(
        results[("BTC-USD", start + timedelta(minutes=index))].is_volume_spike
        is False
        for index in range(5)
    )

    eth_flat_volume = results[("ETH-USD", start + timedelta(minutes=5))]
    assert eth_flat_volume.history_count == 5
    assert eth_flat_volume.volume_zscore is None
    assert eth_flat_volume.is_volume_spike is False


def test_detect_volume_spikes_rejects_too_small_lookback(spark):
    empty_gold_df = spark.createDataFrame(
        [],
        "product_id string, minute_start timestamp, notional_volume decimal(20, 8)",
    )

    with pytest.raises(ValueError, match="lookback must be at least 2"):
        detect_volume_spikes(empty_gold_df, lookback=1)


def test_calculate_rolling_volatility_uses_only_full_product_history(spark):
    start = datetime(2026, 10, 14, 15, 30)
    rows = [
        ("BTC-USD", start + timedelta(minutes=index), price_return)
        for index, price_return in enumerate([0.01, -0.01, 0.02, -0.02, 0.00, 0.50])
    ]
    rows.extend(
        ("ETH-USD", start + timedelta(minutes=index), 0.01)
        for index in range(6)
    )
    returns_df = spark.createDataFrame(
        rows,
        "product_id string, minute_start timestamp, price_return double",
    )

    result = calculate_rolling_volatility(returns_df, lookback=5)
    results = {
        (row.product_id, row.minute_start): row
        for row in result.collect()
    }

    btc_current = results[("BTC-USD", start + timedelta(minutes=5))]
    assert btc_current.return_history_count == 5
    assert btc_current.rolling_volatility == pytest.approx(
        0.0158113883,
        abs=1e-10,
    )

    eth_current = results[("ETH-USD", start + timedelta(minutes=5))]
    assert eth_current.return_history_count == 5
    assert eth_current.rolling_volatility == pytest.approx(0.0, abs=1e-10)

    assert all(
        results[("BTC-USD", start + timedelta(minutes=index))].rolling_volatility
        is None
        for index in range(5)
    )


def test_calculate_rolling_volatility_rejects_too_small_lookback(spark):
    empty_returns_df = spark.createDataFrame(
        [],
        "product_id string, minute_start timestamp, price_return double",
    )

    with pytest.raises(ValueError, match="lookback must be at least 2"):
        calculate_rolling_volatility(empty_returns_df, lookback=1)


def test_price_returns_and_rolling_volatility(spark):
    start = datetime(2026, 10, 14, 12, 0)
    rows = [
        ("BTC-USD", start, Decimal("60000.00")),
        ("BTC-USD", start + timedelta(minutes=1), Decimal("60600.00")),
        ("BTC-USD", start + timedelta(minutes=2), Decimal("59994.00")),
        ("BTC-USD", start + timedelta(minutes=3), Decimal("59994.00")),
        ("BTC-USD", start + timedelta(minutes=4), Decimal("61193.88")),
        ("ETH-USD", start, Decimal("0.00")),
        ("ETH-USD", start + timedelta(minutes=1), Decimal("2500.00")),
        ("ETH-USD", start + timedelta(minutes=2), Decimal("2525.00")),
    ]
    gold_df = spark.createDataFrame(
        rows,
        "product_id string, minute_start timestamp, vwap decimal(20, 8)",
    )

    returns_df = calculate_price_returns(gold_df)
    result = calculate_rolling_volatility(returns_df, lookback=3)
    results = {
        (row.product_id, row.minute_start): row
        for row in result.collect()
    }
    tolerance = Decimal("0.000001")

    expected_btc_returns = [None, "0.01", "-0.01", "0", "0.02"]
    for minute, expected in enumerate(expected_btc_returns):
        actual = results[("BTC-USD", start + timedelta(minutes=minute))].price_return
        if expected is None:
            assert actual is None
        else:
            assert abs(actual - Decimal(expected)) <= tolerance

    btc_at_1204 = results[("BTC-USD", start + timedelta(minutes=4))]
    assert btc_at_1204.return_history_count == 3
    assert btc_at_1204.rolling_volatility == pytest.approx(0.01, abs=1e-10)
    assert results[("BTC-USD", start + timedelta(minutes=3))].rolling_volatility is None

    assert results[("ETH-USD", start + timedelta(minutes=1))].price_return is None
    eth_at_1202 = results[("ETH-USD", start + timedelta(minutes=2))]
    assert abs(eth_at_1202.price_return - Decimal("0.01")) <= tolerance


def test_price_return_is_null_across_a_missing_minute(spark):
    start = datetime(2026, 10, 14, 12, 0)
    gold_df = spark.createDataFrame(
        [
            ("BTC-USD", start + timedelta(minutes=1), Decimal("60600.00")),
            ("BTC-USD", start + timedelta(minutes=4), Decimal("61193.88")),
        ],
        "product_id string, minute_start timestamp, vwap decimal(20, 8)",
    )

    results = calculate_price_returns(gold_df).collect()

    assert results[0].price_return is None
    assert results[1].price_return is None


def test_detect_price_jumps_flags_positive_and_negative_moves(spark):
    start = datetime(2026, 10, 14, 12, 0)
    returns_df = spark.createDataFrame(
        [
            ("BTC-USD", start, None),
            ("BTC-USD", start + timedelta(minutes=1), 0.01),
            ("BTC-USD", start + timedelta(minutes=2), 0.02),
            ("BTC-USD", start + timedelta(minutes=3), 0.021),
            ("BTC-USD", start + timedelta(minutes=4), -0.03),
            ("ETH-USD", start, -0.01),
        ],
        "product_id string, minute_start timestamp, price_return double",
    )

    results = detect_price_jumps(returns_df, threshold=0.02).collect()
    flags = {
        (row.product_id, row.minute_start): row.is_price_jump
        for row in results
    }

    assert flags[("BTC-USD", start)] is False
    assert flags[("BTC-USD", start + timedelta(minutes=1))] is False
    assert flags[("BTC-USD", start + timedelta(minutes=2))] is False
    assert flags[("BTC-USD", start + timedelta(minutes=3))] is True
    assert flags[("BTC-USD", start + timedelta(minutes=4))] is True
    assert flags[("ETH-USD", start)] is False


def test_detect_price_jumps_rejects_negative_threshold(spark):
    empty_returns_df = spark.createDataFrame(
        [],
        "product_id string, minute_start timestamp, price_return double",
    )

    with pytest.raises(ValueError, match="threshold must be non-negative"):
        detect_price_jumps(empty_returns_df, threshold=-0.01)


def test_detect_statistical_price_jumps_uses_historical_returns(spark):
    start = datetime(2026, 10, 14, 12, 0)
    rows = [
        ("BTC-USD", start + timedelta(minutes=index), price_return)
        for index, price_return in enumerate([0.01, -0.01, 0.02, -0.02, 0.00, 0.50])
    ]
    rows.extend(
        ("ETH-USD", start + timedelta(minutes=index), price_return)
        for index, price_return in enumerate([0.02, -0.02, 0.01, -0.01, 0.00, -0.50])
    )
    rows.extend(
        ("SOL-USD", start + timedelta(minutes=index), 0.01)
        for index in range(6)
    )
    returns_df = spark.createDataFrame(
        rows,
        "product_id string, minute_start timestamp, price_return double",
    )

    results = {
        (row.product_id, row.minute_start): row
        for row in detect_statistical_price_jumps(
            returns_df,
            lookback=5,
            threshold=3.0,
        ).collect()
    }

    btc_jump = results[("BTC-USD", start + timedelta(minutes=5))]
    assert btc_jump.return_history_count == 5
    assert btc_jump.return_zscore > 3.0
    assert btc_jump.is_statistical_price_jump is True

    eth_jump = results[("ETH-USD", start + timedelta(minutes=5))]
    assert eth_jump.return_zscore < -3.0
    assert eth_jump.is_statistical_price_jump is True

    flat_history = results[("SOL-USD", start + timedelta(minutes=5))]
    assert flat_history.return_zscore is None
    assert flat_history.is_statistical_price_jump is False

    assert all(
        results[("BTC-USD", start + timedelta(minutes=index))]
        .is_statistical_price_jump
        is False
        for index in range(5)
    )


@pytest.mark.parametrize(
    ("lookback", "threshold", "message"),
    [
        (1, 3.0, "lookback must be at least 2"),
        (5, -1.0, "threshold must be non-negative"),
    ],
)
def test_detect_statistical_price_jumps_validates_parameters(
    spark,
    lookback,
    threshold,
    message,
):
    empty_returns_df = spark.createDataFrame(
        [],
        "product_id string, minute_start timestamp, price_return double",
    )

    with pytest.raises(ValueError, match=message):
        detect_statistical_price_jumps(
            empty_returns_df,
            lookback=lookback,
            threshold=threshold,
        )

import os
from datetime import datetime, timedelta
from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.anomoly_pipeline import build_anomaly_table


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .appName("MarketGuard-AnomalyPipeline-Test")
        .master("local[1]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_anomaly_table_classifies_all_anomaly_types(spark):
    start = datetime(2026, 10, 14, 12, 0)
    historical_returns = [0.01, -0.01, 0.02, -0.02, 0.0]
    baseline_volumes = [95.0, 100.0, 110.0, 90.0, 100.0, 100.0]
    scenarios = {
        "BOTH-USD": (1000.0, 0.50, "BOTH"),
        "VOLUME-USD": (1000.0, 0.01, "VOLUME"),
        "PRICE-USD": (100.0, -0.50, "PRICE"),
        "NONE-USD": (100.0, 0.01, "NONE"),
    }
    rows = []

    for product_id, (current_volume, current_return, _) in scenarios.items():
        vwap = 100.0
        rows.append((product_id, start, 1, 1.0, baseline_volumes[0], vwap))
        for minute, price_return in enumerate(historical_returns, start=1):
            vwap *= 1 + price_return
            rows.append(
                (
                    product_id,
                    start + timedelta(minutes=minute),
                    1,
                    1.0,
                    baseline_volumes[minute],
                    vwap,
                )
            )
        vwap *= 1 + current_return
        rows.append(
            (
                product_id,
                start + timedelta(minutes=6),
                1,
                1.0,
                current_volume,
                vwap,
            )
        )

    gold_df = spark.createDataFrame(
        rows,
        "product_id string, minute_start timestamp, trade_count long, "
        "base_volume double, notional_volume double, vwap double",
    )

    anomaly_df = build_anomaly_table(gold_df)
    anomaly_df.show(truncate=False)
    assert anomaly_df.count() == gold_df.count()
    assert anomaly_df.select("product_id", "minute_start").distinct().count() == (
        anomaly_df.count()
    )

    latest_rows = {
        row.product_id: row
        for row in anomaly_df.filter(
            anomaly_df.minute_start == start + timedelta(minutes=6)
        ).collect()
    }

    for product_id, (_, _, expected_type) in scenarios.items():
        row = latest_rows[product_id]
        assert row.anomaly_type == expected_type
        assert row.is_anomaly is (expected_type != "NONE")

    assert latest_rows["BOTH-USD"].is_volume_spike is True
    assert latest_rows["BOTH-USD"].is_statistical_price_jump is True
    assert latest_rows["VOLUME-USD"].is_volume_spike is True
    assert latest_rows["VOLUME-USD"].is_statistical_price_jump is False
    assert latest_rows["PRICE-USD"].is_volume_spike is False
    assert latest_rows["PRICE-USD"].is_statistical_price_jump is True

    insufficient_history_rows = anomaly_df.filter(
        anomaly_df.minute_start < start + timedelta(minutes=5)
    ).collect()
    assert insufficient_history_rows
    assert all(row.is_anomaly is False for row in insufficient_history_rows)
    assert all(row.anomaly_type == "NONE" for row in insufficient_history_rows)


def test_build_anomaly_table_resets_history_after_a_data_gap(spark):
    start = datetime(2026, 10, 14, 12, 0)
    rows = [
        ("BTC-USD", start + timedelta(minutes=minute), 1, 1.0, volume, 100.0)
        for minute, volume in enumerate([90.0, 100.0, 110.0, 90.0, 100.0, 100.0])
    ]
    rows.append(
        (
            "BTC-USD",
            start + timedelta(minutes=10),
            1,
            1.0,
            1000.0,
            150.0,
        )
    )
    gold_df = spark.createDataFrame(
        rows,
        "product_id string, minute_start timestamp, trade_count long, "
        "base_volume double, notional_volume double, vwap double",
    )

    gap_row = build_anomaly_table(gold_df).orderBy("minute_start").collect()[-1]

    assert gap_row.missing_minutes == 4
    assert gap_row.is_data_gap is True
    assert gap_row.segment_id == 1
    assert gap_row.volume_zscore is None
    assert gap_row.price_return is None
    assert gap_row.return_zscore is None
    assert gap_row.is_anomaly is False
    assert gap_row.anomaly_type == "NONE"

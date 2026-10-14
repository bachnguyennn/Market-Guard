import os
from decimal import Decimal
from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.spark.bronze_to_silver import calculate_market_metrics, transform_trades


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .appName("MarketGuard-SilverToGold-Test")
        .master("local[1]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_calculate_market_metrics_groups_by_product_and_minute(spark):
    trades = [
        {
            "type": "match",
            "trade_id": 2001,
            "product_id": "BTC-USD",
            "price": "100.00",
            "size": "2.00",
            "time": "2026-10-14T15:30:10Z",
        },
        {
            "type": "match",
            "trade_id": 2002,
            "product_id": "BTC-USD",
            "price": "110.00",
            "size": "1.00",
            "time": "2026-10-14T15:30:30Z",
        },
        {
            "type": "match",
            "trade_id": 2003,
            "product_id": "BTC-USD",
            "price": "90.00",
            "size": "1.00",
            "time": "2026-10-14T15:30:50Z",
        },
        {
            "type": "match",
            "trade_id": 2004,
            "product_id": "BTC-USD",
            "price": "120.00",
            "size": "0.50",
            "time": "2026-10-14T15:31:05Z",
        },
        {
            "type": "match",
            "trade_id": 2005,
            "product_id": "ETH-USD",
            "price": "20.00",
            "size": "4.00",
            "time": "2026-10-14T15:30:20Z",
        },
    ]

    silver_df = transform_trades(spark.createDataFrame(trades))
    gold_df = calculate_market_metrics(silver_df)

    gold_df.printSchema()
    gold_df.show(truncate=False)
    assert gold_df.count() == 3

    rows = {
        (row.product_id, row.minute_start_utc): row
        for row in gold_df.withColumn(
            "minute_start_utc",
            F.date_format("minute_start", "yyyy-MM-dd HH:mm:ss"),
        ).collect()
    }
    tolerance = Decimal("0.000001")

    first_btc_minute = rows[("BTC-USD", "2026-10-14 15:30:00")]
    assert first_btc_minute.trade_count == 3
    assert abs(first_btc_minute.base_volume - Decimal("4.00")) <= tolerance
    assert abs(first_btc_minute.notional_volume - Decimal("400.00")) <= tolerance
    assert abs(first_btc_minute.vwap - Decimal("100.00")) <= tolerance

    next_btc_minute = rows[("BTC-USD", "2026-10-14 15:31:00")]
    assert next_btc_minute.trade_count == 1
    assert abs(next_btc_minute.base_volume - Decimal("0.50")) <= tolerance
    assert abs(next_btc_minute.notional_volume - Decimal("60.00")) <= tolerance
    assert abs(next_btc_minute.vwap - Decimal("120.00")) <= tolerance

    eth_minute = rows[("ETH-USD", "2026-10-14 15:30:00")]
    assert eth_minute.trade_count == 1
    assert abs(eth_minute.base_volume - Decimal("4.00")) <= tolerance
    assert abs(eth_minute.notional_volume - Decimal("80.00")) <= tolerance
    assert abs(eth_minute.vwap - Decimal("20.00")) <= tolerance

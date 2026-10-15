import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.spark.bronze_to_silver import transform_trades


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .appName("MarketGuard-BronzeToSilver-Test")
        .master("local[1]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_transform_trades_keeps_only_valid_unique_trades(spark):
    btc_trade = {
        "type": "match",
        "trade_id": 1001,
        "product_id": "BTC-USD",
        "price": "68000.50",
        "size": "0.025",
        "time": "2026-10-14T15:30:00Z",
    }
    trades = [
        btc_trade,
        btc_trade.copy(),
        {
            "type": "match",
            "trade_id": 1002,
            "product_id": "ETH-USD",
            "price": "2400.25",
            "size": "0.50",
            "time": "2026-10-14T15:31:00Z",
        },
        {
            "type": "match",
            "trade_id": 1003,
            "product_id": "BTC-USD",
            "price": "-1.00",
            "size": "0.10",
            "time": "2026-10-14T15:32:00Z",
        },
        {
            "type": "match",
            "trade_id": None,
            "product_id": "ETH-USD",
            "price": "2401.00",
            "size": "0.20",
            "time": "2026-10-14T15:33:00Z",
        },
    ]

    result = transform_trades(spark.createDataFrame(trades))

    assert result.count() == 2
    assert {
        (row.product_id, row.trade_id)
        for row in result.select("product_id", "trade_id").collect()
    } == {("BTC-USD", 1001), ("ETH-USD", 1002)}

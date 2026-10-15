import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.spark.delta_pipeline import upsert_silver_trades


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder
        .appName("MarketGuard-DeltaPipeline-Test")
        .master("local[1]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def trades_df(spark, rows):
    return spark.createDataFrame(
        [
            (product_id, trade_id, Decimal(str(price)))
            for product_id, trade_id, price in rows
        ],
        "product_id string, trade_id long, price decimal(20, 8)",
    )


def test_upsert_silver_trades_is_idempotent(spark, tmp_path):
    table_path = str(tmp_path / "silver")

    upsert_silver_trades(
        spark,
        trades_df(
            spark,
            [
                ("BTC-USD", 1001, 68000),
                ("BTC-USD", 1002, 68100),
            ],
        ),
        table_path,
    )
    assert spark.read.format("delta").load(table_path).count() == 2

    second_batch = trades_df(
        spark,
        [
            ("BTC-USD", 1002, 99999),
            ("BTC-USD", 1003, 68200),
        ],
    )
    upsert_silver_trades(spark, second_batch, table_path)
    assert spark.read.format("delta").load(table_path).count() == 3

    upsert_silver_trades(spark, second_batch, table_path)
    assert spark.read.format("delta").load(table_path).count() == 3

    upsert_silver_trades(
        spark,
        trades_df(spark, [("ETH-USD", 1002, 2500)]),
        table_path,
    )
    silver_df = spark.read.format("delta").load(table_path)
    assert silver_df.count() == 4
    assert silver_df.filter(
        "product_id = 'BTC-USD' AND trade_id = 1002"
    ).first().price == 68100


def test_upsert_silver_trades_deduplicates_incoming_batch(spark, tmp_path):
    table_path = str(tmp_path / "silver")
    duplicate_batch = trades_df(
        spark,
        [
            ("BTC-USD", 1001, 68000),
            ("BTC-USD", 1001, 68000),
            ("ETH-USD", 1001, 2500),
        ],
    )

    upsert_silver_trades(spark, duplicate_batch, table_path)

    silver_df = spark.read.format("delta").load(table_path)
    assert silver_df.count() == 2
    assert silver_df.select("product_id", "trade_id").distinct().count() == 2

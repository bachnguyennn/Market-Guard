import json
import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.spark.streaming_pipeline import start_silver_stream


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder
        .appName("MarketGuard-Streaming-Test")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def test_file_stream_validates_discovers_new_files_and_resumes(spark, tmp_path):
    input_path = tmp_path / "input"
    table_path = tmp_path / "silver"
    checkpoint_path = tmp_path / "checkpoint"
    input_path.mkdir()
    trade = {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": 1001,
        "price": "68000.00",
        "size": "0.15",
        "time": "2026-10-14T12:00:00Z",
    }
    initial_rows = [
        trade,
        trade.copy(),
        {**trade, "price": "-1"},
        {**trade, "size": "0"},
        {**trade, "product_id": None},
        {**trade, "trade_id": None},
        {**trade, "price": "bad-price"},
        {**trade, "time": "bad-time"},
        {"type": "heartbeat", "product_id": "BTC-USD"},
    ]
    (input_path / "batch_001.jsonl").write_text(
        "\n".join(json.dumps(row) for row in initial_rows) + "\n",
        encoding="utf-8",
    )
    paths = (str(input_path), str(table_path), str(checkpoint_path))

    query = start_silver_stream(spark, *paths)
    try:
        query.processAllAvailable()
        silver_df = spark.read.format("delta").load(str(table_path))
        assert silver_df.count() == 2
        assert silver_df.first().price == Decimal("68000.00000000")
        assert silver_df.first().size == Decimal("0.15000000")
        assert query.lastProgress["stateOperators"] == []

        (input_path / "batch_002.jsonl").write_text(
            json.dumps({**trade, "product_id": "ETH-USD", "trade_id": 1002}) + "\n",
            encoding="utf-8",
        )
        query.processAllAvailable()
        assert spark.read.format("delta").load(str(table_path)).count() == 3
    finally:
        query.stop()

    query = start_silver_stream(spark, *paths)
    try:
        query.processAllAvailable()
        assert spark.read.format("delta").load(str(table_path)).count() == 3

        (input_path / "batch_003.jsonl").write_text(
            json.dumps({**trade, "trade_id": 1003}) + "\n",
            encoding="utf-8",
        )
        query.processAllAvailable()
        rows = spark.read.format("delta").load(str(table_path)).collect()
        assert len(rows) == 4
        assert {(row.product_id, row.trade_id) for row in rows} == {
            ("BTC-USD", 1001),
            ("ETH-USD", 1002),
            ("BTC-USD", 1003),
        }
        assert (checkpoint_path / "commits").is_dir()
        assert (table_path / "_delta_log").is_dir()
    finally:
        query.stop()

import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.spark.bronze_to_silver import transform_trades
from src.spark.delta_pipeline import upsert_silver_trades
from src.spark.incremental_stream import process_microbatch, start_stream
from src.spark.streaming_pipeline import RAW_TRADE_SCHEMA


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder
        .appName("MarketGuard-IncrementalStream-Test")
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


def make_trade(trade_id, **fields):
    return {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": trade_id,
        "price": "68000.00",
        "size": "0.15",
        "time": "2026-10-14T12:00:00Z",
        **fields,
    }


def publish_file(input_path, filename, trades):
    staging_path = input_path.parent / filename
    staging_path.write_text(
        "\n".join(json.dumps(trade) for trade in trades) + "\n",
        encoding="utf-8",
    )
    staging_path.rename(input_path / filename)


def test_incremental_stream_recovers_with_same_checkpoint(spark, tmp_path):
    input_path = tmp_path / "input"
    input_path.mkdir()
    table_path = str(tmp_path / "silver")
    checkpoint_path = str(tmp_path / "checkpoint")
    empty_df = spark.createDataFrame([], RAW_TRADE_SCHEMA)
    upsert_silver_trades(spark, transform_trades(empty_df), table_path)

    def silver_count():
        return spark.read.format("delta").load(table_path).count()

    counts = [silver_count()]
    raw_df = spark.readStream.schema(RAW_TRADE_SCHEMA).json(str(input_path))
    query = start_stream(raw_df, table_path, checkpoint_path, "1 second")
    try:
        publish_file(input_path, "batch_001.jsonl", [make_trade(1001), make_trade(1002)])
        query.processAllAvailable()
        counts.append(silver_count())

        publish_file(input_path, "batch_002.jsonl", [make_trade(1002), make_trade(1003)])
        query.processAllAvailable()
        counts.append(silver_count())
    finally:
        query.stop()

    query = start_stream(raw_df, table_path, checkpoint_path, "1 second")
    try:
        query.processAllAvailable()
        counts.append(silver_count())

        publish_file(input_path, "batch_003.jsonl", [make_trade(1004)])
        query.processAllAvailable()
        counts.append(silver_count())
    finally:
        query.stop()

    assert counts == [0, 2, 3, 3, 4]
    print(f"Recovery Silver counts: {counts}")
    rows = spark.read.format("delta").load(table_path).collect()
    assert {row.trade_id for row in rows} == {1001, 1002, 1003, 1004}
    assert (Path(checkpoint_path) / "commits").is_dir()


def test_process_microbatch_replay_is_idempotent(spark, tmp_path):
    table_path = str(tmp_path / "silver")
    batch_df = spark.createDataFrame(
        [make_trade(1001), make_trade(1001), make_trade(1002), make_trade(9999, size="0")],
        RAW_TRADE_SCHEMA,
    )

    process_microbatch(batch_df, batch_id=0, table_path=table_path)
    before = spark.read.format("delta").load(table_path).collect()
    process_microbatch(batch_df, batch_id=0, table_path=table_path)
    after = spark.read.format("delta").load(table_path).collect()

    assert len(before) == len(after) == 2
    assert {tuple(row) for row in before} == {tuple(row) for row in after}
    assert {row.trade_id for row in after} == {1001, 1002}
    assert all(row.price == Decimal("68000.00000000") for row in after)


@pytest.mark.parametrize("trades", [[], [make_trade(1001, price="invalid")]])
def test_process_microbatch_skips_empty_validated_batches(spark, monkeypatch, trades):
    upsert = Mock()
    monkeypatch.setattr("src.spark.incremental_stream.upsert_silver_trades", upsert)
    batch_df = spark.createDataFrame(trades, RAW_TRADE_SCHEMA)

    process_microbatch(batch_df, batch_id=0)

    upsert.assert_not_called()


def test_process_microbatch_propagates_delta_failure(spark, monkeypatch):
    upsert = Mock(side_effect=RuntimeError("Delta commit failed"))
    monkeypatch.setattr("src.spark.incremental_stream.upsert_silver_trades", upsert)
    batch_df = spark.createDataFrame([make_trade(1001)], RAW_TRADE_SCHEMA)

    with pytest.raises(RuntimeError, match="Delta commit failed"):
        process_microbatch(batch_df, batch_id=0)

    upsert.assert_called_once()

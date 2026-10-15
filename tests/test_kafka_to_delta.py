import json
import logging
import os
import sys
from unittest.mock import Mock

import pytest
from marketguard.spark import kafka_to_delta
from pyspark.errors import StreamingQueryException


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = kafka_to_delta.create_spark_session("MarketGuard-KafkaToDelta-Test", "local[1]")
    session.conf.set("spark.databricks.delta.snapshotPartitions", "1")
    yield session
    session.stop()


def trade(index):
    return {
        "type": "match",
        "product_id": "BTC-USD" if index < 5 else "ETH-USD",
        "trade_id": 1001 + index % 5,
        "price": "68000.00",
        "size": "0.15",
        "time": "2026-10-14T12:00:00Z",
    }


def kafka_batch(spark, trades):
    return spark.createDataFrame(
        [(json.dumps(event).encode("utf-8"),) for event in trades], "value binary"
    )


def test_kafka_batch_duplicate_replay_does_not_increase_silver_count(spark, tmp_path, caplog):
    silver_path = str(tmp_path / "silver")
    trades = [trade(index) for index in range(10)]
    batch = kafka_batch(spark, [*trades, trades[0], {**trades[0], "price": "invalid"}])

    with caplog.at_level(logging.INFO, logger=kafka_to_delta.__name__):
        kafka_to_delta.process_kafka_batch(batch, 0, silver_path)
    first_rows = spark.read.format("delta").load(silver_path).collect()
    kafka_to_delta.process_kafka_batch(batch, 1, silver_path)
    replayed_rows = spark.read.format("delta").load(silver_path).collect()

    assert len(first_rows) == len(replayed_rows) == 10
    assert {tuple(row) for row in first_rows} == {tuple(row) for row in replayed_rows}
    assert "batch 0 successfully merged 10" in caplog.text

    new_batch = kafka_batch(spark, [{**trade(index), "trade_id": 2001 + index} for index in range(5)])
    kafka_to_delta.process_kafka_batch(new_batch, 2, silver_path)
    silver_df = spark.read.format("delta").load(silver_path)
    assert silver_df.count() == 15
    assert silver_df.select("product_id", "trade_id").distinct().count() == 15


@pytest.mark.parametrize("payloads", [[], [(b"not-json",), (None,), (b'{"type":"heartbeat"}',)]])
def test_kafka_batch_skips_empty_or_invalid_records(spark, monkeypatch, payloads):
    upsert = Mock()
    monkeypatch.setattr(kafka_to_delta, "upsert_silver_trades", upsert)

    kafka_to_delta.process_kafka_batch(spark.createDataFrame(payloads, "value binary"), 0)

    upsert.assert_not_called()


def test_kafka_batch_write_failure_propagates(spark, monkeypatch, tmp_path, caplog):
    upsert = Mock(side_effect=RuntimeError("Delta write failed"))
    monkeypatch.setattr(kafka_to_delta, "upsert_silver_trades", upsert)

    with (
        caplog.at_level(logging.INFO, logger=kafka_to_delta.__name__),
        pytest.raises(RuntimeError, match="Delta write failed"),
    ):
        kafka_to_delta.process_kafka_batch(
            kafka_batch(spark, [trade(0)]), 0, str(tmp_path / "silver")
        )

    upsert.assert_called_once()
    assert "successfully merged" not in caplog.text
    assert not upsert.call_args.args[1].is_cached


def test_delta_commit_before_checkpoint_failure_replays_safely(spark, monkeypatch, tmp_path):
    input_path = tmp_path / "input"
    input_path.mkdir()
    silver_path = str(tmp_path / "silver")
    checkpoint_path = tmp_path / "checkpoint"
    (input_path / "batch.jsonl").write_text(
        "\n".join(json.dumps({"value": json.dumps(trade(index))}) for index in range(10)),
        encoding="utf-8",
    )
    source = spark.readStream.schema("value string").json(str(input_path))
    real_upsert = kafka_to_delta.upsert_silver_trades

    def commit_then_fail(session, batch, table_path):
        real_upsert(session, batch, table_path)
        raise RuntimeError("Injected failure after Delta commit")

    monkeypatch.setattr(kafka_to_delta, "upsert_silver_trades", commit_then_fail)
    query = kafka_to_delta.start_stream(
        source, silver_path, str(checkpoint_path), available_now=True
    )
    try:
        with pytest.raises(StreamingQueryException, match="Injected failure after Delta commit"):
            query.awaitTermination(60)
    finally:
        if query.isActive:
            query.stop()

    assert spark.read.format("delta").load(silver_path).count() == 10
    assert (checkpoint_path / "offsets" / "0").is_file()
    assert not (checkpoint_path / "commits" / "0").exists()

    monkeypatch.setattr(kafka_to_delta, "upsert_silver_trades", real_upsert)
    query = kafka_to_delta.start_stream(
        source, silver_path, str(checkpoint_path), available_now=True
    )
    try:
        assert query.awaitTermination(60)
    finally:
        if query.isActive:
            query.stop()

    assert spark.read.format("delta").load(silver_path).count() == 10
    assert (checkpoint_path / "commits" / "0").is_file()

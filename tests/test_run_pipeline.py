import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from marketguard.run_pipeline import run_batch
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .appName("MarketGuard-BatchPipeline-Test")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_trade(trade_id, minute, price, **fields):
    return {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": trade_id,
        "price": price,
        "size": "1.00",
        "time": f"2026-10-14T12:{minute:02d}:00Z",
        **fields,
    }


def test_batch_pipeline_persists_verified_outputs_and_detects_anomaly(spark, tmp_path):
    prices = ["100", "101", "99.99", "101.9898", "99.950004", "99.950004", "149.925006"]
    trades = [
        make_trade(1000 + minute, minute, price)
        for minute, price in enumerate(prices)
    ]
    trades[-1]["size"] = "10.00"
    trades.extend(
        [
            make_trade(1001, 0, "20", product_id="ETH-USD"),
            make_trade(1002, 1, "21", product_id="ETH-USD"),
            trades[0].copy(),
            make_trade(9999, 0, "-1"),
            {"type": "heartbeat", "product_id": "BTC-USD"},
        ]
    )
    bronze_path = tmp_path / "bronze.jsonl"
    bronze_path.write_text(
        "\n".join(json.dumps(trade) for trade in trades) + "\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "outputs"

    summary = run_batch(spark, bronze_path, output_dir)

    assert summary["bronze_count"] == 12
    assert summary["silver_count"] == 9
    assert summary["excluded_count"] == 3
    assert summary["gold_count"] == 9
    assert summary["anomaly_count"] == 9
    assert summary["alert_count"] == 1
    for stage, expected_count in [("silver", 9), ("gold", 9), ("anomalies", 9)]:
        output_path = Path(summary["outputs"][stage])
        assert (output_path / "_SUCCESS").is_file()
        assert list(output_path.glob("*.parquet"))
        assert spark.read.parquet(str(output_path)).count() == expected_count

    silver_df = spark.read.parquet(summary["outputs"]["silver"])
    assert silver_df.select("product_id", "trade_id").distinct().count() == 9
    gold_df = spark.read.parquet(summary["outputs"]["gold"])
    anomaly_df = spark.read.parquet(summary["outputs"]["anomalies"])
    gold_keys = {(row.product_id, row.minute_start) for row in gold_df.collect()}
    anomaly_keys = {(row.product_id, row.minute_start) for row in anomaly_df.collect()}
    assert gold_keys == anomaly_keys
    alert = anomaly_df.filter("is_anomaly").first()
    assert alert.product_id == "BTC-USD"
    assert alert.anomaly_type == "BOTH"


def test_batch_pipeline_rejects_invalid_only_input(spark, tmp_path):
    bronze_path = tmp_path / "bronze.jsonl"
    bronze_path.write_text(json.dumps(make_trade(1001, 0, "invalid")) + "\n")

    with pytest.raises(ValueError, match="no valid trades"):
        run_batch(spark, bronze_path, tmp_path / "outputs")

    assert not (tmp_path / "outputs").exists()


def test_batch_pipeline_rejects_missing_input(spark, tmp_path):
    with pytest.raises(FileNotFoundError, match="Bronze input does not exist"):
        run_batch(spark, tmp_path / "missing", tmp_path / "outputs")


def test_batch_pipeline_rejects_overlapping_input_and_outputs(spark, tmp_path):
    bronze_path = tmp_path / "bronze"
    bronze_path.mkdir()

    with pytest.raises(ValueError, match="must not overlap"):
        run_batch(spark, bronze_path, bronze_path / "outputs")


def test_installed_pipeline_module_is_executable_from_another_directory(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "marketguard.run_pipeline", "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )

    assert "--bronze" in result.stdout
    assert "--output-dir" in result.stdout

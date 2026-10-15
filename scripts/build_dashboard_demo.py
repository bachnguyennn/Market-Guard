import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

import pandas as pd
from marketguard.anomoly_pipeline import build_anomaly_table
from marketguard.demo_data import synthetic_market_history
from marketguard.demo_verifier import verify_gold_records
from marketguard.run_pipeline import write_and_verify
from marketguard.spark.bronze_to_silver import calculate_market_metrics, transform_trades
from pyspark.sql import SparkSession


def main() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    output = Path(__file__).resolve().parents[1] / "data" / "demo"
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("MarketGuard-PublicDemoSnapshot")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    try:
        trades = synthetic_market_history()
        silver = transform_trades(spark.createDataFrame(trades))
        if silver.count() != 100:
            raise RuntimeError("Synthetic Silver must contain 100 unique trades")
        with TemporaryDirectory(prefix="marketguard-public-demo-") as temporary:
            temporary_path = Path(temporary)
            gold, _ = write_and_verify(calculate_market_metrics(silver), temporary_path / "gold")
            write_and_verify(build_anomaly_table(gold), temporary_path / "anomalies")
            verify_gold_records(trades, [row.asDict() for row in gold.selectExpr(
                "*", "cast(minute_start as long) as minute_epoch",
            ).collect()])
            gold_data = pd.read_parquet(temporary_path / "gold")
            anomaly_data = pd.read_parquet(temporary_path / "anomalies")
            for frame in [gold_data, anomaly_data]:
                frame["minute_start"] = pd.to_datetime(frame["minute_start"], utc=True)
                frame.sort_values(["product_id", "minute_start"], inplace=True)
            alerts = anomaly_data.loc[anomaly_data["is_anomaly"]]
            expected_alerts = {
                ("BTC-USD", pd.Timestamp("2026-10-14T12:12:00Z"), "VOLUME"),
                ("ETH-USD", pd.Timestamp("2026-10-14T12:13:00Z"), "PRICE"),
            }
            actual_alerts = set(alerts[["product_id", "minute_start", "anomaly_type"]].itertuples(index=False, name=None))
            if len(gold_data) != 30 or len(anomaly_data) != 30 or actual_alerts != expected_alerts:
                raise RuntimeError("The synthetic dashboard snapshot failed its alert checks")
            output.mkdir(parents=True, exist_ok=True)
            gold_data.to_parquet(output / "gold.parquet", index=False)
            anomaly_data.to_parquet(output / "anomalies.parquet", index=False)
        print(json.dumps({"synthetic": True, "trade_count": 100, "gold_rows": 30, "anomaly_rows": 30, "alert_count": 2}, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

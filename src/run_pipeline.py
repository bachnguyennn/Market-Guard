import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .anomoly_pipeline import build_anomaly_table
from .spark.bronze_to_silver import calculate_market_metrics, transform_trades
from .spark.streaming_pipeline import RAW_TRADE_SCHEMA


def write_and_verify(df: DataFrame, output_path: Path) -> tuple[DataFrame, int]:
    """Write a batch snapshot and verify that its saved records can be read."""
    cached_df = df.persist()
    try:
        expected_count = cached_df.count()
        cached_df.write.mode("overwrite").parquet(str(output_path))
    finally:
        cached_df.unpersist()

    if not (output_path / "_SUCCESS").is_file():
        raise RuntimeError(f"Missing successful-write marker: {output_path}")
    if not any(output_path.glob("*.parquet")):
        raise RuntimeError(f"No Parquet files were written: {output_path}")

    saved_df = df.sparkSession.read.parquet(str(output_path))
    saved_count = saved_df.count()
    if saved_count != expected_count:
        raise RuntimeError(
            f"Record count changed at {output_path}: "
            f"expected {expected_count}, read {saved_count}"
        )
    return saved_df, saved_count


def run_batch(
    spark: SparkSession,
    bronze_path: str | Path,
    output_dir: str | Path,
) -> dict:
    """Run a finite local Bronze-to-anomalies batch and verify every output."""
    bronze_path = Path(bronze_path).resolve()
    output_dir = Path(output_dir).resolve()
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze input does not exist: {bronze_path}")
    if bronze_path.is_relative_to(output_dir) or output_dir.is_relative_to(bronze_path):
        raise ValueError("Bronze input and output directories must not overlap")

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    bronze_df = spark.read.schema(RAW_TRADE_SCHEMA).json(str(bronze_path))
    bronze_count = bronze_df.count()
    silver_df = transform_trades(bronze_df)
    if silver_df.isEmpty():
        raise ValueError("Bronze input contains no valid trades")

    silver_path = output_dir / "silver"
    gold_path = output_dir / "gold"
    anomaly_path = output_dir / "anomalies"
    saved_silver, silver_count = write_and_verify(silver_df, silver_path)
    gold_df = calculate_market_metrics(saved_silver)
    saved_gold, gold_count = write_and_verify(gold_df, gold_path)
    aggregated_count = saved_gold.agg(F.sum("trade_count")).first()[0]
    if aggregated_count != silver_count:
        raise RuntimeError("Gold trade counts do not match the Silver record count")

    anomaly_df = build_anomaly_table(saved_gold)
    saved_anomalies, anomaly_count = write_and_verify(anomaly_df, anomaly_path)
    if anomaly_count != gold_count:
        raise RuntimeError("Anomaly output does not preserve the Gold record count")

    return {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "excluded_count": bronze_count - silver_count,
        "gold_count": gold_count,
        "anomaly_count": anomaly_count,
        "alert_count": saved_anomalies.filter(F.col("is_anomaly")).count(),
        "outputs": {
            "silver": str(silver_path),
            "gold": str(gold_path),
            "anomalies": str(anomaly_path),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a finite local Bronze → Silver → Gold → anomalies batch."
    )
    parser.add_argument("--bronze", default="data/spark_input")
    parser.add_argument("--output-dir", default="data/batch_output")
    args = parser.parse_args()
    spark = (
        SparkSession.builder
        .appName("MarketGuard-BatchPipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        summary = run_batch(spark, args.bronze, args.output_dir)
        print(json.dumps(summary, indent=2))
        print("Pipeline completed successfully; all outputs verified.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

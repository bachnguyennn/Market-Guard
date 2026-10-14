
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .spark.anomoly_detection import (
    calculate_price_returns,
    detect_statistical_price_jumps,
    detect_volume_spikes,
)


def build_anomaly_table(gold_df: DataFrame) -> DataFrame:
    """
    Build unified anomaly indicators from
    one-minute Gold market metrics.
    """

    volume_df = detect_volume_spikes(gold_df)
    returns_df = calculate_price_returns(volume_df)
    scored_df = detect_statistical_price_jumps(returns_df)

    is_volume_spike = F.col("is_volume_spike")
    is_price_jump = F.col("is_statistical_price_jump")

    return (
        scored_df.withColumn(
            "is_anomaly",
            is_volume_spike | is_price_jump,
        )
        .withColumn(
            "anomaly_type",
            F.when(is_volume_spike & is_price_jump, F.lit("BOTH"))
            .when(is_volume_spike, F.lit("VOLUME"))
            .when(is_price_jump, F.lit("PRICE"))
            .otherwise(F.lit("NONE")),
        )
        .select(
            "product_id",
            "minute_start",
            "trade_count",
            "base_volume",
            "notional_volume",
            "vwap",
            "volume_zscore",
            "price_return",
            "return_zscore",
            "is_volume_spike",
            "is_statistical_price_jump",
            "is_anomaly",
            "anomaly_type",
        )
        .orderBy("product_id", "minute_start")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("MarketGuard-AnomalyPipeline")
        .master("local[*]")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        gold_df = spark.read.parquet("data/gold")
        anomaly_df = build_anomaly_table(gold_df)
        anomaly_df.select(
            "product_id",
            "minute_start",
            "volume_zscore",
            "return_zscore",
            "anomaly_type",
        ).show(truncate=False)
        anomaly_df.write.mode("overwrite").parquet("data/anomalies")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

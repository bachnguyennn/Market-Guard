from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType


def transform_trades(df: DataFrame) -> DataFrame:
    """
    Convert raw Coinbase trade data
    into validated Silver records.
    """

    return validate_trades(df).dropDuplicates(["product_id", "trade_id"])


def validate_trades(df: DataFrame) -> DataFrame:
    """Validate and cast trades without maintaining deduplication state."""
    return (
        df.filter(F.col("type") == "match")
        .withColumn("price", F.col("price").try_cast(DecimalType(20, 8)))
        .withColumn("size", F.col("size").try_cast(DecimalType(20, 8)))
        .withColumn("trade_id", F.col("trade_id").try_cast(LongType()))
        .withColumn("time", F.try_to_timestamp("time"))
        .filter(
            F.col("trade_id").isNotNull()
            & F.col("product_id").isNotNull()
            & (F.col("price") > 0)
            & (F.col("size") > 0)
            & F.col("time").isNotNull()
        )
    )


def main():
    spark = (
        SparkSession.builder
        .appName("MarketGuard-BronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        bronze_df = spark.read.json("data/spark_input")
        bronze_count = bronze_df.count()
        print(f"Bronze records: {bronze_count}")

        silver_df = transform_trades(bronze_df)

        silver_df.printSchema()
        silver_count = silver_df.count()
        print(f"Silver records before write: {silver_count}")

        excluded_count = bronze_count - silver_count
        print(f"Excluded records: {excluded_count}")

        silver_df.write.mode("overwrite").parquet("data/silver")

        saved_silver_df = spark.read.parquet("data/silver")
        saved_count = saved_silver_df.count()
        print(f"Silver records after read: {saved_count}")

        if saved_count != silver_count:
            raise RuntimeError(
                "Silver record count changed after writing Parquet"
            )

        gold_df = calculate_market_metrics(saved_silver_df)
        gold_df.show(truncate=False)
        gold_df.write.mode("overwrite").parquet("data/gold")
    finally:
        spark.stop()


def calculate_market_metrics(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Aggregate validated trades into
    one-minute market metrics.
    """

    return (
        silver_df.withColumn("notional", F.col("price") * F.col("size"))
        .groupBy("product_id", F.window(F.col("time"), "1 minute"))
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("size").alias("base_volume"),
            F.sum("notional").alias("notional_volume"),
        )
        .withColumn("vwap", F.col("notional_volume") / F.col("base_volume"))
        .select(
            "product_id",
            F.col("window.start").alias("minute_start"),
            "trade_count",
            "base_volume",
            "notional_volume",
            "vwap",
        )
        .orderBy("product_id", "minute_start")
    )


if __name__ == "__main__":
    main()

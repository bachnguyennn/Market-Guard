from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import LongType, StringType, StructField, StructType

from .bronze_to_silver import validate_trades

RAW_TRADE_SCHEMA = StructType(
    [
        StructField("type", StringType()),
        StructField("product_id", StringType()),
        StructField("trade_id", LongType()),
        StructField("price", StringType()),
        StructField("size", StringType()),
        StructField("time", StringType()),
        StructField("side", StringType()),
        StructField("sequence", LongType()),
        StructField("maker_order_id", StringType()),
        StructField("taker_order_id", StringType()),
    ]
)


def start_silver_stream(
    spark: SparkSession,
    input_path: str = "data/stream_input",
    table_path: str = "data/silver_stream_delta",
    checkpoint_path: str = "data/checkpoints/silver_stream",
) -> StreamingQuery:
    """Append validated local JSONL trades to Delta using a durable checkpoint."""
    Path(input_path).mkdir(parents=True, exist_ok=True)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    raw_df = (
        spark.readStream.schema(RAW_TRADE_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(input_path)
    )
    cleaned_df = validate_trades(raw_df)

    return (
        cleaned_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(table_path)
    )


def main() -> None:
    builder = (
        SparkSession.builder
        .appName("MarketGuard-Streaming")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    query = None
    try:
        query = start_silver_stream(spark)
        query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()

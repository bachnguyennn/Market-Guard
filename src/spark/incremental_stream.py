from functools import partial
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from .bronze_to_silver import transform_trades
from .delta_pipeline import upsert_silver_trades
from .streaming_pipeline import RAW_TRADE_SCHEMA

INPUT_PATH = "data/incremental_stream_input"
DELTA_PATH = "data/silver_delta"
CHECKPOINT_PATH = "data/checkpoints/incremental_silver"


def process_microbatch(
    batch_df: DataFrame,
    batch_id: int,
    table_path: str = DELTA_PATH,
) -> None:
    """Validate and merge one batch, allowing failed writes to propagate."""
    silver_df = transform_trades(batch_df)
    if silver_df.isEmpty():
        return

    upsert_silver_trades(batch_df.sparkSession, silver_df, table_path)


def start_stream(
    streaming_df: DataFrame,
    table_path: str = DELTA_PATH,
    checkpoint_path: str = CHECKPOINT_PATH,
    processing_time: str = "10 seconds",
) -> StreamingQuery:
    """Incrementally merge raw events using a stable query checkpoint."""
    return (
        streaming_df.writeStream
        .foreachBatch(partial(process_microbatch, table_path=table_path))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=processing_time)
        .start()
    )


def main() -> None:
    builder = (
        SparkSession.builder
        .appName("MarketGuard-IncrementalSilver")
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
        Path(INPUT_PATH).mkdir(parents=True, exist_ok=True)
        raw_df = (
            spark.readStream.schema(RAW_TRADE_SCHEMA)
            .option("maxFilesPerTrigger", 1)
            .json(INPUT_PATH)
        )
        query = start_stream(raw_df)
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

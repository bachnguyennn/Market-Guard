import argparse
import logging
from functools import partial

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from .bronze_to_silver import transform_trades
from .delta_pipeline import upsert_silver_trades
from .kafka_stream import BOOTSTRAP_SERVERS, KAFKA_PACKAGE, TOPIC, parse_trade_messages

SILVER_PATH = "data/streaming/silver_delta"
CHECKPOINT_PATH = "data/checkpoints/kafka_to_delta"
LOGGER = logging.getLogger(__name__)


def process_kafka_batch(
    batch_df: DataFrame,
    batch_id: int,
    silver_path: str = SILVER_PATH,
) -> None:
    """Parse, validate, and MERGE a batch; propagate failures for checkpoint retry."""
    silver_df = transform_trades(parse_trade_messages(batch_df)).persist()
    try:
        row_count = silver_df.count()
        if row_count == 0:
            LOGGER.info("Kafka batch %s skipped: no valid trades", batch_id)
            return

        upsert_silver_trades(batch_df.sparkSession, silver_df, silver_path)
        LOGGER.info(
            "Kafka batch %s successfully merged %s validated trades into %s",
            batch_id,
            row_count,
            silver_path,
        )
    finally:
        silver_df.unpersist()


def create_spark_session(
    app_name: str = "MarketGuard-KafkaToDelta",
    master: str = "local[*]",
) -> SparkSession:
    """Load matching Kafka and Delta JVM dependencies into one Spark session."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(
        builder, extra_packages=[KAFKA_PACKAGE]
    ).getOrCreate()


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    topic: str = TOPIC,
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def start_stream(
    kafka_df: DataFrame,
    silver_path: str = SILVER_PATH,
    checkpoint_path: str = CHECKPOINT_PATH,
    available_now: bool = False,
    processing_time: str = "10 seconds",
) -> StreamingQuery:
    writer = (
        kafka_df.writeStream
        .foreachBatch(partial(process_kafka_batch, silver_path=silver_path))
        .option("checkpointLocation", checkpoint_path)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=processing_time)
    return writer.start()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Kafka trades into Silver Delta.")
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--silver-path", default=SILVER_PATH)
    parser.add_argument("--checkpoint", default=CHECKPOINT_PATH)
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--processing-time", default="10 seconds")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spark = create_spark_session(master=args.master)
    query = None
    try:
        kafka_df = read_kafka_stream(spark, args.bootstrap_servers, args.topic)
        query = start_stream(
            kafka_df, args.silver_path, args.checkpoint, args.available_now,
            processing_time=args.processing_time,
        )
        query.awaitTermination()
        if DeltaTable.isDeltaTable(spark, args.silver_path):
            count = spark.read.format("delta").load(args.silver_path).count()
            print(f"Silver unique trades: {count}")
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

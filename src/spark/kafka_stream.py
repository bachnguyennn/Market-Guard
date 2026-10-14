import argparse

from pyspark import __version__ as spark_version
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery

TOPIC = "marketguard.trades.raw"
BOOTSTRAP_SERVERS = "localhost:9092"
CHECKPOINT_PATH = "data/checkpoints/kafka_console"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"

TRADE_SCHEMA = T.StructType(
    [
        T.StructField("type", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("trade_id", T.StringType()),
        T.StructField("price", T.StringType()),
        T.StructField("size", T.StringType()),
        T.StructField("time", T.StringType()),
    ]
)


def parse_trade_messages(kafka_df: DataFrame) -> DataFrame:
    """Decode Kafka's binary message values using an explicit raw trade schema."""
    return kafka_df.select(
        F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("trade")
    ).select("trade.*")


def start_console_stream(
    spark: SparkSession,
    checkpoint_path: str = CHECKPOINT_PATH,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    topic: str = TOPIC,
    available_now: bool = False,
) -> StreamingQuery:
    """Display parsed Kafka records and checkpoint this query's progress."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    writer = (
        parse_trade_messages(kafka_df).writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_path)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="5 seconds")
    return writer.start()


def main() -> None:
    parser = argparse.ArgumentParser(description="Display Coinbase trades from Kafka.")
    parser.add_argument("--checkpoint", default=CHECKPOINT_PATH)
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process currently available records and exit.",
    )
    args = parser.parse_args()
    spark = (
        SparkSession.builder
        .appName("MarketGuard-KafkaStreaming")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    query = None
    try:
        query = start_console_stream(
            spark,
            checkpoint_path=args.checkpoint,
            available_now=args.available_now,
        )
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

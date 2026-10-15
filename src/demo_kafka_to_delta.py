import argparse
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .anomoly_pipeline import build_anomaly_table
from .run_pipeline import write_and_verify
from .spark.bronze_to_silver import calculate_market_metrics
from .spark.kafka_to_delta import create_spark_session, read_kafka_stream, start_stream


def synthetic_trades() -> list[dict]:
    prices = {
        "BTC-USD": ["100", "101", "99.99", "101.9898", "99.950004", "99.950004", "149.925006", "150"],
        "ETH-USD": ["20", "20.2", "19.998", "20.39796", "19.9900008", "19.9900008", "19.99"],
    }
    start = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
    trades = []
    for index in range(15):
        product_id = "BTC-USD" if index % 2 == 0 else "ETH-USD"
        minute = index // 2
        trades.append(
            {
                "type": "match",
                "product_id": product_id,
                "trade_id": 1001 + index,
                "price": prices[product_id][minute],
                "size": "10" if product_id == "BTC-USD" and minute == 6 else "1",
                "time": (start + timedelta(minutes=minute)).isoformat(),
            }
        )
    return trades


def publish_trades(producer: Producer, topic: str, trades: list[dict]) -> dict[int, int]:
    failures = []
    offsets = {}

    def delivery_report(error, message):
        if error is not None:
            failures.append(str(error))
        else:
            partition = message.partition()
            offsets[partition] = max(offsets.get(partition, 0), message.offset() + 1)

    for trade in trades:
        producer.produce(
            topic=topic,
            key=trade["product_id"],
            value=json.dumps(trade),
            on_delivery=delivery_report,
        )
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining or failures:
        raise RuntimeError(f"Kafka delivery failed: pending={remaining}, errors={failures}")
    return offsets


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Kafka replay and refresh dashboard data.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--run-dir")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run_id = uuid4().hex
    run_dir = Path(args.run_dir or f"data/streaming/demo_{run_id[:8]}").resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    topic = f"marketguard.trades.demo.{run_id}"
    broker_config = {
        "bootstrap.servers": args.bootstrap_servers,
        "broker.address.family": "v4",
    }
    admin = AdminClient(broker_config)
    admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])[
        topic
    ].result(timeout=30)
    producer = Producer({**broker_config, "client.id": "marketguard-replay-demo"})
    silver_path = str(run_dir / "silver_delta")
    checkpoint_path = str(run_dir / "checkpoint")
    trades = synthetic_trades()
    spark = create_spark_session("MarketGuard-ReplayDemo", "local[2]")

    def consume_available() -> int:
        kafka_df = read_kafka_stream(spark, args.bootstrap_servers, topic)
        query = start_stream(kafka_df, silver_path, checkpoint_path, available_now=True)
        try:
            query.awaitTermination()
        finally:
            if query.isActive:
                query.stop()
        return spark.read.format("delta").load(silver_path).count()

    try:
        counts = []
        publish_trades(producer, topic, trades[:10])
        counts.append(consume_available())
        publish_trades(producer, topic, trades[:10])
        counts.append(consume_available())
        spark.stop()
        spark = create_spark_session("MarketGuard-ReplayDemo-Restart", "local[2]")
        counts.append(consume_available())
        publish_trades(producer, topic, trades[10:])
        counts.append(consume_available())
        if counts != [10, 10, 10, 15]:
            raise RuntimeError(f"Replay verification failed: {counts}")

        silver_df = spark.read.format("delta").load(silver_path)
        if silver_df.select("product_id", "trade_id").distinct().count() != 15:
            raise RuntimeError("Silver trade keys are not unique")
        output_dir = run_dir / "dashboard"
        gold_df, gold_count = write_and_verify(
            calculate_market_metrics(silver_df), output_dir / "gold"
        )
        anomalies_df, anomaly_count = write_and_verify(
            build_anomaly_table(gold_df), output_dir / "anomalies"
        )
        summary = {
            "topic": topic,
            "silver_counts": counts,
            "silver_path": silver_path,
            "checkpoint_path": checkpoint_path,
            "gold_path": str(output_dir / "gold"),
            "anomaly_path": str(output_dir / "anomalies"),
            "gold_count": gold_count,
            "anomaly_count": anomaly_count,
            "alert_count": anomalies_df.filter("is_anomaly").count(),
        }
        (run_dir / "summary.json").write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps(summary, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

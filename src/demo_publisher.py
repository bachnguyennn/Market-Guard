import argparse
import json
from pathlib import Path

from confluent_kafka import Producer

from .demo_data import synthetic_market_history


def publish_trades(producer: Producer, topic: str, trades: list[dict]) -> dict[int, int]:
    """Publish events and return acknowledged next offsets, per partition."""
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
            topic=topic, key=trade["product_id"], value=json.dumps(trade),
            on_delivery=delivery_report,
        )
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining or failures:
        raise RuntimeError(f"Kafka delivery failed: pending={remaining}, errors={failures}")
    return offsets


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish 100 deterministic Coinbase-style demo trades.")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers, "broker.address.family": "v4",
        "client.id": "marketguard-demo-publisher",
    })
    trades = synthetic_market_history()
    offsets = publish_trades(producer, args.topic, trades)
    receipt = json.dumps({"topic": args.topic, "trade_count": len(trades), "offsets": offsets}, indent=2)
    if args.receipt:
        args.receipt.write_text(receipt + "\n", encoding="utf-8")
    print(receipt)


if __name__ == "__main__":
    main()

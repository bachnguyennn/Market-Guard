
import json

from confluent_kafka import Producer

TOPIC = "marketguard.trades.raw"


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": "localhost:9092",
        "client.id": "marketguard-producer",
    })


def publish_trade(
    producer: Producer,
    trade: dict,
) -> None:
    """
    Publish one trade event to Kafka.
    """
    product_id = trade["product_id"]
    payload = json.dumps(trade)

    producer.produce(
        topic=TOPIC,
        key=product_id,
        value=payload,
    )
    producer.poll(0)


def main() -> None:
    trade = {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": 1001,
        "price": "68000.00",
        "size": "0.15",
        "time": "2026-10-14T12:00:00Z",
    }
    producer = create_producer()
    publish_trade(producer, trade)

    undelivered_count = producer.flush()
    if undelivered_count:
        raise RuntimeError(
            f"Kafka producer still has {undelivered_count} undelivered message(s)"
        )

    print(f"Published synthetic trade {trade['trade_id']} to {TOPIC}")


if __name__ == "__main__":
    main()

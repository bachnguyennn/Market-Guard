import json

from confluent_kafka import Consumer, KafkaError, KafkaException

TOPIC = "marketguard.trades.raw"


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "marketguard-bronze-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def consume_trades(
    max_messages: int | None = None,
    poll_timeout: float = 1.0,
) -> None:
    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    processed_count = 0

    try:
        while max_messages is None or processed_count < max_messages:
            message = consumer.poll(poll_timeout)
            if message is None:
                continue

            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)

            trade = json.loads(message.value().decode("utf-8"))
            print(
                f"Consumed trade {trade['trade_id']} "
                f"for {trade['product_id']}"
            )
            consumer.commit(message=message, asynchronous=False)
            processed_count += 1
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_trades()

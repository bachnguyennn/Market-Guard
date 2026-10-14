import asyncio
import json

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from ..pipeline import process_batch

TOPIC = "marketguard.trades.raw"
BATCH_SIZE = 10


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "marketguard-bronze-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )


def commit_offsets(
    consumer: Consumer,
    last_offsets: dict[tuple[str, int], int],
) -> None:
    offsets = [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in sorted(last_offsets.items())
    ]
    committed_offsets = consumer.commit(
        offsets=offsets,
        asynchronous=False,
    )

    for committed_offset in committed_offsets or []:
        if committed_offset.error is not None:
            raise KafkaException(committed_offset.error)


async def run_consumer(
    batch_size: int = BATCH_SIZE,
    max_batches: int | None = None,
    poll_timeout: float = 1.0,
) -> None:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    buffer = []
    last_offsets: dict[tuple[str, int], int] = {}
    completed_batches = 0

    try:
        while max_batches is None or completed_batches < max_batches:
            message = await asyncio.to_thread(consumer.poll, poll_timeout)
            if message is None:
                continue

            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)

            trade = json.loads(message.value().decode("utf-8"))
            buffer.append(trade)
            offset_key = (message.topic(), message.partition())
            last_offsets[offset_key] = message.offset()

            if len(buffer) < batch_size:
                continue

            await process_batch(buffer)
            commit_offsets(consumer, last_offsets)
            buffer = []
            last_offsets = {}
            completed_batches += 1
    finally:
        consumer.close()


if __name__ == "__main__":
    asyncio.run(run_consumer())

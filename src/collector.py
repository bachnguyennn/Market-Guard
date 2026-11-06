
import asyncio
import json
from datetime import UTC, datetime

import websockets

from .pipeline import process_batch, process_quote_batch

COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"
BATCH_SIZE = 100
FLUSH_INTERVAL = 5
# Quotes arrive about as fast as trades, so longer batches limit the blob count.
QUOTE_BATCH_SIZE = 1000
QUOTE_FLUSH_INTERVAL = 60
buffer = []
quote_buffer = []


def hand_off_buffer(task_group: asyncio.TaskGroup) -> None:
    global buffer

    if not buffer:
        return

    batch = buffer
    buffer = []
    task_group.create_task(process_batch(batch))


def hand_off_quote_buffer(task_group: asyncio.TaskGroup) -> None:
    global quote_buffer

    if not quote_buffer:
        return

    batch = quote_buffer
    quote_buffer = []
    task_group.create_task(process_quote_batch(batch))


async def collect_trades(task_group: asyncio.TaskGroup) -> None:
    subscription = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["matches", "heartbeat", "ticker"],
    }

    async with websockets.connect(COINBASE_URL) as websocket:
        
        await websocket.send(json.dumps(subscription))

        async for message in websocket:
            response = json.loads(message)
            if response.get("type") == "match":
                buffer.append(response)
                if len(buffer) >= BATCH_SIZE:
                    hand_off_buffer(task_group)
            elif response.get("type") == "ticker":
                response["received_at"] = datetime.now(UTC).isoformat()
                quote_buffer.append(response)
                if len(quote_buffer) >= QUOTE_BATCH_SIZE:
                    hand_off_quote_buffer(task_group)


async def flush_buffer(task_group: asyncio.TaskGroup) -> None:
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        hand_off_buffer(task_group)


async def flush_quote_buffer(task_group: asyncio.TaskGroup) -> None:
    while True:
        await asyncio.sleep(QUOTE_FLUSH_INTERVAL)
        hand_off_quote_buffer(task_group)


async def main():
    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(collect_trades(task_group))
        task_group.create_task(flush_buffer(task_group))
        task_group.create_task(flush_quote_buffer(task_group))


if __name__ == "__main__":
    asyncio.run(main())

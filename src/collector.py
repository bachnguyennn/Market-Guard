
import asyncio
import json

import websockets

from .pipeline import process_batch

COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"
BATCH_SIZE = 100
FLUSH_INTERVAL = 5
buffer = []


def hand_off_buffer(task_group: asyncio.TaskGroup) -> None:
    global buffer

    if not buffer:
        return

    batch = buffer
    buffer = []
    task_group.create_task(process_batch(batch))


async def collect_trades(task_group: asyncio.TaskGroup) -> None:
    subscription = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["matches", "heartbeat"],
    }

    async with websockets.connect(COINBASE_URL) as websocket:
        
        await websocket.send(json.dumps(subscription))

        async for message in websocket:
            response = json.loads(message)
            if response.get("type") == "match":
                buffer.append(response)
                if len(buffer) >= BATCH_SIZE:
                    hand_off_buffer(task_group)


async def flush_buffer(task_group: asyncio.TaskGroup) -> None:
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        hand_off_buffer(task_group)


async def main():
    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(collect_trades(task_group))
        task_group.create_task(flush_buffer(task_group))


if __name__ == "__main__":
    asyncio.run(main())

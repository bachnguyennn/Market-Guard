
import asyncio
import json

import websockets

COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"
buffer = []
async def collect_trades():
    subscription = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["matches", "heartbeat"],
    }

    async with websockets.connect(COINBASE_URL) as websocket:
        
    # Hint: json.dumps()
        await websocket.send(json.dumps(subscription))

        async for message in websocket:
            response = json.loads(message)
            if response.get("type") == "match":
                buffer.append(response)


async def receive_trades():
    while True:
        # Simulate receiving a trade
        await asyncio.sleep(1)

async def flush_buffer():
    while True:
        await asyncio.sleep(5)

        if buffer:
            try:
                with open("trades.jsonl", "a") as f:
                    for trade in buffer:
                        json.dump(trade, f)
                        f.write("\n")

                print(f"Saved {len(buffer)} trades")
                buffer.clear()

            except OSError as error:
                print(f"Error saving batch: {error}")

async def main():
    await asyncio.gather(
        collect_trades(),
        flush_buffer()
    )
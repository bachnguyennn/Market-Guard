from datetime import UTC, datetime, timedelta
from decimal import Decimal


def synthetic_market_history() -> list[dict]:
    """Generate 15 consecutive minutes per pair, with a volume and a price alert."""
    start = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
    returns = [Decimal(value) for value in ["0.001", "-0.001", "0.002", "-0.002", "0"]]
    sizes = ["1", "1.1", "0.9", "1.05", "0.95"]
    prices = {"BTC-USD": Decimal(60000), "ETH-USD": Decimal(2500)}
    trades = []
    for minute in range(15):
        for product_id, price in prices.items():
            if minute:
                price_return = returns[(minute - 1) % len(returns)]
                if product_id == "ETH-USD" and minute == 13:
                    price_return = Decimal("-0.05")
                price = (price * (1 + price_return)).quantize(Decimal("0.00000001"))
                prices[product_id] = price
            trades.append(
                {
                    "type": "match",
                    "product_id": product_id,
                    "trade_id": 1001 + minute,
                    "price": str(price),
                    "size": "25" if product_id == "BTC-USD" and minute == 12 else sizes[minute % 5],
                    "time": (start + timedelta(minutes=minute)).isoformat(),
                }
            )
    return trades

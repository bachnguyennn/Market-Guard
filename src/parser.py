from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone


def parse_trade(raw_message: dict) -> dict | None:
    """
    Normalize a Coinbase trade message.

    Return None for messages that are not trades.
    Raise an appropriate error for malformed trade data.
    """
    if not isinstance(raw_message, dict):
        raise ValueError("Malformed trade data: expected a dictionary.")
    if raw_message.get("type") != "match":
        return None

    required = {"trade_id", "product_id", "price", "size", "side", "time"}
    missing = required - raw_message.keys()
    if missing:
        raise ValueError(f"Malformed trade data: missing keys {missing}")

    trade_id = raw_message["trade_id"]
    if type(trade_id) is not int or trade_id < 0:
        raise ValueError("trade_id must be a nonnegative integer")

    if raw_message["product_id"] not in {"BTC-USD", "ETH-USD"}:
        raise ValueError(f"Malformed trade data: unexpected product_id {raw_message['product_id']}")
    if raw_message["side"] not in {"buy", "sell"}:
        raise ValueError(f"Malformed trade data: unexpected side {raw_message['side']}")
    for field in ("price", "size"):
        value = raw_message[field]

        if not isinstance(value, str):
            raise ValueError(f"{field} must be a numeric string")

        try:
            number = Decimal(value)
        except InvalidOperation:
            raise ValueError(f"{field} must be numeric") from None

        if not number.is_finite() or number <= 0:
            raise ValueError(f"{field} must be finite and greater than zero")
    value = raw_message["time"]
    if not isinstance(value, str):
        raise ValueError("time must be a string")

    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("time must be an ISO timestamp") from None

    if timestamp.tzinfo is None:
        raise ValueError("time must include a timezone")

    return {
        "type": "match",
        "trade_id": trade_id,
        "product_id": raw_message["product_id"],
        "price": raw_message["price"],
        "size": raw_message["size"],
        "side": raw_message["side"],
        "event_time": timestamp.isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "coinbase",
    }

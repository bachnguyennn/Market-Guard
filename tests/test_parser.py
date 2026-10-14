
import json


def save_event(event: dict, file_path: str) -> None:
    """
    Append an event to a JSONL file.
    """

    try:
        with open(file_path, "a") as f:
            json.dump(event, f)
            f.write("\n")
    except Exception as e:
        print(f"Error saving event: {e}")
        
    pass


from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parser import parse_trade


def test_heartbeat_ignored():
    heartbeat = {
        "type": "heartbeat",
        "product_id": "BTC-USD",
        "last_trade_id": 1001
    }

    result = parse_trade(heartbeat)

    assert result is None

def test_valid_trade():
    trade = {
        "type": "match",
        "trade_id": 1002,
        "product_id": "BTC-USD",
        "price": "68000.50",
        "size": "0.025",
        "side": "sell",
        "time": "2026-10-14T15:30:00Z"
    }

    result = parse_trade(trade)

    assert result["product_id"] == "BTC-USD"
    assert result["price"] == "68000.50"
    assert "ingested_at" in result

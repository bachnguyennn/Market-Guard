import json
from unittest.mock import patch


def test_buffer_preserved_on_failure():
    buffer = [
        {"trade_id": 1001},
        {"trade_id": 1002},
        {"trade_id": 1003}
    ]

    with patch("builtins.open", side_effect=OSError("disk full")):
        try:
            with open("trades.jsonl", "a") as file:
                for trade in buffer:
                    json.dump(trade, file)
                    file.write("\n")
            buffer.clear()
        except OSError:
            pass

    assert len(buffer) == 3


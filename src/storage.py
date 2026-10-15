import json
import uuid
from pathlib import Path


def create_batch_file(
    trades: list[dict],
    directory: str
) -> Path:
    """
    Save a batch of trades to a uniquely named
    JSONL file.

    Return the path to the saved file.
    """

    directory_path = Path(directory)
    directory_path.mkdir(parents=True, exist_ok=True)

    batch_path = directory_path / f"batch_{uuid.uuid4()}.jsonl"
    temp_path = batch_path.with_suffix(".jsonl.tmp")

    try:
        with temp_path.open("w", encoding="utf-8") as batch_file:
            for trade in trades:
                json.dump(trade, batch_file)
                batch_file.write("\n")

        temp_path.replace(batch_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise

    return batch_path

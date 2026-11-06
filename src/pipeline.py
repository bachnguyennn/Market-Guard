
import asyncio
from datetime import UTC, datetime
from pathlib import Path

from .azure_storage import upload_with_retry
from .storage import BRONZE_PREFIX, QUOTES_PREFIX, bronze_hour_prefix, create_batch_file

PENDING_TRADES_DIRECTORY = "data/pending"
PENDING_QUOTES_DIRECTORY = "data/pending_quotes"


def bronze_blob_name(batch_path: Path, prefix: str = BRONZE_PREFIX) -> str:
    """Name a batch by its upload hour so refreshes can list a bounded window."""
    return f"{bronze_hour_prefix(datetime.now(UTC), prefix)}{batch_path.name}"


async def persist_batch(records: list[dict], directory: str, prefix: str) -> None:
    """Save records locally, upload them, and remove the local file."""
    batch_path = create_batch_file(records, directory)

    await asyncio.to_thread(
        upload_with_retry,
        batch_path,
        bronze_blob_name(batch_path, prefix),
    )

    batch_path.unlink()


async def process_batch(trades: list[dict]) -> None:
    """
    Persist trades locally, then upload to Azure.
    """
    if not trades:
        return

    await persist_batch(trades, PENDING_TRADES_DIRECTORY, BRONZE_PREFIX)


async def process_quote_batch(quotes: list[dict]) -> None:
    """
    Persist quotes without letting a failure stop trade collection.

    The collector cancels every task when one fails, and quotes must not
    interrupt the trade stream. A failed batch stays in the pending directory.
    """
    if not quotes:
        return

    try:
        await persist_batch(quotes, PENDING_QUOTES_DIRECTORY, QUOTES_PREFIX)
    except Exception as error:  # noqa: BLE001 - quotes must never stop trade collection
        print(f"Quote batch upload failed: {error}")


async def recover_pending_batches() -> None:
    for directory, prefix in (
        (PENDING_TRADES_DIRECTORY, BRONZE_PREFIX),
        (PENDING_QUOTES_DIRECTORY, QUOTES_PREFIX),
    ):
        for batch_path in sorted(Path(directory).glob("*.jsonl")):
            await asyncio.to_thread(
                upload_with_retry,
                batch_path,
                bronze_blob_name(batch_path, prefix),
            )

            batch_path.unlink()

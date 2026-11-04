
import asyncio
from datetime import UTC, datetime
from pathlib import Path

from .azure_storage import upload_with_retry
from .storage import bronze_hour_prefix, create_batch_file


def bronze_blob_name(batch_path: Path) -> str:
    """Name a batch by its upload hour so refreshes can list a bounded window."""
    return f"{bronze_hour_prefix(datetime.now(UTC))}{batch_path.name}"


async def process_batch(trades: list[dict]) -> None:
    """
    Persist trades locally, then upload to Azure.
    """
    if not trades:
        return

    batch_path = create_batch_file(trades, "data/pending")
    blob_name = bronze_blob_name(batch_path)

    await asyncio.to_thread(
        upload_with_retry,
        batch_path,
        blob_name,
    )

    batch_path.unlink()
async def recover_pending_batches() -> None:
    pending_directory = Path("data/pending")

    if not pending_directory.exists():
        return

    for batch_path in sorted(pending_directory.glob("*.jsonl")):
        blob_name = bronze_blob_name(batch_path)

        await asyncio.to_thread(
            upload_with_retry,
            batch_path,
            blob_name,
        )

        batch_path.unlink()

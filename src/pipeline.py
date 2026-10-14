
import asyncio
from pathlib import Path

from .azure_storage import upload_with_retry
from .storage import create_batch_file


async def process_batch(trades: list[dict]) -> None:
    """
    Persist trades locally, then upload to Azure.
    """
    if not trades:
        return

    batch_path = create_batch_file(trades, "data/pending")
    blob_name = f"coinbase/{batch_path.name}"

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
        blob_name = f"coinbase/{batch_path.name}"

        await asyncio.to_thread(
            upload_with_retry,
            batch_path,
            blob_name,
        )

        batch_path.unlink()

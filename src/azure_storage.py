import os
import time


def upload_batch(file_path, blob_name):
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    account_url = os.environ["AZURE_STORAGE_ACCOUNT_URL"]
    container_name = os.environ["AZURE_STORAGE_CONTAINER"]
    credential = DefaultAzureCredential(
        managed_identity_client_id=os.getenv("AZURE_CLIENT_ID")
    )   
    service_client = BlobServiceClient(account_url, credential=credential)
    blob_client = service_client.get_blob_client(
        container=container_name,
        blob=blob_name,
    )

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=False)


def upload_with_retry(file_path, blob_name, max_retries=3):
    if max_retries < 1:
        raise ValueError("max_retries must be at least 1")

    for attempt in range(max_retries):
        try:
            upload_batch(file_path, blob_name)
            print("Upload successful!")
            return

        except Exception as error:
            print(f"Upload failed: {error}")

            if attempt == max_retries - 1:
                raise

            delay = 2**attempt
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)

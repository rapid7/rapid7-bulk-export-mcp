"""Azure Blob backend for the artifact store.

Isolated in its own module so :mod:`src.artifact_store` depends only on the
BlobBackend Protocol and stays unit-testable with a fake — the Azure SDK is
imported here and nowhere else, and only when hosted storage is configured.

Authentication is managed identity via DefaultAzureCredential, so no secret is
carried: the same user-assigned identity that reads Key Vault reaches Blob. The
blob is transferred over HTTPS and written to local disk; it is never mounted.
"""

from pathlib import Path
from typing import BinaryIO, List

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


class AzureBlobBackend:
    """Blob operations against one container, authenticated by managed identity."""

    def __init__(self, account_url: str, container: str) -> None:
        self._client = BlobServiceClient(
            account_url=account_url,
            credential=DefaultAzureCredential(),
        ).get_container_client(container)

    def upload(self, blob_name: str, data: BinaryIO) -> None:
        # The SDK reads the stream in blocks, so a large database is never held
        # in memory whole.
        self._client.upload_blob(name=blob_name, data=data, overwrite=True)

    def download(self, blob_name: str, dest: Path) -> None:
        with open(dest, "wb") as fh:
            self._client.download_blob(blob_name).readinto(fh)

    def list_names(self, prefix: str) -> List[str]:
        return [b.name for b in self._client.list_blobs(name_starts_with=prefix)]

    def delete(self, blob_name: str) -> None:
        # Swallow a missing blob so pruning is idempotent: a prune interrupted
        # between the marker and the data leaves a half-deleted pair, and the next
        # run must be able to finish the job rather than failing on the part that
        # is already gone.
        try:
            self._client.delete_blob(blob_name)
        except ResourceNotFoundError:
            pass

    def exists(self, blob_name: str) -> bool:
        return self._client.get_blob_client(blob_name).exists()

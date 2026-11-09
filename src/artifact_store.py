"""Blob Storage as a courier for the finished DuckDB artifact.

Container Apps offers no block storage and cannot mount Blob, and DuckDB's
single-writer model breaks on the SMB/NFS mounts that are the only alternative.
So the database is never mapped as a filesystem: the refresh job builds it on
local ephemeral disk, uploads the finished file to Blob over HTTPS as a
*versioned* artifact, and each serving replica downloads one copy to its own
local disk and opens it read-only. Blob is a transfer channel, not storage the
engine touches.

A version is only *resolvable* once a small completion marker lands beside its
data blob. Upload the data first and the marker last, so a replica that resolves
mid-upload sees no complete version rather than a truncated database. The flip
itself is a new Container Apps revision (revision history is the rollback path);
this module only publishes and resolves, and pins nothing in place.

When no Blob configuration is present the store is absent and the local stdio
path is untouched — the courier only exists in hosted mode.
"""

import io
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, List, Optional, Protocol

# The finished database and its completion marker live under a per-version
# prefix, so publishing a new version never overwrites an older one and rollback
# is a matter of pointing a revision at an earlier version.
# Under the refresh CLI's logger root so src/cli.py's handler formats these lines.
logger = logging.getLogger("rapid7.refresh.artifact")

_ARTIFACT_BLOB = "rapid7_bulk_export.db"
_COMPLETE_MARKER = "COMPLETE"
_VERSION_PREFIX = "versions/"

# Environment contract, mirroring the read/validate/warn style used across the
# codebase (see src/config.py, src/db_utils.py).
ENV_CONTAINER = "ARTIFACT_BLOB_CONTAINER"
ENV_ACCOUNT_URL = "ARTIFACT_BLOB_ACCOUNT_URL"
# Optional: pin the exact version a revision serves. Absent, the newest complete
# version is resolved — both paths refuse an incomplete one.
ENV_VERSION = "ARTIFACT_VERSION"
# Optional: how many complete versions to keep. Every refresh publishes a whole new
# database, so without pruning the container grows by one copy per run forever and
# becomes the largest cost in the deployment, holding data nothing ever reads.
ENV_RETAIN = "ARTIFACT_RETAIN_VERSIONS"
_DEFAULT_RETAIN = 3


class BlobBackend(Protocol):
    """The narrow slice of Blob operations the store needs.

    Kept as a Protocol so tests inject a fake and neither credentials nor
    network are required, and so the concrete Azure client is constructed only
    when hosted configuration is actually present.
    """

    def upload(self, blob_name: str, data: BinaryIO) -> None:
        """Upload a binary stream to ``blob_name``, overwriting any existing blob."""

    def download(self, blob_name: str, dest: Path) -> None:
        """Download ``blob_name`` to the local path ``dest``."""

    def list_names(self, prefix: str) -> List[str]:
        """Return blob names under ``prefix``."""

    def delete(self, blob_name: str) -> None:
        """Remove a blob. Must not raise when the blob is already absent."""
        ...  # pragma: no cover

    def exists(self, blob_name: str) -> bool:
        """True when ``blob_name`` is present."""


@dataclass
class ArtifactStore:
    """Publishes and resolves versioned database artifacts over a BlobBackend."""

    backend: BlobBackend

    def publish(self, db_path: str, version: str) -> str:
        """Upload the finished database as ``version``, then mark it complete.

        The completion marker is written strictly last: a resolver that races an
        in-progress publish must never select a half-uploaded database, and the
        only signal that separates the two is the marker's presence.

        Returns the published version.
        """
        # Streamed rather than read into memory: the database is the largest thing
        # the job handles, and holding a second full copy in RAM would set its
        # memory floor by data size.
        with open(db_path, "rb") as fh:
            self.backend.upload(self._data_name(version), fh)
        # Written last, and only on a successful data upload, so its presence is
        # the definition of "this version is safe to download".
        self.backend.upload(self._marker_name(version), io.BytesIO(b""))
        return version

    def prune(self, retain: int) -> List[str]:
        """Delete all but the newest ``retain`` complete versions.

        Returns the versions removed.

        An unpinned replica's version is never deleted: the newest complete version
        is always inside the retained set, because ``retain`` is floored at 1 and the
        sort matches ``resolve_current``. A version pinned with ARTIFACT_VERSION is
        not protected — the pin lives on the app, out of the job's sight — so it can
        be pruned once it falls outside the retained set.

        The completion MARKER is deleted before the data, which is the opposite of
        the publish order and equally deliberate. Removing the marker first makes the
        version immediately unresolvable, so a replica that starts mid-prune cannot
        select a version whose data is about to disappear. The reverse order would
        leave a window where a version looks complete but its database is gone.

        Retention is by COUNT, not age. An age rule — including a storage lifecycle
        policy, which cannot express "keep the newest N" — would delete the last good
        artifact if refreshes failed for longer than the threshold, leaving nothing
        for a replica to serve.
        """
        keep = max(1, retain)
        complete = sorted(self._complete_versions(), reverse=True)
        doomed = complete[keep:]
        for version in doomed:
            # Marker first. See the docstring: this ordering is what makes a prune
            # interrupted halfway safe.
            self.backend.delete(self._marker_name(version))
            self.backend.delete(self._data_name(version))
        return doomed

    def resolve_current(self) -> Optional[str]:
        """Return the version a replica should serve, or None if none is ready.

        Honours ARTIFACT_VERSION when set (a revision pinning its version), but
        still refuses it unless its completion marker is present. Otherwise picks
        the lexicographically newest complete version — callers must therefore
        mint monotonically sortable version strings (see mint_version).
        """
        pinned = os.environ.get(ENV_VERSION, "").strip()
        if pinned:
            return pinned if self.backend.exists(self._marker_name(pinned)) else None

        complete = sorted(self._complete_versions(), reverse=True)
        return complete[0] if complete else None

    def download_current(self, dest: Path) -> str:
        """Download the current complete artifact to ``dest``; return its version.

        Raises LookupError when no complete version exists. The server treats that
        as "nothing published yet" and starts with its read tools explaining there
        is no data, rather than serving an empty database as if it were real.
        """
        version = self.resolve_current()
        if version is None:
            raise LookupError("no complete artifact version is available to download")
        dest.parent.mkdir(parents=True, exist_ok=True)
        self.backend.download(self._data_name(version), dest)
        return version

    def _complete_versions(self) -> List[str]:
        """Versions whose completion marker is present."""
        versions = []
        for name in self.backend.list_names(_VERSION_PREFIX):
            rest = name[len(_VERSION_PREFIX) :]
            version, _, leaf = rest.partition("/")
            if leaf == _COMPLETE_MARKER:
                versions.append(version)
        return versions

    @staticmethod
    def _data_name(version: str) -> str:
        return f"{_VERSION_PREFIX}{version}/{_ARTIFACT_BLOB}"

    @staticmethod
    def _marker_name(version: str) -> str:
        return f"{_VERSION_PREFIX}{version}/{_COMPLETE_MARKER}"


def mint_version() -> str:
    """A monotonically sortable version string for a fresh publish.

    UTC timestamp to microseconds: lexicographic order matches chronological
    order, which is what resolve_current() relies on to pick the newest.
    """
    import datetime as _dt

    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def storage_configured() -> bool:
    """True when both Blob settings are present, i.e. the data is a published copy.

    Checks the environment only, without constructing a store or importing the
    Azure SDK, so it is cheap enough to call on every query.
    """
    return bool(os.environ.get(ENV_ACCOUNT_URL, "").strip() and os.environ.get(ENV_CONTAINER, "").strip())


def _configured_store() -> Optional[ArtifactStore]:
    """Build a store from the environment, or None when Blob is not configured.

    Absent or partial configuration returns None, so both the refresh job and the
    server degrade to purely-local behaviour. A partial configuration warns,
    matching the codebase's read/validate/warn pattern, because a half-set store
    in a hosted deployment is an operator error worth surfacing rather than
    silently falling back to local.
    """
    account_url = os.environ.get(ENV_ACCOUNT_URL, "").strip()
    container = os.environ.get(ENV_CONTAINER, "").strip()
    if not account_url and not container:
        return None
    if not (account_url and container):
        print(
            f"Warning: ignoring partial artifact storage config; set both {ENV_ACCOUNT_URL} "
            f"and {ENV_CONTAINER} to enable Blob artifacts, or neither for local mode",
            file=sys.stderr,
        )
        return None

    # Import lazily so the Azure SDK is only required when hosted storage is
    # actually configured; the local stdio path never imports it.
    from .azure_blob import AzureBlobBackend

    return ArtifactStore(backend=AzureBlobBackend(account_url=account_url, container=container))


def publish_artifact(db_path: str) -> Optional[str]:
    """Publish ``db_path`` as a new version when Blob is configured.

    Returns the published version, or None when no Blob storage is configured
    (local mode), leaving the finished database on local disk untouched.
    """
    store = _configured_store()
    if store is None:
        return None
    version = store.publish(db_path, mint_version())

    # Pruning must never turn a SUCCESSFUL publish into a failed refresh: the new
    # artifact is already complete and servable, and a storage permission or
    # transient error while deleting old copies is a cost problem, not a
    # correctness one. Warn and carry on; the next run retries.
    try:
        removed = store.prune(_retain_count())
        if removed:
            logger.info("pruned %d old artifact version(s): %s", len(removed), ", ".join(removed))
    except Exception as e:  # noqa: BLE001 - deliberately broad; see comment above
        logger.warning("could not prune old artifact versions (published %s regardless): %s", version, e)

    return version


def _retain_count() -> int:
    """How many complete versions to keep, floored at 1.

    An unparseable value falls back to the default rather than failing the refresh,
    and is warned about, because a typo in configuration should not stop data
    reaching the server.
    """
    raw = os.environ.get(ENV_RETAIN, "").strip()
    if not raw:
        return _DEFAULT_RETAIN
    try:
        return max(1, int(raw))
    except ValueError:
        logger.warning("%s=%r is not an integer; keeping %d versions", ENV_RETAIN, raw, _DEFAULT_RETAIN)
        return _DEFAULT_RETAIN


def download_artifact(dest: Path) -> Optional[str]:
    """Download the current artifact to ``dest`` when Blob is configured.

    Returns the downloaded version, or None when no Blob storage is configured
    (local mode). Raises LookupError when hosted but no complete version exists,
    so the caller can refuse readiness rather than serve an empty database.
    """
    store = _configured_store()
    if store is None:
        return None
    return store.download_current(dest)

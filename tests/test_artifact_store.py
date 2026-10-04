"""Tests for the Blob artifact store.

A fake in-memory backend stands in for Blob, so these run with no Azure SDK
credentials and no network — the store's logic is what is under test, not the
transport.
"""

import io
import logging
from pathlib import Path
from typing import BinaryIO, Dict, List

import pytest

from src import artifact_store
from src.artifact_store import (
    ENV_ACCOUNT_URL,
    ENV_CONTAINER,
    ENV_VERSION,
    ArtifactStore,
    download_artifact,
    mint_version,
    publish_artifact,
)


class FakeBackend:
    """In-memory BlobBackend recording blobs by name."""

    def __init__(self) -> None:
        self.blobs: Dict[str, bytes] = {}
        self.deleted: List[str] = []

    def upload(self, blob_name: str, data: BinaryIO) -> None:
        # Record that a stream, not bytes, crossed the contract, so a regression
        # back to reading the whole file into memory fails here.
        assert not isinstance(data, (bytes, bytearray)), "upload must be given a stream"
        self.blobs[blob_name] = data.read()

    def download(self, blob_name: str, dest: Path) -> None:
        dest.write_bytes(self.blobs[blob_name])

    def list_names(self, prefix: str) -> List[str]:
        return [n for n in self.blobs if n.startswith(prefix)]

    def delete(self, blob_name: str) -> None:
        # Records the order deletions happen in, because prune's marker-before-data
        # ordering is a correctness property and not an implementation detail.
        self.deleted.append(blob_name)
        self.blobs.pop(blob_name, None)

    def exists(self, blob_name: str) -> bool:
        return blob_name in self.blobs


@pytest.fixture(autouse=True)
def _clear_version_pin(monkeypatch):
    """Most tests resolve the newest version; keep an inherited pin out of the way."""
    monkeypatch.delenv(ENV_VERSION, raising=False)


def _write_db(tmp_path: Path, content: bytes = b"duckdb-bytes") -> str:
    db = tmp_path / "rapid7_bulk_export.db"
    db.write_bytes(content)
    return str(db)


def test_publish_then_resolve_round_trips(tmp_path):
    """A published version resolves as current and downloads byte-for-byte."""
    store = ArtifactStore(backend=FakeBackend())
    db_path = _write_db(tmp_path, b"the-database")

    version = store.publish(db_path, mint_version())

    assert store.resolve_current() == version
    dest = tmp_path / "local" / "copy.db"
    downloaded = store.download_current(dest)
    assert downloaded == version
    assert dest.read_bytes() == b"the-database"


def test_partial_upload_is_never_selected(tmp_path):
    """A data blob with no completion marker is not a resolvable version.

    Simulates a publish that uploaded the database but died before the marker.
    """
    backend = FakeBackend()
    store = ArtifactStore(backend=backend)
    # Data present, marker absent — exactly the mid-upload state.
    backend.upload("versions/20260101T000000000000Z/rapid7_bulk_export.db", io.BytesIO(b"partial"))

    assert store.resolve_current() is None
    with pytest.raises(LookupError):
        store.download_current(tmp_path / "copy.db")


def test_newest_complete_version_wins_over_older_and_partial(tmp_path):
    """resolve_current picks the newest COMPLETE version, ignoring a newer partial."""
    store = ArtifactStore(backend=FakeBackend())
    old = store.publish(_write_db(tmp_path, b"old"), "20260101T000000000000Z")
    new = store.publish(_write_db(tmp_path, b"new"), "20260201T000000000000Z")
    # A still-newer version whose upload has not finished (no marker).
    store.backend.upload("versions/20260301T000000000000Z/rapid7_bulk_export.db", io.BytesIO(b"in-flight"))

    assert old < new
    assert store.resolve_current() == new
    dest = tmp_path / "copy.db"
    assert store.download_current(dest) == new
    assert dest.read_bytes() == b"new"


def test_pinned_version_is_honoured_only_when_complete(tmp_path, monkeypatch):
    """ARTIFACT_VERSION pins the served version, but still refuses an incomplete one."""
    store = ArtifactStore(backend=FakeBackend())
    store.publish(_write_db(tmp_path, b"v1"), "20260101T000000000000Z")
    store.publish(_write_db(tmp_path, b"v2"), "20260201T000000000000Z")

    monkeypatch.setenv(ENV_VERSION, "20260101T000000000000Z")
    assert store.resolve_current() == "20260101T000000000000Z"

    monkeypatch.setenv(ENV_VERSION, "20269999T000000000000Z")  # never published
    assert store.resolve_current() is None


def test_mint_version_is_monotonically_sortable():
    """Lexicographic order must match chronological order for resolve_current."""
    first = mint_version()
    second = mint_version()
    assert first < second or first == second  # microsecond clock may collide
    assert sorted([second, first]) == [first, second]


def test_absent_config_publish_is_noop(tmp_path, monkeypatch):
    """With no Blob config, publish is a no-op and the local file is untouched."""
    monkeypatch.delenv(ENV_ACCOUNT_URL, raising=False)
    monkeypatch.delenv(ENV_CONTAINER, raising=False)
    db_path = _write_db(tmp_path, b"local-only")

    assert publish_artifact(db_path) is None
    assert Path(db_path).read_bytes() == b"local-only"


def test_absent_config_download_is_noop(tmp_path, monkeypatch):
    """With no Blob config, download is a no-op returning None (local mode)."""
    monkeypatch.delenv(ENV_ACCOUNT_URL, raising=False)
    monkeypatch.delenv(ENV_CONTAINER, raising=False)

    dest = tmp_path / "should-not-be-written.db"
    assert download_artifact(dest) is None
    assert not dest.exists()


def test_partial_config_warns_and_stays_local(tmp_path, monkeypatch, capsys):
    """Half-set config is an operator error: warn and fall back to local, not fail."""
    monkeypatch.setenv(ENV_ACCOUNT_URL, "https://acct.blob.core.windows.net")
    monkeypatch.delenv(ENV_CONTAINER, raising=False)

    assert publish_artifact(_write_db(tmp_path)) is None
    assert "partial artifact storage config" in capsys.readouterr().err


def test_configured_store_builds_azure_backend(monkeypatch):
    """Full config builds a store whose backend is constructed from the Azure module.

    The Azure client construction is patched so no credentials or network are
    needed; this verifies the wiring, not the SDK.
    """
    monkeypatch.setenv(ENV_ACCOUNT_URL, "https://acct.blob.core.windows.net")
    monkeypatch.setenv(ENV_CONTAINER, "artifacts")

    built = {}

    class _FakeAzureBackend:
        def __init__(self, account_url, container):
            built["account_url"] = account_url
            built["container"] = container

    monkeypatch.setattr("src.azure_blob.AzureBlobBackend", _FakeAzureBackend)

    store = artifact_store._configured_store()
    assert isinstance(store, ArtifactStore)
    assert built == {
        "account_url": "https://acct.blob.core.windows.net",
        "container": "artifacts",
    }


class TestPrune:
    """Retention of published artifact versions.

    Every refresh publishes a whole new database, so without pruning the container
    grows by one copy per run indefinitely, holding data nothing ever reads.
    """

    def _publish(self, store, tmp_path, versions):
        db = tmp_path / "db.duckdb"
        db.write_bytes(b"x")
        for v in versions:
            store.publish(str(db), v)

    def test_keeps_newest_and_removes_the_rest(self, tmp_path):
        backend = FakeBackend()
        store = artifact_store.ArtifactStore(backend)
        self._publish(store, tmp_path, ["20260101T000000Z", "20260102T000000Z", "20260103T000000Z"])

        removed = store.prune(retain=2)

        assert removed == ["20260101T000000Z"]
        # The two newest survive intact, data and marker.
        assert store.resolve_current() == "20260103T000000Z"
        assert backend.exists("versions/20260102T000000Z/COMPLETE")
        assert backend.exists("versions/20260102T000000Z/rapid7_bulk_export.db")
        # The oldest is gone entirely — no orphaned data blob left paying for storage.
        assert not backend.exists("versions/20260101T000000Z/COMPLETE")
        assert not backend.exists("versions/20260101T000000Z/rapid7_bulk_export.db")

    def test_deletes_marker_before_data(self, tmp_path):
        """Ordering is a correctness property, not an implementation detail.

        Removing the marker first makes a version instantly unresolvable, so a replica
        starting mid-prune cannot select a version whose database is about to vanish.
        The reverse order leaves a window where a version looks complete but its data
        is already gone.
        """
        backend = FakeBackend()
        store = artifact_store.ArtifactStore(backend)
        self._publish(store, tmp_path, ["20260101T000000Z", "20260102T000000Z"])

        store.prune(retain=1)

        assert backend.deleted == [
            "versions/20260101T000000Z/COMPLETE",
            "versions/20260101T000000Z/rapid7_bulk_export.db",
        ]

    def test_never_deletes_the_only_version_even_when_asked(self, tmp_path):
        """retain is floored at 1: pruning must not leave a replica nothing to serve."""
        backend = FakeBackend()
        store = artifact_store.ArtifactStore(backend)
        self._publish(store, tmp_path, ["20260101T000000Z"])

        assert store.prune(retain=0) == []
        assert store.resolve_current() == "20260101T000000Z"

    def test_prune_failure_does_not_fail_a_successful_publish(self, tmp_path, monkeypatch, caplog):
        """A publish that worked must not be reported as failed because cleanup did not.

        The new artifact is already complete and servable; a permission or transient
        error while deleting old copies is a cost problem, not a correctness one.
        """

        class ExplodingBackend(FakeBackend):
            def delete(self, blob_name: str) -> None:
                raise RuntimeError("storage said no")

        backend = ExplodingBackend()
        db = tmp_path / "db.duckdb"
        db.write_bytes(b"x")
        store = artifact_store.ArtifactStore(backend)
        store.publish(str(db), "20260101T000000Z")
        store.publish(str(db), "20260102T000000Z")

        monkeypatch.setattr(artifact_store, "_configured_store", lambda: store)
        monkeypatch.setenv(artifact_store.ENV_RETAIN, "1")

        caplog.set_level(logging.WARNING, logger="rapid7.refresh.artifact")
        version = artifact_store.publish_artifact(str(db))

        assert version is not None
        assert "could not prune" in caplog.text
        # And the freshly published artifact is still resolvable.
        assert store.resolve_current() is not None

    def test_retain_count_reads_env_and_survives_a_typo(self, monkeypatch, caplog):
        monkeypatch.setenv(artifact_store.ENV_RETAIN, "7")
        assert artifact_store._retain_count() == 7

        monkeypatch.setenv(artifact_store.ENV_RETAIN, "not-a-number")
        caplog.set_level(logging.WARNING, logger="rapid7.refresh.artifact")
        assert artifact_store._retain_count() == artifact_store._DEFAULT_RETAIN
        assert "not an integer" in caplog.text

        monkeypatch.delenv(artifact_store.ENV_RETAIN, raising=False)
        assert artifact_store._retain_count() == artifact_store._DEFAULT_RETAIN

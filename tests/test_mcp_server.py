"""
Unit tests for the async download/load job behavior in mcp_server.py.

These tests cover the fix for a real-world failure mode: loading a large
export (potentially millions of rows) can take longer than an MCP client's
tool-call timeout. Some clients (e.g. Claude Desktop) hard-cancel a tool
call after a few minutes and, if the server later tries to respond to that
already-cancelled request, the whole stdio server process can crash.

download_rapid7_export() now starts the download+load in a background
thread and returns immediately; check_download_status() polls the result.
"""

import time

import pytest

from src import mcp_server


class FakeDB:
    """Minimal stand-in for VulnerabilityDatabase, with a hook to
    simulate a slow load so tests can exercise in-progress states."""

    def __init__(self, load_delay: float = 0.0, row_counts=None, has_data_value: bool = True):
        self.load_delay = load_delay
        self.row_counts = row_counts if row_counts is not None else {"vulnerabilities": 3, "assets": 2}
        self.has_data_value = has_data_value
        self.load_calls = []

    def load_parquet_files_by_prefix(self, prefix_file_map, skip_prefixes=None, append=False):
        if self.load_delay:
            time.sleep(self.load_delay)
        self.load_calls.append(dict(prefix_file_map))
        return dict(self.row_counts)

    def get_stats(self):
        return {"vulnerabilities": {"total_rows": self.row_counts.get("vulnerabilities", 0)}}

    def get_schema(self):
        return {"vulnerabilities": {"columns": ["assetId", "vulnId"]}}

    def query(self, sql):
        return [{"ok": 1}]

    def has_data(self):
        return self.has_data_value


@pytest.fixture(autouse=True)
def reset_mcp_server_state(monkeypatch, tmp_path):
    """Give every test a clean slate: no leftover jobs, no leftover db,
    and a tracker pointed at a throwaway temp directory."""
    mcp_server._download_jobs.clear()
    monkeypatch.setattr(mcp_server, "db", None)
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path)
    yield
    mcp_server._download_jobs.clear()
    monkeypatch.setattr(mcp_server, "db", None)


def _fake_status_complete(export_id="exp-1", file_count=2):
    urls = [f"https://example.test/file{i}.parquet" for i in range(file_count)]
    return {
        "status": "COMPLETE",
        "parquetFiles": urls,
        "result": [{"prefix": "asset_vulnerability", "urls": urls}],
    }


def _patch_common(monkeypatch, status=None, file_bytes=b"x" * 200, db=None):
    status = status or _fake_status_complete()
    monkeypatch.setattr(mcp_server, "load_config", lambda: {"api_key": "test-key", "endpoint": "https://example.test"})
    monkeypatch.setattr(mcp_server, "get_export_status", lambda config, export_id: status)
    monkeypatch.setattr(mcp_server, "download_all_files", lambda urls, api_key: [file_bytes for _ in urls])
    if db is not None:
        monkeypatch.setattr(mcp_server, "db", db)
    return status


def _wait_for_job_state(export_id, target_states, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = mcp_server._get_job(export_id)
        if job is not None and job.get("state") in target_states:
            return job
        time.sleep(0.02)
    raise AssertionError(f"Job for {export_id} did not reach {target_states} within {timeout}s")


class TestDownloadRapid7ExportIsNonBlocking:
    def test_returns_immediately_with_started_message(self, monkeypatch):
        """The whole point of the fix: this call must not block on the load."""
        fake_db = FakeDB(load_delay=0.3)
        _patch_common(monkeypatch, db=fake_db)

        start = time.time()
        result = mcp_server.download_rapid7_export(export_id="exp-immediate", export_type="vulnerability")
        elapsed = time.time() - start

        assert elapsed < 0.5, f"download_rapid7_export blocked for {elapsed}s instead of returning immediately"
        assert "Started downloading and loading" in result
        assert "check_download_status" in result

        # Drain the background thread so it can't leak into the next test.
        _wait_for_job_state("exp-immediate", {"complete", "failed"})

    def test_background_job_eventually_completes(self, monkeypatch):
        fake_db = FakeDB(load_delay=0.1, row_counts={"vulnerabilities": 42})
        _patch_common(monkeypatch, db=fake_db)

        mcp_server.download_rapid7_export(export_id="exp-completes", export_type="vulnerability")
        job = _wait_for_job_state("exp-completes", {"complete", "failed"})

        assert job["state"] == "complete"
        status_text = mcp_server.check_download_status(export_id="exp-completes")
        assert "loaded successfully" in status_text
        assert "Rows loaded: 42" in status_text

    def test_failed_export_status_returns_synchronously_without_starting_job(self, monkeypatch):
        """If the export isn't ready yet, there's nothing to background — fail fast."""
        _patch_common(monkeypatch, status={"status": "IN_PROGRESS", "parquetFiles": []})

        result = mcp_server.download_rapid7_export(export_id="exp-not-ready", export_type="vulnerability")

        assert "not yet complete" in result
        assert mcp_server._get_job("exp-not-ready") is None

    def test_duplicate_call_while_job_running_does_not_start_a_second_job(self, monkeypatch):
        fake_db = FakeDB(load_delay=0.4)
        _patch_common(monkeypatch, db=fake_db)

        first = mcp_server.download_rapid7_export(export_id="exp-duplicate", export_type="vulnerability")
        assert "Started downloading" in first

        second = mcp_server.download_rapid7_export(export_id="exp-duplicate", export_type="vulnerability")
        assert "already in progress" in second

        _wait_for_job_state("exp-duplicate", {"complete", "failed"})
        assert len(fake_db.load_calls) == 1, "a second background job was started for the same export"

    def test_background_failure_is_captured_not_raised(self, monkeypatch):
        """An exception in the background thread must surface through
        check_download_status, not crash the process."""

        class ExplodingDB(FakeDB):
            def load_parquet_files_by_prefix(self, *a, **kw):
                raise RuntimeError("simulated load failure")

        _patch_common(monkeypatch, db=ExplodingDB())

        mcp_server.download_rapid7_export(export_id="exp-fails", export_type="vulnerability")
        job = _wait_for_job_state("exp-fails", {"complete", "failed"})

        assert job["state"] == "failed"
        status_text = mcp_server.check_download_status(export_id="exp-fails")
        assert "Error downloading/loading" in status_text
        assert "simulated load failure" in job["error"]


class TestCheckDownloadStatus:
    def test_unknown_export_id_returns_helpful_message(self):
        result = mcp_server.check_download_status(export_id="never-started")
        assert "No background download job found" in result

    def test_in_progress_shows_file_counts(self, monkeypatch):
        fake_db = FakeDB(load_delay=0.3)
        _patch_common(monkeypatch, status=_fake_status_complete(file_count=5), db=fake_db)

        mcp_server.download_rapid7_export(export_id="exp-progress", export_type="vulnerability")
        # Catch it while still downloading/loading, before the background thread finishes.
        result = mcp_server.check_download_status(export_id="exp-progress")

        assert "Job is" in result
        _wait_for_job_state("exp-progress", {"complete", "failed"})


class TestDbLockProtectsReadsDuringLoad:
    def test_query_returns_busy_message_when_lock_held(self, monkeypatch):
        fake_db = FakeDB()
        monkeypatch.setattr(mcp_server, "db", fake_db)

        mcp_server._db_lock.acquire()
        try:
            result = mcp_server.query_rapid7(sql="SELECT 1")
        finally:
            mcp_server._db_lock.release()

        assert "background download/load is currently in progress" in result

    def test_query_works_normally_when_lock_free(self, monkeypatch):
        fake_db = FakeDB()
        monkeypatch.setattr(mcp_server, "db", fake_db)

        result = mcp_server.query_rapid7(sql="SELECT 1")

        assert "Query executed successfully" in result

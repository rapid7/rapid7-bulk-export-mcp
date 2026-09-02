"""
Unit tests for the async download/load job behavior in mcp_server.py.

These tests cover the fix for a real-world failure mode: loading a large
export (potentially millions of rows) can take longer than an MCP client's
tool-call timeout. Some clients (e.g. Claude Desktop) hard-cancel a tool
call after a few minutes and, if the server later tries to respond to that
already-cancelled request, the whole stdio server process can crash.

download_rapid7_export() now starts the download+load in a background
thread and returns immediately. Progress and results are recorded as phase
transitions on the durable ExportTracker row, and reported by
check_rapid7_export_status() and list_rapid7_exports() — there is no
separate status tool.
"""

import time

import pytest

from src import mcp_server
from src.export_tracker import ExportTracker


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

    def purge(self):
        self.has_data_value = False


@pytest.fixture(autouse=True)
def reset_mcp_server_state(monkeypatch, tmp_path):
    """Give every test a clean slate: a fresh tracker DB under a throwaway
    temp dir, and no leftover db handle."""
    monkeypatch.setattr(mcp_server, "db", None)
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path)
    yield
    monkeypatch.setattr(mcp_server, "db", None)


def _tracker(tmp_path):
    return ExportTracker(str(tmp_path / "rapid7_bulk_export_tracking.db"))


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


def _wait_for_phase(tmp_path, export_id, target_states, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = _tracker(tmp_path).get_export_by_id(export_id)
        if row is not None and row.get("status") in target_states:
            return row
        time.sleep(0.02)
    raise AssertionError(f"Export {export_id} did not reach {target_states} within {timeout}s")


class TestDownloadRapid7ExportIsNonBlocking:
    def test_returns_immediately_with_started_message(self, monkeypatch, tmp_path):
        """The whole point of the fix: this call must not block on the load.
        The fake load takes 2s; a non-blocking call returns well under that
        even under CI load, so a 1s threshold cleanly separates the two
        without flaking on runner jitter."""
        fake_db = FakeDB(load_delay=2.0)
        _patch_common(monkeypatch, db=fake_db)

        start = time.time()
        result = mcp_server.download_rapid7_export(export_id="exp-immediate", export_type="vulnerability")
        elapsed = time.time() - start

        assert elapsed < 1.0, f"download_rapid7_export blocked for {elapsed}s instead of returning immediately"
        assert "Started downloading and loading" in result
        assert "check_rapid7_export_status" in result

        # Drain the background thread so it can't leak into the next test.
        _wait_for_phase(tmp_path, "exp-immediate", {mcp_server.PHASE_COMPLETE, mcp_server.PHASE_FAILED})

    def test_background_job_eventually_completes(self, monkeypatch, tmp_path):
        fake_db = FakeDB(load_delay=0.1, row_counts={"vulnerabilities": 42})
        _patch_common(monkeypatch, db=fake_db)

        mcp_server.download_rapid7_export(export_id="exp-completes", export_type="vulnerability")
        row = _wait_for_phase(tmp_path, "exp-completes", {mcp_server.PHASE_COMPLETE, mcp_server.PHASE_FAILED})

        assert row["status"] == mcp_server.PHASE_COMPLETE
        assert row["row_count"] == 42
        status_text = mcp_server.check_rapid7_export_status(export_id="exp-completes")
        assert "loaded successfully" in status_text
        assert "Rows loaded: 42" in status_text

    def test_failed_export_status_returns_synchronously_without_starting_job(self, monkeypatch, tmp_path):
        """If the export isn't ready yet, there's nothing to background — fail fast."""
        _patch_common(monkeypatch, status={"status": "IN_PROGRESS", "parquetFiles": []})

        result = mcp_server.download_rapid7_export(export_id="exp-not-ready", export_type="vulnerability")

        assert "not yet complete" in result
        assert _tracker(tmp_path).get_export_by_id("exp-not-ready") is None

    def test_duplicate_call_while_job_running_does_not_start_a_second_job(self, monkeypatch, tmp_path):
        fake_db = FakeDB(load_delay=0.4)
        _patch_common(monkeypatch, db=fake_db)

        first = mcp_server.download_rapid7_export(export_id="exp-duplicate", export_type="vulnerability")
        assert "Started downloading" in first

        second = mcp_server.download_rapid7_export(export_id="exp-duplicate", export_type="vulnerability")
        assert "already in progress" in second

        _wait_for_phase(tmp_path, "exp-duplicate", {mcp_server.PHASE_COMPLETE, mcp_server.PHASE_FAILED})
        assert len(fake_db.load_calls) == 1, "a second background job was started for the same export"

    def test_background_failure_is_captured_not_raised(self, monkeypatch, tmp_path):
        """An exception in the background thread must surface through the
        tracker phase, not crash the process."""

        class ExplodingDB(FakeDB):
            def load_parquet_files_by_prefix(self, *a, **kw):
                raise RuntimeError("simulated load failure")

        _patch_common(monkeypatch, db=ExplodingDB())

        mcp_server.download_rapid7_export(export_id="exp-fails", export_type="vulnerability")
        row = _wait_for_phase(tmp_path, "exp-fails", {mcp_server.PHASE_COMPLETE, mcp_server.PHASE_FAILED})

        assert row["status"] == mcp_server.PHASE_FAILED
        status_text = mcp_server.check_rapid7_export_status(export_id="exp-fails")
        assert "Error downloading/loading" in status_text
        assert "simulated load failure" in status_text


class TestCheckRapid7ExportStatusReportsLocalPhase:
    def test_unknown_export_id_falls_through_to_platform_status(self, monkeypatch):
        """No local tracker row -> query the Rapid7 API for platform status."""
        _patch_common(monkeypatch, status={"status": "PROCESSING", "parquetFiles": []})

        result = mcp_server.check_rapid7_export_status(export_id="never-started")
        assert "still processing" in result

    def test_in_progress_shows_local_phase_and_progress(self, monkeypatch, tmp_path):
        fake_db = FakeDB(load_delay=0.4)
        _patch_common(monkeypatch, status=_fake_status_complete(file_count=5), db=fake_db)

        mcp_server.download_rapid7_export(export_id="exp-progress", export_type="vulnerability")
        # Catch it before the background thread finishes.
        result = mcp_server.check_rapid7_export_status(export_id="exp-progress")

        assert "in progress locally" in result
        # Mid-flight the detail names the 5-file count (either the initial
        # "0 / 5 files downloaded" or the worker's "downloading 5 file(s)").
        assert "5" in result
        _wait_for_phase(tmp_path, "exp-progress", {mcp_server.PHASE_COMPLETE, mcp_server.PHASE_FAILED})

    def test_active_load_short_circuits_platform_api_call(self, monkeypatch, tmp_path):
        """While a local load is active, the platform-side API must NOT be
        called (it's already known complete)."""
        _tracker(tmp_path).save_export(
            export_id="exp-active",
            status=mcp_server.PHASE_LOADING,
            parquet_urls=["u1"],
            export_type="vulnerability",
        )

        def _boom(config, export_id):
            raise AssertionError("get_export_status must not be called while a local load is active")

        monkeypatch.setattr(mcp_server, "load_config", lambda: {"api_key": "k"})
        monkeypatch.setattr(mcp_server, "get_export_status", _boom)

        result = mcp_server.check_rapid7_export_status(export_id="exp-active")
        assert "in progress locally" in result


class TestListExportsShowsLivePhase:
    def test_list_shows_progress_line_for_in_flight_load(self, tmp_path):
        tracker = _tracker(tmp_path)
        tracker.save_export(
            export_id="exp-list",
            status=mcp_server.PHASE_DOWNLOADING,
            parquet_urls=["u1", "u2"],
            export_type="vulnerability",
        )
        tracker.set_phase(
            "exp-list",
            mcp_server.PHASE_DOWNLOADING,
            phase_detail="1 / 2 files downloaded",
        )

        result = mcp_server.list_rapid7_exports(limit=10)
        assert "exp-list" in result
        assert "DOWNLOADING" in result
        assert "Progress: 1 / 2 files downloaded" in result


class TestDbLockProtectsReadsDuringLoad:
    @staticmethod
    def _run_holding_lock(fn):
        """Hold _db_lock on a *separate* thread (as a background load does)
        while running fn on this thread, so the non-blocking acquire in the
        tool is genuinely contended — an RLock would let the same thread
        straight through, which is not the real scenario."""
        import threading

        holding = threading.Event()
        release = threading.Event()

        def holder():
            with mcp_server._db_lock:
                holding.set()
                release.wait(timeout=5)

        t = threading.Thread(target=holder, daemon=True)
        t.start()
        holding.wait(timeout=5)
        try:
            return fn()
        finally:
            release.set()
            t.join(timeout=5)

    def test_query_returns_busy_message_when_lock_held(self, monkeypatch):
        fake_db = FakeDB()
        monkeypatch.setattr(mcp_server, "db", fake_db)

        result = self._run_holding_lock(lambda: mcp_server.query_rapid7(sql="SELECT 1"))

        assert "background download/load is currently in progress" in result

    def test_query_works_normally_when_lock_free(self, monkeypatch):
        fake_db = FakeDB()
        monkeypatch.setattr(mcp_server, "db", fake_db)

        result = mcp_server.query_rapid7(sql="SELECT 1")

        assert "Query executed successfully" in result

    def test_purge_returns_busy_message_when_lock_held(self, monkeypatch):
        """Finding #1: purge must not run while a background load holds the DB."""
        fake_db = FakeDB()
        monkeypatch.setattr(mcp_server, "db", fake_db)

        result = self._run_holding_lock(mcp_server.purge_rapid7_data)

        assert "can't be purged right now" in result


class TestDbLockIsReentrant:
    def test_db_lock_can_be_acquired_twice_by_same_thread(self):
        """Finding #2: RLock prevents self-deadlock if one locked helper
        calls another that also locks."""
        with mcp_server._db_lock:
            acquired = mcp_server._db_lock.acquire(blocking=False)
            assert acquired, "_db_lock is not re-entrant"
            mcp_server._db_lock.release()


def _patch_remediation(monkeypatch, created_ids, load_side_effect=None, create_side_effect=None):
    """Wire the remediation orchestrator's platform calls.

    created_ids: appended (start, end) per create so tests can assert distinct
                 IDs and date order.
    """
    monkeypatch.setattr(mcp_server, "load_config", lambda: {"api_key": "k", "endpoint": "https://example.test"})

    def _create(config, start, end):
        if create_side_effect is not None:
            create_side_effect(start, end)
        eid = f"rem-{start}"
        created_ids.append({"id": eid, "start": start, "end": end})
        return eid

    monkeypatch.setattr(mcp_server, "create_remediation_export", _create)
    monkeypatch.setattr(mcp_server, "poll_until_complete", lambda config, eid: ["https://example.test/f.parquet"])
    monkeypatch.setattr(
        mcp_server,
        "get_export_status",
        lambda config, eid: {
            "status": "COMPLETE",
            "parquetFiles": ["https://example.test/f.parquet"],
            "result": [{"prefix": "remediation", "urls": ["https://example.test/f.parquet"]}],
        },
    )
    monkeypatch.setattr(mcp_server, "download_all_files", lambda urls, api_key: [b"x" * 200 for _ in urls])


def _wait_for_job(tmp_path, job_id, target, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = _tracker(tmp_path).get_job(job_id)
        if job is not None and job.get("status") in target:
            return job
        time.sleep(0.02)
    raise AssertionError(f"Job {job_id} did not reach {target} within {timeout}s")


def _job_id_from_start(result_text):
    for line in result_text.splitlines():
        if line.strip().startswith("Job ID:"):
            return line.split("Job ID:", 1)[1].strip()
    raise AssertionError(f"no Job ID in: {result_text}")


class TestMultiChunkRemediation:
    def test_six_month_range_creates_six_distinct_ids_in_date_order(self, monkeypatch, tmp_path):
        """Direct regression: a >1-month range must create one export per
        window, each with its OWN id and date range — never collapse to one."""
        fake_db = FakeDB(row_counts={"vulnerability_remediation": 100})
        monkeypatch.setattr(mcp_server, "db", fake_db)
        created = []
        _patch_remediation(monkeypatch, created)

        result = mcp_server.start_rapid7_export(
            export_type="remediation", start_date="2026-01-01", end_date="2026-06-30"
        )
        job_id = _job_id_from_start(result)
        job = _wait_for_job(tmp_path, job_id, {mcp_server.JOB_COMPLETE, mcp_server.JOB_FAILED})

        assert job["status"] == mcp_server.JOB_COMPLETE
        ids = [c["export_id"] for c in job["chunks"]]
        assert len(ids) == len(set(ids)) == 6, f"expected 6 distinct ids, got {ids}"
        starts = [c["start"] for c in created]
        assert starts == sorted(starts), "chunks not created in date order"

    def test_all_windows_append_row_counts_sum(self, monkeypatch, tmp_path):
        fake_db = FakeDB(row_counts={"vulnerability_remediation": 100})
        monkeypatch.setattr(mcp_server, "db", fake_db)
        _patch_remediation(monkeypatch, [])

        result = mcp_server.start_rapid7_export(
            export_type="remediation", start_date="2026-01-01", end_date="2026-03-15"
        )
        job_id = _job_id_from_start(result)
        job = _wait_for_job(tmp_path, job_id, {mcp_server.JOB_COMPLETE})

        n = len(job["chunks"])
        assert n == 3
        # Every chunk appended (load called once per chunk), row total is the sum.
        assert len(fake_db.load_calls) == n
        assert sum(c["row_count"] for c in job["chunks"]) == 100 * n

    def test_in_progress_on_a_chunk_retries_same_chunk(self, monkeypatch, tmp_path):
        """A chunk hitting ExportInProgressError must retry ITS OWN range, not
        skip or adopt a foreign id."""
        monkeypatch.setattr(mcp_server, "_IN_PROGRESS_RETRY_SECS", 0.01)
        fake_db = FakeDB(row_counts={"vulnerability_remediation": 5})
        monkeypatch.setattr(mcp_server, "db", fake_db)

        state = {"failed_once": set()}

        def _create_side_effect(start, end):
            # First attempt on the 2nd chunk raises in-progress once, then succeeds.
            if start not in state["failed_once"] and start == "2026-02-01":
                state["failed_once"].add(start)
                raise mcp_server.ExportInProgressError("someone-elses-export==")

        created = []
        _patch_remediation(monkeypatch, created, create_side_effect=_create_side_effect)

        result = mcp_server.start_rapid7_export(
            export_type="remediation", start_date="2026-01-01", end_date="2026-02-15"
        )
        job_id = _job_id_from_start(result)
        job = _wait_for_job(tmp_path, job_id, {mcp_server.JOB_COMPLETE})

        ids = [c["export_id"] for c in job["chunks"]]
        assert "someone-elses-export==" not in ids, "adopted a foreign in-flight id"
        assert job["chunks"][1]["export_id"] == "rem-2026-02-01", "did not recreate its own chunk"

    def test_partial_failure_names_loaded_and_missing_windows(self, monkeypatch, tmp_path):
        monkeypatch.setattr(mcp_server, "_IN_PROGRESS_RETRY_SECS", 0.01)
        fake_db = FakeDB(row_counts={"vulnerability_remediation": 5})
        monkeypatch.setattr(mcp_server, "db", fake_db)
        _patch_remediation(monkeypatch, [])

        # Fail the load on the third chunk only.
        real_loader = mcp_server._download_and_load_files
        calls = {"n": 0}

        def _flaky(export_type, status_info, api_key):
            calls["n"] += 1
            if calls["n"] == 3:
                raise RuntimeError("simulated load failure on chunk 3")
            return real_loader(export_type, status_info, api_key)

        monkeypatch.setattr(mcp_server, "_download_and_load_files", _flaky)

        result = mcp_server.start_rapid7_export(
            export_type="remediation", start_date="2026-01-01", end_date="2026-03-15"
        )
        job_id = _job_id_from_start(result)
        _wait_for_job(tmp_path, job_id, {mcp_server.JOB_FAILED})

        status_text = mcp_server.check_rapid7_export_status(export_id=job_id)
        assert "failed partway" in status_text
        assert "2026-01-01" in status_text  # a loaded window is named
        assert "2026-03-04" in status_text  # the missing window (3rd chunk) is named

    def test_status_tool_reports_job_progress_with_window(self, monkeypatch, tmp_path):
        """A job id passed to check_rapid7_export_status reports the current window."""
        tracker = _tracker(tmp_path)
        tracker.create_job(
            job_id="remediation-abc",
            export_type="remediation",
            start_date="2026-01-01",
            end_date="2026-03-01",
            chunks=[{"start": "2026-02-01", "end": "2026-03-01", "export_id": None, "status": "loading"}],
            status=mcp_server.JOB_RUNNING,
        )
        tracker.update_job("remediation-abc", current_index=0, message="chunk 1/1 (2026-02-01 → 2026-03-01), loading")

        result = mcp_server.check_rapid7_export_status(export_id="remediation-abc")
        assert "in progress" in result
        assert "2026-02-01 → 2026-03-01" in result


class TestPurgeRefusesActiveWork:
    def test_purge_refused_while_a_job_is_running(self, monkeypatch, tmp_path):
        """C2: purge must refuse while a multi-window job is RUNNING, even
        though _db_lock is free between windows."""
        fake_db = FakeDB()
        monkeypatch.setattr(mcp_server, "db", fake_db)
        tracker = _tracker(tmp_path)
        tracker.create_job(
            job_id="remediation-active",
            export_type="remediation",
            start_date="2026-01-01",
            end_date="2026-02-01",
            chunks=[{"start": "2026-01-01", "end": "2026-02-01", "export_id": None, "status": "loading"}],
            status=mcp_server.JOB_RUNNING,
        )

        result = mcp_server.purge_rapid7_data()

        assert "still active" in result
        assert fake_db.has_data_value is True, "purge ran despite an active job"

    def test_purge_refused_while_an_export_is_downloading(self, monkeypatch, tmp_path):
        fake_db = FakeDB()
        monkeypatch.setattr(mcp_server, "db", fake_db)
        tracker = _tracker(tmp_path)
        tracker.save_export(
            export_id="exp-dl",
            status=mcp_server.PHASE_DOWNLOADING,
            parquet_urls=["u1"],
            export_type="vulnerability",
        )

        result = mcp_server.purge_rapid7_data()

        assert "still active" in result
        assert fake_db.has_data_value is True

    def test_purge_succeeds_when_no_active_work(self, monkeypatch, tmp_path):
        fake_db = FakeDB()
        monkeypatch.setattr(mcp_server, "db", fake_db)

        result = mcp_server.purge_rapid7_data()

        assert "purged" in result
        assert fake_db.has_data_value is False


class TestReadToolsLockBeforeHasData:
    """I3: the busy response must win even though has_data() opens its own
    DuckDB connection — the lock has to be held before has_data() is called.
    A FakeDB whose has_data() raises stands in for the RO/RW config conflict a
    real load would trigger; if the tool checked has_data() before locking, the
    exception would surface instead of the busy message."""

    class ExplodingHasData(FakeDB):
        def has_data(self):
            raise RuntimeError("DuckDB config conflict (simulated)")

    def _held_on_other_thread(self, fn):
        import threading

        holding, release = threading.Event(), threading.Event()

        def holder():
            with mcp_server._db_lock:
                holding.set()
                release.wait(timeout=5)

        t = threading.Thread(target=holder, daemon=True)
        t.start()
        holding.wait(timeout=5)
        try:
            return fn()
        finally:
            release.set()
            t.join(timeout=5)

    def test_query_busy_before_has_data(self, monkeypatch):
        monkeypatch.setattr(mcp_server, "db", self.ExplodingHasData())
        result = self._held_on_other_thread(lambda: mcp_server.query_rapid7(sql="SELECT 1"))
        assert "background download/load is currently in progress" in result

    def test_schema_busy_before_has_data(self, monkeypatch):
        monkeypatch.setattr(mcp_server, "db", self.ExplodingHasData())
        result = self._held_on_other_thread(mcp_server.get_rapid7_schema)
        assert "background download/load is currently in progress" in result

    def test_stats_busy_before_has_data(self, monkeypatch):
        monkeypatch.setattr(mcp_server, "db", self.ExplodingHasData())
        result = self._held_on_other_thread(mcp_server.get_rapid7_stats)
        assert "background download/load is currently in progress" in result

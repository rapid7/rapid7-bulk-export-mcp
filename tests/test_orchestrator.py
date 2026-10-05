"""Unit tests for the synchronous export orchestrator.

These deliberately import only ``src.orchestrator`` — never ``src.mcp_server``
— to prove the orchestration logic is usable without FastMCP. Platform calls
and the database are injected through an OrchestratorContext built here, so the
create → poll → download → load pipeline runs entirely against fakes.
"""

import threading
from pathlib import Path

from src import orchestrator
from src.export_manager import ExportInProgressError


class FakeDB:
    """Minimal stand-in for VulnerabilityDatabase capturing load calls."""

    def __init__(self, row_counts=None):
        self.row_counts = row_counts if row_counts is not None else {"vulnerabilities": 3}
        self.load_calls = []

    def load_parquet_files_by_prefix(self, prefix_file_map, skip_prefixes=None, append=False):
        self.load_calls.append({"map": dict(prefix_file_map), "skip": skip_prefixes, "append": append})
        return dict(self.row_counts)

    def get_stats(self):
        return {"vulnerabilities": {"total_rows": self.row_counts.get("vulnerabilities", 0)}}


def _status(prefix="asset_vulnerability", n=1):
    urls = [f"https://example.test/f{i}.parquet" for i in range(n)]
    return {"status": "COMPLETE", "parquetFiles": urls, "result": [{"prefix": prefix, "urls": urls}]}


def _context(tmp_path, db, **overrides):
    """Build a context bound to a shared FakeDB and stubbed platform calls."""
    holder = {"db": db}
    defaults = dict(
        data_dir=Path(tmp_path),
        db_lock=threading.RLock(),
        get_db=lambda: holder["db"],
        ensure_db=lambda: holder["db"],
        set_db=lambda v: holder.__setitem__("db", v),
        load_config=lambda: {"api_key": "k", "endpoint": "https://example.test"},
        get_export_status=lambda config, eid: _status(),
        download_all_files=lambda urls, api_key: [b"x" * 200 for _ in urls],
        create_remediation_export=lambda config, s, e: f"rem-{s}",
        poll_until_complete=lambda config, eid: ["https://example.test/f0.parquet"],
        snapshot_creators={
            "vulnerability": lambda config: "vuln-1",
            "policy": lambda config: "pol-1",
            "asset_software": lambda config: "asw-1",
        },
        in_progress_retry_secs=0.01,
    )
    defaults.update(overrides)
    return orchestrator.OrchestratorContext(**defaults)


class TestDownloadAndLoadFiles:
    def test_routes_and_loads_and_reports_rows(self, tmp_path):
        db = FakeDB(row_counts={"vulnerabilities": 5, "assets": 2})
        ctx = _context(tmp_path, db)
        row_count, row_counts, stats, warnings = orchestrator.download_and_load_files(
            ctx, "vulnerability", _status(n=2), "k"
        )
        assert row_count == 7
        assert warnings == []
        assert len(db.load_calls) == 1

    def test_policy_skips_asset_prefix(self, tmp_path):
        db = FakeDB()
        ctx = _context(tmp_path, db)
        orchestrator.download_and_load_files(ctx, "policy", _status(prefix="policy"), "k")
        assert db.load_calls[0]["skip"] == {"asset"}

    def test_remediation_appends(self, tmp_path):
        db = FakeDB()
        ctx = _context(tmp_path, db)
        orchestrator.download_and_load_files(ctx, "remediation", _status(prefix="remediation"), "k")
        assert db.load_calls[0]["append"] is True

    def test_on_downloaded_called_before_load(self, tmp_path):
        db = FakeDB()
        order = []
        db_load = db.load_parquet_files_by_prefix

        def _tracking_load(*a, **kw):
            order.append("load")
            return db_load(*a, **kw)

        db.load_parquet_files_by_prefix = _tracking_load
        ctx = _context(tmp_path, db)
        orchestrator.download_and_load_files(
            ctx, "vulnerability", _status(), "k", on_downloaded=lambda: order.append("downloaded")
        )
        assert order == ["downloaded", "load"]


class TestRunSnapshotRefresh:
    def test_loads_and_returns_ok_window(self, tmp_path):
        db = FakeDB(row_counts={"vulnerabilities": 9})
        ctx = _context(tmp_path, db)
        result = orchestrator.run_snapshot_refresh(
            "vulnerability", config={"api_key": "k"}, db_path=str(tmp_path / "x.db"), data_dir=Path(tmp_path), ctx=ctx
        )
        assert result.ok
        assert result.kind == "vulnerability"
        assert result.row_count == 9

    def test_unsupported_type_fails_softly(self, tmp_path):
        db = FakeDB()
        ctx = _context(tmp_path, db)
        result = orchestrator.run_snapshot_refresh(
            "nope", config={"api_key": "k"}, db_path=str(tmp_path / "x.db"), data_dir=Path(tmp_path), ctx=ctx
        )
        assert not result.ok
        assert "unsupported" in result.error

    def test_platform_error_is_captured_not_raised(self, tmp_path):
        db = FakeDB()

        def _boom(config, eid):
            raise RuntimeError("platform down")

        ctx = _context(tmp_path, db, poll_until_complete=_boom)
        result = orchestrator.run_snapshot_refresh(
            "vulnerability", config={"api_key": "k"}, db_path=str(tmp_path / "x.db"), data_dir=Path(tmp_path), ctx=ctx
        )
        assert not result.ok
        assert "platform down" in result.error


class TestRunRemediationRefresh:
    def test_multi_window_sums_rows(self, tmp_path):
        db = FakeDB(row_counts={"vulnerability_remediation": 10})
        ctx = _context(tmp_path, db)
        result = orchestrator.run_remediation_refresh(
            "2026-01-01",
            "2026-03-15",
            config={"api_key": "k"},
            db_path=str(tmp_path / "x.db"),
            data_dir=Path(tmp_path),
            ctx=ctx,
        )
        assert result.ok
        assert len(db.load_calls) == 3  # a >31-day range splits into 3 windows
        assert result.row_count == 30

    def test_in_progress_retries_its_own_range(self, tmp_path):
        db = FakeDB(row_counts={"vulnerability_remediation": 1})
        failed_once = set()

        def _create(config, s, e):
            if s == "2026-02-01" and s not in failed_once:
                failed_once.add(s)
                raise ExportInProgressError("someone-else==")
            return f"rem-{s}"

        ctx = _context(tmp_path, db, create_remediation_export=_create)
        result = orchestrator.run_remediation_refresh(
            "2026-01-01",
            "2026-02-15",
            config={"api_key": "k"},
            db_path=str(tmp_path / "x.db"),
            data_dir=Path(tmp_path),
            ctx=ctx,
        )
        assert result.ok

    def test_failure_reports_partial_row_count(self, tmp_path):
        db = FakeDB(row_counts={"vulnerability_remediation": 4})
        calls = {"n": 0}
        base = db.load_parquet_files_by_prefix

        def _flaky(*a, **kw):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("load blew up")
            return base(*a, **kw)

        db.load_parquet_files_by_prefix = _flaky
        ctx = _context(tmp_path, db)
        result = orchestrator.run_remediation_refresh(
            "2026-01-01",
            "2026-03-15",
            config={"api_key": "k"},
            db_path=str(tmp_path / "x.db"),
            data_dir=Path(tmp_path),
            ctx=ctx,
        )
        assert not result.ok
        assert result.row_count == 4  # first window loaded before the second failed
        assert "load blew up" in result.error


class TestRefreshAll:
    def test_every_window_attempted_and_ok(self, tmp_path, monkeypatch):
        def fake_snapshot(export_type, *, config, db_path, data_dir, ctx=None):
            return orchestrator.WindowResult(kind=export_type, ok=True, row_count=2)

        monkeypatch.setattr(orchestrator, "run_snapshot_refresh", fake_snapshot)
        result = orchestrator.refresh_all(
            ["vulnerability", "policy"],
            config={"api_key": "k"},
            db_path=str(tmp_path / "x.db"),
            data_dir=Path(tmp_path),
        )
        assert isinstance(result, orchestrator.RefreshResult)
        assert result.ok
        assert len(result.windows) == 2
        assert result.total_rows == 4

    def test_one_failure_does_not_abort_the_rest(self, tmp_path, monkeypatch):
        # vulnerability fails to poll; policy still runs and the result names both.
        def fake_snapshot(export_type, *, config, db_path, data_dir, ctx=None):
            ok = export_type != "vulnerability"
            return orchestrator.WindowResult(kind=export_type, ok=ok, error="" if ok else "boom")

        monkeypatch.setattr(orchestrator, "run_snapshot_refresh", fake_snapshot)
        result = orchestrator.refresh_all(
            ["vulnerability", "policy"],
            config={"api_key": "k"},
            db_path=str(tmp_path / "x.db"),
            data_dir=Path(tmp_path),
        )
        assert not result.ok
        assert {w.kind for w in result.windows} == {"vulnerability", "policy"}
        assert [w.kind for w in result.failed] == ["vulnerability"]


class TestForegroundIsSingleProcessNoThreads:
    def test_refresh_all_spawns_no_threads(self, tmp_path, monkeypatch):
        """The foreground path must not hand work to a background thread the way
        the tool path does — a job would die with it."""
        started = []
        real_thread = threading.Thread

        def _spy(*a, **kw):
            started.append(kw.get("target"))
            return real_thread(*a, **kw)

        monkeypatch.setattr(
            orchestrator,
            "run_snapshot_refresh",
            lambda export_type, **kw: orchestrator.WindowResult(kind=export_type, ok=True),
        )
        monkeypatch.setattr(threading, "Thread", _spy)
        orchestrator.refresh_all(
            ["vulnerability"], config={"api_key": "k"}, db_path=str(tmp_path / "x.db"), data_dir=Path(tmp_path)
        )
        # No thread targeting the orchestrator's workers was started.
        assert orchestrator.run_remediation_job not in started
        assert orchestrator.run_download_and_load not in started

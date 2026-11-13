"""Tests for the generate_rapid7_dashboard_report MCP tool."""

import os
import stat
import threading
from datetime import datetime

import pytest

from src import mcp_server
from src.dashboard_queries import CARDS
from tests.test_dashboard_queries import _load


class _Tracker:
    def __init__(self, loaded_today):
        self._loaded_today = set(loaded_today)

    def get_today_export(self, export_type, org_label):
        return {"export_id": "x"} if org_label in self._loaded_today else None

    def close(self):
        pass


@pytest.fixture
def loaded(tmp_path, monkeypatch):
    database = _load(tmp_path)
    monkeypatch.setattr(mcp_server, "db", database)
    monkeypatch.setattr(mcp_server, "_AWAITING_FIRST_REFRESH", False)
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(mcp_server, "load_org_labels", lambda: ["org-a", "org-b"])
    monkeypatch.setattr(mcp_server, "_tracker", lambda: _Tracker({"org-a", "org-b"}))
    yield tmp_path / "data"
    database.close()


def _reports(data_dir):
    return sorted((data_dir / "reports").glob("multi-org-report-*.html"))


def test_writes_every_card_to_a_file_only_its_owner_can_read(loaded):
    result = mcp_server.generate_rapid7_dashboard_report()

    [report] = _reports(loaded)
    assert str(report) in result
    assert f"Open it: [{report.name}]({report.as_uri()})" in result
    assert "Coverage: complete." in result
    assert stat.S_IMODE(report.stat().st_mode) == 0o600
    assert stat.S_IMODE(report.parent.stat().st_mode) == 0o700
    html = report.read_text(encoding="utf-8")
    for card in CARDS.values():
        assert card.title in html
    assert "Coverage is incomplete." not in html


def test_an_org_that_did_not_load_today_leaves_out_the_combined_figures(loaded, monkeypatch):
    monkeypatch.setattr(mcp_server, "_tracker", lambda: _Tracker({"org-a"}))

    result = mcp_server.generate_rapid7_dashboard_report()

    assert "Coverage: incomplete" in result
    assert "org-b" in result
    html = _reports(loaded)[0].read_text(encoding="utf-8")
    assert "Coverage is incomplete." in html
    assert "org-b" in html
    assert "Left out: this combines organisations" in html


def test_a_report_never_overwrites_an_existing_file(loaded, monkeypatch):
    monkeypatch.setattr(mcp_server, "utc_now", lambda: datetime(2026, 9, 23, 9, 0, 0))
    mcp_server.generate_rapid7_dashboard_report()
    [first] = _reports(loaded)
    before = first.read_text(encoding="utf-8")

    result = mcp_server.generate_rapid7_dashboard_report()

    assert result.startswith("✗ Could not write the report")
    assert _reports(loaded) == [first]
    assert first.read_text(encoding="utf-8") == before


def test_an_existing_loose_reports_directory_is_made_private(loaded):
    (loaded / "reports").mkdir(parents=True)
    os.chmod(loaded / "reports", 0o755)

    mcp_server.generate_rapid7_dashboard_report()

    assert stat.S_IMODE((loaded / "reports").stat().st_mode) == 0o700


@pytest.mark.parametrize("org_list", ["empty", "broken"])
def test_several_orgs_with_no_readable_org_list_is_not_called_complete(loaded, monkeypatch, org_list):
    def _broken():
        raise ValueError("bad org file")

    monkeypatch.setattr(mcp_server, "load_org_labels", (lambda: []) if org_list == "empty" else _broken)

    result = mcp_server.generate_rapid7_dashboard_report()

    assert "Coverage: incomplete" in result
    assert "coverage cannot be confirmed" in result
    assert "Left out: this combines organisations" in _reports(loaded)[0].read_text(encoding="utf-8")


def test_a_tracker_failure_is_reported_and_writes_no_file(loaded, monkeypatch):
    def _failing_tracker():
        raise RuntimeError("tracker unavailable")

    monkeypatch.setattr(mcp_server, "_tracker", _failing_tracker)

    result = mcp_server.generate_rapid7_dashboard_report()

    assert result.startswith("✗ Error building the dashboard report")
    assert not (loaded / "reports").exists()


def test_no_data_writes_no_file(loaded, monkeypatch):
    monkeypatch.setattr(mcp_server, "db", None)

    result = mcp_server.generate_rapid7_dashboard_report()

    assert result.startswith("Error: No data loaded.")
    assert not (loaded / "reports").exists()


def test_refuses_while_a_load_holds_the_lock(loaded):
    """_db_lock is re-entrant, so the lock must be held from another thread, as a real load does."""
    held, release = threading.Event(), threading.Event()

    def _load_in_progress():
        with mcp_server._db_lock:
            held.set()
            release.wait(timeout=10)

    loader = threading.Thread(target=_load_in_progress)
    loader.start()
    held.wait(timeout=10)
    try:
        result = mcp_server.generate_rapid7_dashboard_report()
    finally:
        release.set()
        loader.join(timeout=10)

    assert result.startswith("⏳")
    assert not (loaded / "reports").exists()

"""Tests for the generate_rapid7_dashboard_report MCP tool."""

import stat
import threading

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


def test_two_reports_never_overwrite_each_other(loaded):
    mcp_server.generate_rapid7_dashboard_report()
    mcp_server.generate_rapid7_dashboard_report()

    assert len(_reports(loaded)) == 2


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

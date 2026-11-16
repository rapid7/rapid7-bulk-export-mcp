"""The CISA KEV catalogue refreshes after a multi-org load, and never fails or slows the load."""

import pytest

from src import orchestrator
from tests import test_orchestrator as base
from tests.test_orchestrator import FakeDB, OrgFakeDB, _context

_track = base.TestRunDownloadAndLoadForAnOrg._track
_row = base.TestRunDownloadAndLoadForAnOrg._row


class _Refresher:
    def __init__(self, result="updated"):
        self.result, self.calls = result, []

    def __call__(self, db_path, write_lock):
        self.calls.append((db_path, write_lock))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _org_ctx(tmp_path, db, refresher):
    db.db_path = str(tmp_path / "data.db")
    return _context(
        tmp_path,
        db,
        config_for_org=lambda label: {"api_key": f"{label}-key", "endpoint": "https://example.test"},
        refresh_kev=refresher,
    )


@pytest.mark.parametrize(
    ("result", "note"),
    [("updated", "CISA KEV catalogue: updated."), ("unchanged", "CISA KEV catalogue: already up to date.")],
)
def test_an_org_load_refreshes_the_catalogue_with_the_database_write_lock(tmp_path, result, note):
    refresher = _Refresher(result)
    ctx = _org_ctx(tmp_path, OrgFakeDB(), refresher)
    _track(ctx, "exp-japan", "japan")

    orchestrator.run_download_and_load(ctx, "exp-japan", "vulnerability")

    row = _row(ctx, "exp-japan")
    assert row["status"] == orchestrator.PHASE_COMPLETE
    assert note in row["message"]
    assert refresher.calls == [(str(tmp_path / "data.db"), ctx.db_lock)]


def test_a_failed_refresh_still_completes_the_load_and_says_so(tmp_path):
    ctx = _org_ctx(tmp_path, OrgFakeDB(), _Refresher(RuntimeError("cisa.gov unreachable")))
    _track(ctx, "exp-japan", "japan")

    orchestrator.run_download_and_load(ctx, "exp-japan", "vulnerability")

    row = _row(ctx, "exp-japan")
    assert row["status"] == orchestrator.PHASE_COMPLETE
    assert "CISA KEV catalogue not refreshed: cisa.gov unreachable" in row["message"]
    assert "keeps its stand-ins" in row["message"]


def test_a_single_org_load_makes_no_kev_call(tmp_path):
    refresher = _Refresher()
    db = FakeDB()
    db.db_path = str(tmp_path / "data.db")
    ctx = _context(tmp_path, db, refresh_kev=refresher)
    _track(ctx, "exp-single", None)

    orchestrator.run_download_and_load(ctx, "exp-single", "vulnerability")

    assert _row(ctx, "exp-single")["status"] == orchestrator.PHASE_COMPLETE
    assert refresher.calls == []

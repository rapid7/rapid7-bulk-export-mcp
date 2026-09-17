"""Live multi-org test against the real Rapid7 API.

Requires RAPID7_ORGS_FILE to point at an organizations file whose keys all resolve.
Skipped automatically when it is absent, so it never runs in CI.

Run it:
    RAPID7_ORGS_FILE=~/.rapid7_mcp/orgs.json uv run pytest tests/test_local_multi_org_e2e.py -v -s
    # or
    make local-multi-org-test

This is the fastest way to validate real organization keys, before involving an MCP
client. It exercises the same tools an agent would call, in the same order, and prints
the per-organization row counts and the wall clock time for the whole wave, which is the
number the POC needs to report.

The organizations do not have to belong to one customer. Nothing in the export API or in
this tool inspects a customer relationship: an export is scoped to the org in the calling
token, and the union keys on the orgId inside the exported data. Three unrelated
VM-enabled orgs exercise the fan-out exactly as three orgs of one customer would.

It uses a temporary DATA_DIR, so it never touches ~/.rapid7_mcp, and cleans up after
itself.
"""

import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from src import mcp_server
from src.config import load_org_configs
from src.duckdb_loader import VulnerabilityDatabase

pytestmark = pytest.mark.skipif(
    not os.environ.get("RAPID7_ORGS_FILE"),
    reason="RAPID7_ORGS_FILE not set; live multi-org test skipped",
)

# A real export takes minutes on the platform, and organizations run concurrently.
_LOAD_TIMEOUT_SECS = 45 * 60
_POLL_INTERVAL_SECS = 30


@pytest.fixture
def live_data_dir(monkeypatch):
    """Give the run its own data directory and database, then remove them."""
    data_dir = Path(tempfile.mkdtemp(prefix="r7-multi-org-live-"))
    monkeypatch.setattr(mcp_server, "_DATA_DIR", data_dir)
    monkeypatch.setattr(mcp_server, "db", VulnerabilityDatabase(str(data_dir / "live.db")))
    yield data_dir
    shutil.rmtree(data_dir, ignore_errors=True)


def _counts_by_org():
    try:
        rows = mcp_server.db.query('SELECT "orgId" AS org_id, COUNT(*) AS c FROM vulnerabilities GROUP BY "orgId"')
    except ValueError:
        return {}
    return {row["org_id"]: row["c"] for row in rows}


def test_live_multi_org_fan_out(live_data_dir):
    """Fan out across every configured organization and prove the union is complete."""
    configured = [config["label"] for config in load_org_configs() if config.get("label")]
    assert len(configured) >= 2, (
        f"need at least two organizations to prove a fan-out, found {len(configured)}: {configured}"
    )
    print(f"\nConfigured organizations ({len(configured)}): {', '.join(configured)}")

    listed = mcp_server.list_rapid7_orgs()
    print(f"\n{listed}\n")
    assert "not usable" not in listed, "fix the organizations file before running the live test"

    started_at = time.time()
    started = mcp_server.start_rapid7_multi_org_export()
    print(f"{started}\n")
    assert not started.startswith("✗"), "no exports were started"
    assert "Failed:" not in started, (
        "at least one organization could not start an export. A common cause is that bulk "
        "export is not enabled for that org, which is separate from having VM data."
    )

    deadline = time.time() + _LOAD_TIMEOUT_SECS
    status = ""
    while time.time() < deadline:
        status = mcp_server.check_rapid7_multi_org_export()
        print(f"[{time.strftime('%H:%M:%S')}] {status.splitlines()[0]}")
        for line in status.splitlines():
            if line.startswith("  "):
                print(line)
        if f"Loaded: {len(configured)}" in status:
            break
        time.sleep(_POLL_INTERVAL_SECS)
    else:
        pytest.fail(f"organizations did not all load within {_LOAD_TIMEOUT_SECS}s. Last status:\n{status}")

    elapsed = time.time() - started_at

    counts = _counts_by_org()
    print(f"\nWall clock for the whole wave: {elapsed / 60:.1f} minutes")
    print(f"Distinct orgIds loaded: {len(counts)}")
    for org_id, count in sorted(counts.items(), key=lambda item: -item[1]):
        print(f"  {org_id}: {count} findings")
    print(f"\nPer-organization cost if this scaled linearly: {elapsed / len(configured) / 60:.1f} minutes each")

    assert len(counts) == len(configured), (
        f"{len(configured)} organizations configured but {len(counts)} orgIds in the data. "
        f"Two labels may share a tenant, or an organization loaded no rows."
    )

    coverage = mcp_server.get_rapid7_org_coverage()
    print(f"\n{coverage}")
    assert "⚠️" not in coverage, "coverage flagged a problem; do not report totals from this run"


def test_live_refresh_one_org_leaves_the_others_alone(live_data_dir):
    """Re-running one organization must not disturb the rest of the portfolio.

    This is the behaviour the whole change exists to provide, so it is worth proving
    against the real API and not only against fixtures.
    """
    configured = [config["label"] for config in load_org_configs() if config.get("label")]
    assert len(configured) >= 2, "need at least two organizations"

    mcp_server.start_rapid7_multi_org_export()
    deadline = time.time() + _LOAD_TIMEOUT_SECS
    while time.time() < deadline:
        if f"Loaded: {len(configured)}" in mcp_server.check_rapid7_multi_org_export():
            break
        time.sleep(_POLL_INTERVAL_SECS)
    else:
        pytest.fail("initial load did not complete")

    before = _counts_by_org()
    assert before, "no data loaded"

    target = configured[0]
    tracker = mcp_server._tracker()
    try:
        row = tracker.get_today_export(export_type="vulnerability", org_label=target)
    finally:
        tracker.close()
    assert row is not None, f"no completed export recorded for '{target}'"

    print(f"\nReloading '{target}' (export {row['export_id']})")
    mcp_server.download_rapid7_export(export_id=row["export_id"], export_type="vulnerability")

    reload_deadline = time.time() + _LOAD_TIMEOUT_SECS
    while time.time() < reload_deadline:
        current = mcp_server.check_rapid7_multi_org_export()
        if f"Loaded: {len(configured)}" in current:
            break
        time.sleep(_POLL_INTERVAL_SECS)

    after = _counts_by_org()
    print(f"before: {before}\nafter:  {after}")
    assert set(after) == set(before), "reloading one organization changed which organizations are present"

"""End-to-end tests for multi-org reporting, with the Rapid7 platform stubbed.

These drive the real MCP tools and the real DuckDB loader. Only the three network
boundaries are replaced: export creation, export status, and file download. That
makes them the standing proof that the multi-org path works, without needing a
tenant or any API key, so the behaviour is verified before a demo rather than
during one.

The fake platform deliberately refuses to serve one org's export to another org's
key. The bulk export API behaves that way, and a test that tolerated it would hide
the exact mistake that produces a confident, wrong portfolio report.
"""

import json
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src import mcp_server
from src.db_utils import duckdb_connection
from src.duckdb_loader import VulnerabilityDatabase

# label -> (api key, orgId in the data, finding ids)
ORGS = {
    "japan": ("key-japan", "ORG-JP", ["V1", "V2", "V3"]),
    "us": ("key-us", "ORG-US", ["V1", "V2"]),
    "europe": ("key-europe", "ORG-EU", ["V9"]),
}


def _write_vuln_parquet(path, org_id, finding_ids):
    table = pa.table(
        {
            "orgId": [org_id] * len(finding_ids),
            "vulnId": list(finding_ids),
            "assetId": [f"ASSET-{i}" for i in range(len(finding_ids))],
            "severity": ["Critical"] * len(finding_ids),
            "cvssV3Severity": ["Critical"] * len(finding_ids),
        }
    )
    pq.write_table(table, path)


class FakePlatform:
    """Stands in for the Rapid7 export API, one tenant per API key."""

    def __init__(self, tmp_path, orgs=ORGS):
        self.key_to_label = {key: label for label, (key, _, _) in orgs.items()}
        self.files = {}
        for label, (_, org_id, finding_ids) in orgs.items():
            path = tmp_path / f"{label}.parquet"
            _write_vuln_parquet(path, org_id, finding_ids)
            self.files[label] = path
        self.created = []
        self.status_calls = []

    def _label_for_key(self, api_key):
        label = self.key_to_label.get(api_key)
        if label is None:
            raise AssertionError(f"unknown API key used: {api_key!r}")
        return label

    def create_export(self, config):
        label = self._label_for_key(config["api_key"])
        self.created.append(label)
        return f"export-{label}"

    def get_status(self, config, export_id):
        label = self._label_for_key(config["api_key"])
        self.status_calls.append((label, export_id))
        owner = export_id.removeprefix("export-")
        if owner != label:
            # This is what the real API does, and the reason the export's owning org
            # has to be recorded and used for every later call.
            raise AssertionError(f"export '{export_id}' polled with org '{label}' credentials")
        url = f"https://fake.invalid/{label}.parquet"
        return {
            "status": "COMPLETE",
            "parquetFiles": [url],
            "result": [{"prefix": "asset_vulnerability", "urls": [url]}],
        }

    def download(self, urls, api_key):
        label = self._label_for_key(api_key)
        payloads = []
        for url in urls:
            requested = url.rsplit("/", 1)[-1].removesuffix(".parquet")
            if requested != label:
                raise AssertionError(f"file for '{requested}' downloaded with org '{label}' credentials")
            payloads.append(self.files[label].read_bytes())
        return payloads

    def refresh(self, label, finding_ids):
        """Change what an org's export returns, as a later day's export would."""
        _write_vuln_parquet(self.files[label], ORGS[label][1], finding_ids)


@pytest.fixture
def platform(monkeypatch, tmp_path):
    """Wire a stubbed platform, a temp data dir, and a real database."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(mcp_server, "_DATA_DIR", data_dir)
    monkeypatch.setattr(mcp_server, "db", VulnerabilityDatabase(str(data_dir / "test.db")))

    orgs_file = tmp_path / "orgs.json"
    orgs_file.write_text(
        json.dumps({"orgs": [{"label": label, "key_ref": f"R7_KEY_{label.upper()}"} for label in ORGS]}),
        encoding="utf-8",
    )
    monkeypatch.setenv("RAPID7_ORGS_FILE", str(orgs_file))
    monkeypatch.setenv("RAPID7_REGION", "us")
    for label, (key, _, _) in ORGS.items():
        monkeypatch.setenv(f"R7_KEY_{label.upper()}", key)

    fake = FakePlatform(tmp_path)
    monkeypatch.setattr(mcp_server, "create_vulnerability_export", fake.create_export)
    monkeypatch.setattr(mcp_server, "get_export_status", fake.get_status)
    monkeypatch.setattr(mcp_server, "download_all_files", fake.download)
    return fake


def _load_all_orgs(timeout=30.0):
    """Poll the multi-org check tool until every org is loaded, as an agent would."""
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        # wait_seconds=0 keeps the offline suite fast; the waiting behaviour has its
        # own test rather than slowing every other case down.
        last = mcp_server.check_rapid7_multi_org_export(wait_seconds=0)
        if f"Loaded: {len(ORGS)}" in last:
            return last
        time.sleep(0.2)
    raise AssertionError(f"orgs did not all load within {timeout}s. Last status:\n{last}")


def _counts_by_org():
    try:
        rows = mcp_server.db.query('SELECT "orgId" AS org_id, COUNT(*) AS c FROM vulnerabilities GROUP BY "orgId"')
    except ValueError:
        # The table only exists once the first load has run, and callers poll from
        # before that point.
        return {}
    return {row["org_id"]: row["c"] for row in rows}


def _load_one_org(label, expected_rows=None, timeout=30.0):
    """Export and load a single org, waiting for the background load to finish."""
    mcp_server.start_rapid7_export(export_type="vulnerability", org_label=label)
    mcp_server.download_rapid7_export(export_id=f"export-{label}", export_type="vulnerability")

    org_id = ORGS[label][1]
    target = len(ORGS[label][2]) if expected_rows is None else expected_rows
    deadline = time.time() + timeout
    while time.time() < deadline:
        if target == 0:
            # An org with no findings contributes no GROUP BY row, so there is no count
            # to wait on. Wait for the load to be recorded complete instead.
            tracker = mcp_server._tracker()
            try:
                if tracker.get_today_export(export_type="vulnerability", org_label=label) is not None:
                    return
            finally:
                tracker.close()
        elif _counts_by_org().get(org_id) == target:
            return
        time.sleep(0.2)
    raise AssertionError(f"org '{label}' did not reach {target} rows within {timeout}s: {_counts_by_org()}")


def test_MultiOrg_ThreeOrgsLandInOneWorkspace(platform):
    """The headline claim: N orgs, one workspace, correct per-org totals."""
    listed = mcp_server.list_rapid7_orgs()
    for label in ORGS:
        assert label in listed

    started = mcp_server.start_rapid7_multi_org_export()
    assert f"Started vulnerability exports for {len(ORGS)} of {len(ORGS)}" in started
    assert sorted(platform.created) == sorted(ORGS)

    _load_all_orgs()

    assert _counts_by_org() == {"ORG-JP": 3, "ORG-US": 2, "ORG-EU": 1}
    distinct = mcp_server.db.query('SELECT COUNT(DISTINCT "orgId") AS c FROM vulnerabilities')[0]["c"]
    assert distinct == len(ORGS)

    coverage = mcp_server.get_rapid7_org_coverage()
    assert "Counts match" in coverage
    for org_id in ("ORG-JP", "ORG-US", "ORG-EU"):
        assert org_id in coverage

    # The query tool itself works over the union, which is what the report uses.
    answer = mcp_server.query_rapid7("SELECT COUNT(*) AS total FROM vulnerabilities WHERE severity = 'Critical'")
    assert "6" in answer


def test_MultiOrg_RefreshingOneOrgLeavesTheOthersAlone(platform):
    """A same-day re-run of one org must not disturb the rest of the portfolio."""
    mcp_server.start_rapid7_multi_org_export()
    _load_all_orgs()
    assert _counts_by_org() == {"ORG-JP": 3, "ORG-US": 2, "ORG-EU": 1}

    platform.refresh("japan", ["V1", "V2", "V3", "V4"])
    reloaded = mcp_server.download_rapid7_export(export_id="export-japan", export_type="vulnerability")
    assert reloaded.startswith("▶️")
    deadline = time.time() + 30.0
    while time.time() < deadline and _counts_by_org().get("ORG-JP") != 4:
        time.sleep(0.2)

    assert _counts_by_org() == {"ORG-JP": 4, "ORG-US": 2, "ORG-EU": 1}


def test_MultiOrg_SameDayStartReusesEachOrgsOwnExport(platform):
    """The reuse cache is per org, so a second run creates no duplicate exports."""
    mcp_server.start_rapid7_multi_org_export()
    _load_all_orgs()
    created_first = list(platform.created)

    again = mcp_server.start_rapid7_multi_org_export()

    assert platform.created == created_first, "a second run must not create new exports"
    assert again.count("already exists") == len(ORGS)


def test_MultiOrg_MissingKeyStopsTheRunAndNamesTheOrg(platform, monkeypatch):
    """One unreachable org fails the whole run loudly rather than reporting a subset."""
    monkeypatch.delenv("R7_KEY_EUROPE")
    monkeypatch.setattr("src.config._get_key_from_keychain", lambda service_name: None)

    listed = mcp_server.list_rapid7_orgs()
    assert "not usable" in listed
    assert "europe" in listed

    started = mcp_server.start_rapid7_multi_org_export()
    assert started.startswith("✗")
    assert "europe" in started
    assert not platform.created, "no export should be created when the org list is unusable"


def test_MultiOrg_CoverageWarnsWhenAnOrgIsMissingFromTheData(platform):
    """Coverage must flag a shortfall, so nobody presents partial totals as complete."""
    for label in ("japan", "us"):
        _load_one_org(label)

    assert set(_counts_by_org()) == {"ORG-JP", "ORG-US"}

    coverage = mcp_server.get_rapid7_org_coverage()
    assert "⚠️" in coverage
    assert "have not loaded today: europe" in coverage, "the missing org must be named, not just counted"
    assert "Do not present these as portfolio totals" in coverage


def test_MultiOrg_PolicyIsRefusedPerOrg(platform):
    """Policy exports carry NULL-org shared rows, so they cannot be unioned per org."""
    assert mcp_server.start_rapid7_multi_org_export(export_type="policy").startswith("✗")
    assert mcp_server.start_rapid7_export(export_type="policy", org_label="japan").startswith("✗")


def test_MultiOrg_UnknownOrgLabelIsRejected(platform):
    """An unrecognised label must not silently fall back to the default credential."""
    result = mcp_server.start_rapid7_export(export_type="vulnerability", org_label="atlantis")
    assert result.startswith("✗")
    assert "atlantis" in result
    assert not platform.created


def test_MultiOrg_MismatchedExportTypeOnRetryIsRefused(platform):
    """A retry with the wrong export_type must not snapshot-load and wipe the other orgs.

    The runbook tells the operator to retry a single org with download_rapid7_export.
    If a mismatched type were accepted, the load would take a snapshot branch, drop the
    vulnerabilities table, and leave only the retried org behind.
    """
    mcp_server.start_rapid7_multi_org_export()
    _load_all_orgs()
    before = _counts_by_org()

    refused = mcp_server.download_rapid7_export(export_id="export-japan", export_type="policy")

    assert refused.startswith("✗")
    assert "is a vulnerability export" in refused
    assert _counts_by_org() == before, "a refused retry must not touch the data"


def test_MultiOrg_OrgScopedLoadRefusesNonMultiOrgType(platform):
    """The loader itself fails closed, not just the tool that calls it.

    org_scoped is evaluated before the per-type branches, so a mismatched type can never
    fall through into a snapshot load that discards the other orgs.
    """
    status_info = {
        "status": "COMPLETE",
        "parquetFiles": ["https://fake.invalid/japan.parquet"],
        "result": [{"prefix": "asset_vulnerability", "urls": ["https://fake.invalid/japan.parquet"]}],
    }

    with pytest.raises(ValueError, match="Refusing to load export_type 'policy'"):
        mcp_server._download_and_load_files("policy", status_info, "key-japan", org_scoped=True)


def test_MultiOrg_CoverageDoesNotLetAStaleOrgMaskAMissingOne(platform):
    """Coverage must compare identity, not counts.

    With a leftover org in the table, the number of orgIds can equal the number of
    configured orgs while a configured org is entirely absent. Counting alone would
    report "counts match" over a portfolio that is both short and polluted.
    """
    for label in ("japan", "us"):
        _load_one_org(label)

    stale = pa.table(
        {
            "orgId": ["ORG-ATLANTIS"],
            "vulnId": ["V1"],
            "assetId": ["ASSET-0"],
            "severity": ["Critical"],
            "cvssV3Severity": ["Critical"],
        }
    )
    stale_path = platform.files["japan"].parent / "stale.parquet"
    pq.write_table(stale, stale_path)
    mcp_server.db.load_parquet_files_by_prefix({"asset_vulnerability": [str(stale_path)]}, org_scoped=True)

    assert len(_counts_by_org()) == 3, "three orgIds present, matching the three configured labels"

    coverage = mcp_server.get_rapid7_org_coverage()
    assert "Counts match" not in coverage
    assert "have not loaded today: europe" in coverage
    assert "rows from an earlier run" in coverage


def test_MultiOrg_CoverageAcceptsAnOrgWithNoFindings(platform):
    """An org that loaded cleanly but has no findings must not read as a failed load.

    Otherwise the gate warns on every future run and the operator learns to ignore it.
    """
    platform.refresh("europe", [])
    for label in ("japan", "us", "europe"):
        _load_one_org(label, expected_rows=0 if label == "europe" else None)

    coverage = mcp_server.get_rapid7_org_coverage()
    assert "⚠️" not in coverage
    assert "zero vulnerability findings" in coverage


def test_MultiOrg_YesterdaysExportIsNotReloadedAsToday(platform):
    """A stale export must not be downloaded and presented as today's data.

    The platform keeps exports for 30 days, so yesterday's id still resolves. Serving
    it as current is worse than reporting no data.
    """
    tracker = mcp_server._tracker()
    try:
        tracker.save_export(
            export_id="export-japan",
            status=mcp_server.PHASE_COMPLETE,
            parquet_urls=["https://fake.invalid/japan.parquet"],
            row_count=3,
            export_type="vulnerability",
            org_label="japan",
        )
        with duckdb_connection(tracker.db_path) as conn:
            conn.execute("UPDATE exports SET export_date = DATE '2020-01-01' WHERE export_id = 'export-japan'")

        stale = mcp_server._latest_export_for_org(tracker, "vulnerability", "japan")
        assert stale is None, "a stale export must not be treated as current"
    finally:
        tracker.close()

    status = mcp_server.check_rapid7_multi_org_export(wait_seconds=0)
    assert "japan: no export today" in status


def test_MultiOrg_CoverageRefusesToAnswerMidLoad(platform, monkeypatch):
    """Coverage must decline while a load is running rather than read a half-loaded table.

    It is the gate before quoting a portfolio total, so a reading taken mid-fan-out
    could show a shortfall that is not real, or a match before the last org has landed.
    """
    for label in ("japan", "us", "europe"):
        _load_one_org(label)
    assert "Counts match" in mcp_server.get_rapid7_org_coverage()

    mcp_server._db_lock.acquire()
    try:
        held = mcp_server.get_rapid7_org_coverage()
    finally:
        mcp_server._db_lock.release()

    # The lock is re-entrant, so it has to be held by another thread to be contended.
    assert "Counts match" in held

    blocked: list = []

    def _read_from_other_thread():
        blocked.append(mcp_server.get_rapid7_org_coverage())

    mcp_server._db_lock.acquire()
    try:
        thread = threading.Thread(target=_read_from_other_thread)
        thread.start()
        thread.join(timeout=10)
    finally:
        mcp_server._db_lock.release()

    assert blocked and "can't be read yet" in blocked[0]

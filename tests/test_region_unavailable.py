"""A region that is down fails fast and clearly, and the outage is recorded for coverage."""

from datetime import date, datetime

import pytest
import requests
import responses

from src import graphql_client, mcp_server
from src.export_tracker import ExportTracker
from src.graphql_client import RegionUnavailableError, send_graphql_request
from tests import test_multi_org_e2e

platform = test_multi_org_e2e.platform

ENDPOINT = "https://me1.api.insight.rapid7.com/export/graphql"


@pytest.mark.parametrize("status", [502, 503, 504])
@responses.activate
def test_a_gateway_error_reads_as_region_unavailable(status):
    responses.add(responses.POST, ENDPOINT, status=status)

    with pytest.raises(RegionUnavailableError, match=f"me1.api.insight.rapid7.com answered HTTP {status}"):
        send_graphql_request(ENDPOINT, "key", "query { x }")


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        (requests.ConnectTimeout("slow"), "did not respond in time"),
        (requests.ConnectionError("refused"), "could not be reached"),
    ],
)
@responses.activate
def test_no_answer_reads_as_region_unavailable(failure, reason):
    responses.add(responses.POST, ENDPOINT, body=failure)

    with pytest.raises(RegionUnavailableError, match=reason):
        send_graphql_request(ENDPOINT, "key", "query { x }")


@responses.activate
def test_an_auth_failure_is_not_an_outage():
    responses.add(responses.POST, ENDPOINT, status=401)

    with pytest.raises(requests.HTTPError) as raised:
        send_graphql_request(ENDPOINT, "key", "query { x }")

    assert not isinstance(raised.value, RegionUnavailableError)


def test_the_request_timeout_bounds_connect_and_read(monkeypatch):
    seen = {}

    def _post(*args, timeout, **kwargs):
        seen["timeout"] = timeout
        raise requests.ReadTimeout("slow")

    monkeypatch.setattr(graphql_client.requests, "post", _post)

    with pytest.raises(RegionUnavailableError):
        send_graphql_request(ENDPOINT, "key", "query { x }")

    assert seen["timeout"] == graphql_client.TIMEOUT_SECONDS == (10, 30)


def test_the_error_names_the_host_and_never_the_key(monkeypatch):
    def _post(*args, **kwargs):
        raise requests.ConnectionError("secret-key-value in a proxy message")

    monkeypatch.setattr(graphql_client.requests, "post", _post)

    with pytest.raises(RegionUnavailableError) as raised:
        send_graphql_request(ENDPOINT, "secret-key-value", "query { x }")

    assert "secret-key-value" not in str(raised.value)


@pytest.fixture
def tracker(tmp_path):
    return ExportTracker(str(tmp_path / "tracking.db"))


def test_an_outage_is_recorded_for_today_and_cleared_by_a_success(tracker):
    tracker.record_unavailable("vm-me-central-1", "vulnerability", "me1", "answered HTTP 504")

    [outage] = tracker.get_unavailable_today("vulnerability").values()
    assert (outage["region"], outage["reason"]) == ("me1", "answered HTTP 504")
    assert isinstance(outage["last_tried"], datetime)

    tracker.clear_unavailable("vm-me-central-1", "vulnerability")
    assert tracker.get_unavailable_today("vulnerability") == {}


def test_a_second_outage_updates_the_first(tracker):
    tracker.record_unavailable("vm-me-central-1", "vulnerability", "me1", "answered HTTP 504")
    tracker.record_unavailable("vm-me-central-1", "vulnerability", "me1", "did not respond in time")

    assert tracker.get_unavailable_today("vulnerability")["vm-me-central-1"]["reason"] == "did not respond in time"


def test_an_outage_from_another_day_or_type_does_not_count(tracker, monkeypatch):
    tracker.record_unavailable("vm-me-central-1", "policy", "me1", "answered HTTP 504")

    assert tracker.get_unavailable_today("vulnerability") == {}

    class _Tomorrow(date):
        @classmethod
        def today(cls):
            return date(2099, 1, 1)

    tracker.record_unavailable("vm-ap-south-2", "vulnerability", "aps2", "answered HTTP 503")
    monkeypatch.setattr("src.export_tracker.date", _Tomorrow)
    assert tracker.get_unavailable_today("vulnerability") == {}


def _tracker():
    return ExportTracker(str(mcp_server._DATA_DIR / "rapid7_bulk_export_tracking.db"))


def _europe_is_down(platform, monkeypatch):
    real = platform.create_export

    def _create(config):
        if config["api_key"] == "key-europe":
            raise RegionUnavailableError("eu.api.insight.rapid7.com answered HTTP 504")
        return real(config)

    monkeypatch.setattr(mcp_server, "create_vulnerability_export", _create)
    return real


def test_a_down_region_names_the_org_and_records_the_outage(platform, monkeypatch):
    _europe_is_down(platform, monkeypatch)

    result = mcp_server.start_rapid7_export(export_type="vulnerability", org_label="europe")

    assert result.startswith("✗ Organization 'europe' is unavailable: its region (us) did not answer.")
    assert "answered HTTP 504" in result and "Retrying later is safe" in result
    outage = _tracker().get_unavailable_today("vulnerability")["europe"]
    assert (outage["region"], outage["reason"]) == ("us", "eu.api.insight.rapid7.com answered HTTP 504")


def test_the_fan_out_starts_the_rest_and_explains_the_outage(platform, monkeypatch):
    _europe_is_down(platform, monkeypatch)

    result = mcp_server.start_rapid7_multi_org_export()

    assert result.startswith("Started vulnerability exports for 2 of 3 organization(s).")
    assert "europe: ✗ Organization 'europe' is unavailable" in result
    assert "each outage is recorded" in result and "Resolve these before reporting" not in result


def test_a_later_success_clears_the_outage(platform, monkeypatch):
    real = _europe_is_down(platform, monkeypatch)
    mcp_server.start_rapid7_export(export_type="vulnerability", org_label="europe")
    monkeypatch.setattr(mcp_server, "create_vulnerability_export", real)

    assert mcp_server.start_rapid7_export(export_type="vulnerability", org_label="europe").startswith("✓")
    assert _tracker().get_unavailable_today("vulnerability") == {}


def test_any_other_failure_keeps_the_strict_advice(platform, monkeypatch):
    def _create(config):
        if config["api_key"] == "key-europe":
            raise ValueError("GraphQL errors: forbidden")
        return platform.create_export(config)

    monkeypatch.setattr(mcp_server, "create_vulnerability_export", _create)

    result = mcp_server.start_rapid7_multi_org_export()

    assert "Resolve these before reporting" in result
    assert _tracker().get_unavailable_today("vulnerability") == {}


def test_coverage_is_partial_when_the_only_gap_is_a_recorded_outage(platform, monkeypatch):
    _europe_is_down(platform, monkeypatch)
    mcp_server.start_rapid7_export(export_type="vulnerability", org_label="europe")
    for label in ("japan", "us"):
        test_multi_org_e2e._load_one_org(label)

    coverage = mcp_server.get_rapid7_org_coverage()

    assert "Partial coverage: 2 of 3 organizations." in coverage
    assert "europe (us): region unavailable, last tried" in coverage
    assert "eu.api.insight.rapid7.com answered HTTP 504." in coverage
    assert "Do not present these as portfolio totals" not in coverage


def test_each_gap_is_named_with_its_own_reason(platform, monkeypatch):
    _europe_is_down(platform, monkeypatch)
    mcp_server.start_rapid7_export(export_type="vulnerability", org_label="europe")
    test_multi_org_e2e._load_one_org("japan")

    coverage = mcp_server.get_rapid7_org_coverage()

    assert "Partial coverage: 1 of 3 organizations." in coverage
    assert "europe (us): region unavailable" in coverage
    assert "us: not loaded today." in coverage


OUTAGE = {
    "region": "me1",
    "reason": "me1.api.insight.rapid7.com answered HTTP 504",
    "last_tried": datetime(2026, 11, 18, 14, 20),
}


@pytest.fixture
def report(tmp_path, monkeypatch):
    """A real database with two loaded orgs, and the report tool's coverage state set per test."""
    from src.duckdb_loader import VulnerabilityDatabase
    from tests.test_org_labels import _load

    database = VulnerabilityDatabase(str(tmp_path / "data.db"))
    _load(database, tmp_path, "ORG-A", "japan")
    _load(database, tmp_path, "ORG-B", "us")
    monkeypatch.setattr(mcp_server, "db", database)
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path)
    written = {}
    monkeypatch.setattr(mcp_server, "_write_private_file", lambda path, html: written.update(html=html))
    monkeypatch.setattr(mcp_server, "_open_report", lambda path: True)

    def _run(warnings, outages):
        state = (["japan", "us", "europe"], ["japan", "us"], list(warnings), dict(outages))
        monkeypatch.setattr(mcp_server, "_coverage_warnings", lambda orgs_in_data: state)
        return mcp_server.generate_rapid7_dashboard_report(), written.get("html", "")

    yield _run
    database.close()


def test_the_report_shows_labelled_totals_when_a_region_is_down(report):
    summary, html = report([], {"europe": OUTAGE})

    assert "Coverage: partial, 2 of 3 organizations." in summary
    assert "europe (me1): region unavailable, last tried 14:20 UTC." in summary
    assert "<strong>Coverage: 2 of 3 organisations.</strong> europe (me1): region unavailable" in html
    assert "Figures below exclude it." in html and "Coverage is incomplete" not in html
    portfolio = html.split('id="portfolio_summary"')[1].split("</section>")[0]
    comparison = html.split('id="org_comparison"')[1].split("</section>")[0]
    assert '<p class="scope">2 of 3 organisations</p>' in portfolio
    assert "Total critical findings" in portfolio, "the combined figures are shown"
    assert 'class="scope"' not in comparison, "a per-organisation card needs no scope line"


def test_the_report_still_hides_totals_when_old_rows_would_make_them_wrong(report):
    summary, html = report(["The data holds 3 organization(s) but only 2 loaded today."], {"europe": {}})

    assert "Coverage: incomplete" in summary
    assert "Coverage is incomplete" in html and 'class="scope"' not in html


def test_the_renderer_refuses_exclusions_without_a_count():
    from src.dashboard_report import render_report

    with pytest.raises(ValueError, match="configured_count"):
        render_report([], "2026-11-18T14:30:00+00:00", coverage_complete=True, excluded=["x (me1): down."])


def test_more_than_one_excluded_organisation_reads_as_plural(report):
    _, html = report([], {"europe": OUTAGE, "asia": {}})

    assert "Coverage: 1 of 3 organisations.</strong> europe (me1)" in html
    assert "asia: not loaded today." in html and "Figures below exclude them." in html

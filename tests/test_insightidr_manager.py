"""
Unit tests for the InsightIDR investigations manager module.

Covers list_investigations, get_investigation, and close_investigation:
happy paths, filter/param handling, and the disposition-validation guard
that close_investigation enforces before ever calling the API.

Author: rozumeyroman@gmail.com
"""

import json

import pytest
import responses

from src.insightidr_manager import (
    VALID_DISPOSITIONS,
    assign_investigation,
    close_investigation,
    get_alert_evidence,
    get_investigation,
    list_investigation_alerts,
    list_investigations,
    list_known_assignees,
)

CONFIG = {"api_key": "test-api-key", "idr_base": "https://eu.api.insight.rapid7.com"}


class TestListInvestigations:
    @responses.activate
    def test_default_call_sets_default_paging_params(self):
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations",
            json={"data": [{"id": "inv-1"}], "metadata": {"total_data": 1}},
            status=200,
        )

        result = list_investigations(CONFIG)

        assert result["data"] == [{"id": "inv-1"}]
        request_url = responses.calls[0].request.url
        assert "size=20" in request_url
        assert "index=0" in request_url
        assert responses.calls[0].request.headers["Accept-version"] == "investigations-preview"

    @responses.activate
    def test_filters_are_included_when_provided(self):
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations",
            json={"data": [], "metadata": {}},
            status=200,
        )

        list_investigations(
            CONFIG,
            status="OPEN",
            priorities="CRITICAL,HIGH",
            assignee_email="analyst@example.com",
            sources="ALERT,MANUAL",
            tags="Incident",
            size=50,
            index=2,
        )

        request_url = responses.calls[0].request.url
        # The API's query param is "statuses" (plural), not "status" — see
        # references/investigations-api-v2.md. This was a real bug: "status"
        # was silently ignored by the API, returning unfiltered results.
        assert "statuses=OPEN" in request_url
        assert "priorities=CRITICAL%2CHIGH" in request_url
        assert "assignee.email=analyst%40example.com" in request_url
        assert "sources=ALERT%2CMANUAL" in request_url
        assert "tags=Incident" in request_url
        assert "size=50" in request_url
        assert "index=2" in request_url

    @responses.activate
    def test_omitted_filters_are_not_sent(self):
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations",
            json={"data": [], "metadata": {}},
            status=200,
        )

        list_investigations(CONFIG)

        request_url = responses.calls[0].request.url
        assert "statuses=" not in request_url
        assert "sources=" not in request_url
        assert "tags=" not in request_url
        assert "priorities=" not in request_url
        assert "assignee.email=" not in request_url


class TestGetInvestigation:
    @responses.activate
    def test_fetches_by_id(self):
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations/inv-1",
            json={"id": "inv-1", "status": "OPEN"},
            status=200,
        )

        result = get_investigation(CONFIG, "inv-1")

        assert result == {"id": "inv-1", "status": "OPEN"}
        assert responses.calls[0].request.headers["Accept-version"] == "investigations-preview"


class TestListInvestigationAlerts:
    @responses.activate
    def test_fetches_alerts_for_investigation(self):
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations/inv-1/alerts",
            json={"data": [{"id": "alert-1"}], "metadata": {"total_data": 1}},
            status=200,
        )

        result = list_investigation_alerts(CONFIG, "inv-1")

        assert result["data"] == [{"id": "alert-1"}]
        request_url = responses.calls[0].request.url
        assert "/investigations/inv-1/alerts" in request_url
        assert "size=20" in request_url
        assert "index=0" in request_url
        assert responses.calls[0].request.headers["Accept-version"] == "investigations-preview"


class TestListKnownAssignees:
    @responses.activate
    def test_dedupes_and_sorts_assignees_from_a_single_page(self):
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations",
            json={
                "data": [
                    {"assignee": {"email": "b@example.com", "name": "Bob"}},
                    {"assignee": {"email": "a@example.com", "name": "Alice"}},
                    {"assignee": {"email": "b@example.com", "name": "Bob"}},
                    {"assignee": None},
                    {},
                ],
                "metadata": {"total_pages": 1},
            },
            status=200,
        )

        result = list_known_assignees(CONFIG)

        assert result == [
            {"email": "a@example.com", "name": "Alice"},
            {"email": "b@example.com", "name": "Bob"},
        ]
        assert len(responses.calls) == 1

    @responses.activate
    def test_paginates_until_total_pages_reached(self):
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations",
            json={"data": [{"assignee": {"email": "p1@example.com", "name": "P1"}}], "metadata": {"total_pages": 2}},
            status=200,
        )
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations",
            json={"data": [{"assignee": {"email": "p2@example.com", "name": "P2"}}], "metadata": {"total_pages": 2}},
            status=200,
        )

        result = list_known_assignees(CONFIG)

        assert result == [
            {"email": "p1@example.com", "name": "P1"},
            {"email": "p2@example.com", "name": "P2"},
        ]
        assert len(responses.calls) == 2

    @responses.activate
    def test_respects_max_pages_safety_cap(self):
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations",
            json={"data": [], "metadata": {"total_pages": 100}},
            status=200,
        )

        list_known_assignees(CONFIG, max_pages=3)

        assert len(responses.calls) == 3

    @responses.activate
    def test_no_investigations_returns_empty_list(self):
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/v2/investigations",
            json={"data": [], "metadata": {"total_pages": 1}},
            status=200,
        )

        assert list_known_assignees(CONFIG) == []


class TestAssignInvestigation:
    @responses.activate
    def test_sends_email_to_assignee_endpoint(self):
        responses.add(
            responses.PUT,
            f"{CONFIG['idr_base']}/idr/v2/investigations/inv-1/assignee",
            json={"id": "inv-1", "assignee": {"email": "analyst@example.com"}},
            status=200,
        )

        result = assign_investigation(CONFIG, "inv-1", "analyst@example.com")

        assert result["assignee"]["email"] == "analyst@example.com"
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body == {"user_email_address": "analyst@example.com"}
        assert responses.calls[0].request.headers["Accept-version"] == "investigations-preview"


class TestGetAlertEvidence:
    @responses.activate
    def test_fetches_evidence_for_alert(self):
        alert_id = "rrn:alerts:eu:org:alert:1:abc123"
        responses.add(
            responses.GET,
            f"{CONFIG['idr_base']}/idr/at/alerts/{alert_id}/evidences",
            json={
                "evidences": [
                    {"event_type": "ingress_auth", "external_source": "IDR ABA", "data": '{"source_ip": "1.2.3.4"}'}
                ],
                "metadata": {"total_items": 1},
            },
            status=200,
        )

        result = get_alert_evidence(CONFIG, alert_id)

        assert result["evidences"][0]["event_type"] == "ingress_auth"
        request_url = responses.calls[0].request.url
        assert f"/idr/at/alerts/{alert_id}/evidences" in request_url
        assert "size=20" in request_url
        assert "index=0" in request_url
        assert responses.calls[0].request.headers["Accept-version"] == "investigations-preview"


class TestCloseInvestigation:
    @pytest.mark.parametrize("disposition", VALID_DISPOSITIONS)
    @responses.activate
    def test_valid_disposition_calls_status_endpoint(self, disposition):
        responses.add(
            responses.PUT,
            f"{CONFIG['idr_base']}/idr/v2/investigations/inv-1/status/CLOSED",
            json={"id": "inv-1", "status": "CLOSED", "disposition": disposition},
            status=200,
        )

        result = close_investigation(CONFIG, "inv-1", disposition)

        assert result["status"] == "CLOSED"
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body == {"disposition": disposition}

    def test_invalid_disposition_raises_value_error_without_calling_api(self):
        with pytest.raises(ValueError, match="Invalid disposition"):
            close_investigation(CONFIG, "inv-1", "NOT_A_REAL_DISPOSITION")

    @responses.activate
    def test_targets_the_specific_investigation_id_not_bulk_close(self):
        responses.add(
            responses.PUT,
            f"{CONFIG['idr_base']}/idr/v2/investigations/inv-42/status/CLOSED",
            json={"id": "inv-42", "status": "CLOSED"},
            status=200,
        )

        close_investigation(CONFIG, "inv-42", "BENIGN")

        assert "inv-42" in responses.calls[0].request.url
        assert "bulk_close" not in responses.calls[0].request.url

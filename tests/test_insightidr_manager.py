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
    close_investigation,
    get_investigation,
    list_investigation_alerts,
    list_investigations,
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
            size=50,
            index=2,
        )

        request_url = responses.calls[0].request.url
        assert "status=OPEN" in request_url
        assert "priorities=CRITICAL%2CHIGH" in request_url
        assert "assignee.email=analyst%40example.com" in request_url
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
        assert "status=" not in request_url
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

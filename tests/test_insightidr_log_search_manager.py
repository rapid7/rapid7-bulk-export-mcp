"""
Unit tests for the InsightIDR Log Search manager module.

Covers list_logs, list_logsets, query_logs, saved query CRUD, and the
poll-until-ready loop that follows a query's "self" link when the API
returns 202 with results not yet available.

Author: rozumeyroman@gmail.com
"""

import json

import responses

from src.insightidr_log_search_manager import (
    create_saved_query,
    delete_saved_query,
    list_logs,
    list_logsets,
    list_saved_queries,
    query_logs,
    run_saved_query,
)

CONFIG = {"api_key": "test-api-key", "idr_base": "https://eu.api.insight.rapid7.com"}
BASE = f"{CONFIG['idr_base']}/log_search"


class TestListLogs:
    @responses.activate
    def test_lists_logs(self):
        responses.add(
            responses.GET, f"{BASE}/management/logs", json={"logs": [{"id": "l1", "name": "O365"}]}, status=200
        )

        result = list_logs(CONFIG)

        assert result["logs"][0]["name"] == "O365"
        # Log Search API is stable — no Accept-version header required
        assert "Accept-version" not in responses.calls[0].request.headers


class TestListLogsets:
    @responses.activate
    def test_lists_logsets(self):
        responses.add(
            responses.GET, f"{BASE}/management/logsets", json={"logsets": [{"id": "ls1", "name": "Auth"}]}, status=200
        )

        result = list_logsets(CONFIG)

        assert result["logsets"][0]["name"] == "Auth"


class TestQueryLogs:
    @responses.activate
    def test_immediate_results_no_polling(self):
        responses.add(
            responses.POST,
            f"{BASE}/query/logs",
            json={"events": [{"log_id": "l1", "message": "hi"}]},
            status=200,
        )

        result = query_logs(CONFIG, ["l1"], "where(user)", time_range="Today")

        assert len(result["events"]) == 1
        assert len(responses.calls) == 1
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body == {"logs": ["l1"], "leql": {"statement": "where(user)", "during": {"time_range": "Today"}}}

    @responses.activate
    def test_uses_explicit_from_to_when_no_time_range(self):
        responses.add(responses.POST, f"{BASE}/query/logs", json={"events": []}, status=200)

        query_logs(CONFIG, ["l1"], "where(user)", from_ms=100, to_ms=200)

        request_body = json.loads(responses.calls[0].request.body)
        assert request_body["leql"]["during"] == {"from": 100, "to": 200}

    @responses.activate
    def test_polls_pending_query_until_events_available(self):
        # A pending (202) response carries an "events": [] key — it's the
        # "links" self-href that distinguishes pending from final.
        poll_url = "https://eu.api.insight.rapid7.com/log_search/query/abc-123"
        responses.add(
            responses.POST,
            f"{BASE}/query/logs",
            json={"id": "abc-123", "events": [], "links": [{"rel": "self", "href": poll_url}]},
            status=202,
        )
        pending = {"id": "abc-123", "events": [], "links": [{"rel": "self", "href": poll_url}]}
        responses.add(responses.GET, poll_url, json=pending, status=202)
        responses.add(responses.GET, poll_url, json={"events": [{"log_id": "l1"}]}, status=200)

        result = query_logs(CONFIG, ["l1"], "where(user)", time_range="Today", poll_interval_seconds=0.01)

        assert len(result["events"]) == 1
        assert len(responses.calls) == 3

    @responses.activate
    def test_gives_up_after_max_wait_without_events(self):
        poll_url = "https://eu.api.insight.rapid7.com/log_search/query/abc-123"
        pending = {"id": "abc-123", "events": [], "links": [{"rel": "self", "href": poll_url}]}
        responses.add(responses.POST, f"{BASE}/query/logs", json=pending, status=202)
        responses.add(responses.GET, poll_url, json=pending, status=202)
        responses.add(responses.GET, poll_url, json=pending, status=202)

        result = query_logs(
            CONFIG, ["l1"], "where(user)", time_range="Today", max_wait_seconds=0.02, poll_interval_seconds=0.01
        )

        # Still pending after giving up: "links" self-href is still there.
        assert any(link.get("rel", "").lower() == "self" for link in result.get("links", []))
        assert result["id"] == "abc-123"


class TestSavedQueries:
    @responses.activate
    def test_list_saved_queries(self):
        responses.add(responses.GET, f"{BASE}/query/saved_queries", json={"saved_queries": [{"id": "sq1"}]}, status=200)

        result = list_saved_queries(CONFIG)

        assert result["saved_queries"][0]["id"] == "sq1"

    @responses.activate
    def test_create_saved_query_wraps_body_correctly(self):
        responses.add(
            responses.POST,
            f"{BASE}/query/saved_queries",
            json={"saved_query": {"id": "sq1", "name": "test"}},
            status=201,
        )

        result = create_saved_query(CONFIG, "test", ["l1", "l2"], "where(user)", time_range="Last 1 Day")

        assert result["saved_query"]["id"] == "sq1"
        request_body = json.loads(responses.calls[0].request.body)
        # Body must be wrapped in a top-level "saved_query" key.
        assert request_body == {
            "saved_query": {
                "name": "test",
                "logs": ["l1", "l2"],
                "leql": {"statement": "where(user)", "during": {"time_range": "Last 1 Day"}},
            }
        }

    @responses.activate
    def test_delete_saved_query(self):
        responses.add(responses.DELETE, f"{BASE}/query/saved_queries/sq1", body="", status=204)

        result = delete_saved_query(CONFIG, "sq1")

        assert result == {}

    @responses.activate
    def test_run_saved_query_uses_singular_path(self):
        responses.add(responses.GET, f"{BASE}/query/saved_query/sq1", json={"events": []}, status=200)

        run_saved_query(CONFIG, "sq1")

        assert "/query/saved_query/sq1" in responses.calls[0].request.url
        assert "/query/saved_queries/" not in responses.calls[0].request.url


class TestBuildDuring:
    @responses.activate
    def test_time_range_takes_precedence_over_from_to(self):
        responses.add(responses.POST, f"{BASE}/query/logs", json={"events": []}, status=200)

        query_logs(CONFIG, ["l1"], "where(user)", time_range="Today", from_ms=1, to_ms=2)

        request_body = json.loads(responses.calls[0].request.body)
        assert request_body["leql"]["during"] == {"time_range": "Today"}

"""
Unit tests for the InsightIDR REST client module.

Tests cover successful requests, header handling, empty responses, and
HTTP/network errors.

Author: rozumeyroman@gmail.com
"""

import json

import pytest
import requests
import responses

from src.insightidr_client import INVESTIGATIONS_ACCEPT_VERSION, send_idr_request


class TestSendIdrRequest:
    """Test suite for send_idr_request function."""

    @responses.activate
    def test_get_request_returns_parsed_json(self):
        base_url = "https://eu.api.insight.rapid7.com"
        path = "/idr/v2/investigations"
        api_key = "test-api-key"
        expected_response = {"data": [{"id": "inv-1"}], "metadata": {"total_data": 1}}

        responses.add(responses.GET, f"{base_url}{path}", json=expected_response, status=200)

        result = send_idr_request("GET", base_url, path, api_key)

        assert result == expected_response
        assert responses.calls[0].request.headers["X-Api-Key"] == api_key

    @responses.activate
    def test_query_params_are_sent(self):
        base_url = "https://eu.api.insight.rapid7.com"
        path = "/idr/v2/investigations"
        api_key = "test-api-key"

        responses.add(responses.GET, f"{base_url}{path}", json={"data": [], "metadata": {}}, status=200)

        send_idr_request("GET", base_url, path, api_key, params={"status": "OPEN", "size": 20})

        request_url = responses.calls[0].request.url
        assert "status=OPEN" in request_url
        assert "size=20" in request_url

    @responses.activate
    def test_accept_version_header_is_set_when_provided(self):
        base_url = "https://eu.api.insight.rapid7.com"
        path = "/idr/v2/investigations"
        api_key = "test-api-key"

        responses.add(responses.GET, f"{base_url}{path}", json={"data": [], "metadata": {}}, status=200)

        send_idr_request("GET", base_url, path, api_key, accept_version=INVESTIGATIONS_ACCEPT_VERSION)

        assert responses.calls[0].request.headers["Accept-version"] == "investigations-preview"

    @responses.activate
    def test_accept_version_header_omitted_when_not_provided(self):
        base_url = "https://eu.api.insight.rapid7.com"
        path = "/idr/v2/investigations"
        api_key = "test-api-key"

        responses.add(responses.GET, f"{base_url}{path}", json={"data": [], "metadata": {}}, status=200)

        send_idr_request("GET", base_url, path, api_key)

        assert "Accept-version" not in responses.calls[0].request.headers

    @responses.activate
    def test_put_request_sends_json_body(self):
        base_url = "https://eu.api.insight.rapid7.com"
        path = "/idr/v2/investigations/inv-1/status/CLOSED"
        api_key = "test-api-key"

        responses.add(responses.PUT, f"{base_url}{path}", json={"id": "inv-1", "status": "CLOSED"}, status=200)

        result = send_idr_request("PUT", base_url, path, api_key, json_body={"disposition": "BENIGN"})

        assert result == {"id": "inv-1", "status": "CLOSED"}
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body == {"disposition": "BENIGN"}

    @responses.activate
    def test_empty_response_body_returns_empty_dict(self):
        base_url = "https://eu.api.insight.rapid7.com"
        path = "/idr/v2/investigations/bulk_close"
        api_key = "test-api-key"

        responses.add(responses.POST, f"{base_url}{path}", body="", status=200)

        result = send_idr_request("POST", base_url, path, api_key)

        assert result == {}

    @responses.activate
    def test_http_error_raises_http_error(self):
        base_url = "https://eu.api.insight.rapid7.com"
        path = "/idr/v2/investigations"
        api_key = "test-api-key"

        responses.add(responses.GET, f"{base_url}{path}", json={"error": "Unauthorized"}, status=401)

        with pytest.raises(requests.HTTPError) as exc_info:
            send_idr_request("GET", base_url, path, api_key)

        assert "401" in str(exc_info.value)

    @responses.activate
    def test_network_error_raises_request_exception(self):
        base_url = "https://eu.api.insight.rapid7.com"
        path = "/idr/v2/investigations"
        api_key = "test-api-key"

        responses.add(responses.GET, f"{base_url}{path}", body=requests.exceptions.ConnectionError("Connection failed"))

        with pytest.raises(requests.exceptions.ConnectionError):
            send_idr_request("GET", base_url, path, api_key)

    @responses.activate
    def test_api_key_and_content_type_headers(self):
        base_url = "https://eu.api.insight.rapid7.com"
        path = "/idr/v2/investigations"
        api_key = "my-secret-key"

        responses.add(responses.GET, f"{base_url}{path}", json={"data": [], "metadata": {}}, status=200)

        send_idr_request("GET", base_url, path, api_key)

        assert responses.calls[0].request.headers["X-Api-Key"] == api_key
        assert responses.calls[0].request.headers["Content-Type"] == "application/json"

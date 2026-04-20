"""
Unit tests for the GraphQL client module.

Tests cover successful requests, HTTP errors, GraphQL errors, and network errors.
"""

import pytest
import requests
import responses

from src.graphql_client import send_graphql_request


class TestSendGraphQLRequest:
    """Test suite for send_graphql_request function."""

    @responses.activate
    def test_successful_request_returns_parsed_json(self):
        """Test that a successful request returns the parsed JSON response."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "test-api-key"
        query = "query { test }"
        expected_response = {"data": {"test": "value"}}

        responses.add(responses.POST, endpoint, json=expected_response, status=200)

        result = send_graphql_request(endpoint, api_key, query)

        assert result == expected_response
        assert len(responses.calls) == 1
        assert responses.calls[0].request.headers["X-Api-Key"] == api_key
        assert responses.calls[0].request.headers["Content-Type"] == "application/json"

    @responses.activate
    def test_request_with_variables(self):
        """Test that variables are correctly included in the request body."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "test-api-key"
        query = "query($id: ID!) { vulnerabilityExport(id: $id) { status } }"
        variables = {"id": "export-123"}
        expected_response = {"data": {"vulnerabilityExport": {"status": "COMPLETE"}}}

        responses.add(responses.POST, endpoint, json=expected_response, status=200)

        result = send_graphql_request(endpoint, api_key, query, variables)

        assert result == expected_response
        # Verify the request body contains both query and variables
        import json

        request_body = json.loads(responses.calls[0].request.body)
        assert request_body["query"] == query
        assert request_body["variables"] == variables

    @responses.activate
    def test_http_error_raises_http_error(self):
        """Test that HTTP errors raise HTTPError with details."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "test-api-key"
        query = "query { test }"

        responses.add(responses.POST, endpoint, json={"error": "Unauthorized"}, status=401)

        with pytest.raises(requests.HTTPError) as exc_info:
            send_graphql_request(endpoint, api_key, query)

        assert "401" in str(exc_info.value)

    @responses.activate
    def test_graphql_error_raises_value_error(self):
        """Test that GraphQL errors raise ValueError with error messages."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "test-api-key"
        query = "query { test }"

        responses.add(
            responses.POST,
            endpoint,
            json={"errors": [{"message": 'Field "test" not found'}, {"message": "Invalid query syntax"}]},
            status=200,
        )

        with pytest.raises(ValueError) as exc_info:
            send_graphql_request(endpoint, api_key, query)

        error_message = str(exc_info.value)
        assert "GraphQL errors" in error_message
        assert 'Field "test" not found' in error_message
        assert "Invalid query syntax" in error_message

    @responses.activate
    def test_graphql_error_without_message_field(self):
        """Test that GraphQL errors without message field are handled."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "test-api-key"
        query = "query { test }"

        responses.add(
            responses.POST,
            endpoint,
            json={"errors": [{"code": "UNKNOWN_ERROR", "details": "Something went wrong"}]},
            status=200,
        )

        with pytest.raises(ValueError) as exc_info:
            send_graphql_request(endpoint, api_key, query)

        assert "GraphQL errors" in str(exc_info.value)

    @responses.activate
    def test_network_error_raises_request_exception(self):
        """Test that network errors raise RequestException."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "test-api-key"
        query = "query { test }"

        responses.add(responses.POST, endpoint, body=requests.exceptions.ConnectionError("Connection failed"))

        with pytest.raises(requests.exceptions.ConnectionError):
            send_graphql_request(endpoint, api_key, query)

    @responses.activate
    def test_500_server_error_raises_http_error(self):
        """Test that 500 server errors raise HTTPError."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "test-api-key"
        query = "query { test }"

        responses.add(responses.POST, endpoint, json={"error": "Internal Server Error"}, status=500)

        with pytest.raises(requests.HTTPError) as exc_info:
            send_graphql_request(endpoint, api_key, query)

        assert "500" in str(exc_info.value)

    @responses.activate
    def test_empty_variables_not_included_in_body(self):
        """Test that None variables are not included in the request body."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "test-api-key"
        query = "mutation { createVulnerabilityExport { id } }"
        expected_response = {"data": {"createVulnerabilityExport": {"id": "export-123"}}}

        responses.add(responses.POST, endpoint, json=expected_response, status=200)

        result = send_graphql_request(endpoint, api_key, query, variables=None)

        assert result == expected_response
        # Verify the request body contains only query, not variables
        import json

        request_body = json.loads(responses.calls[0].request.body)
        assert request_body["query"] == query
        assert "variables" not in request_body

    @responses.activate
    def test_api_key_header_is_set_correctly(self):
        """Test that the X-Api-Key header is set with the provided API key."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "my-secret-api-key-12345"
        query = "query { test }"

        responses.add(responses.POST, endpoint, json={"data": {"test": "value"}}, status=200)

        send_graphql_request(endpoint, api_key, query)

        assert responses.calls[0].request.headers["X-Api-Key"] == api_key

    @responses.activate
    def test_content_type_header_is_set_correctly(self):
        """Test that the Content-Type header is set to application/json."""
        endpoint = "https://us.api.insight.rapid7.com/export/graphql"
        api_key = "test-api-key"
        query = "query { test }"

        responses.add(responses.POST, endpoint, json={"data": {"test": "value"}}, status=200)

        send_graphql_request(endpoint, api_key, query)

        assert responses.calls[0].request.headers["Content-Type"] == "application/json"

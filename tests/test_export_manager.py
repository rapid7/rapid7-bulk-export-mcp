"""
Unit tests for the export_manager module.

Tests the export lifecycle functions including creating exports,
querying status, and polling for completion.
"""

import pytest
import responses

from src.export_manager import create_vulnerability_export, get_export_status


class TestCreateVulnerabilityExport:
    """Tests for the create_vulnerability_export function."""

    @responses.activate
    def test_create_export_returns_export_id(self):
        """Test that create_vulnerability_export returns the export ID from the response."""
        # Mock the GraphQL API response
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={"data": {"createVulnerabilityExport": {"id": "test-export-id-12345"}}},
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-api-key"}

        export_id = create_vulnerability_export(config)

        assert export_id == "test-export-id-12345"

    @responses.activate
    def test_create_export_sends_correct_mutation(self):
        """Test that create_vulnerability_export sends the correct GraphQL mutation."""
        # Mock the GraphQL API response
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={"data": {"createVulnerabilityExport": {"id": "test-id"}}},
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        export_id = create_vulnerability_export(config)

        # Verify the request was made
        assert len(responses.calls) == 1
        request = responses.calls[0].request

        # Verify headers
        assert "X-Api-Key" in request.headers
        assert request.headers["X-Api-Key"] == "test-key"
        assert request.headers["Content-Type"] == "application/json"

        # Verify the mutation is in the request body
        import json

        body = json.loads(request.body)
        assert "query" in body
        assert "createVulnerabilityExport" in body["query"]
        assert "mutation" in body["query"]

        # Verify the export ID was returned
        assert export_id == "test-id"

    @responses.activate
    def test_create_export_with_graphql_error(self):
        """Test that create_vulnerability_export raises ValueError on GraphQL errors."""
        # Mock a GraphQL error response
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={"errors": [{"message": "Authentication failed", "extensions": {"code": "UNAUTHENTICATED"}}]},
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "invalid-key"}

        with pytest.raises(ValueError, match="GraphQL errors.*Authentication failed"):
            create_vulnerability_export(config)

    @responses.activate
    def test_create_export_with_http_error(self):
        """Test that create_vulnerability_export raises HTTPError on non-200 status."""
        # Mock an HTTP error response
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={"error": "Internal Server Error"},
            status=500,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        with pytest.raises(Exception):  # requests.HTTPError
            create_vulnerability_export(config)

    @responses.activate
    def test_create_export_with_different_regions(self):
        """Test that create_vulnerability_export works with different region endpoints."""
        regions_and_endpoints = [
            ("us", "https://us.api.insight.rapid7.com/export/graphql"),
            ("eu", "https://eu.api.insight.rapid7.com/export/graphql"),
            ("ap", "https://ap.api.insight.rapid7.com/export/graphql"),
        ]

        for region, endpoint in regions_and_endpoints:
            # Mock the response for this endpoint
            responses.add(
                responses.POST,
                endpoint,
                json={"data": {"createVulnerabilityExport": {"id": f"{region}-export-id"}}},
                status=200,
            )

            config = {"endpoint": endpoint, "api_key": "test-key", "region": region}

            export_id = create_vulnerability_export(config)
            assert export_id == f"{region}-export-id"


class TestGetExportStatus:
    """Tests for the get_export_status function."""

    @responses.activate
    def test_get_export_status_returns_status_info(self):
        """Test that get_export_status returns the correct status information."""
        # Mock the GraphQL API response using the export(id:) query format
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export-id",
                        "status": "COMPLETE",
                        "dataset": "vulnerability",
                        "timestamp": "2024-01-01T00:00:00Z",
                        "result": [
                            {"prefix": "asset", "urls": ["https://example.com/file1.parquet"]},
                            {"prefix": "asset_vulnerability", "urls": ["https://example.com/file2.parquet"]},
                        ],
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-api-key"}

        status = get_export_status(config, "test-export-id")

        assert status["id"] == "test-export-id"
        assert status["status"] == "COMPLETE"
        assert len(status["parquetFiles"]) == 2
        assert status["parquetFiles"][0] == "https://example.com/file1.parquet"
        # Verify the new result field is present
        assert "result" in status
        assert len(status["result"]) == 2
        assert status["result"][0]["prefix"] == "asset"
        assert status["result"][1]["prefix"] == "asset_vulnerability"

    @responses.activate
    def test_get_export_status_sends_correct_query(self):
        """Test that get_export_status sends the correct GraphQL query."""
        # Mock the GraphQL API response
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "export-123",
                        "status": "PROCESSING",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        status = get_export_status(config, "export-123")

        # Verify the request was made
        assert len(responses.calls) == 1
        request = responses.calls[0].request

        # Verify headers
        assert "X-Api-Key" in request.headers
        assert request.headers["X-Api-Key"] == "test-key"

        # Verify the query in the request body
        import json

        body = json.loads(request.body)
        assert "query" in body
        assert "export" in body["query"]
        assert "export-123" in body["query"]

        # Verify the status was returned correctly
        assert status["status"] == "PROCESSING"
        assert status["result"] == []

    @responses.activate
    def test_get_export_status_with_pending_status(self):
        """Test get_export_status with PENDING status and no result."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "pending-export",
                        "status": "PENDING",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        status = get_export_status(config, "pending-export")

        assert status["id"] == "pending-export"
        assert status["status"] == "PENDING"
        assert status["parquetFiles"] == []
        assert status["result"] == []

    @responses.activate
    def test_get_export_status_with_failed_status(self):
        """Test get_export_status with FAILED status."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "failed-export",
                        "status": "FAILED",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        status = get_export_status(config, "failed-export")

        assert status["status"] == "FAILED"
        assert status["result"] == []

    @responses.activate
    def test_get_export_status_result_field_with_multiple_prefixes(self):
        """Test that get_export_status returns the full result list with prefix metadata."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "policy-export",
                        "status": "COMPLETE",
                        "dataset": "policy",
                        "timestamp": "2024-01-01T00:00:00Z",
                        "result": [
                            {"prefix": "asset", "urls": ["https://example.com/asset1.parquet"]},
                            {
                                "prefix": "asset_policy",
                                "urls": ["https://example.com/policy1.parquet", "https://example.com/policy2.parquet"],
                            },
                            {"prefix": "asset_scan_policy", "urls": ["https://example.com/scan1.parquet"]},
                        ],
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        status = get_export_status(config, "policy-export")

        # parquetFiles should be a flat list of all URLs
        assert len(status["parquetFiles"]) == 4
        # result should preserve the prefix structure
        assert len(status["result"]) == 3
        assert status["result"][0] == {"prefix": "asset", "urls": ["https://example.com/asset1.parquet"]}
        assert status["result"][1]["prefix"] == "asset_policy"
        assert len(status["result"][1]["urls"]) == 2
        assert status["result"][2]["prefix"] == "asset_scan_policy"

    @responses.activate
    def test_get_export_status_with_graphql_error(self):
        """Test that get_export_status raises ValueError on GraphQL errors."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={"errors": [{"message": "Export not found", "extensions": {"code": "NOT_FOUND"}}]},
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        with pytest.raises(ValueError, match="GraphQL errors.*Export not found"):
            get_export_status(config, "nonexistent-export")

    @responses.activate
    def test_get_export_status_with_http_error(self):
        """Test that get_export_status raises HTTPError on non-200 status."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={"error": "Service Unavailable"},
            status=503,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        with pytest.raises(Exception):  # requests.HTTPError
            get_export_status(config, "test-export")


class TestPollUntilComplete:
    """Tests for the poll_until_complete function."""

    @responses.activate
    def test_poll_until_complete_with_immediate_complete(self):
        """Test poll_until_complete when export is already COMPLETE."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "COMPLETE",
                        "dataset": "vulnerability",
                        "timestamp": "2024-01-01T00:00:00Z",
                        "result": [
                            {"prefix": "asset", "urls": ["https://example.com/file1.parquet"]},
                            {"prefix": "asset_vulnerability", "urls": ["https://example.com/file2.parquet"]},
                        ],
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        from src.export_manager import poll_until_complete

        urls = poll_until_complete(config, "test-export", interval=1)

        assert len(urls) == 2
        assert urls[0] == "https://example.com/file1.parquet"
        assert urls[1] == "https://example.com/file2.parquet"
        assert len(responses.calls) == 1

    @responses.activate
    def test_poll_until_complete_with_pending_then_complete(self, capsys):
        """Test poll_until_complete when export transitions from PENDING to COMPLETE."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "PENDING",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "COMPLETE",
                        "dataset": "vulnerability",
                        "timestamp": "2024-01-01T00:00:00Z",
                        "result": [{"prefix": "asset_vulnerability", "urls": ["https://example.com/file.parquet"]}],
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        from src.export_manager import poll_until_complete

        urls = poll_until_complete(config, "test-export", interval=0.1)

        assert len(urls) == 1
        assert urls[0] == "https://example.com/file.parquet"
        assert len(responses.calls) == 2

        # Status updates are printed to stderr
        captured = capsys.readouterr()
        assert "Export status: PENDING" in captured.err
        assert "Export status: COMPLETE" in captured.err

    @responses.activate
    def test_poll_until_complete_with_processing_then_complete(self):
        """Test poll_until_complete when export transitions from PROCESSING to COMPLETE."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "PROCESSING",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "COMPLETE",
                        "dataset": "vulnerability",
                        "timestamp": "2024-01-01T00:00:00Z",
                        "result": [{"prefix": "asset_vulnerability", "urls": ["https://example.com/file.parquet"]}],
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        from src.export_manager import poll_until_complete

        urls = poll_until_complete(config, "test-export", interval=0.1)

        assert len(urls) == 1
        assert len(responses.calls) == 2

    @responses.activate
    def test_poll_until_complete_with_failed_status(self):
        """Test poll_until_complete raises error when export status is FAILED."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "failed-export",
                        "status": "FAILED",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        from src.export_manager import poll_until_complete

        with pytest.raises(ValueError, match="Export failed-export failed"):
            poll_until_complete(config, "failed-export", interval=1)

    @responses.activate
    def test_poll_until_complete_with_multiple_transitions(self, capsys):
        """Test poll_until_complete with multiple status transitions."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "PENDING",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "PROCESSING",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "COMPLETE",
                        "dataset": "vulnerability",
                        "timestamp": "2024-01-01T00:00:00Z",
                        "result": [
                            {"prefix": "asset", "urls": ["https://example.com/file1.parquet"]},
                            {
                                "prefix": "asset_vulnerability",
                                "urls": ["https://example.com/file2.parquet", "https://example.com/file3.parquet"],
                            },
                        ],
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        from src.export_manager import poll_until_complete

        urls = poll_until_complete(config, "test-export", interval=0.1)

        assert len(urls) == 3
        assert len(responses.calls) == 3

        captured = capsys.readouterr()
        assert "Export status: PENDING" in captured.err
        assert "Export status: PROCESSING" in captured.err
        assert "Export status: COMPLETE" in captured.err

    @responses.activate
    def test_poll_until_complete_respects_interval(self):
        """Test that poll_until_complete waits for the specified interval between polls."""
        import time

        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "PENDING",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "COMPLETE",
                        "dataset": "vulnerability",
                        "timestamp": "2024-01-01T00:00:00Z",
                        "result": [{"prefix": "asset_vulnerability", "urls": ["https://example.com/file.parquet"]}],
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        from src.export_manager import poll_until_complete

        start_time = time.time()
        urls = poll_until_complete(config, "test-export", interval=0.5)
        elapsed_time = time.time() - start_time

        assert elapsed_time >= 0.5
        assert len(urls) == 1

    @responses.activate
    def test_poll_until_complete_with_unexpected_status(self):
        """Test poll_until_complete raises error for unexpected status values."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "UNKNOWN_STATUS",
                        "dataset": "vulnerability",
                        "timestamp": None,
                        "result": None,
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        from src.export_manager import poll_until_complete

        with pytest.raises(ValueError, match="Unexpected export status: UNKNOWN_STATUS"):
            poll_until_complete(config, "test-export", interval=1)

    @responses.activate
    def test_poll_until_complete_with_empty_parquet_files(self):
        """Test poll_until_complete returns empty list when no Parquet files are available."""
        responses.add(
            responses.POST,
            "https://us.api.insight.rapid7.com/export/graphql",
            json={
                "data": {
                    "export": {
                        "id": "test-export",
                        "status": "COMPLETE",
                        "dataset": "vulnerability",
                        "timestamp": "2024-01-01T00:00:00Z",
                        "result": [],
                    }
                }
            },
            status=200,
        )

        config = {"endpoint": "https://us.api.insight.rapid7.com/export/graphql", "api_key": "test-key"}

        from src.export_manager import poll_until_complete

        urls = poll_until_complete(config, "test-export", interval=1)

        assert urls == []
        assert len(responses.calls) == 1

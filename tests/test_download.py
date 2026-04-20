"""
Unit tests for the download module.
"""

import pytest
import responses
from requests import HTTPError

from src.download import download_all_files, download_parquet_file


class TestDownloadParquetFile:
    """Tests for download_parquet_file function."""

    @responses.activate
    def test_download_parquet_file_success(self):
        """Test successful download returns file content."""
        url = "https://example.com/file.parquet"
        api_key = "test-api-key"
        expected_content = b"parquet file content"

        responses.add(responses.GET, url, body=expected_content, status=200)

        result = download_parquet_file(url, api_key)

        assert result == expected_content
        assert len(responses.calls) == 1
        assert responses.calls[0].request.headers["X-Api-Key"] == api_key

    @responses.activate
    def test_download_parquet_file_includes_api_key_header(self):
        """Test that X-Api-Key header is included in the request."""
        url = "https://example.com/file.parquet"
        api_key = "my-secret-key"

        responses.add(responses.GET, url, body=b"content", status=200)

        download_parquet_file(url, api_key)

        assert "X-Api-Key" in responses.calls[0].request.headers
        assert responses.calls[0].request.headers["X-Api-Key"] == api_key

    @responses.activate
    def test_download_parquet_file_http_error(self):
        """Test that HTTP error raises HTTPError."""
        url = "https://example.com/file.parquet"
        api_key = "test-api-key"

        responses.add(responses.GET, url, body="Not Found", status=404)

        with pytest.raises(HTTPError) as exc_info:
            download_parquet_file(url, api_key)

        assert "404" in str(exc_info.value)

    @responses.activate
    def test_download_parquet_file_server_error(self):
        """Test that server error raises HTTPError."""
        url = "https://example.com/file.parquet"
        api_key = "test-api-key"

        responses.add(responses.GET, url, body="Internal Server Error", status=500)

        with pytest.raises(HTTPError) as exc_info:
            download_parquet_file(url, api_key)

        assert "500" in str(exc_info.value)

    @responses.activate
    def test_download_parquet_file_unauthorized(self):
        """Test that unauthorized error raises HTTPError."""
        url = "https://example.com/file.parquet"
        api_key = "invalid-key"

        responses.add(responses.GET, url, body="Unauthorized", status=401)

        with pytest.raises(HTTPError) as exc_info:
            download_parquet_file(url, api_key)

        assert "401" in str(exc_info.value)


class TestDownloadAllFiles:
    """Tests for download_all_files function."""

    @responses.activate
    def test_download_all_files_single_url(self):
        """Test downloading a single file returns list with one element."""
        urls = ["https://example.com/file1.parquet"]
        api_key = "test-api-key"
        expected_content = b"file1 content"

        responses.add(responses.GET, urls[0], body=expected_content, status=200)

        result = download_all_files(urls, api_key)

        assert len(result) == 1
        assert result[0] == expected_content
        assert len(responses.calls) == 1

    @responses.activate
    def test_download_all_files_multiple_urls(self):
        """Test downloading multiple files returns all contents in order."""
        urls = [
            "https://example.com/file1.parquet",
            "https://example.com/file2.parquet",
            "https://example.com/file3.parquet",
        ]
        api_key = "test-api-key"
        expected_contents = [b"file1 content", b"file2 content", b"file3 content"]

        for url, content in zip(urls, expected_contents):
            responses.add(responses.GET, url, body=content, status=200)

        result = download_all_files(urls, api_key)

        assert len(result) == len(urls)
        assert result == expected_contents
        assert len(responses.calls) == len(urls)

    @responses.activate
    def test_download_all_files_empty_list(self):
        """Test downloading with empty URL list returns empty list."""
        urls = []
        api_key = "test-api-key"

        result = download_all_files(urls, api_key)

        assert result == []
        assert len(responses.calls) == 0

    @responses.activate
    def test_download_all_files_includes_api_key_in_all_requests(self):
        """Test that API key is included in all download requests."""
        urls = ["https://example.com/file1.parquet", "https://example.com/file2.parquet"]
        api_key = "my-secret-key"

        for url in urls:
            responses.add(responses.GET, url, body=b"content", status=200)

        download_all_files(urls, api_key)

        assert len(responses.calls) == 2
        for call in responses.calls:
            assert "X-Api-Key" in call.request.headers
            assert call.request.headers["X-Api-Key"] == api_key

    @responses.activate
    def test_download_all_files_http_error_on_first_file(self):
        """Test that HTTP error on first file raises HTTPError."""
        urls = ["https://example.com/file1.parquet", "https://example.com/file2.parquet"]
        api_key = "test-api-key"

        responses.add(responses.GET, urls[0], body="Not Found", status=404)

        with pytest.raises(HTTPError) as exc_info:
            download_all_files(urls, api_key)

        assert "404" in str(exc_info.value)
        # Should only have attempted the first download
        assert len(responses.calls) == 1

    @responses.activate
    def test_download_all_files_http_error_on_second_file(self):
        """Test that HTTP error on second file raises HTTPError after first succeeds."""
        urls = ["https://example.com/file1.parquet", "https://example.com/file2.parquet"]
        api_key = "test-api-key"

        responses.add(responses.GET, urls[0], body=b"file1 content", status=200)
        responses.add(responses.GET, urls[1], body="Internal Server Error", status=500)

        with pytest.raises(HTTPError) as exc_info:
            download_all_files(urls, api_key)

        assert "500" in str(exc_info.value)
        # Should have attempted both downloads
        assert len(responses.calls) == 2

    @responses.activate
    def test_download_all_files_preserves_order(self):
        """Test that files are returned in the same order as URLs."""
        urls = [
            "https://example.com/file3.parquet",
            "https://example.com/file1.parquet",
            "https://example.com/file2.parquet",
        ]
        api_key = "test-api-key"
        # Use distinct content to verify order
        contents = [b"third file", b"first file", b"second file"]

        for url, content in zip(urls, contents):
            responses.add(responses.GET, url, body=content, status=200)

        result = download_all_files(urls, api_key)

        # Verify order matches input URL order
        assert result[0] == b"third file"
        assert result[1] == b"first file"
        assert result[2] == b"second file"

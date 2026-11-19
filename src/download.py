"""
Download module for Rapid7 Vulnerability Export.

This module handles downloading Parquet files from the Rapid7 API. It provides
functions to download individual files or multiple files in sequence, with
progress reporting and proper error handling.
"""

import sys
from typing import List
from urllib.parse import urlparse

import requests

from .config import USER_AGENT

_RAPID7_API_SUFFIX = ".api.insight.rapid7.com"


def _is_rapid7_api(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    return parsed.scheme == "https" and host.endswith(_RAPID7_API_SUFFIX)


def download_parquet_file(url: str, api_key: str) -> bytes:
    """
    Download a single Parquet file from the provided URL.

    This function downloads a Parquet file from the Rapid7 API using streaming
    to handle large files efficiently. It includes the API key in the request
    headers for authentication.

    Args:
        url: The URL to download the Parquet file from
        api_key: The API key for authorization

    Returns:
        bytes: The file content as bytes

    Raises:
        requests.HTTPError: If the download fails with a non-200 status code
        requests.RequestException: If the network request fails

    Example:
        >>> url = "https://example.com/export.parquet"
        >>> api_key = "my-api-key"
        >>> content = download_parquet_file(url, api_key)
        >>> print(f"Downloaded {len(content)} bytes")
    """
    headers = {"User-Agent": USER_AGENT}
    # Export file URLs are signed, so they need no key. The key goes only to Rapid7's own
    # API hosts, never to the storage service behind a signed URL.
    if _is_rapid7_api(url):
        headers["X-Api-Key"] = api_key

    # Stream download for memory efficiency
    response = requests.get(url, headers=headers, stream=True, timeout=30)

    # Raise HTTPError for failed downloads
    response.raise_for_status()

    # Return file content as bytes
    return response.content


def download_all_files(urls: List[str], api_key: str) -> List[bytes]:
    """
    Download all Parquet files from the provided URLs.

    This function downloads multiple Parquet files sequentially, printing
    progress information for each file. It maintains the order of files
    as specified in the input URL list.

    Args:
        urls: List of URLs to download Parquet files from
        api_key: The API key for authorization

    Returns:
        List[bytes]: List of file contents as bytes, in the same order as the input URLs

    Raises:
        requests.HTTPError: If any download fails with a non-200 status code
        requests.RequestException: If any network request fails

    Example:
        >>> urls = ["https://example.com/file1.parquet", "https://example.com/file2.parquet"]
        >>> api_key = "my-api-key"
        >>> files = download_all_files(urls, api_key)
        >>> print(f"Downloaded {len(files)} files")
    """
    file_contents = []

    for i, url in enumerate(urls, start=1):
        print(f"Downloading file {i} of {len(urls)}...", file=sys.stderr)
        content = download_parquet_file(url, api_key)
        file_contents.append(content)
        print(f"Downloaded file {i} ({len(content)} bytes)", file=sys.stderr)

    return file_contents

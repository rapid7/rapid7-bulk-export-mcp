"""
Download module for Rapid7 Vulnerability Export.

This module handles downloading Parquet files from the Rapid7 API. It provides
functions to download individual files or multiple files in sequence, with
progress reporting and proper error handling.
"""

import sys
from typing import List

import requests

from .config import USER_AGENT


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

    Requirements:
        - 4.1: Download Parquet files from provided URLs
        - 4.2: Include API key in request header for authorization
        - 4.4: Raise HTTPError for failed downloads

    Example:
        >>> url = "https://example.com/export.parquet"
        >>> api_key = "my-api-key"
        >>> content = download_parquet_file(url, api_key)
        >>> print(f"Downloaded {len(content)} bytes")
    """
    headers = {
        "X-Api-Key": api_key,
        "User-Agent": USER_AGENT,
    }

    # Stream download for memory efficiency
    response = requests.get(url, headers=headers, stream=True)

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

    Requirements:
        - 4.1: Download each Parquet file from provided URLs
        - 4.5: Handle multiple Parquet file URLs

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

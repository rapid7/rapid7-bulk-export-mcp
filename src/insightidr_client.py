"""
InsightIDR REST Client Module

This module handles authenticated REST communication with the Rapid7
InsightIDR (Investigations) Command Platform API. It is a JSON REST
transport, distinct from the GraphQL Bulk Export transport in
graphql_client.py — same API key and region, different product.

Author: rozumeyroman@gmail.com
"""

from typing import Any, Dict, Optional

import requests

from .config import USER_AGENT

# The InsightIDR v2 Investigations API is in preview mode and requires this
# header to be accessed. See docs.rapid7.com/insightidr/insightidr-rest-api/
INVESTIGATIONS_ACCEPT_VERSION = "investigations-preview"


def send_idr_request(
    method: str,
    base_url: str,
    path: str,
    api_key: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    accept_version: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Send a REST request to the InsightIDR API.

    Args:
        method: HTTP method (GET, POST, PATCH, PUT, ...).
        base_url: Regional base URL, e.g. https://eu.api.insight.rapid7.com
        path: API path starting with a slash, e.g. /idr/v2/investigations
        api_key: The API key for authentication
        params: Optional query string parameters
        json_body: Optional JSON request body
        accept_version: Optional value for the Accept-version header,
            required by APIs still in preview mode

    Returns:
        The parsed JSON response as a dictionary, or an empty dict for
        responses with no body (e.g. some PUT/POST confirmations).

    Raises:
        requests.HTTPError: If the HTTP response status code is not 2xx.
        requests.RequestException: If the network request fails.
    """
    headers = {
        "X-Api-Key": api_key,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    if accept_version:
        headers["Accept-version"] = accept_version

    response = requests.request(
        method,
        f"{base_url}{path}",
        headers=headers,
        params=params,
        json=json_body,
        timeout=30,
    )
    response.raise_for_status()

    if not response.content:
        return {}
    return response.json()

"""
GraphQL Client Module

This module handles GraphQL communication with the Rapid7 Bulk Export API.
It provides functionality to send GraphQL queries and mutations with proper
authentication and error handling.
"""

from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests

from .config import USER_AGENT

TIMEOUT_SECONDS = (10, 30)
"""Connect and read timeouts. A region that is down must fail in seconds, not minutes."""

_GATEWAY_ERRORS = frozenset({502, 503, 504})


class RegionUnavailableError(requests.RequestException):
    """The region's API could not be reached, or its gateway answered 502, 503, or 504."""


def send_graphql_request(
    endpoint: str,
    api_key: str,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    timeout: Tuple[float, float] = TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """
    Send a GraphQL request to the Rapid7 API.

    This function sends a POST request to the specified GraphQL endpoint with
    the provided query and optional variables. It handles authentication via
    the X-Api-Key header and validates the response for both HTTP and GraphQL
    errors.

    Args:
        endpoint: The GraphQL API endpoint URL
        api_key: The API key for authentication
        query: The GraphQL query or mutation string
        variables: Optional dictionary of GraphQL variables
        timeout: Connect and read timeouts in seconds. Defaults to TIMEOUT_SECONDS.

    Returns:
        The parsed JSON response as a dictionary

    Raises:
        RegionUnavailableError: If the region cannot be reached or answers 502, 503, or 504
        requests.HTTPError: If the HTTP response status code is not 200
        ValueError: If the response contains GraphQL errors
        requests.RequestException: If the network request fails

    Example:
        >>> query = "query { vulnerabilityExport(id: $id) { status } }"
        >>> variables = {"id": "export-123"}
        >>> response = send_graphql_request(endpoint, api_key, query, variables)
    """
    # Set required headers
    headers = {
        "X-Api-Key": api_key,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }

    # Build request body
    body: Dict[str, Any] = {"query": query}
    if variables is not None:
        body["variables"] = variables

    host = urlparse(endpoint).hostname or endpoint
    # The underlying message is dropped on purpose: a proxy or adapter can echo the
    # request, and this text reaches the transcript.
    try:
        response = requests.post(endpoint, headers=headers, json=body, timeout=timeout)
    except requests.Timeout:
        raise RegionUnavailableError(f"{host} did not respond in time") from None
    except requests.ConnectionError:
        raise RegionUnavailableError(f"{host} could not be reached") from None
    if response.status_code in _GATEWAY_ERRORS:
        raise RegionUnavailableError(f"{host} answered HTTP {response.status_code}")

    # Raise HTTPError for non-200 status codes
    response.raise_for_status()

    # Parse JSON response
    response_data = response.json()

    # Check for GraphQL errors
    if "errors" in response_data:
        error_messages = [error.get("message", str(error)) for error in response_data["errors"]]
        raise ValueError(f"GraphQL errors: {'; '.join(error_messages)}")

    return response_data

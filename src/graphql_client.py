"""
GraphQL Client Module

This module handles GraphQL communication with the Rapid7 Bulk Export API.
It provides functionality to send GraphQL queries and mutations with proper
authentication and error handling.
"""

from typing import Any, Dict, Optional

import requests

from .config import USER_AGENT


def send_graphql_request(
    endpoint: str, api_key: str, query: str, variables: Optional[Dict[str, Any]] = None
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

    Returns:
        The parsed JSON response as a dictionary

    Raises:
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

    # Send POST request
    response = requests.post(endpoint, headers=headers, json=body, timeout=30)

    # Raise HTTPError for non-200 status codes
    response.raise_for_status()

    # Parse JSON response
    response_data = response.json()

    # Check for GraphQL errors
    if "errors" in response_data:
        error_messages = [error.get("message", str(error)) for error in response_data["errors"]]
        raise ValueError(f"GraphQL errors: {'; '.join(error_messages)}")

    return response_data

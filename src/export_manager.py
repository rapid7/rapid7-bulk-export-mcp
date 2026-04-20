"""
Export Manager Module

This module manages the export lifecycle for Rapid7 vulnerability exports,
including creating exports, polling for status, and retrieving download URLs.
"""

import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from .graphql_client import send_graphql_request


def build_remediation_date_chunks(start_date: str, end_date: str, max_days: int = 31) -> List[Tuple[str, str]]:
    """Split a date range into chunks of at most max_days.

    The Rapid7 remediation export API limits each request to 31 days.
    This helper breaks a larger range into compliant chunks.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        max_days: Maximum days per chunk (default 31, the API limit).

    Returns:
        List of (start, end) date string tuples, each spanning at most max_days.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start >= end:
        raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date}).")

    chunks: List[Tuple[str, str]] = []
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=max_days), end)
        chunks.append((cursor.isoformat(), chunk_end.isoformat()))
        cursor = chunk_end

    return chunks


def create_vulnerability_export(config: Dict[str, str]) -> str:
    """
    Create a vulnerability export job and return the export ID.

    This function sends a GraphQL CreateVulnerabilityExport mutation to the
    Rapid7 API to initiate a new vulnerability export job. The export job
    will be processed asynchronously by Rapid7, and the returned ID can be
    used to poll for status and retrieve download URLs when complete.

    If an export is already in progress, this function will extract and return
    the existing export ID from the error message.

    Args:
        config: Configuration dictionary containing:
            - endpoint (str): The GraphQL API endpoint URL
            - api_key (str): The API key for authentication

    Returns:
        str: The export ID that can be used to query export status

    Raises:
        requests.HTTPError: If the HTTP response status code is not 200
        ValueError: If the response contains GraphQL errors (except in-progress)
        requests.RequestException: If the network request fails

    Requirements:
        - 2.1: Send GraphQL CreateVulnerabilityExport mutation
        - 2.3: Extract and store export ID from response

    Example:
        >>> config = load_config()
        >>> export_id = create_vulnerability_export(config)
        >>> print(f"Export created with ID: {export_id}")
    """
    # Build the CreateVulnerabilityExport mutation as specified in the design
    # Note: The API requires an input argument (can be empty object)
    mutation = """
    mutation {
      createVulnerabilityExport(input: {}) {
        id
      }
    }
    """

    # Send the GraphQL request
    try:
        response = send_graphql_request(endpoint=config["endpoint"], api_key=config["api_key"], query=mutation)

        # Extract and return the export ID from the response
        export_id = response["data"]["createVulnerabilityExport"]["id"]
        return export_id

    except ValueError as e:
        # Check if this is an "already in-progress" error
        error_msg = str(e)
        if "already in-progress" in error_msg and "exportId:" in error_msg:
            # Extract the export ID from the error message
            # Format: "...In-progress exportId: <ID>"
            match = re.search(r"exportId:\s*([A-Za-z0-9+/=]+)", error_msg)
            if match:
                export_id = match.group(1)
                # Don't print the error, just return the ID
                return export_id

        # Re-raise if it's a different error
        raise


def create_policy_export(config: Dict[str, str]) -> str:
    """
    Create a policy export job and return the export ID.

    This function sends a GraphQL createPolicyExport mutation to the
    Rapid7 API to initiate a new policy export job. The export job
    will be processed asynchronously by Rapid7, and the returned ID can be
    used to poll for status and retrieve download URLs when complete.

    If an export is already in progress, this function will extract and return
    the existing export ID from the error message.

    Args:
        config: Configuration dictionary containing:
            - endpoint (str): The GraphQL API endpoint URL
            - api_key (str): The API key for authentication

    Returns:
        str: The export ID that can be used to query export status

    Raises:
        requests.HTTPError: If the HTTP response status code is not 200
        ValueError: If the response contains GraphQL errors (except in-progress)
        requests.RequestException: If the network request fails

    Requirements:
        - 1.1: Send GraphQL createPolicyExport mutation
        - 1.2: Extract and store export ID from response
        - 1.3: Handle already-in-progress error

    Example:
        >>> config = load_config()
        >>> export_id = create_policy_export(config)
        >>> print(f"Policy export created with ID: {export_id}")
    """
    mutation = """
    mutation {
      createPolicyExport(input: {}) {
        id
      }
    }
    """

    try:
        response = send_graphql_request(endpoint=config["endpoint"], api_key=config["api_key"], query=mutation)

        export_id = response["data"]["createPolicyExport"]["id"]
        return export_id

    except ValueError as e:
        error_msg = str(e)
        if "already in-progress" in error_msg and "exportId:" in error_msg:
            match = re.search(r"exportId:\s*([A-Za-z0-9+/=]+)", error_msg)
            if match:
                export_id = match.group(1)
                return export_id

        raise


def create_remediation_export(config: Dict[str, str], start_date: str, end_date: str) -> str:
    """
    Create a vulnerability remediation export job and return the export ID.

    This function validates the date range, then sends a GraphQL
    createVulnerabilityRemediationExport mutation to the Rapid7 API to
    initiate a new remediation export job. The export job will be processed
    asynchronously by Rapid7, and the returned ID can be used to poll for
    status and retrieve download URLs when complete.

    If an export is already in progress, this function will extract and return
    the existing export ID from the error message.

    Args:
        config: Configuration dictionary containing:
            - endpoint (str): The GraphQL API endpoint URL
            - api_key (str): The API key for authentication
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        str: The export ID that can be used to query export status

    Raises:
        ValueError: If date validation fails or the response contains
            GraphQL errors (except in-progress)
        requests.HTTPError: If the HTTP response status code is not 200
        requests.RequestException: If the network request fails

    Requirements:
        - 2.1: Send GraphQL createVulnerabilityRemediationExport mutation
        - 2.2: Format date range variables as {"input": {"startDate": ..., "endDate": ...}}
        - 2.3: Reject request when start_date equals end_date
        - 2.4: Reject request when date range exceeds 31 days
        - 2.5: Extract and store export ID from response
        - 2.6: Handle already-in-progress error
        - 7.6: Validate date format (YYYY-MM-DD)

    Example:
        >>> config = load_config()
        >>> export_id = create_remediation_export(config, "2024-01-01", "2024-01-31")
        >>> print(f"Remediation export created with ID: {export_id}")
    """
    # Validate date formats (YYYY-MM-DD) and reject invalid calendar dates
    try:
        parsed_start = datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"Invalid start_date format: '{start_date}'. Expected YYYY-MM-DD format with a valid calendar date."
        )

    try:
        parsed_end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"Invalid end_date format: '{end_date}'. Expected YYYY-MM-DD format with a valid calendar date."
        )

    # Validate start_date != end_date
    if start_date == end_date:
        raise ValueError(f"start_date and end_date must not be equal. Both are '{start_date}'.")

    # Validate date range does not exceed 31 days
    delta = abs((parsed_end - parsed_start).days)
    if delta > 31:
        raise ValueError(f"Date range must not exceed 31 days. Got {delta} days (from '{start_date}' to '{end_date}').")

    # Build the createVulnerabilityRemediationExport mutation
    mutation = """
    mutation CreateVulnerabilityRemediationExport(
      $input: VulnerabilityRemediationExportConfiguration!
    ) {
      createVulnerabilityRemediationExport(input: $input) {
        id
      }
    }
    """

    variables = {"input": {"startDate": start_date, "endDate": end_date}}

    try:
        response = send_graphql_request(
            endpoint=config["endpoint"], api_key=config["api_key"], query=mutation, variables=variables
        )

        export_id = response["data"]["createVulnerabilityRemediationExport"]["id"]
        return export_id

    except ValueError as e:
        error_msg = str(e)
        if "already in-progress" in error_msg and "exportId:" in error_msg:
            match = re.search(r"exportId:\s*([A-Za-z0-9+/=]+)", error_msg)
            if match:
                export_id = match.group(1)
                return export_id

        raise


def get_export_status(config: Dict[str, str], export_id: str) -> Dict[str, Any]:
    """
    Query the status of a vulnerability export job.

    This function sends a GraphQL query to retrieve the current status of an
    export job, including its completion state and available Parquet file URLs.

    Args:
        config: Configuration dictionary containing:
            - endpoint (str): The GraphQL API endpoint URL
            - api_key (str): The API key for authentication
        export_id: The export ID to query status for

    Returns:
        dict: Export status information containing:
            - id (str): The export ID
            - status (str): Current status (PENDING, PROCESSING, COMPLETE, FAILED)
            - parquetFiles (list[str]): Flat list of Parquet file URLs (backward compat)
            - result (list[dict]): List of {prefix, urls} objects from the API response

    Raises:
        requests.HTTPError: If the HTTP response status code is not 200
        ValueError: If the response contains GraphQL errors
        requests.RequestException: If the network request fails

    Requirements:
        - 3.1: Query export status using stored export ID

    Example:
        >>> config = load_config()
        >>> status = get_export_status(config, "export-123")
        >>> print(f"Status: {status['status']}")
    """
    # Build the export query
    # Using the correct query format from Rapid7 API documentation
    query = (
        """
    {
      export(id: "%s") {
        id
        status
        dataset
        timestamp
        result {
          prefix
          urls
        }
      }
    }
    """
        % export_id
    )

    # Send the GraphQL request
    response = send_graphql_request(endpoint=config["endpoint"], api_key=config["api_key"], query=query)

    # Extract and return the export status from the response
    export_data = response["data"]["export"]

    # Extract URLs from the result structure
    # result is a list of objects with prefix and urls
    parquet_urls = []
    result_list = export_data.get("result") or []
    if result_list:
        if isinstance(result_list, list):
            # result is a list of {prefix, urls} objects
            for item in result_list:
                if isinstance(item, dict) and "urls" in item:
                    parquet_urls.extend(item["urls"])
        elif isinstance(result_list, dict) and "urls" in result_list:
            # result is a single {prefix, urls} object
            parquet_urls = result_list["urls"]
            result_list = [result_list]

    return {
        "id": export_data["id"],
        "status": export_data["status"],
        "parquetFiles": parquet_urls,
        "result": result_list,
    }


def poll_until_complete(config: Dict[str, str], export_id: str, interval: int = 10) -> List[str]:
    """
    Poll the export status until it completes and return Parquet file URLs.

    This function continuously polls the export status at regular intervals until
    the export job reaches a terminal state (COMPLETE or FAILED). It prints status
    updates to stdout to provide user feedback during the polling process.

    Args:
        config: Configuration dictionary containing:
            - endpoint (str): The GraphQL API endpoint URL
            - api_key (str): The API key for authentication
        export_id: The export ID to poll status for
        interval: Number of seconds to wait between status checks (default: 10)

    Returns:
        list[str]: List of Parquet file URLs when export is COMPLETE

    Raises:
        ValueError: If the export status becomes FAILED
        requests.HTTPError: If the HTTP response status code is not 200
        requests.RequestException: If the network request fails

    Requirements:
        - 3.2: Poll status at regular intervals while not COMPLETE
        - 3.3: Extract Parquet file URLs when status is COMPLETE
        - 3.4: Raise error if status is FAILED
        - 3.5: Implement reasonable polling interval

    Example:
        >>> config = load_config()
        >>> export_id = create_vulnerability_export(config)
        >>> urls = poll_until_complete(config, export_id, interval=10)
        >>> print(f"Export complete. {len(urls)} file(s) ready for download.")
    """
    while True:
        # Query the current export status
        try:
            status_info = get_export_status(config, export_id)
            current_status = status_info["status"]

            # Print status update to stdout
            print(f"Export status: {current_status}", file=sys.stderr)

            # Check if export is complete
            if current_status in ["COMPLETE", "SUCCEEDED"]:
                parquet_urls = status_info["parquetFiles"]
                return parquet_urls

            # Check if export failed
            if current_status == "FAILED":
                raise ValueError(f"Export {export_id} failed")

            # Continue polling if status is PENDING, PROCESSING, or IN_PROGRESS
            if current_status in ["PENDING", "PROCESSING", "IN_PROGRESS"]:
                # Sleep for the specified interval before next poll
                time.sleep(interval)
            else:
                # Unexpected status - raise an error
                raise ValueError(f"Unexpected export status: {current_status}")

        except ValueError as e:
            error_msg = str(e)
            # If it's a GraphQL error, show helpful message
            if "GraphQL errors" in error_msg:
                print(f"\nError querying export status: {error_msg}", file=sys.stderr)
                print("\nThis may indicate:", file=sys.stderr)
                print(f"  - Invalid API endpoint (current: {config['endpoint']})", file=sys.stderr)
                print("  - Invalid API key", file=sys.stderr)
                print("  - API schema mismatch", file=sys.stderr)
                print("\nPlease verify your RAPID7_API_KEY and RAPID7_REGION settings.", file=sys.stderr)
            raise

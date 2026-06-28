"""
Export Manager Module

This module manages the export lifecycle for Rapid7 vulnerability exports,
including creating exports, polling for status, and retrieving download URLs.
"""

import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .graphql_client import send_graphql_request


def _extract_in_progress_id(error_msg: str) -> Optional[str]:
    """Extract an in-progress export ID from a Rapid7 'already in-progress' error message."""
    if "already in-progress" in error_msg and "exportId:" in error_msg:
        match = re.search(r"exportId:\s*([A-Za-z0-9+/=]+)", error_msg)
        if match:
            return match.group(1)
    return None


def _create_simple_export(config: Dict[str, str], mutation_name: str, response_key: str) -> str:
    """Send a zero-argument GraphQL export mutation and return the export ID."""
    mutation = f"""
    mutation {{
      {mutation_name}(input: {{}}) {{
        id
      }}
    }}
    """
    try:
        response = send_graphql_request(endpoint=config["endpoint"], api_key=config["api_key"], query=mutation)
        return response["data"][response_key]["id"]
    except ValueError as e:
        export_id = _extract_in_progress_id(str(e))
        if export_id:
            return export_id
        raise


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
    """Create a vulnerability export job and return the export ID.

    Sends a GraphQL CreateVulnerabilityExport mutation to initiate a new
    vulnerability export job. The returned ID can be used to poll for status
    and retrieve download URLs when complete.

    If an export is already in progress, returns the existing export ID from
    the error message.

    Args:
        config: Configuration dictionary containing endpoint and api_key.

    Returns:
        str: The export ID that can be used to query export status

    Raises:
        requests.HTTPError: If the HTTP response status code is not 200
        ValueError: If the response contains GraphQL errors (except in-progress)
        requests.RequestException: If the network request fails
    """
    return _create_simple_export(config, "createVulnerabilityExport", "createVulnerabilityExport")


def create_policy_export(config: Dict[str, str]) -> str:
    """Create a policy export job and return the export ID.

    Sends a GraphQL createPolicyExport mutation to initiate a new policy
    export job. The returned ID can be used to poll for status and retrieve
    download URLs when complete.

    If an export is already in progress, returns the existing export ID from
    the error message.

    Args:
        config: Configuration dictionary containing endpoint and api_key.

    Returns:
        str: The export ID that can be used to query export status

    Raises:
        requests.HTTPError: If the HTTP response status code is not 200
        ValueError: If the response contains GraphQL errors (except in-progress)
        requests.RequestException: If the network request fails
    """
    return _create_simple_export(config, "createPolicyExport", "createPolicyExport")


def create_remediation_export(config: Dict[str, str], start_date: str, end_date: str) -> str:
    """Create a vulnerability remediation export job and return the export ID.

    Validates the date range, then sends a GraphQL
    createVulnerabilityRemediationExport mutation to initiate a new remediation
    export job. The returned ID can be used to poll for status and retrieve
    download URLs when complete.

    If an export is already in progress, returns the existing export ID from
    the error message.

    Args:
        config: Configuration dictionary containing endpoint and api_key.
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        str: The export ID that can be used to query export status

    Raises:
        ValueError: If date validation fails or the response contains
            GraphQL errors (except in-progress)
        requests.HTTPError: If the HTTP response status code is not 200
        requests.RequestException: If the network request fails
    """
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

    if start_date == end_date:
        raise ValueError(f"start_date and end_date must not be equal. Both are '{start_date}'.")

    delta = abs((parsed_end - parsed_start).days)
    if delta > 31:
        raise ValueError(f"Date range must not exceed 31 days. Got {delta} days (from '{start_date}' to '{end_date}').")

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
        return response["data"]["createVulnerabilityRemediationExport"]["id"]

    except ValueError as e:
        export_id = _extract_in_progress_id(str(e))
        if export_id:
            return export_id
        raise


def create_asset_software_export(config: Dict[str, str]) -> str:
    """Create an asset software export job and return the export ID.

    Sends a GraphQL CreateAssetSoftwareExport mutation to initiate an export
    of installed software packages for all IVM-managed assets.

    Args:
        config: Configuration dictionary containing endpoint and api_key.

    Returns:
        str: The export ID that can be used to query export status

    Raises:
        requests.HTTPError: If the HTTP response status code is not 200
        ValueError: If the response contains GraphQL errors (except in-progress)
        requests.RequestException: If the network request fails
    """
    mutation = """
    mutation CreateAssetSoftwareExport($input: AssetSoftwareExportConfiguration!) {
        createAssetSoftwareExport(input: $input) {
            id
        }
    }
    """
    variables = {"input": {"source": "IVM", "format": "PARQUET"}}

    try:
        response = send_graphql_request(
            endpoint=config["endpoint"], api_key=config["api_key"], query=mutation, variables=variables
        )
        return response["data"]["createAssetSoftwareExport"]["id"]

    except ValueError as e:
        export_id = _extract_in_progress_id(str(e))
        if export_id:
            return export_id
        raise


def get_export_status(config: Dict[str, str], export_id: str) -> Dict[str, Any]:
    """Query the status of a vulnerability export job.

    Sends a GraphQL query to retrieve the current status of an export job,
    including its completion state and available Parquet file URLs.

    Args:
        config: Configuration dictionary containing endpoint and api_key.
        export_id: The export ID to query status for

    Returns:
        dict: Export status information containing:
            - id (str): The export ID
            - status (str): Current status (PENDING, PROCESSING, COMPLETE, FAILED)
            - parquetFiles (list[str]): Flat list of Parquet file URLs
            - result (list[dict]): List of {prefix, urls} objects from the API response

    Raises:
        requests.HTTPError: If the HTTP response status code is not 200
        ValueError: If the response contains GraphQL errors
        requests.RequestException: If the network request fails
    """
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

    response = send_graphql_request(endpoint=config["endpoint"], api_key=config["api_key"], query=query)
    export_data = response["data"]["export"]

    parquet_urls = []
    result_list = export_data.get("result") or []
    for item in result_list:
        if isinstance(item, dict) and "urls" in item:
            parquet_urls.extend(item["urls"])

    return {
        "id": export_data["id"],
        "status": export_data["status"],
        "parquetFiles": parquet_urls,
        "result": result_list,
    }


def poll_until_complete(config: Dict[str, str], export_id: str, interval: int = 10) -> List[str]:
    """Poll the export status until it completes and return Parquet file URLs.

    Continuously polls the export status at regular intervals until the export
    job reaches a terminal state (COMPLETE or FAILED). Prints status updates to
    stderr to provide user feedback during the polling process.

    Args:
        config: Configuration dictionary containing endpoint and api_key.
        export_id: The export ID to poll status for
        interval: Number of seconds to wait between status checks (default: 10)

    Returns:
        list[str]: List of Parquet file URLs when export is COMPLETE

    Raises:
        ValueError: If the export status becomes FAILED
        requests.HTTPError: If the HTTP response status code is not 200
        requests.RequestException: If the network request fails
    """
    while True:
        try:
            status_info = get_export_status(config, export_id)
            current_status = status_info["status"]

            print(f"Export status: {current_status}", file=sys.stderr)

            if current_status in ["COMPLETE", "SUCCEEDED"]:
                return status_info["parquetFiles"]

            if current_status == "FAILED":
                raise ValueError(f"Export {export_id} failed")

            if current_status in ["PENDING", "PROCESSING", "IN_PROGRESS"]:
                time.sleep(interval)
            else:
                raise ValueError(f"Unexpected export status: {current_status}")

        except ValueError as e:
            error_msg = str(e)
            if "GraphQL errors" in error_msg:
                print(f"\nError querying export status: {error_msg}", file=sys.stderr)
                print("\nThis may indicate:", file=sys.stderr)
                print(f"  - Invalid API endpoint (current: {config['endpoint']})", file=sys.stderr)
                print("  - Invalid API key", file=sys.stderr)
                print("  - API schema mismatch", file=sys.stderr)
                print("\nPlease verify your RAPID7_API_KEY and RAPID7_REGION settings.", file=sys.stderr)
            raise

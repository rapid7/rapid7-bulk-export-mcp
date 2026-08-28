#!/usr/bin/env python3
"""
FastMCP Server for Rapid7 Vulnerability Data

This server exposes vulnerability data through the Model Context Protocol,
allowing AI assistants to query and analyze the data.
"""

import datetime as _dt
import glob
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional

import duckdb as _duckdb
from fastmcp import FastMCP
from mcp.types import ToolAnnotations

from .config import load_config
from .download import download_all_files
from .duckdb_loader import VulnerabilityDatabase
from .export_manager import (
    build_remediation_date_chunks,
    create_asset_software_export,
    create_policy_export,
    create_remediation_export,
    create_vulnerability_export,
    get_export_status,
)
from .export_tracker import ExportTracker
from .insightidr_log_search_manager import (
    create_saved_query as idr_create_saved_query,
)
from .insightidr_log_search_manager import (
    delete_saved_query as idr_delete_saved_query,
)
from .insightidr_log_search_manager import (
    list_logs as idr_list_logs,
)
from .insightidr_log_search_manager import (
    list_logsets as idr_list_logsets,
)
from .insightidr_log_search_manager import (
    list_saved_queries as idr_list_saved_queries,
)
from .insightidr_log_search_manager import (
    query_logs as idr_query_logs,
)
from .insightidr_log_search_manager import (
    run_saved_query as idr_run_saved_query,
)
from .insightidr_manager import (
    VALID_DISPOSITIONS,
)
from .insightidr_manager import (
    assign_investigation as idr_assign_investigation,
)
from .insightidr_manager import (
    close_investigation as idr_close_investigation,
)
from .insightidr_manager import (
    create_comment as idr_create_comment,
)
from .insightidr_manager import (
    get_alert_evidence as idr_get_alert_evidence,
)
from .insightidr_manager import (
    get_investigation as idr_get_investigation,
)
from .insightidr_manager import (
    list_comments as idr_list_comments,
)
from .insightidr_manager import (
    list_investigation_alerts as idr_list_investigation_alerts,
)
from .insightidr_manager import (
    list_investigation_product_alerts as idr_list_investigation_product_alerts,
)
from .insightidr_manager import (
    list_investigations as idr_list_investigations,
)
from .insightidr_manager import (
    list_known_assignees as idr_list_known_assignees,
)

# Initialize FastMCP server
mcp = FastMCP("rapid7-bulk-export")

# Global database instance
db: Optional[VulnerabilityDatabase] = None

# Data directory — resolved once at startup, used for all database paths.
# Defaults to ~/.rapid7_mcp so relative-path writes never hit a read-only CWD.
_DATA_DIR: Path = Path(os.environ.get("DATA_DIR", "~/.rapid7_mcp")).expanduser().resolve()

VALID_EXPORT_TYPES = ("vulnerability", "policy", "remediation", "asset_software")


def initialize_database(db_path: Optional[str] = None) -> VulnerabilityDatabase:
    """Initialize the vulnerability database."""
    global db
    if db is None:
        resolved = db_path or str(_DATA_DIR / "rapid7_bulk_export.db")
        db = VulnerabilityDatabase(resolved)
    return db


@mcp.tool(
    annotations=ToolAnnotations(
        title="Load Rapid7 Parquet File",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def load_rapid7_parquet(parquet_path: str) -> str:
    """Load vulnerability data from existing Parquet file(s).

    Use this if you already have Parquet files downloaded and want to skip
    the export process. This is much faster than running a full export.

    Args:
        parquet_path: Path to a Parquet file or directory containing Parquet files

    Returns:
        Summary of loaded data including row count and statistics.
    """
    global db

    try:
        ALLOWED_ROOT = (_DATA_DIR / "imports").resolve()

        # Resolve and validate path is within allowed root
        resolved = Path(parquet_path).resolve()
        try:
            resolved.relative_to(ALLOWED_ROOT)
        except ValueError:
            return (
                f"✗ Error: Path must be within {ALLOWED_ROOT}\n"
                f"Resolved path '{resolved}' is outside the allowed directory.\n"
                f"Please copy your Parquet files into {ALLOWED_ROOT} first."
            )

        # Check if path exists
        if not resolved.exists():
            return f"✗ Error: Path does not exist: {resolved}"

        # Get list of parquet files
        if resolved.is_file():
            parquet_files = [str(resolved)]
        else:
            parquet_files = glob.glob(str(resolved / "*.parquet"))

        if not parquet_files:
            return f"✗ Error: No Parquet files found at: {resolved}"

        # Initialize database if needed
        if db is None:
            initialize_database()

        # Detect file types by peeking at schema and build prefix map
        prefix_file_map: dict = {}
        for pf in parquet_files:
            try:
                cols = [
                    desc[0]
                    for desc in _duckdb.execute(
                        f"SELECT * FROM read_parquet('{pf}') LIMIT 0"  # nosec B608
                    ).description
                ]
                if "vulnId" in cols or "checkId" in cols:
                    prefix_file_map.setdefault("asset_vulnerability", []).append(pf)
                else:
                    prefix_file_map.setdefault("asset", []).append(pf)
            except Exception:
                # If we can't determine type, skip the file
                continue

        if not prefix_file_map:
            return f"✗ Error: Could not determine schema for any Parquet files at: {resolved}"

        # Load into database
        row_counts = db.load_parquet_files_by_prefix(prefix_file_map)
        row_count = sum(row_counts.values())

        # Get statistics
        stats = db.get_stats()

        return (
            f"✓ Successfully loaded {row_count} rows from {len(parquet_files)} file(s).\n\n"
            f"Per-table row counts: {json.dumps(row_counts, default=str)}\n\n"
            f"Statistics:\n{json.dumps(stats, indent=2, default=str)}\n\n"
            f"You can now query the data using query_rapid7, get_rapid7_schema, or get_rapid7_stats tools."
        )

    except Exception as e:
        return f"✗ Error loading Parquet files: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Start Rapid7 Export",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def start_rapid7_export(
    export_type: str = "vulnerability",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Start a new Rapid7 export job (non-blocking).

    This is a fast, non-blocking call that creates an export job on the
    Rapid7 platform and returns the export ID immediately. The export
    will process in the background on Rapid7's servers (typically 3-5
    minutes).

    Use check_rapid7_export_status(export_id) to monitor progress, then
    download_rapid7_export(export_id, export_type="...") once it completes.

    If an export from today already exists, returns that export's ID
    instead of creating a duplicate.

    For remediation exports, the Rapid7 API limits each request to 31 days.
    If the date range exceeds 31 days, this tool automatically splits it
    into multiple 31-day chunks and kicks off an export for each chunk.

    Args:
        export_type: Type of export to create. One of "vulnerability",
                     "policy", or "remediation".
        start_date: Start date in YYYY-MM-DD format (only for remediation exports).
                    Defaults to 30 days ago if not specified.
        end_date: End date in YYYY-MM-DD format (only for remediation exports).
                  Defaults to today if not specified.

    Returns:
        The export ID and next steps.
    """
    if export_type not in VALID_EXPORT_TYPES:
        return f"✗ Invalid export_type: '{export_type}'. Valid values are: {', '.join(VALID_EXPORT_TYPES)}"

    try:
        config = load_config()

        tracker = ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))

        # Return a cached export from today unless it's remediation (which is date-range keyed)
        today_export = tracker.get_today_export(export_type=export_type)
        if today_export and export_type != "remediation":
            tracker.close()
            eid = today_export["export_id"]
            return (
                f"♻️ A {export_type} export from today already exists.\n\n"
                f"Export ID: {eid}\n"
                f"Status: COMPLETE\n"
                f"Created: {today_export['created_at']}\n"
                f"Rows: {today_export['row_count']}\n\n"
                f"Load it with: "
                f"download_rapid7_export("
                f'export_id="{eid}", '
                f'export_type="{export_type}")'
            )

        # Create the export based on type
        if export_type == "vulnerability":
            print("Creating new vulnerability export...", file=sys.stderr)
            new_id = create_vulnerability_export(config)
            print(f"Created {export_type} export with ID: {new_id}", file=sys.stderr)
            tracker.save_export(export_id=new_id, status="PENDING", parquet_urls=[], export_type=export_type)
            tracker.close()

            return (
                f"✓ Vulnerability export job created.\n\n"
                f"Export ID: {new_id}\n"
                f"Status: PENDING\n\n"
                f"The export is now processing on Rapid7's servers "
                f"(typically 3-5 minutes).\n"
                f'Check progress: check_rapid7_export_status(export_id="{new_id}")\n'
                f"Once COMPLETE, load with: "
                f'download_rapid7_export(export_id="{new_id}", export_type="vulnerability")'
            )

        elif export_type == "policy":
            print("Creating new policy export...", file=sys.stderr)
            new_id = create_policy_export(config)
            print(f"Created {export_type} export with ID: {new_id}", file=sys.stderr)
            tracker.save_export(export_id=new_id, status="PENDING", parquet_urls=[], export_type=export_type)
            tracker.close()

            return (
                f"✓ Policy export job created.\n\n"
                f"Export ID: {new_id}\n"
                f"Status: PENDING\n\n"
                f"The export is now processing on Rapid7's servers "
                f"(typically 3-5 minutes).\n"
                f'Check progress: check_rapid7_export_status(export_id="{new_id}")\n'
                f"Once COMPLETE, load with: "
                f'download_rapid7_export(export_id="{new_id}", export_type="policy")'
            )

        elif export_type == "remediation":
            if not start_date:
                start_date = (_dt.date.today() - _dt.timedelta(days=30)).isoformat()
            if not end_date:
                end_date = _dt.date.today().isoformat()

            chunks = build_remediation_date_chunks(start_date, end_date)

            export_ids = []
            for chunk_start, chunk_end in chunks:
                print(f"Creating remediation export: {chunk_start} → {chunk_end}", file=sys.stderr)
                eid = create_remediation_export(config, chunk_start, chunk_end)
                export_ids.append({"id": eid, "start": chunk_start, "end": chunk_end})
                tracker.save_export(export_id=eid, status="PENDING", parquet_urls=[], export_type="remediation")
            tracker.close()

            lines = [
                f"✓ Created {len(export_ids)} remediation export(s) covering {start_date} → {end_date}.\n",
            ]
            for i, info in enumerate(export_ids, 1):
                lines.append(f"  {i}. {info['start']} → {info['end']}  Export ID: {info['id']}")

            lines.append("")
            lines.append("Each export takes ~3-5 minutes to process.")
            lines.append('Check progress with: check_rapid7_export_status(export_id="...")')
            lines.append(
                'Once COMPLETE, load each with: download_rapid7_export(export_id="...", export_type="remediation")'
            )
            lines.append("All chunks load into the same vulnerability_remediation table.")

            return "\n".join(lines)

        elif export_type == "asset_software":
            new_id = create_asset_software_export(config)
            print(f"Created asset_software export with ID: {new_id}", file=sys.stderr)
            tracker.save_export(export_id=new_id, status="PENDING", parquet_urls=[], export_type="asset_software")
            tracker.close()

            return (
                f"✓ Asset software export job created.\n\n"
                f"Export ID: {new_id}\n"
                f"Status: PENDING\n\n"
                f"The export is now processing on Rapid7's servers "
                f"(typically 3-5 minutes).\n"
                f'Check progress: check_rapid7_export_status(export_id="{new_id}")\n'
                f"Once COMPLETE, load with: "
                f'download_rapid7_export(export_id="{new_id}", export_type="asset_software")'
            )

    except Exception as e:
        return f"✗ Error starting {export_type} export: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Check Rapid7 Export Status",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def check_rapid7_export_status(export_id: str) -> str:
    """Check the current status of a Rapid7 export job.

    This is a fast, non-blocking call that queries the Rapid7 API once
    and returns the current status. Does NOT poll or wait.

    Args:
        export_id: The export ID returned by start_rapid7_export.

    Returns:
        Current export status and next steps.
    """
    try:
        config = load_config()
        status_info = get_export_status(config, export_id)
        current_status = status_info["status"]
        file_count = len(status_info.get("parquetFiles", []))

        if current_status in ["COMPLETE", "SUCCEEDED"]:
            return (
                f"✓ Export is complete.\n\n"
                f"Export ID: {export_id}\n"
                f"Status: {current_status}\n"
                f"Files ready: {file_count}\n\n"
                f"Load the data with: "
                f"download_rapid7_export("
                f'export_id="{export_id}", '
                f'export_type="...")'
            )
        elif current_status == "FAILED":
            return (
                f"✗ Export failed.\n\n"
                f"Export ID: {export_id}\n"
                f"Status: FAILED\n\n"
                f"Start a new export with: start_rapid7_export()"
            )
        else:
            return (
                f"⏳ Export still processing.\n\n"
                f"Export ID: {export_id}\n"
                f"Status: {current_status}\n\n"
                f"Check again in 30-60 seconds with: "
                f"check_rapid7_export_status("
                f'export_id="{export_id}")'
            )

    except Exception as e:
        return f"✗ Error checking export status: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Download Rapid7 Export",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def download_rapid7_export(export_id: str, export_type: str = "vulnerability") -> str:
    """Download a completed Rapid7 export and load into the database.

    Call this after check_rapid7_export_status confirms the export is COMPLETE.
    Downloads the Parquet files and loads them into the local DuckDB
    database for querying.

    Args:
        export_id: The export ID of a completed export.
        export_type: Type of export. One of "vulnerability", "policy",
                     or "remediation".

    Returns:
        Summary of loaded data including row counts and statistics.
    """
    global db

    if export_type not in VALID_EXPORT_TYPES:
        return f"✗ Invalid export_type: '{export_type}'. Valid values are: {', '.join(VALID_EXPORT_TYPES)}"

    try:
        config = load_config()

        # Verify export is complete
        status_info = get_export_status(config, export_id)
        current_status = status_info["status"]

        if current_status not in ["COMPLETE", "SUCCEEDED"]:
            return (
                f"✗ Export is not yet complete.\n\n"
                f"Export ID: {export_id}\n"
                f"Status: {current_status}\n\n"
                f"Check again with: "
                f"check_rapid7_export_status("
                f'export_id="{export_id}")'
            )

        parquet_urls = status_info["parquetFiles"]
        if not parquet_urls:
            return f"✗ Export complete but has no files.\n\nExport ID: {export_id}"

        # Download files
        print(f"Downloading {len(parquet_urls)} {export_type} files...", file=sys.stderr)
        file_data = download_all_files(parquet_urls, config["api_key"])

        # Initialize database if needed
        if db is None:
            initialize_database()

        temp_dir = tempfile.mkdtemp()
        validation_warnings = []

        try:
            # All export types use prefix-based routing from the API response
            result_list = status_info.get("result") or []

            url_to_prefix = {}
            for item in result_list:
                prefix = item.get("prefix", "")
                for url in item.get("urls", []):
                    url_to_prefix[url] = prefix

            prefix_file_map = {}
            for i, (url, data) in enumerate(zip(parquet_urls, file_data)):
                temp_path = Path(temp_dir) / f"{export_type}_export_{i}.parquet"
                temp_path.write_bytes(data)
                prefix = url_to_prefix.get(url, "unknown")
                prefix_file_map.setdefault(prefix, []).append(str(temp_path))
                # Validate file has content
                if len(data) < 100:
                    validation_warnings.append(f"File {i + 1} (prefix={prefix}): unusually small ({len(data)} bytes)")

            if export_type == "policy":
                row_counts = db.load_parquet_files_by_prefix(prefix_file_map, skip_prefixes={"asset"})
            elif export_type == "remediation":
                row_counts = db.load_parquet_files_by_prefix(prefix_file_map, append=True)
            else:
                row_counts = db.load_parquet_files_by_prefix(prefix_file_map)

            row_count = sum(row_counts.values())
            row_info = f"Rows loaded: {row_count}\nPer-table row counts: {json.dumps(row_counts, default=str)}"

            if row_count == 0 and len(file_data) > 0:
                validation_warnings.append(
                    f"⚠️  {len(file_data)} file(s) downloaded but 0 rows loaded. "
                    f"Prefixes received: {list(prefix_file_map.keys())}. "
                    f"Check that prefixes match expected routing."
                )

            # Save export metadata
            tracker = ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))
            tracker.save_export(
                export_id=export_id,
                status="COMPLETE",
                parquet_urls=parquet_urls,
                row_count=row_count,
                export_type=export_type,
            )
            tracker.close()

            # Get statistics
            stats = db.get_stats()

        finally:
            # Clean up temp files
            shutil.rmtree(temp_dir)

        # Build validation warnings section
        warnings_section = ""
        if validation_warnings:
            warnings_section = "\nValidation Warnings:\n" + "\n".join(f"  {w}" for w in validation_warnings) + "\n"

        return (
            f"✓ {export_type.capitalize()} data loaded successfully.\n\n"
            f"Export ID: {export_id}\n"
            f"Files processed: {len(parquet_urls)}\n"
            f"{row_info}\n"
            f"{warnings_section}\n"
            f"Statistics:\n"
            f"{json.dumps(stats, indent=2, default=str)}\n\n"
            f"Query the data with query_rapid7, "
            f"get_rapid7_schema, or get_rapid7_stats."
        )

    except Exception as e:
        return (
            f"✗ Error downloading/loading {export_type}: {str(e)}\n\n"
            f"Export ID: {export_id}\n"
            f"Retry with: download_rapid7_export("
            f'export_id="{export_id}", '
            f'export_type="{export_type}")'
        )


@mcp.tool(
    annotations=ToolAnnotations(
        title="Query Rapid7 Data",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def query_rapid7(sql: str) -> str:
    """Execute a SQL query against the Rapid7 database.

    The database contains the following tables loaded from Rapid7 InsightVM
    Bulk Export API Parquet files:

    **assets** — Asset inventory data:
      Key fields: orgId, assetId, agentId, hostName, ip, mac, osFamily,
      osProduct, osVersion, osDescription, riskScore, sites, assetGroups, tags,
      awsInstanceId, azureResourceId, gcpObjectId

    **vulnerabilities** — Combined asset + vulnerability data:
      Key fields: orgId, assetId, vulnId, checkId, port, protocol, title,
      description, severity, severityRank, cvssScore, cvssV3Score,
      cvssV3Severity, hasExploits, epssscore, epsspercentile, riskScoreV2_0,
      cves, firstFoundTimestamp, reintroducedTimestamp, dateAdded,
      dateModified, datePublished, pciCompliant, pciSeverity

    **vulnerability_exceptions** — Vulnerability exceptions (waived/accepted risk):
      Key fields: orgId, assetId, vulnId, checkId, key, port, protocol,
      nic, proof, firstFoundTimestamp, reintroducedTimestamp, exceptionDetails

    **policies** — Policy compliance results (agent and scan based):
      Key fields: orgId, assetId, benchmarkNaturalId, profileNaturalId,
      benchmarkVersion, ruleNaturalId, ruleTitle, finalStatus, proof,
      lastAssessmentTimestamp, benchmarkTitle, profileTitle, publisher,
      fixTexts, rationales, source ('agent' or 'scan')

    **vulnerability_remediation** — Vulnerability remediation tracking:
      Key fields: orgId, assetId, cveId, vulnId, proof, firstFoundTimestamp,
      reintroducedTimestamp, lastDetected, lastRemoved, title, description,
      cvssV2Score, cvssV3Score, cvssV2Severity, cvssV3Severity,
      cvssV2AttackVector, cvssV3AttackVector, riskScoreV2_0, datePublished,
      dateAdded, dateModified, epssscore, epsspercentile

    Use this tool to query any of the above tables. You can filter, aggregate,
    join across tables, or perform any SQL-based analysis supported by DuckDB.

    Examples:
    - SELECT * FROM vulnerabilities WHERE severity = 'Critical' LIMIT 10
    - SELECT severity, COUNT(*) FROM vulnerabilities GROUP BY severity
    - SELECT * FROM policies WHERE finalStatus = 'fail' LIMIT 10
    - SELECT cveId, COUNT(*) FROM vulnerability_remediation GROUP BY cveId

    Args:
        sql: SQL query to execute against the database

    Returns:
        Query results as formatted JSON
    """
    global db

    if db is None or not db.has_data():
        return "Error: No data loaded. Please run start_rapid7_export and download_rapid7_export first."

    try:
        results = db.query(sql)
        result_text = json.dumps(results, indent=2, default=str)
        return f"Query executed successfully. {len(results)} rows returned.\n\n{result_text}"
    except Exception as e:
        return f"Error executing query: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Rapid7 Schema",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def get_rapid7_schema() -> str:
    """Get the schema of all database tables.

    Returns column names and data types for all existing tables:
    assets, vulnerabilities, policies, and vulnerability_remediation.
    Tables that have not been loaded yet are omitted.

    Use this to understand what data is available before writing queries.

    Returns:
        Table schemas as formatted JSON, keyed by table name
    """
    global db

    if db is None or not db.has_data():
        return "Error: No data loaded. Please run start_rapid7_export and download_rapid7_export first."

    try:
        schema = db.get_schema()
        schema_text = json.dumps(schema, indent=2)
        return f"Database schema:\n\n{schema_text}"
    except Exception as e:
        return f"Error getting schema: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Rapid7 Statistics",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def get_rapid7_stats() -> str:
    """Get summary statistics for all database tables.

    Returns row counts and relevant distributions for all existing tables:
    assets, vulnerabilities, policies, and vulnerability_remediation.
    Tables that have not been loaded yet are omitted.

    Useful for getting an overview of the data across all loaded datasets.

    Returns:
        Summary statistics as formatted JSON, keyed by table name
    """
    global db

    if db is None or not db.has_data():
        return "Error: No data loaded. Please run start_rapid7_export and download_rapid7_export first."

    try:
        stats = db.get_stats()
        stats_text = json.dumps(stats, indent=2, default=str)
        return f"Database statistics:\n\n{stats_text}"
    except Exception as e:
        return f"Error getting statistics: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Purge Rapid7 Data",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def purge_rapid7_data() -> str:
    """Permanently delete all local Rapid7 data and tracking databases.

    This removes:
    - The main vulnerability database (rapid7_bulk_export.db)
    - The export tracking database (rapid7_bulk_export_tracking.db)
    - Any associated WAL files

    Use this when you are done with your analysis session, before handing
    off a machine, or to free disk space. After purging, you will need to
    run a new export to query data again.

    Returns:
        Confirmation of purged data.
    """
    global db

    try:
        # Purge main database
        if db is not None:
            db.purge()

        # Purge tracking database
        tracker = ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))
        tracker.purge()

        return (
            "✓ All local Rapid7 data has been purged.\n\n"
            "Deleted:\n"
            "  - Vulnerability database (rapid7_bulk_export.db)\n"
            "  - Export tracking database (rapid7_bulk_export_tracking.db)\n\n"
            "To load new data, run start_rapid7_export() followed by download_rapid7_export()."
        )

    except Exception as e:
        return f"✗ Error purging data: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Rapid7 Exports",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def list_rapid7_exports(limit: int = 10) -> str:
    """List recent Rapid7 exports tracked in the system.

    Shows export metadata including export ID, date, status, type, and row counts.
    Useful for understanding what exports are available for reuse.

    Args:
        limit: Maximum number of exports to return (default: 10)

    Returns:
        Formatted list of recent exports
    """
    try:
        tracker = ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))
        exports = tracker.list_exports(limit=limit)
        tracker.close()

        if not exports:
            return "No exports found in the tracker database."

        result = f"Recent Exports (showing up to {limit}):\n\n"
        for exp in exports:
            result += f"Export ID: {exp['export_id']}\n"
            result += f"  Type: {exp.get('export_type', 'vulnerability')}\n"
            result += f"  Date: {exp['export_date']}\n"
            result += f"  Created: {exp['created_at']}\n"
            result += f"  Status: {exp['status']}\n"
            result += f"  Files: {exp['file_count']}\n"
            result += f"  Rows: {exp['row_count']}\n\n"

        return result

    except Exception as e:
        return f"✗ Error listing exports: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Rapid7 InsightIDR Investigations",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def get_investigations(
    status: str = "",
    priorities: str = "",
    assignee_email: str = "",
    sources: str = "",
    tags: str = "",
    limit: int = 20,
) -> str:
    """List InsightIDR investigations matching the given filters.

    Args:
        status: Comma-separated statuses to include, e.g. "OPEN,INVESTIGATING".
                Valid values: "OPEN", "INVESTIGATING", "WAITING", "CLOSED".
                Leave empty to include all statuses.
        priorities: Comma-separated priorities to include, e.g. "CRITICAL,HIGH".
                    Leave empty to include all priorities.
        assignee_email: Only return investigations assigned to this user.
        sources: Comma-separated sources to include, e.g. "MANUAL,HUNT,ALERT".
                 Leave empty to include all sources.
        tags: Comma-separated tags; only investigations with ALL given tags
              are included. Leave empty to not filter by tags.
        limit: Maximum number of investigations to return (1-100, default 20).

    Returns:
        A formatted list of matching investigations with their ID, title,
        status, priority, disposition, and assignee.
    """
    try:
        config = load_config()
        response = idr_list_investigations(
            config,
            status=status or None,
            priorities=priorities or None,
            assignee_email=assignee_email or None,
            sources=sources or None,
            tags=tags or None,
            size=limit,
        )
        investigations = response.get("data", [])
        metadata = response.get("metadata", {})

        if not investigations:
            return "No investigations found matching the given filters."

        lines = [f"Found {len(investigations)} investigation(s) (total matching: {metadata.get('total_data', '?')}):\n"]
        for inv in investigations:
            assignee = inv.get("assignee") or {}
            lines.append(
                f"- [{inv.get('priority', '?')}] {inv.get('title', '(no title)')}\n"
                f"    id: {inv.get('rrn', inv.get('id', '?'))}\n"
                f"    status: {inv.get('status', '?')}  disposition: {inv.get('disposition', '-')}\n"
                f"    assignee: {assignee.get('name', 'Unassigned')} ({assignee.get('email', '-')})\n"
                f"    created: {inv.get('created_time', '?')}  latest alert: {inv.get('latest_alert_time', '?')}"
            )
        return "\n\n".join(lines)

    except Exception as e:
        return f"✗ Error listing investigations: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Rapid7 InsightIDR Investigation Details",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def get_investigation_details(investigation_id: str) -> str:
    """Get full details for a single InsightIDR investigation, including its associated alerts.

    Args:
        investigation_id: The investigation ID or RRN (from get_investigations).

    Returns:
        The complete investigation record (all fields returned by the API)
        plus the list of alerts/evidence associated with it.
    """
    try:
        config = load_config()
        investigation = idr_get_investigation(config, investigation_id)

        if not investigation:
            return f"✗ No investigation found for ID: {investigation_id}"

        alerts_response = idr_list_investigation_alerts(config, investigation_id, size=100)
        alerts = alerts_response.get("data", [])

        sections = [
            "Investigation details:",
            json.dumps(investigation, indent=2, default=str),
        ]

        if alerts:
            sections.append(f"\nAssociated alerts ({len(alerts)}):")
            sections.append(json.dumps(alerts, indent=2, default=str))
        else:
            sections.append("\nNo associated alerts found.")

        return "\n".join(sections)

    except Exception as e:
        return f"✗ Error getting investigation details: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Rapid7 InsightIDR Alert Evidence",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def get_alert_evidence(alert_id: str) -> str:
    """Get the raw event evidence (source data) behind a specific InsightIDR alert.

    Use this after get_investigation_details when you need event-level
    detail (source IP, user/account, geolocation, service, result) that
    the investigation/alert summary doesn't include.

    PRIVACY NOTE: This can return personal data tied to a real person in
    the organization (name, email, IP address, device info) if the alert
    involves a user action such as authentication. Treat it accordingly —
    it is sensitive organizational data, not test data.

    Args:
        alert_id: The alert ID/RRN, from the "Associated alerts" section
                  of get_investigation_details.

    Returns:
        Evidence records for the alert: event type, timestamp, and the
        parsed source event payload.
    """
    try:
        config = load_config()
        response = idr_get_alert_evidence(config, alert_id)
        evidences = response.get("evidences", [])

        if not evidences:
            return f"No evidence found for alert: {alert_id}"

        sections = [f"Found {len(evidences)} evidence record(s) for alert {alert_id}:"]
        for ev in evidences:
            sections.append(
                f"\n- event_type: {ev.get('event_type', '?')}  source: {ev.get('external_source', '?')}"
                f"\n  evented_at: {ev.get('evented_at', '?')}"
            )
            raw_data = ev.get("data")
            if raw_data:
                try:
                    parsed = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
                    sections.append(f"  event data:\n{json.dumps(parsed, indent=2, ensure_ascii=False, default=str)}")
                except (TypeError, ValueError):
                    sections.append(f"  event data (raw): {raw_data}")

        return "\n".join(sections)

    except Exception as e:
        return f"✗ Error getting alert evidence: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Rapid7 Product Alerts for InsightIDR Investigation",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def get_investigation_product_alerts(investigation_id: str) -> str:
    """List alerts from OTHER Rapid7 products (not InsightIDR) linked to this investigation.

    Covers products like Threat Command (external threat intel) or Insight
    Agent (endpoint), if the org has an active license and this
    investigation is tied to one of their alerts. This is separate from
    get_investigation_details's "Associated alerts", which only covers
    InsightIDR's own alerts — most investigations will have none of these,
    which is normal, not an error.

    Args:
        investigation_id: The investigation ID or RRN (from get_investigations).

    Returns:
        A formatted list of linked product alerts (product type, and for
        Threat Command, the close reasons that API expects when this
        investigation is eventually closed), or a note that none exist.
    """
    try:
        config = load_config()
        product_alerts = idr_list_investigation_product_alerts(config, investigation_id)

        if not product_alerts:
            return "No Rapid7 product alerts (from products other than InsightIDR) are linked to this investigation."

        lines = [f"Found {len(product_alerts)} linked product alert(s):"]
        for pa in product_alerts:
            lines.append(f"\n- Product: {pa.get('type', '?')}")

            threat_command = pa.get("threat_command_details")
            if threat_command:
                reasons = ", ".join(threat_command.get("applicable_close_reasons", []))
                lines.append(
                    f"  Threat Command alert_id: {threat_command.get('alert_id', '?')}"
                    f"  type: {threat_command.get('alert_type', '?')}\n"
                    f"  Applicable close reasons: {reasons or '(none listed)'}"
                )

            for agent_alert in pa.get("insight_agent_details") or []:
                lines.append(
                    f"  Insight Agent alert_id: {agent_alert.get('alert_id', '?')}"
                    f"  type: {agent_alert.get('alert_type', '?')}"
                    f"  action taken: {agent_alert.get('agent_action_taken', '?')}"
                )

        return "\n".join(lines)

    except Exception as e:
        return f"✗ Error getting product alerts: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Rapid7 InsightIDR Known Assignees",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def list_investigation_assignees() -> str:
    """List people previously assigned to InsightIDR investigations.

    NOTE: This is NOT an exhaustive list of everyone eligible to be
    assigned. InsightIDR's Investigations API has no endpoint to list all
    eligible platform users (that requires a separate, permission-gated
    account-management API this key does not have access to). This only
    surfaces people observed as an assignee on at least one investigation
    so far. To assign someone not listed here, call assign_investigation
    with their email directly — the API validates eligibility itself and
    returns an error if the email is invalid or ineligible.

    Returns:
        Known assignee names and emails observed in investigation history.
    """
    try:
        config = load_config()
        assignees = idr_list_known_assignees(config)

        if not assignees:
            return (
                "No known assignees found — no investigations have been assigned to anyone yet.\n"
                "You can still call assign_investigation(investigation_id, user_email) with any "
                "email; the API validates eligibility."
            )

        lines = ["Known assignees (from investigation history):"]
        for a in assignees:
            lines.append(f"- {a['name'] or '(no name)'} <{a['email']}>")
        lines.append(
            "\nThis list is not exhaustive — it's only people assigned before. "
            "To assign someone not listed, call assign_investigation with their email directly."
        )
        return "\n".join(lines)

    except Exception as e:
        return f"✗ Error listing assignees: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Rapid7 InsightIDR Investigation Comments",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def list_investigation_comments(investigation_id: str, limit: int = 20) -> str:
    """List comments on an InsightIDR investigation, newest first.

    Args:
        investigation_id: The investigation RRN (use the full rrn from
                           get_investigations, not a short id).
        limit: Maximum number of comments to return (1-100, default 20).

    Returns:
        A formatted list of comments with author, time, visibility, and body.
    """
    try:
        config = load_config()
        response = idr_list_comments(config, investigation_id, size=limit)
        comments = response.get("data", [])

        if not comments:
            return "No comments found on this investigation."

        lines = [f"Found {len(comments)} comment(s):"]
        for c in comments:
            creator = c.get("creator") or {}
            lines.append(
                f"\n- {creator.get('name', '?')} ({creator.get('type', '?')}) at {c.get('created_time', '?')}"
                f"\n  visibility: {c.get('visibility', '?')}\n  {c.get('body', '')}"
            )
        return "\n".join(lines)

    except Exception as e:
        return f"✗ Error listing comments: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Add Rapid7 InsightIDR Investigation Comment",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=True,
    )
)
def add_investigation_comment(investigation_id: str, body: str) -> str:
    """Add a comment to an InsightIDR investigation. WRITE operation, applied immediately.

    Unlike close_investigation/assign_investigation, this does NOT require
    a confirm step — a comment doesn't change the investigation's status,
    priority, or disposition, and isn't destructive (it can be reviewed or
    removed by an analyst in the InsightIDR UI). Still, only post comments
    the user actually asked for or clearly approved — don't narrate routine
    tool activity into the investigation's comment thread unprompted.

    Args:
        investigation_id: The investigation RRN (use the full rrn from
                           get_investigations, not a short id).
        body: The comment text.

    Returns:
        Confirmation of the created comment, including its visibility
        (set by the API — not something this call controls).
    """
    if not body or not body.strip():
        return "✗ Comment body cannot be empty."

    try:
        config = load_config()
        result = idr_create_comment(config, investigation_id, body)
        return (
            f"✓ Comment added.\n\n"
            f"Investigation: {investigation_id}\n"
            f"Visibility: {result.get('visibility', '?')}\n"
            f"Body: {result.get('body', body)}"
        )

    except Exception as e:
        return f"✗ Error adding comment: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Assign Rapid7 InsightIDR Investigation",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def assign_investigation(investigation_id: str, user_email: str, confirm: bool = False) -> str:
    """Assign an InsightIDR investigation to a user. WRITE operation — the assigned user is notified by email.

    SAFETY: Call this once WITHOUT confirm=True first — it previews the
    investigation and target assignee, and makes no changes. Only call it
    again with confirm=True, after the user explicitly approves, to
    actually assign it.

    Args:
        investigation_id: The investigation ID or RRN (from get_investigations).
        user_email: Email of the user to assign. Must be a platform
                    administrator, product administrator, or read/write
                    user with InsightIDR/InsightUBA access — use
                    list_investigation_assignees for known candidates, or
                    pass any email directly and let the API validate it.
        confirm: Must be True to actually perform the assignment.

    Returns:
        A preview of the change (if confirm=False), or confirmation that
        the investigation was assigned (if confirm=True).
    """
    try:
        config = load_config()
        current = idr_get_investigation(config, investigation_id)
        current_assignee = (current.get("assignee") or {}).get("email", "Unassigned")

        if not confirm:
            return (
                f"⚠️ This will ASSIGN the following investigation to {user_email}. "
                f"They will receive a notification email. No changes have been made yet.\n\n"
                f"Title: {current.get('title', '?')}\n"
                f"ID: {investigation_id}\n"
                f"Current assignee: {current_assignee}\n"
                f"New assignee: {user_email}\n\n"
                f'To proceed, call assign_investigation(investigation_id="{investigation_id}", '
                f'user_email="{user_email}", confirm=True)'
            )

        result = idr_assign_investigation(config, investigation_id, user_email)
        return (
            f"✓ Investigation assigned.\n\n"
            f"Title: {result.get('title', current.get('title', '?'))}\n"
            f"ID: {investigation_id}\n"
            f"Assignee: {(result.get('assignee') or {}).get('email', user_email)}"
        )

    except Exception as e:
        return f"✗ Error assigning investigation: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Close Rapid7 InsightIDR Investigation",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def close_investigation(investigation_id: str, disposition: str, confirm: bool = False) -> str:
    """Close a single InsightIDR investigation. This is a WRITE operation on live SIEM data.

    SAFETY: Call this once WITHOUT confirm=True first — it will preview the
    investigation and make no changes. Only call it again with confirm=True,
    after showing the preview to the user and getting their go-ahead, to
    actually close it.

    Args:
        investigation_id: The investigation ID or RRN to close (from get_investigations).
        disposition: One of "BENIGN", "MALICIOUS", "NOT_APPLICABLE" — required
                     whenever an investigation is closed.
        confirm: Must be True to actually perform the close. Defaults to
                 False, which only previews the investigation and its
                 current state without changing anything.

    Returns:
        A preview of the investigation to be closed (if confirm=False), or
        confirmation that it was closed (if confirm=True).
    """
    if disposition not in VALID_DISPOSITIONS:
        return f"✗ Invalid disposition: '{disposition}'. Valid values are: {', '.join(VALID_DISPOSITIONS)}"

    try:
        config = load_config()
        current = idr_get_investigation(config, investigation_id)

        if current.get("status") == "CLOSED":
            return (
                f"ℹ️ Investigation is already CLOSED — no action taken.\n\n"
                f"Title: {current.get('title', '?')}\n"
                f"ID: {investigation_id}\n"
                f"Disposition: {current.get('disposition', '-')}"
            )

        if not confirm:
            assignee = current.get("assignee") or {}
            return (
                f"⚠️ This will CLOSE the following investigation with disposition '{disposition}'. "
                f"No changes have been made yet.\n\n"
                f"Title: {current.get('title', '?')}\n"
                f"ID: {investigation_id}\n"
                f"Current status: {current.get('status', '?')}\n"
                f"Priority: {current.get('priority', '?')}\n"
                f"Assignee: {assignee.get('email', 'Unassigned')}\n\n"
                f'To proceed, call close_investigation(investigation_id="{investigation_id}", '
                f'disposition="{disposition}", confirm=True)'
            )

        result = idr_close_investigation(config, investigation_id, disposition)
        return (
            f"✓ Investigation closed.\n\n"
            f"Title: {result.get('title', current.get('title', '?'))}\n"
            f"ID: {investigation_id}\n"
            f"Status: {result.get('status', 'CLOSED')}\n"
            f"Disposition: {result.get('disposition', disposition)}"
        )

    except Exception as e:
        return f"✗ Error closing investigation: {str(e)}"


def _log_query_still_pending(result: dict) -> bool:
    """True if a log search result is a still-pending 202, not a final answer.

    A pending response already carries an "events": [] key — it's not the
    presence of "events" that signals completion, only the disappearance
    of the "links" self-href (present while polling, absent once the API
    returns the final 200). See src/insightidr_log_search_manager.py's
    _poll_until_ready docstring for the live-verified bug this fixes.
    """
    return any(link.get("rel", "").lower() == "self" for link in result.get("links", []))


def _format_log_events(events: list) -> str:
    """Format LEQL query results for display, parsing each event's JSON message body."""
    if not events:
        return "(no matching events)"

    lines = []
    for ev in events:
        raw = ev.get("message")
        if isinstance(raw, str):
            try:
                body = json.dumps(json.loads(raw), indent=2, ensure_ascii=False, default=str)
            except (TypeError, ValueError):
                body = raw
        else:
            body = json.dumps(raw, indent=2, ensure_ascii=False, default=str) if raw else "(empty)"
        lines.append(f"- log_id: {ev.get('log_id', '?')}  timestamp: {ev.get('timestamp', '?')}\n{body}")

    return "\n\n".join(lines)


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Rapid7 InsightIDR Logs",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def list_logs() -> str:
    """List all logs configured for this account, with their IDs.

    Use this to find log IDs for query_logs, or to look up a log's name.

    Returns:
        Formatted list of logs (id, name).
    """
    try:
        config = load_config()
        response = idr_list_logs(config)
        logs = response.get("logs", [])

        if not logs:
            return "No logs found."

        lines = [f"Found {len(logs)} log(s):"]
        for log in logs:
            lines.append(f"- {log.get('name', '?')}  id: {log.get('id', '?')}")
        return "\n".join(lines)

    except Exception as e:
        return f"✗ Error listing logs: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Rapid7 InsightIDR Logsets",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def list_logsets() -> str:
    """List all logsets (named groupings of logs) configured for this account.

    Returns:
        Formatted list of logsets (name, id, and the logs each one contains).
    """
    try:
        config = load_config()
        response = idr_list_logsets(config)
        logsets = response.get("logsets", [])

        if not logsets:
            return "No logsets found."

        lines = [f"Found {len(logsets)} logset(s):"]
        for logset in logsets:
            log_names = ", ".join(li.get("name", "?") for li in logset.get("logs_info", []))
            lines.append(f"- {logset.get('name', '?')}  id: {logset.get('id', '?')}\n  logs: {log_names or '(none)'}")
        return "\n".join(lines)

    except Exception as e:
        return f"✗ Error listing logsets: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Query Rapid7 InsightIDR Log Data",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def query_logs(
    log_ids: str,
    statement: str,
    time_range: str = "Last 1 Day",
    from_ms: int = 0,
    to_ms: int = 0,
    max_wait_seconds: float = 30.0,
    max_events: int = 5000,
) -> str:
    """Run a LEQL query against one or more logs and return matching events.

    The underlying API returns results in chunks (observed chunk size: 50)
    behind a polling link — this tool follows every chunk automatically and
    returns them newest-first, up to max_events or until max_wait_seconds
    elapses, whichever comes first. Raise max_wait_seconds for a log with a
    high event volume if the result looks truncated.

    PRIVACY NOTE: Raw log events can contain personal data (usernames,
    emails, IP addresses, actions taken) — this is real organizational
    data, not test data. Only fetch what's needed to answer the question at
    hand, extract the relevant fields when presenting results rather than
    dumping every raw message by default, and never forward this output to
    an external system without the user's explicit instruction to do so.

    Args:
        log_ids: One or more log IDs, comma-separated (from list_logs).
        statement: A LEQL statement, e.g. "where(user)" or
                   "where(result=FAILED) groupby(source_ip)".
        time_range: A relative window, e.g. "Last 1 Day", "Today", "Last 7
                    Days". Ignored if from_ms and to_ms are both set.
        from_ms: Start of an explicit time window, Unix milliseconds.
                 Use with to_ms instead of time_range for a precise range.
        to_ms: End of an explicit time window, Unix milliseconds.
        max_wait_seconds: How long to keep following chunk links before
                           returning whatever's been collected so far.
        max_events: Stop accumulating once this many events are collected.

    Returns:
        Matching log events, newest first. If the query is still producing
        more chunks when max_wait_seconds/max_events is hit, the events
        collected so far are still returned, with a note that more may be
        available.
    """
    try:
        config = load_config()
        ids = [i.strip() for i in log_ids.split(",") if i.strip()]
        if not ids:
            return "✗ log_ids must contain at least one log ID."

        kwargs = {"time_range": time_range} if not (from_ms and to_ms) else {"from_ms": from_ms, "to_ms": to_ms}
        result = idr_query_logs(
            config, ids, statement, max_wait_seconds=max_wait_seconds, max_events=max_events, **kwargs
        )

        events = result.get("events", [])
        header = f"Found {len(events)} matching event(s)"
        if _log_query_still_pending(result):
            header += " (query still has more chunks — raise max_wait_seconds/max_events for more)"
        return f"{header}:\n\n{_format_log_events(events)}"

    except Exception as e:
        return f"✗ Error querying logs: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Rapid7 InsightIDR Saved Queries",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def list_saved_queries() -> str:
    """List all saved LEQL queries for this account.

    Returns:
        Formatted list of saved queries (name, id, logs, LEQL statement).
    """
    try:
        config = load_config()
        response = idr_list_saved_queries(config)
        queries = response.get("saved_queries", [])

        if not queries:
            return "No saved queries found."

        lines = [f"Found {len(queries)} saved quer{'y' if len(queries) == 1 else 'ies'}:"]
        for q in queries:
            leql = q.get("leql", {})
            lines.append(
                f"- {q.get('name', '?')}  id: {q.get('id', '?')}\n"
                f"  logs: {', '.join(q.get('logs', []))}\n"
                f"  statement: {leql.get('statement', '?')}  during: {leql.get('during', {})}"
            )
        return "\n".join(lines)

    except Exception as e:
        return f"✗ Error listing saved queries: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Run Rapid7 InsightIDR Saved Query",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def run_saved_query(saved_query_id: str, max_wait_seconds: float = 30.0, max_events: int = 5000) -> str:
    """Run a previously saved LEQL query and return matching events.

    Follows every result chunk automatically (see query_logs) and returns
    events newest-first, up to max_events or max_wait_seconds.

    PRIVACY NOTE: same as query_logs — raw log events can contain real
    personal data. Handle accordingly.

    Args:
        saved_query_id: The saved query's ID (from list_saved_queries).
        max_wait_seconds: How long to keep following chunk links before
                           returning whatever's been collected so far.
        max_events: Stop accumulating once this many events are collected.

    Returns:
        Matching log events, newest first, with a note if more chunks
        remain beyond max_wait_seconds/max_events.
    """
    try:
        config = load_config()
        result = idr_run_saved_query(config, saved_query_id, max_wait_seconds=max_wait_seconds, max_events=max_events)

        events = result.get("events", [])
        header = f"Found {len(events)} matching event(s)"
        if _log_query_still_pending(result):
            header += " (query still has more chunks — raise max_wait_seconds/max_events for more)"
        return f"{header}:\n\n{_format_log_events(events)}"

    except Exception as e:
        return f"✗ Error running saved query: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Create Rapid7 InsightIDR Saved Query",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=True,
    )
)
def create_saved_query(
    name: str,
    log_ids: str,
    statement: str,
    time_range: str = "Last 1 Day",
    from_ms: int = 0,
    to_ms: int = 0,
    confirm: bool = False,
) -> str:
    """Create a saved LEQL query. WRITE operation.

    SAFETY: Call this once WITHOUT confirm=True first — it previews what
    would be created and makes no changes. Only call it again with
    confirm=True, after the user approves, to actually create it.

    Args:
        name: A name for the saved query.
        log_ids: One or more log IDs, comma-separated (from list_logs).
        statement: A LEQL statement.
        time_range: A relative window, e.g. "Last 1 Day". Ignored if
                    from_ms and to_ms are both set.
        from_ms: Start of an explicit time window, Unix milliseconds.
        to_ms: End of an explicit time window, Unix milliseconds.
        confirm: Must be True to actually create the saved query.

    Returns:
        A preview (if confirm=False), or confirmation of creation.
    """
    ids = [i.strip() for i in log_ids.split(",") if i.strip()]
    if not ids:
        return "✗ log_ids must contain at least one log ID."

    if not confirm:
        return (
            f"⚠️ This will CREATE a saved query. No changes have been made yet.\n\n"
            f"Name: {name}\n"
            f"Logs: {', '.join(ids)}\n"
            f"Statement: {statement}\n"
            f"Time range: {time_range if not (from_ms and to_ms) else f'{from_ms} to {to_ms} (ms)'}\n\n"
            f'To proceed, call create_saved_query(name="{name}", log_ids="{log_ids}", '
            f'statement="{statement}", confirm=True)'
        )

    try:
        config = load_config()
        kwargs = {"time_range": time_range} if not (from_ms and to_ms) else {"from_ms": from_ms, "to_ms": to_ms}
        result = idr_create_saved_query(config, name, ids, statement, **kwargs)
        saved = result.get("saved_query", result)
        return f"✓ Saved query created.\n\nName: {saved.get('name', name)}\nID: {saved.get('id', '?')}"

    except Exception as e:
        return f"✗ Error creating saved query: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Delete Rapid7 InsightIDR Saved Query",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def delete_saved_query(saved_query_id: str, confirm: bool = False) -> str:
    """Delete a saved query. WRITE (destructive) operation.

    SAFETY: Call this once WITHOUT confirm=True first — it previews the
    saved query (if found) and makes no changes. Only call it again with
    confirm=True, after the user approves, to actually delete it.

    Args:
        saved_query_id: The saved query's ID (from list_saved_queries).
        confirm: Must be True to actually delete it.

    Returns:
        A preview (if confirm=False), or confirmation of deletion.
    """
    try:
        config = load_config()

        if not confirm:
            existing = idr_list_saved_queries(config).get("saved_queries", [])
            match = next((q for q in existing if q.get("id") == saved_query_id), None)
            if match:
                leql = match.get("leql", {})
                preview = f"Name: {match.get('name', '?')}\nStatement: {leql.get('statement', '?')}"
            else:
                preview = "(Not found in the current saved queries list — double-check the ID before deleting.)"
            return (
                f"⚠️ This will DELETE the following saved query. No changes have been made yet.\n\n"
                f"ID: {saved_query_id}\n{preview}\n\n"
                f'To proceed, call delete_saved_query(saved_query_id="{saved_query_id}", confirm=True)'
            )

        idr_delete_saved_query(config, saved_query_id)
        return f"✓ Saved query deleted.\n\nID: {saved_query_id}"

    except Exception as e:
        return f"✗ Error deleting saved query: {str(e)}"


def main():
    """Entry point for the MCP server command."""
    # Handle help flag
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h"]:
        print("Usage: rapid7-mcp-server [database_path]")
        print()
        print("Start the MCP server for Rapid7 vulnerability data.")
        print()
        print("Arguments:")
        print("  database_path    Path to the DuckDB database file (optional, overrides DATA_DIR default)")
        print()
        print("Environment Variables:")
        print("  RAPID7_API_KEY    Your Rapid7 InsightVM API key (required)")
        print("  RAPID7_REGION     Your Rapid7 region: us, eu, ca, au, or ap (required)")
        print("  DATA_DIR          Directory for database files (default: ~/.rapid7_mcp)")
        print("  MCP_TRANSPORT     Transport protocol: 'stdio' (default) or 'http'")
        print("  MCP_HOST          HTTP bind address (default: 0.0.0.0)")
        print("  MCP_PORT          HTTP port (default: 8000)")
        print()
        print("Example:")
        print("  rapid7-mcp-server /path/to/rapid7_bulk_export.db")
        print()
        print("The server communicates via stdio by default, or streamable HTTP")
        print("when MCP_TRANSPORT=http (for Docker / remote deployments).")
        print()
        print("See README.md for configuration details.")
        sys.exit(0)

    # Ensure data directory exists
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Get database path from args or use default
    db_path = sys.argv[1] if len(sys.argv) > 1 else str(_DATA_DIR / "rapid7_bulk_export.db")

    # Initialize database
    try:
        initialize_database(db_path)
        print(f"Initialized database from: {db_path}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Could not initialize database: {e}", file=sys.stderr)
        print("Database will be created when data is loaded.", file=sys.stderr)

    # Determine transport mode from environment
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport == "http":
        host = os.environ.get("MCP_HOST", "0.0.0.0")  # nosec B104 - intentional for Docker
        port = int(os.environ.get("MCP_PORT", "8000"))
        print(f"Starting HTTP transport on {host}:{port}", file=sys.stderr)
        mcp.run(transport="http", host=host, port=port, show_banner=False)
    else:
        mcp.run(show_banner=False)


if __name__ == "__main__":
    main()

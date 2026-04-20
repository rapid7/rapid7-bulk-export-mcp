#!/usr/bin/env python3
"""
FastMCP Server for Rapid7 Vulnerability Data

This server exposes vulnerability data through the Model Context Protocol,
allowing AI assistants to query and analyze the data.
"""

import json
import sys
import tempfile
from pathlib import Path
from typing import Optional

from fastmcp import FastMCP
from mcp.types import ToolAnnotations

from .duckdb_loader import VulnerabilityDatabase

# Initialize FastMCP server
mcp = FastMCP("rapid7-vulnerability-server")

# Global database instance
db: Optional[VulnerabilityDatabase] = None

VALID_EXPORT_TYPES = ("vulnerability", "policy", "remediation")


def initialize_database(db_path: str = "rapid7_bulk_export.db") -> VulnerabilityDatabase:
    """Initialize the vulnerability database."""
    global db
    if db is None:
        db = VulnerabilityDatabase(db_path)
    return db


@mcp.tool(
    annotations=ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def load_from_parquet(parquet_path: str) -> str:
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
        import glob
        from pathlib import Path

        # Check if path exists
        path = Path(parquet_path)
        if not path.exists():
            return f"✗ Error: Path does not exist: {parquet_path}"

        # Get list of parquet files
        if path.is_file():
            parquet_files = [str(path)]
        else:
            parquet_files = glob.glob(str(path / "*.parquet"))

        if not parquet_files:
            return f"✗ Error: No Parquet files found at: {parquet_path}"

        # Initialize database if needed
        if db is None:
            initialize_database()

        # Load into database
        row_count = db.load_parquet_files(parquet_files)

        # Get statistics
        stats = db.get_stats()

        return (
            f"✓ Successfully loaded {row_count} vulnerabilities from {len(parquet_files)} file(s).\n\n"
            f"Statistics:\n{json.dumps(stats, indent=2, default=str)}\n\n"
            f"You can now query the data using query, get_schema, or get_stats tools."
        )

    except Exception as e:
        return f"✗ Error loading Parquet files: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def start_export(export_type: str = "vulnerability", start_date: str = "", end_date: str = "") -> str:
    """Start a new Rapid7 export job (non-blocking).

    This is a fast, non-blocking call that creates an export job on the
    Rapid7 platform and returns the export ID immediately. The export
    will process in the background on Rapid7's servers (typically 3-5
    minutes).

    Use check_export_status(export_id) to monitor progress, then
    download_and_load_export(export_id, export_type="...") once it completes.

    If an export from today already exists, returns that export's ID
    instead of creating a duplicate.

    Args:
        export_type: Type of export to create. One of "vulnerability",
                     "policy", or "remediation".
        start_date: Start date in YYYY-MM-DD format (only for remediation exports).
        end_date: End date in YYYY-MM-DD format (only for remediation exports).

    Returns:
        The export ID and next steps.
    """
    if export_type not in VALID_EXPORT_TYPES:
        return f"✗ Invalid export_type: '{export_type}'. Valid values are: {', '.join(VALID_EXPORT_TYPES)}"

    try:
        from .config import load_config
        from .export_tracker import ExportTracker

        config = load_config()

        # Check if we already have a completed export from today
        tracker = ExportTracker()
        today_export = tracker.get_today_export(export_type=export_type)
        tracker.close()

        if today_export:
            eid = today_export["export_id"]
            return (
                f"♻️ A {export_type} export from today already exists.\n\n"
                f"Export ID: {eid}\n"
                f"Status: COMPLETE\n"
                f"Created: {today_export['created_at']}\n"
                f"Rows: {today_export['row_count']}\n\n"
                f"Load it with: "
                f"download_and_load_export("
                f'export_id="{eid}", '
                f'export_type="{export_type}")'
            )

        # Create the export based on type
        if export_type == "vulnerability":
            from .export_manager import create_vulnerability_export

            print("Creating new vulnerability export...", file=sys.stderr)
            new_id = create_vulnerability_export(config)

        elif export_type == "policy":
            from .export_manager import create_policy_export

            print("Creating new policy export...", file=sys.stderr)
            new_id = create_policy_export(config)

        elif export_type == "remediation":
            import datetime as _dt

            if not start_date:
                start_date = (_dt.date.today() - _dt.timedelta(days=30)).isoformat()
            if not end_date:
                end_date = _dt.date.today().isoformat()

            # Validate dates
            import re as _re

            date_pattern = r"^\d{4}-\d{2}-\d{2}$"
            if not _re.match(date_pattern, start_date):
                return f"✗ Invalid start_date format: '{start_date}'. Expected YYYY-MM-DD (e.g. '2024-01-01')."
            if not _re.match(date_pattern, end_date):
                return f"✗ Invalid end_date format: '{end_date}'. Expected YYYY-MM-DD (e.g. '2024-01-31')."

            from .export_manager import create_remediation_export

            print("Creating new remediation export...", file=sys.stderr)
            new_id = create_remediation_export(config, start_date, end_date)

        print(f"Created {export_type} export with ID: {new_id}", file=sys.stderr)

        # Track the export immediately so it can be recovered if the session is lost
        tracker = ExportTracker()
        tracker.save_export(
            export_id=new_id,
            status="PENDING",
            parquet_urls=[],
            export_type=export_type,
        )
        tracker.close()

        return (
            f"✓ {export_type.capitalize()} export job created.\n\n"
            f"Export ID: {new_id}\n"
            f"Status: PENDING\n\n"
            f"The export is now processing on Rapid7's servers "
            f"(typically 3-5 minutes).\n"
            f"Check progress: "
            f"check_export_status("
            f'export_id="{new_id}")\n'
            f"Once COMPLETE, load with: "
            f"download_and_load_export("
            f'export_id="{new_id}", '
            f'export_type="{export_type}")'
        )

    except Exception as e:
        return f"✗ Error starting {export_type} export: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def start_remediation_export(period: str = "3m", start_date: str = "", end_date: str = "") -> str:
    """Start one or more remediation exports covering up to 6 months.

    The Rapid7 API limits each remediation export to 31 days. This tool
    automatically splits longer periods into 31-day chunks and kicks off
    an export for each chunk (all non-blocking).

    Specify either a period shorthand OR explicit start/end dates:
      - period="1m"  → last 1 month
      - period="3m"  → last 3 months (default)
      - period="6m"  → last 6 months
      - start_date / end_date → custom range (YYYY-MM-DD), ignores period

    After all exports complete, load each one with download_and_load_export().
    Results accumulate in the same vulnerability_remediation table.

    Args:
        period: Shorthand for how far back to look. One of "1m", "3m", "6m".
                Ignored when start_date and end_date are both provided.
        start_date: Optional start date (YYYY-MM-DD). Overrides period.
        end_date: Optional end date (YYYY-MM-DD). Defaults to today.

    Returns:
        List of export IDs with instructions for polling and loading.
    """
    import datetime as _dt
    import re as _re

    try:
        from .config import load_config
        from .export_manager import build_remediation_date_chunks, create_remediation_export
        from .export_tracker import ExportTracker

        config = load_config()

        # Resolve date range
        today = _dt.date.today()

        if start_date and end_date:
            # Custom range — validate formats
            date_pattern = r"^\d{4}-\d{2}-\d{2}$"
            if not _re.match(date_pattern, start_date):
                return f"✗ Invalid start_date format: '{start_date}'. Expected YYYY-MM-DD."
            if not _re.match(date_pattern, end_date):
                return f"✗ Invalid end_date format: '{end_date}'. Expected YYYY-MM-DD."
        else:
            # Period-based
            period_map = {
                "1m": 30,
                "3m": 90,
                "6m": 180,
            }
            if period not in period_map:
                return f"✗ Invalid period: '{period}'. Valid values: {', '.join(period_map.keys())}"

            end_date = today.isoformat()
            start_date = (today - _dt.timedelta(days=period_map[period])).isoformat()

        # Build 31-day chunks
        chunks = build_remediation_date_chunks(start_date, end_date)

        # Kick off an export for each chunk
        export_ids = []
        tracker = ExportTracker()
        for chunk_start, chunk_end in chunks:
            print(f"Creating remediation export: {chunk_start} → {chunk_end}", file=sys.stderr)
            eid = create_remediation_export(config, chunk_start, chunk_end)
            export_ids.append({"id": eid, "start": chunk_start, "end": chunk_end})

            tracker.save_export(
                export_id=eid,
                status="PENDING",
                parquet_urls=[],
                export_type="remediation",
            )
        tracker.close()

        # Build response
        lines = [
            f"✓ Created {len(export_ids)} remediation export(s) covering {start_date} → {end_date}.\n",
        ]
        for i, info in enumerate(export_ids, 1):
            lines.append(f"  {i}. {info['start']} → {info['end']}  Export ID: {info['id']}")

        lines.append("")
        lines.append("Each export takes ~3-5 minutes to process.")
        lines.append('Check progress with: check_export_status(export_id="...")')
        lines.append(
            'Once COMPLETE, load each with: download_and_load_export(export_id="...", export_type="remediation")'
        )
        lines.append("All chunks load into the same vulnerability_remediation table.")

        return "\n".join(lines)

    except Exception as e:
        return f"✗ Error starting remediation exports: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def check_export_status(export_id: str) -> str:
    """Check the current status of a Rapid7 export job.

    This is a fast, non-blocking call that queries the Rapid7 API once
    and returns the current status. Does NOT poll or wait.

    Args:
        export_id: The export ID returned by start_export.

    Returns:
        Current export status and next steps.
    """
    try:
        from .config import load_config
        from .export_manager import get_export_status

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
                f"download_and_load_export("
                f'export_id="{export_id}", '
                f'export_type="...")'
            )
        elif current_status == "FAILED":
            return (
                f"✗ Export failed.\n\nExport ID: {export_id}\nStatus: FAILED\n\nStart a new export with: start_export()"
            )
        else:
            return (
                f"⏳ Export still processing.\n\n"
                f"Export ID: {export_id}\n"
                f"Status: {current_status}\n\n"
                f"Check again in 30-60 seconds with: "
                f"check_export_status("
                f'export_id="{export_id}")'
            )

    except Exception as e:
        return f"✗ Error checking export status: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def download_and_load_export(export_id: str, export_type: str = "vulnerability") -> str:
    """Download a completed Rapid7 export and load into the database.

    Call this after check_export_status confirms the export is COMPLETE.
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
        import shutil

        from .config import load_config
        from .download import download_all_files
        from .export_manager import get_export_status
        from .export_tracker import ExportTracker

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
                f"check_export_status("
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

        try:
            if export_type == "vulnerability":
                # Schema-based detection approach
                temp_parquet_paths = []
                for i, data in enumerate(file_data):
                    temp_path = Path(temp_dir) / f"export_{i}.parquet"
                    temp_path.write_bytes(data)
                    temp_parquet_paths.append(str(temp_path))

                row_count = db.load_parquet_files(temp_parquet_paths)
                row_info = f"Rows loaded: {row_count}"

            else:
                # Prefix-based loading for policy and remediation
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

                if export_type == "policy":
                    row_counts = db.load_parquet_files_by_prefix(prefix_file_map, skip_prefixes={"asset"})
                else:
                    # remediation
                    row_counts = db.load_parquet_files_by_prefix(prefix_file_map)

                row_count = sum(row_counts.values())
                row_info = f"Rows loaded: {row_count}\nPer-table row counts: {json.dumps(row_counts, default=str)}"

            # Save export metadata
            tracker = ExportTracker()
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

        return (
            f"✓ {export_type.capitalize()} data loaded successfully.\n\n"
            f"Export ID: {export_id}\n"
            f"Files processed: {len(parquet_urls)}\n"
            f"{row_info}\n\n"
            f"Statistics:\n"
            f"{json.dumps(stats, indent=2, default=str)}\n\n"
            f"Query the data with query, "
            f"get_schema, or get_stats."
        )

    except Exception as e:
        return (
            f"✗ Error downloading/loading {export_type}: {str(e)}\n\n"
            f"Export ID: {export_id}\n"
            f"Retry with: download_and_load_export("
            f'export_id="{export_id}", '
            f'export_type="{export_type}")'
        )


@mcp.tool(
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def query(sql: str) -> str:
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

    if db is None:
        return "Error: Database not initialized. Please run start_export and download_and_load_export first."

    try:
        results = db.query(sql)
        result_text = json.dumps(results, indent=2, default=str)
        return f"Query executed successfully. {len(results)} rows returned.\n\n{result_text}"
    except Exception as e:
        return f"Error executing query: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def get_schema() -> str:
    """Get the schema of all database tables.

    Returns column names and data types for all existing tables:
    assets, vulnerabilities, policies, and vulnerability_remediation.
    Tables that have not been loaded yet are omitted.

    Use this to understand what data is available before writing queries.

    Returns:
        Table schemas as formatted JSON, keyed by table name
    """
    global db

    if db is None:
        return "Error: Database not initialized. Please run start_export and download_and_load_export first."

    try:
        schema = db.get_schema()
        schema_text = json.dumps(schema, indent=2)
        return f"Database schema:\n\n{schema_text}"
    except Exception as e:
        return f"Error getting schema: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def get_stats() -> str:
    """Get summary statistics for all database tables.

    Returns row counts and relevant distributions for all existing tables:
    assets, vulnerabilities, policies, and vulnerability_remediation.
    Tables that have not been loaded yet are omitted.

    Useful for getting an overview of the data across all loaded datasets.

    Returns:
        Summary statistics as formatted JSON, keyed by table name
    """
    global db

    if db is None:
        return "Error: Database not initialized. Please run start_export and download_and_load_export first."

    try:
        stats = db.get_stats()
        stats_text = json.dumps(stats, indent=2, default=str)
        return f"Database statistics:\n\n{stats_text}"
    except Exception as e:
        return f"Error getting statistics: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def list_exports(limit: int = 10) -> str:
    """List recent Rapid7 exports tracked in the system.

    Shows export metadata including export ID, date, status, type, and row counts.
    Useful for understanding what exports are available for reuse.

    Args:
        limit: Maximum number of exports to return (default: 10)

    Returns:
        Formatted list of recent exports
    """
    try:
        from .export_tracker import ExportTracker

        tracker = ExportTracker()
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


def suggest_query(task: str = "") -> str:
    """Get SQL query suggestions for common vulnerability analysis tasks.

    Provides example queries for common use cases like finding critical vulnerabilities,
    analyzing trends, identifying affected assets, etc. All queries use the actual
    Rapid7 Bulk Export API Parquet schema fields.

    Args:
        task: Description of what you want to analyze (optional)

    Returns:
        SQL query suggestions
    """
    suggestions = """
Common SQL Query Patterns for Vulnerability Analysis:

1. Find Critical Vulnerabilities with High Exploitation Risk:
   SELECT assetId, hostName, vulnId, title, cvssV3Score, epssscore, hasExploits
   FROM vulnerabilities
   WHERE severity = 'Critical' AND epssscore > 0.5
   ORDER BY epssscore DESC, cvssV3Score DESC
   LIMIT 20;

2. Severity Distribution:
   SELECT severity, COUNT(*) as count, AVG(cvssV3Score) as avg_cvss
   FROM vulnerabilities
   GROUP BY severity
   ORDER BY count DESC;

3. High CVSS Score Vulnerabilities with Exploits:
   SELECT vulnId, title, cvssV3Score, cvssV3Severity, hasExploits, epssscore
   FROM vulnerabilities
   WHERE cvssV3Score >= 9.0
   ORDER BY cvssV3Score DESC;

4. Recently Discovered Vulnerabilities:
   SELECT assetId, hostName, vulnId, title, severity, firstFoundTimestamp
   FROM vulnerabilities
   ORDER BY firstFoundTimestamp DESC
   LIMIT 20;

5. Vulnerabilities by Asset:
   SELECT assetId, hostName, ip, osDescription,
          COUNT(*) as vuln_count,
          SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) as critical_count
   FROM vulnerabilities
   GROUP BY assetId, hostName, ip, osDescription
   ORDER BY critical_count DESC, vuln_count DESC
   LIMIT 10;

6. Search by CVE:
   SELECT assetId, hostName, vulnId, title, cvssV3Score, cves
   FROM vulnerabilities
   WHERE EXISTS (SELECT 1 FROM unnest(cves) AS cve WHERE cve LIKE '%CVE-2024%');

7. Cloud Asset Vulnerabilities:
   SELECT
     CASE
       WHEN awsInstanceId IS NOT NULL THEN 'AWS'
       WHEN azureResourceId IS NOT NULL THEN 'Azure'
       WHEN gcpObjectId IS NOT NULL THEN 'GCP'
       ELSE 'On-Premise'
     END as cloud_provider,
     COUNT(*) as vuln_count,
     COUNT(DISTINCT assetId) as asset_count
   FROM vulnerabilities
   GROUP BY cloud_provider;

8. EPSS-Based Prioritization:
   SELECT vulnId, title, cvssV3Score, epssscore, epsspercentile,
          COUNT(DISTINCT assetId) as affected_assets
   FROM vulnerabilities
   WHERE epssscore > 0.1
   GROUP BY vulnId, title, cvssV3Score, epssscore, epsspercentile
   HAVING COUNT(DISTINCT assetId) > 5
   ORDER BY epssscore DESC;

9. Reintroduced Vulnerabilities:
   SELECT assetId, hostName, vulnId, title,
          firstFoundTimestamp, reintroducedTimestamp
   FROM vulnerabilities
   WHERE reintroducedTimestamp IS NOT NULL
   ORDER BY reintroducedTimestamp DESC;

10. PCI Compliance Status:
    SELECT pciSeverity, pciCompliant, COUNT(*) as count
    FROM vulnerabilities
    WHERE pciSeverity IS NOT NULL
    GROUP BY pciSeverity, pciCompliant
    ORDER BY pciSeverity DESC;
"""

    if task:
        suggestions = f"Query suggestions for: {task}\n\n" + suggestions

    return suggestions


def main():
    """Entry point for the MCP server command."""
    # Handle help flag
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h"]:
        print("Usage: rapid7-mcp-server [database_path]")
        print()
        print("Start the MCP server for Rapid7 vulnerability data.")
        print()
        print("Arguments:")
        print("  database_path    Path to the DuckDB database file (optional, default: rapid7_bulk_export.db)")
        print()
        print("Environment Variables:")
        print("  RAPID7_API_KEY   Your Rapid7 InsightVM API key (required)")
        print("  RAPID7_REGION    Your Rapid7 region: us, eu, ca, au, or ap (required)")
        print()
        print("Example:")
        print("  rapid7-mcp-server /path/to/rapid7_bulk_export.db")
        print()
        print("The server communicates via stdio using the Model Context Protocol.")
        print("It should be configured in your MCP client (e.g., Kiro, Claude Desktop).")
        print()
        print("See README.md for configuration details.")
        sys.exit(0)

    # Get database path from args or use default
    db_path = sys.argv[1] if len(sys.argv) > 1 else "rapid7_bulk_export.db"

    # Initialize database
    try:
        initialize_database(db_path)
        print(f"Initialized database from: {db_path}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Could not initialize database: {e}", file=sys.stderr)
        print("Database will be created when data is loaded.", file=sys.stderr)

    # Run the FastMCP server
    mcp.run()


if __name__ == "__main__":
    main()

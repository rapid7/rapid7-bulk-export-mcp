---
name: "rapid7-bulk-export"
displayName: "Rapid7 Bulk Export"
description: "Export and analyze Rapid7 InsightVM vulnerability, asset, policy, and remediation data via SQL queries powered by DuckDB"
keywords: ["rapid7", "insightvm", "vulnerability", "security", "bulk-export", "cve", "epss", "compliance", "remediation"]
author: "Rapid7"
---

# Rapid7 Bulk Export Power

## Overview

This power connects to the Rapid7 Bulk Export API to fetch vulnerability, asset, policy, and remediation data from InsightVM, loads it into a local DuckDB database, and exposes SQL query tools for AI-powered security analysis.

Key capabilities:

- **On-demand data export**: Fetch vulnerability, policy, and remediation data from Rapid7 InsightVM
- **Export reuse**: Automatically reuses same-day exports to avoid redundant API calls
- **SQL querying**: Run complex analytical queries against local DuckDB
- **Schema exploration**: Discover available tables and columns
- **Statistics**: Get instant summaries and distributions across all loaded data

## Available Steering Files

- **analysis-workflows** — Common analysis patterns, SQL query examples, and data loading workflows

## Available MCP Servers

### rapid7-bulk-export

Package: `rapid7-vulnerability-export` (pip/uv installable)
Connection: Local stdio MCP server

**Tools:**

- `start_rapid7_export` — Start a new export job (non-blocking, returns instantly)
  - `export_type` (string, optional): One of `"vulnerability"`, `"policy"`, or `"remediation"` (default: `"vulnerability"`)
  - `start_date` (string, optional): Start date in YYYY-MM-DD format (remediation only, defaults to 30 days ago)
  - `end_date` (string, optional): End date in YYYY-MM-DD format (remediation only, defaults to today)
  - Returns: Export ID and next steps

- `check_rapid7_export_status` — Check current status of an export job (non-blocking)
  - `export_id` (string, required): The export ID from start_rapid7_export
  - Returns: Current status (PENDING, PROCESSING, COMPLETE, FAILED)

- `download_rapid7_export` — Download a completed export and load into DuckDB
  - `export_id` (string, required): The export ID of a completed export
  - `export_type` (string, optional): One of `"vulnerability"`, `"policy"`, or `"remediation"` (default: `"vulnerability"`)
  - Returns: Summary of loaded data with row counts and statistics

- `load_rapid7_parquet` — Load existing local Parquet files (skips export)
  - `parquet_path` (string, required): Path to Parquet file or directory
  - Returns: Summary of loaded data

- `query_rapid7` — Execute SQL queries against the loaded data
  - `sql` (string, required): SQL query to execute (DuckDB syntax)
  - Returns: Query results as JSON

- `get_rapid7_schema` — Get table schemas (column names and types)
  - No parameters
  - Returns: Schema for all loaded tables

- `get_rapid7_stats` — Get summary statistics for all loaded tables
  - No parameters
  - Returns: Row counts, distributions, and aggregates

- `list_rapid7_exports` — List recent exports tracked in the system
  - `limit` (integer, optional): Max exports to return (default: 10)
  - Returns: Export metadata including IDs, dates, status, and row counts

## Onboarding

### Step 1: Verify prerequisites

Before using this power, ensure:
- Python >= 3.10 is installed
- The `rapid7-mcp-server` command is available (installed via `pip install rapid7-vulnerability-export` or `uv pip install rapid7-vulnerability-export`)

Verify with:
```bash
rapid7-mcp-server --help
```

### Step 2: Configure environment

The MCP server requires two environment variables:
- `RAPID7_API_KEY` — Your Rapid7 InsightVM API key (generate at Administration → API Key Management in the Rapid7 Insight Platform)
- `RAPID7_REGION` — Your data storage region: `us`, `us2`, `us3`, `eu`, `ca`, `au`, or `ap`

These are configured in the `mcp.json` for this power. Users must set the actual values during installation.

### Step 3: Load data

After the MCP server is connected, load data with this workflow:

1. Call `list_rapid7_exports` to check if data from today already exists
2. If no recent data, call `start_rapid7_export` for each needed type:
   - `start_rapid7_export(export_type="vulnerability")`
   - `start_rapid7_export(export_type="policy")`
   - `start_rapid7_export(export_type="remediation")`
3. Wait ~30 seconds, then check each with `check_rapid7_export_status(export_id="...")`
4. Once COMPLETE, load with `download_rapid7_export(export_id="...", export_type="...")`
5. Query with `query_rapid7(sql="SELECT ...")`

## Database Tables

Once data is loaded, four tables are available:

### `assets`
Asset inventory — hostnames, IPs, OS info, cloud identifiers, risk scores.

Key columns: `assetId`, `hostName`, `ip`, `mac`, `osFamily`, `osProduct`, `osVersion`, `riskScore` (legacy, deprecated Jan 2026), `riskScoreV2_0` (Active Risk, scale: 1–1000), `awsInstanceId`, `azureResourceId`, `gcpObjectId`, `sites`, `assetGroups`, `tags`

### `vulnerabilities`
Vulnerability instances on assets — CVSS scores, EPSS, exploits, severity.

Key columns: `assetId`, `vulnId`, `title`, `severity`, `cvssV3Score`, `cvssV3Severity`, `hasExploits`, `epssscore`, `epsspercentile`, `riskScoreV2_0` (Active Risk, scale: 1–1000), `cves`, `firstFoundTimestamp`, `reintroducedTimestamp`

### `policies`
Policy compliance results (agent and scan based).

Key columns: `assetId`, `benchmarkTitle`, `profileTitle`, `ruleTitle`, `finalStatus`, `source` (`'agent'` or `'scan'`), `fixTexts`, `rationales`, `lastAssessmentTimestamp`

### `vulnerability_remediation`
Vulnerability lifecycle tracking — first found, last detected, last removed.

Key columns: `assetId`, `cveId`, `vulnId`, `title`, `cvssV3Score`, `epssscore`, `firstFoundTimestamp`, `lastDetected`, `lastRemoved`, `reintroducedTimestamp`

## Best Practices

- **Check before exporting**: Always call `list_rapid7_exports` first — same-day exports are reused automatically
- **Use EPSS for prioritization**: `epssscore > 0.5` means >50% probability of exploitation in 30 days
- **Combine severity with exploitability**: Filter on `severity = 'Critical' AND hasExploits = true` for highest-priority items
- **Join tables for context**: Join `vulnerabilities` with `assets` on `assetId` for full asset details
- **Remediation date ranges**: The API limits remediation exports to 31 days; the tool auto-chunks larger ranges
- **DuckDB SQL**: Supports window functions, CTEs, `unnest()` for array columns, `DATEDIFF()`, and `DATE_TRUNC()`

## Troubleshooting

### "Database not initialized"
Data hasn't been loaded yet. Run the export workflow (start → check → download) or load existing Parquet files.

### Export stuck in PENDING/PROCESSING
Exports typically take 3-5 minutes. Check status again after 30-60 seconds. If stuck for >10 minutes, start a new export.

### "RAPID7_API_KEY environment variable is not set"
The MCP server requires `RAPID7_API_KEY` and `RAPID7_REGION` environment variables. Verify they're set in your MCP configuration.

### "Invalid region"
Valid regions: `us`, `us2`, `us3`, `eu`, `ca`, `au`, `ap`. Check your region in the Rapid7 Insight Platform under your account name.

### "Export already in-progress"
Another export of the same type is running. The tool returns the existing export ID — use that to check status and download when complete.

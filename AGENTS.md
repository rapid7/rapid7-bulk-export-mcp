# AGENTS.md

## Project

MCP server that exports Rapid7 data via the Bulk Export API and loads it into DuckDB for SQL analysis. Exposes MCP tools consumed by AI assistants (Claude Desktop, Kiro, etc.).

## Dev Setup

```bash
# Install dependencies
uv sync

# Required env vars
export RAPID7_API_KEY=your-key
export RAPID7_REGION=us   # us, us2, us3, eu, ca, au, ap

# Run the server (stdio)
uv run rapid7-mcp-server

# Or via venv entry point directly (required for Claude Desktop)
.venv/bin/rapid7-mcp-server
```

## Commands

```bash
make test        # run the test suite
make lint        # ruff check + format check
make lint-fix    # auto-fix lint and format issues
make security    # bandit security scan
make help        # list all targets
```

**Version management**
```bash
make version                  # print current version
make check-version            # verify manifest.json, pyproject.toml, plugin.json, SKILL.md are in sync
make bump-version V=0.5.0     # bump all version files (manifest.json, pyproject.toml, plugin.json, SKILL.md) atomically + uv lock
```

**Packaging and release**
```bash
make package          # build .mcpb bundle + skill zip (requires mcpb: npm install -g @anthropic-ai/mcpb)
make package-mcpb     # .mcpb bundle only
make package-skill    # skill zip only
make release          # check-version + package + create GitHub release (requires gh CLI + GH_TOKEN)
make clean            # remove build artifacts
```

## Architecture

| Module | Responsibility |
|---|---|
| `src/mcp_server.py` | FastMCP tool definitions, request routing, startup |
| `src/export_manager.py` | GraphQL mutations to create exports, status polling |
| `src/duckdb_loader.py` | Load Parquet files into DuckDB, prefix → table routing |
| `src/export_tracker.py` | Separate DuckDB tracking DB — avoids redundant daily exports |
| `src/graphql_client.py` | Authenticated GraphQL HTTP client |
| `src/download.py` | Download Parquet files from signed URLs |
| `src/config.py` | Load and validate config from environment variables |

### Database tables

| Table | Populated by |
|---|---|
| `assets` | `asset` prefix from vulnerability export |
| `vulnerabilities` | `asset_vulnerability` prefix |
| `policies` | `asset_policy` (agent) + `asset_scan_policy` (scan) prefixes |
| `vulnerability_remediation` | `vulnerability_remediation` prefix |
| `asset_software` | `asset_software` prefix from asset_software export |

### Export lifecycle

`start_rapid7_export` → `check_rapid7_export_status` → `download_rapid7_export`

Each step is non-blocking. The tracking DB (`rapid7_bulk_export_tracking.db`) records completed exports to avoid redundant API calls on the same day.

## Key conventions

**Data directory**
- Default: `~/.rapid7_mcp/` (underscore, not hyphen)
- Override: `DATA_DIR` env var
- Both database files land here: `rapid7_bulk_export.db` and `rapid7_bulk_export_tracking.db`
- The directory is created at startup — never assume it exists

**Claude Desktop config**
- Point `command` at `.venv/bin/rapid7-mcp-server` directly — do NOT use `uv run` with a `cwd`
- `uv run` with `cwd` resolves to a cached/system package instead of local source
- Example:
  ```json
  {
    "command": "/absolute/path/to/rapid7-bulk-export-mcp/.venv/bin/rapid7-mcp-server",
    "args": [],
    "env": { "RAPID7_API_KEY": "...", "RAPID7_REGION": "us" }
  }
  ```

**Snapshot vs append loads**
- `load_parquet_files_by_prefix` defaults to drop-and-recreate (correct for full snapshots)
- Pass `append=True` for remediation — multiple 31-day chunks accumulate into one table
- `download_rapid7_export` handles this automatically based on `export_type`

**Remediation exports**
- The platform allows only ONE export per type in flight at a time. Remediation
  is date-range scoped, so concurrent windows cannot be created — they must be
  produced strictly one at a time.
- API limit: 31 days per request. A larger range is split into ≤31-day windows.
- `start_rapid7_export(export_type="remediation", start_date, end_date)` hands the
  whole range to a single background job that, per window, sequentially:
  create → poll to COMPLETE → download → load (`append=True`). It returns a
  `job_id` immediately; poll it with `check_rapid7_export_status(export_id=job_id)`.
- If the platform reports another remediation export already in flight, the job
  waits and recreates its OWN window rather than adopting the foreign export id
  (that id may cover a different date range).
- Today-reuse logic is skipped for remediation (date-range scoped, not a snapshot).
- All windows append into a single `vulnerability_remediation` table.
- Partial failure is explicit: if a window fails, the loaded windows are kept and
  the job message names exactly which windows loaded and which are missing, so the
  missing range can be re-run.

**best_solution fields**
- `bestSolutionType`, `bestSolutionSummary`, `bestSolutionFix` appear in the `vulnerabilities` table if the org is enabled
- No code change needed — they come through the existing `asset_vulnerability` prefix automatically
- `bestSolutionFix` may contain HTML markup — strip before displaying as plain text

**Export types**
- `vulnerability` — full snapshot, replaces on load
- `policy` — full snapshot, replaces on load (skips `asset` prefix to avoid duplicate asset data)
- `remediation` — date-range scoped, appends on load, 31-day max per chunk
- `asset_software` — full snapshot, replaces on load

## Testing

Table-driven tests throughout. New export types need tests in `tests/test_export_manager.py` covering: happy path, correct mutation/variables sent, in-progress error handling, auth error.

The in-progress error regex `[A-Za-z0-9+/=]+` matches base64-style IDs — use that format in test fixtures, not hyphenated strings.

## Reference

- [Rapid7 Bulk Export API docs](https://docs.rapid7.com/insightvm/bulk-export-api/) — authoritative source for GraphQL mutations, Parquet schema, field definitions, and export types. Consult this when researching schema fields or adding new export types.
- [Managing Platform API Keys](https://docs.rapid7.com/insight/managing-platform-api-keys)
- [Product APIs and Regions](https://docs.rapid7.com/insight/product-apis)

## MCP tool names

The actual tool names exposed by the server (use these exactly in tool calls):

| Tool | Purpose |
|---|---|
| `start_rapid7_export` | Create an export job — returns an export ID (or a job ID for a multi-window remediation range) immediately |
| `check_rapid7_export_status` | Poll status once (non-blocking): platform export status, local download/load phase, or multi-window job progress — accepts an export ID or a job ID |
| `download_rapid7_export` | Start downloading a completed export and loading it into DuckDB in the background |
| `load_rapid7_parquet` | Load existing Parquet files from `$DATA_DIR/imports/` |
| `query_rapid7` | Execute SQL against loaded tables |
| `get_rapid7_schema` | Return column names and types for all loaded tables |
| `get_rapid7_stats` | Return summary statistics for all loaded tables |
| `list_rapid7_exports` | List recent export history |
| `purge_rapid7_data` | Permanently delete both database files from disk |

Tables available for SQL: `assets`, `vulnerabilities`, `policies`, `vulnerability_remediation`, `asset_software`

## Security

**API key handling — CRITICAL**
- `RAPID7_API_KEY` grants access to all vulnerability data across the entire Rapid7 org — treat it as a high-value secret
- **Never include the API key in prompts, queries, tool arguments, or any text passed to a model**
- The key is loaded from the environment at server startup and never needs to be referenced again
- If a key appears in conversation context, stop and flag it to the user — do not log, echo, or forward it
- For local development, inject via environment variable only — never hardcode in config files or commit to git
- For production, use a secrets manager (1Password CLI, macOS Keychain, AWS Secrets Manager, etc.)
- Requires a Rapid7 Platform Admin to generate; Organization Key is recommended over User Key

**DuckDB sandbox**
- After data is loaded, the DuckDB connection has `enable_external_access=false` enforced at the engine level
- This blocks all filesystem reads (`read_parquet`, `read_csv`, `glob`) and network access from user SQL
- If a query fails with an "external access" error, this is expected behavior — it cannot be bypassed via SQL

## Pre-commit checklist

Before every commit:

```bash
make lint       # must be clean — fix any issues with make lint-fix first
make security   # must be clean
make test       # must pass
```

## Pull requests

Before opening a PR, bump the version — CI will fail if `manifest.json`, `pyproject.toml`, `plugin.json`, and `SKILL.md` are out of sync or unchanged from the last release:

```bash
make bump-version V=x.y.z   # bumps all version files and runs uv lock atomically
```

Then run the full pre-commit checklist above, commit the version bump, and open the PR. The version bump commit should be separate from the feature/fix commits so it's easy to revert if needed.

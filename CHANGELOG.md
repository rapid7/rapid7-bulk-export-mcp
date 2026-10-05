# Changelog

## 0.7.0

### Breaking

- **The HTTP transport now requires inbound authentication.** Docker and other
  `MCP_TRANSPORT=http` deployments refuse to start until `MCP_AUTH_JWKS_URI`,
  `MCP_AUTH_ISSUER` and `MCP_AUTH_AUDIENCE` are set, rather than serving vulnerability data
  to anyone who can reach the port. `docker-compose.yml` passes the three variables through.
  Stdio is unaffected.

### Added

- **Inbound authentication for remote mode.** The HTTP transport validates OIDC bearer
  tokens against any standards-compliant identity provider, configured from environment
  (`MCP_AUTH_JWKS_URI`, `MCP_AUTH_ISSUER`, `MCP_AUTH_AUDIENCE`) and read at startup with no
  image rebuild. Several issuers can be accepted at once. Stdio remains unauthenticated by
  design. See [docs/authentication.md](docs/authentication.md).
- **Read/write scope separation.** The mutating tools — `start_rapid7_export`,
  `download_rapid7_export`, `load_rapid7_parquet` and `purge_rapid7_data` — require a write
  scope (`MCP_AUTH_WRITE_SCOPE`, default `rapid7.write`) on the HTTP transport; read tools
  stay available to any authenticated caller. Gating is inert on stdio.
- **`rapid7-refresh` foreground entrypoint.** A headless console script that creates, polls,
  downloads and loads the requested exports synchronously in a single process — no background
  threads that could die mid-write — for scheduled and hosted refresh. Exits non-zero and
  names the failed windows if any window fails. In hosted mode it publishes the finished
  database to Blob Storage as a versioned artifact.
- **Data-age annotation on hosted query results.** When serving a Blob artifact, successful
  `query_rapid7` responses carry a short data-age note sourced from a load-metadata table
  inside the data database. It is fail-soft and never breaks a query. Local output is
  unchanged.
- **Optional query time limit.** `DUCKDB_QUERY_TIMEOUT_SECONDS` cancels a query that runs
  longer than the limit with an actionable message. Unset by default, so local queries run
  to completion as before; the Azure template sets 90 seconds.
- **Private hosting guide for Microsoft Copilot Studio.** See
  [docs/copilot-studio-hosting.md](docs/copilot-studio-hosting.md) for an internal-ingress
  deployment with Blob artifact storage, a scheduled refresh job, and Key Vault–backed
  secrets.

## 0.6.3

### Fixed

- **Export status and download work again.** v0.6.2 declared the export id in the
  status query as `ID!`. The Rapid7 export schema has no `ID` type, so every status and
  download call failed with `Unknown type 'ID'`. The query now uses `String!`. Export
  creation was not affected.

### Changed

- Bumped `pyjwt` 2.13.0 → 2.15.0 and `urllib3` 2.7.0 → 2.8.0.

## 0.6.1

### Changed

- **Adopted the [Agent Plugins](https://agent-plugins.org/) packaging format.** The
  repository now ships a root `plugin.json` + `mcp.json` and a `skills/rapid7-bulk-export/`
  directory. Kiro and other conformant clients install the MCP server and skill together
  as one power. The `manifest.json` (MCPB) bundle for Claude Desktop connector install is
  unchanged.
- **Removed the legacy `power-rapid7-bulk-export/` (`POWER.md`) layout**, superseded by
  the Agent Plugins format for the same client. Existing installs of the old-format power
  keep working until they are re-pulled; re-install from the repository URL to move to the
  new format.

### Added

- **`PLUGIN_DATA` support for the database location.** When `DATA_DIR` is not set, the
  server now uses the plugin host's `PLUGIN_DATA` directory (per-install, writable,
  survives updates) before falling back to `~/.rapid7_mcp`. An explicit `DATA_DIR` still
  takes precedence, so existing installs are unaffected. This also benefits MCPB installs.

### Fixed

- Reconciled the skill's tool references to the real prefixed tool names
  (`query_rapid7`, `load_rapid7_parquet`) so documentation matches the server.

## 0.6.0

### Fixed

- **Multi-month remediation exports now load every window.** Previously, requesting
  remediation data for a range longer than one month could silently load only a
  single 31-day window. The Rapid7 platform allows only one remediation export in
  flight at a time and splits a longer range into multiple exports; the tool created
  them without sequencing, so all windows collapsed onto one export ID and only one
  month's data was downloaded — and it could be any month in the requested range.

  **Impact:** if you ran a remediation export covering more than 31 days on an earlier
  version, it may have loaded only part of your range. Re-run that export on 0.6.0 to
  get the complete data.

### Changed

- Requesting a remediation range now starts a single background job that creates,
  downloads, and loads each ≤31-day window in order and appends them into one
  `vulnerability_remediation` table. `start_rapid7_export(export_type="remediation", …)`
  returns a job ID; poll it with `check_rapid7_export_status`. Partial failures are
  explicit — the job reports exactly which windows loaded and which are missing so the
  missing range can be re-run.

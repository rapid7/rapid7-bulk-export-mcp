# Changelog

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

# Changelog

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

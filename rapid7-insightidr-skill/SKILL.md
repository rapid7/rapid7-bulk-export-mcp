---
name: Rapid7 InsightIDR Investigations Expert
description: Expert guidance for triaging, reviewing, and closing Rapid7 InsightIDR investigations via the Command Platform REST API (v2, preview)
version: 0.1.0
author: rozumeyroman@gmail.com
tags: [security, siem, xdr, rapid7, insightidr, investigations, incident-response]
---

# Rapid7 InsightIDR Investigations Expert

**CRITICAL REQUIREMENTS:**
1. The Rapid7 MCP server MUST be installed and configured (same server as the Bulk Export skill — `RAPID7_API_KEY` + `RAPID7_REGION`, no separate credentials needed)
2. This skill covers **InsightIDR Investigations** — a different Rapid7 product from Bulk Export/InsightVM. It reads/writes live SIEM state, not exported snapshot data.
3. **Never close an investigation without first previewing it and confirming with the user.** See "Closing Investigations — MANDATORY SAFETY FLOW" below.
4. The underlying API (`/idr/v2/investigations`) is in **preview mode** — behavior may change without notice. Treat unexpected fields or errors as a signal to re-check the current API response shape, not as a bug to silently work around.

## Prerequisites — MANDATORY

This skill ONLY works with the Rapid7 MCP server's InsightIDR tools:

- `get_investigations(status?, priorities?, assignee_email?, limit?)` — list/search investigations
- `get_investigation_details(investigation_id)` — full record + associated alerts for one investigation
- `close_investigation(investigation_id, disposition, confirm?)` — close one investigation (two-step confirm)

If these tools are not available, STOP and tell the user: "The Rapid7 MCP server's InsightIDR tools are not configured or not connected." Do not attempt to fetch investigation data any other way.

## Data Source

- **Product**: InsightIDR (SIEM/XDR), Investigations API v2 (preview)
- **Scope**: Live, current state of your organization's SIEM — not historical exports. There is no local database; every call hits the Rapid7 API directly.
- **Investigation**: A grouping of one or more related alerts that Rapid7 (or a detection rule) has determined represent a single security event worth reviewing.
- **Alert**: The underlying evidence — a specific detection (e.g. "Suspicious Authentication") tied to a detection rule and a timestamp. `get_investigation_details` returns the alerts associated with an investigation.

### Status values
| Status | Meaning |
|---|---|
| `OPEN` | Default status for all new investigations — not yet triaged |
| `INVESTIGATING` | An analyst is actively working the investigation |
| `WAITING` | Paused while more information is gathered |
| `CLOSED` | Resolved — always paired with a disposition |

### Disposition values (required when closing)
| Disposition | Meaning |
|---|---|
| `BENIGN` | Confirmed non-malicious — expected/legitimate activity |
| `MALICIOUS` | Confirmed a real security incident |
| `NOT_APPLICABLE` | Doesn't warrant a benign/malicious judgment (e.g. test activity, duplicate, misconfigured rule) |

### Priority values observed
`LOW`, `MEDIUM`, `HIGH`, `CRITICAL` (highest urgency first when triaging)

## Workflow — TRIAGE

1. **Start broad, then narrow.** Call `get_investigations()` with no filters first to see the current landscape, or filter immediately if the user has a clear ask (e.g. `status="OPEN"`, `priorities="CRITICAL,HIGH"`).
2. **Summarize before diving in.** Group by status/priority and tell the user the shape of what's open before pulling full details on any one investigation — don't dump 20 investigations' full JSON unprompted.
3. **Drill into specifics with `get_investigation_details`.** Use this when the user asks about one particular investigation, or when triaging a small shortlist (e.g. all CRITICAL + OPEN). It returns the full investigation record plus associated alerts — the alerts are the actual evidence (detection rule, alert type, source, timestamps).
4. **Interpret `null` fields as API gaps, not analysis failures.** Fields like `first_alert_time`/`latest_alert_time`/`assignee` are frequently `null` in the current preview API even for real investigations — don't over-interpret their absence as meaningful, and don't guess values to fill them in.

## Closing Investigations — MANDATORY SAFETY FLOW

Closing an investigation is a **write operation on live SIEM data** with no local undo. Always follow this exact two-step sequence:

```
1. Call close_investigation(investigation_id, disposition)  — confirm defaults to False
   → This ONLY previews the investigation (title, current status, priority,
     assignee). It makes NO changes. Show this preview to the user verbatim
     or summarized.

2. WAIT for the user to explicitly approve — do not assume approval from
   context, a prior "yes" about something else, or the fact that they asked
   you to look into the investigation in the first place.

3. Only after explicit approval, call again with confirm=True:
   close_investigation(investigation_id, disposition, confirm=True)
```

**Never skip step 1, and never call with `confirm=True` on the first call for a given investigation in a conversation.** If the user says something like "close all the LOW priority benign ones," still preview each one (or at least list them with your intended disposition) and get one explicit go-ahead before confirming — do not loop `confirm=True` calls autonomously across multiple investigations without the user seeing what's about to happen.

If `close_investigation` reports the investigation is already `CLOSED`, that's informational, not an error — no action was taken and none is needed.

**Choosing a disposition**: base it on the alert evidence from `get_investigation_details`, not on the investigation title alone. If the evidence is genuinely ambiguous, say so and ask the user rather than guessing between `BENIGN` and `NOT_APPLICABLE`.

## Common Analysis Patterns

### Daily/shift triage
```
get_investigations(status="OPEN") → group by priority
→ for CRITICAL/HIGH: get_investigation_details() on each, summarize evidence
→ recommend disposition + status for each, but do not close without approval
```

### Investigating a specific alert type
```
get_investigations() → filter results client-side by title/keyword
  (the API has no free-text title search — filter after listing, or narrow
  with status/priorities/assignee_email first to keep result sets small)
```

### Reviewing an analyst's workload
```
get_investigations(assignee_email="analyst@example.com")
```

### Closing a batch of confirmed-benign investigations
```
For each candidate:
  get_investigation_details(id) → check evidence supports BENIGN
  close_investigation(id, "BENIGN") → show preview to user
After user approves the batch:
  close_investigation(id, "BENIGN", confirm=True) for each, one at a time
  Report a running tally: closed X of Y so far
```

## Error Handling

If `get_investigations`/`get_investigation_details`/`close_investigation` are not available:
1. STOP immediately
2. Tell the user: "InsightIDR tools are not configured in this MCP connection."
3. DO NOT attempt to fabricate investigation data or guess at API responses

If `close_investigation` returns an error:
1. Show the raw error to the user — do not retry automatically, especially not with `confirm=True`
2. Common causes: invalid `disposition` value (must be exact: `BENIGN`, `MALICIOUS`, `NOT_APPLICABLE`), invalid/stale `investigation_id`, insufficient API key permissions
3. Re-fetch with `get_investigation_details` to check current state before retrying

If `get_investigations` returns zero results:
1. Check whether filters are too narrow (e.g. wrong `status` value, `assignee_email` typo)
2. Retry with no filters to confirm the connection itself is working
3. Zero open investigations is a valid, good outcome — don't assume it's an error

## Tips for Analysis

- **This is live data, not a snapshot** — re-fetch (`get_investigations`/`get_investigation_details`) rather than relying on results from earlier in a long conversation; another analyst or automation may have changed state since.
- **Evidence over titles** — investigation titles come from the triggering detection rule and can be generic; always check `get_investigation_details` alerts before recommending a disposition.
- **Preview mode caveat** — mention to the user if API behavior seems inconsistent with this document; the v2 API can change without notice.
- **Never bulk-close via filters** — this MCP server's `close_investigation` always targets one explicit investigation ID, by design, to avoid accidentally closing investigations that merely match a filter. Don't try to work around this by asking for a different bulk-by-filter behavior.
- **API key scope** — the same `RAPID7_API_KEY`/Organization Key used for Bulk Export works here; no separate credential setup needed.

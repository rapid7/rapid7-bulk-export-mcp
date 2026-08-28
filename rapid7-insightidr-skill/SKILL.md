---
name: Rapid7 InsightIDR Investigations Expert
description: Expert guidance for triaging, reviewing, commenting on, assigning, and closing Rapid7 InsightIDR investigations, plus searching raw log data (LEQL) — including paginating past query_logs' ~50-events-per-call cap to get the real last N events, all events over a period, or an approximate top-N by field — via the Command Platform REST API
version: 0.5.0
author: rozumeyroman@gmail.com
tags: [security, siem, xdr, rapid7, insightidr, investigations, incident-response]
---

# Rapid7 InsightIDR Investigations Expert

**CRITICAL REQUIREMENTS:**
1. The Rapid7 MCP server MUST be installed and configured (same server as the Bulk Export skill — `RAPID7_API_KEY` + `RAPID7_REGION`, no separate credentials needed)
2. This skill covers **InsightIDR Investigations** — a different Rapid7 product from Bulk Export/InsightVM. It reads/writes live SIEM state, not exported snapshot data.
3. **Never close or assign an investigation without first previewing it and confirming with the user.** See "Closing Investigations — MANDATORY SAFETY FLOW" and "Assigning Investigations — SAFETY FLOW" below.
4. The underlying API (`/idr/v2/investigations`) is in **preview mode** — behavior may change without notice. Treat unexpected fields or errors as a signal to re-check the current API response shape, not as a bug to silently work around.
5. **Alert evidence and raw log events contain real personal data.** `get_alert_evidence`, `query_logs`, and `run_saved_query` can all return a real employee's name, email, IP address, and other personal details. See "Alert Evidence — PRIVACY" and "Log Search" below before using them.
6. **There is no API to list everyone eligible to be assigned an investigation.** `list_investigation_assignees` only shows people assigned before — see "Assigning Investigations" below for how to handle someone not on that list.
7. **Before assuming an endpoint, parameter, or enum value doesn't exist (or guessing one), check the files in `references/`** in this skill's directory — `investigations-api-v2.md`/`.json` (official spec), `comments-api-v1.md`/`insightidr-api-v1.json`, and `log-search-api.md`. This already caught one real bug in this codebase (a silently-ignored filter param). Guessing instead of checking is how that bug happened.
8. **Never create or delete a saved query without preview/confirm**, same as closing or assigning an investigation. See "Log Search — Saved Queries" below.

## Prerequisites — MANDATORY

This skill ONLY works with the Rapid7 MCP server's InsightIDR tools:

- `get_investigations(status?, priorities?, assignee_email?, sources?, tags?, limit?)` — list/search investigations
- `get_investigation_details(investigation_id)` — full record + associated alerts for one investigation
- `get_alert_evidence(alert_id)` — raw source event behind one alert (user, IP, geolocation, service, result — see privacy note below)
- `get_investigation_product_alerts(investigation_id)` — alerts from OTHER Rapid7 products (Threat Command, Insight Agent) linked to this investigation; most investigations have none, which is normal
- `list_investigation_assignees()` — known assignees observed in investigation history (not exhaustive — see "Assigning Investigations" below)
- `assign_investigation(investigation_id, user_email, confirm?)` — assign one investigation to a user (two-step confirm)
- `list_investigation_comments(investigation_id, limit?)` — list comments on an investigation, newest first
- `add_investigation_comment(investigation_id, body)` — add a comment (no confirm needed — see "Comments" below for why)
- `close_investigation(investigation_id, disposition, confirm?)` — close one investigation (two-step confirm)
- `list_logs()` / `list_logsets()` — logs and named log groupings available for search
- `query_logs(log_ids, statement, time_range?, from_ms?, to_ms?)` — run a LEQL query against raw log data (see "Log Search" below — privacy note applies)
- `list_saved_queries()` / `run_saved_query(saved_query_id)` — list/run saved LEQL queries
- `create_saved_query(name, log_ids, statement, ..., confirm?)` / `delete_saved_query(saved_query_id, confirm?)` — write, two-step confirm

If these tools are not available, STOP and tell the user: "The Rapid7 MCP server's InsightIDR tools are not configured or not connected." Do not attempt to fetch investigation data any other way.

## API Reference — check before searching or guessing

`references/investigations-api-v2.md` (condensed) and `references/investigations-api-v2.json` (the full official OpenAPI spec, verbatim), plus `references/comments-api-v1.md`/`references/insightidr-api-v1.json` for Comments, and `references/log-search-api.md` for Log Search, live in this skill's directory. They cover every endpoint, parameter, and enum this server touches — including several not implemented yet:

- `createInvestigation` (POST), `updateInvestigation` (PATCH — can set title/status/priority/disposition/assignee in one call), `setPriority`, `setDisposition` (single-field versions of what `close_investigation` does for status), `_search` (richer filtering than list, including free-text `title` CONTAINS search), `removeAlertFromInvestigation`.
- Log Search: single-log/logset detail lookups, log/logset create/rename/delete, saved query update-in-place — see `log-search-api.md`'s "Not implemented this iteration" section for the reasoning behind each.

**Note on `log-search-api.md`**: unlike the Investigations spec, no official OpenAPI JSON exists for Log Search — see that file for sources. Treat it as solid but not "verbatim spec" the way `investigations-api-v2.json` is.

If a user asks for something that sounds like it needs a new capability (reassigning + reprioritizing in one step, free-text search by title, unlinking a bad alert from an investigation), check the reference first — it may already be a known, documented endpoint just not wired up as a tool yet, which is a very different conversation than "does this API even support that."

**Keeping the reference current**: `references/.spec-meta.json` tracks when the spec was last checked against Rapid7's published copy and its content hash. `make check-idr-spec` (run from the repo root, not something this skill's chat-facing tools expose) checks weekly and re-downloads `investigations-api-v2.json` automatically if it changed — `make check-idr-spec FORCE=1` bypasses the weekly gate. This is a maintainer/dev-workflow command, not something to run as part of triaging an investigation. If `investigations-api-v2.json` is ever updated by that command, `investigations-api-v2.md` (the condensed table) is NOT auto-regenerated — it needs a manual review to stay accurate.

**Note**: `check-idr-spec` currently only covers the v2 Investigations spec. `insightidr-api-v1.json`/`comments-api-v1.md` were fetched once by hand and are NOT auto-checked for staleness — if Comments behavior seems off, verify against a fresh fetch of `help.rapid7.com/insightidr/en-us/api/v1/insightidr-api-v1.json` rather than assuming the bundled copy is current.

## Data Source

- **Product**: InsightIDR (SIEM/XDR), Investigations API v2 (preview)
- **Scope**: Live, current state of your organization's SIEM — not historical exports. There is no local database; every call hits the Rapid7 API directly.
- **Investigation**: A grouping of one or more related alerts that Rapid7 (or a detection rule) has determined represent a single security event worth reviewing.
- **Alert**: The underlying evidence — a specific detection (e.g. "Suspicious Authentication") tied to a detection rule and a timestamp. `get_investigation_details` returns the alerts associated with an investigation.
- **Product alert**: A DIFFERENT thing from the above — an alert from another Rapid7 product (Threat Command, Insight Agent) that this investigation is linked to, if the org has that product licensed. `get_investigation_product_alerts` covers these; `get_investigation_details`'s alert list does not. Most investigations have zero product alerts — don't call this reflexively for every investigation, only when the user's question suggests a cross-product link (e.g. an endpoint/agent action, or an external threat intel match) or when `source` on the investigation itself hints at it.

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
3. **Drill into specifics with `get_investigation_details`.** Use this when the user asks about one particular investigation, or when triaging a small shortlist (e.g. all CRITICAL + OPEN). It returns the full investigation record plus associated alerts — the alerts are metadata only (detection rule, alert type, timestamps), not the underlying event.
4. **Go one level deeper with `get_alert_evidence` when the user needs event-level detail** — source/destination IP, user or account, geolocation, service, and whether the action succeeded. `get_investigation_details` alone does not include this; you must call `get_alert_evidence` per alert ID. See the privacy note below before doing so.
5. **Interpret `null` fields as API gaps, not analysis failures.** Fields like `first_alert_time`/`latest_alert_time`/`assignee` are frequently `null` in the current preview API even for real investigations — don't over-interpret their absence as meaningful, and don't guess values to fill them in.

## Alert Evidence — PRIVACY

`get_alert_evidence(alert_id)` calls the alert-triage API (`/idr/at/alerts/{id}/evidences`), a separate endpoint from Investigations. It returns the actual source event, which for user-driven alerts (e.g. authentication) typically includes:

- A real employee's name, email, and account/username
- Source IP address and geolocation (city, country, ISP/org)
- Device/browser details, and for MFA events, phone/device metadata
- The full raw event payload as ingested by Rapid7

**This is real personal data about a person in the organization, not test data.** Handle it accordingly:
- Only fetch it when it's actually needed to triage the alert — don't call it reflexively for every alert in a list.
- When presenting it to the user, extract the fields relevant to the security question (who, from where, what result) rather than dumping the entire raw payload by default — offer the full raw JSON if they ask for it specifically.
- Never paste this output into an external system (ticketing, chat, a shared doc) without the user's explicit instruction to do so — the user asking you to investigate is not the same as authorization to export PII elsewhere.
- If the alert turns out to involve a different, unexpected person than expected, flag that explicitly rather than silently proceeding — it may itself be a signal worth the user's attention.

## Assigning Investigations — SAFETY FLOW

Assigning an investigation is a **write operation on live SIEM data** — the assigned person receives a notification email, so this has a real-world effect on a real person, not just a database field. Follow the same two-step pattern as closing:

```
1. Call assign_investigation(investigation_id, user_email)  — confirm defaults to False
   → Previews the investigation and target assignee. Makes NO changes,
     sends NO email. Show this to the user.

2. WAIT for the user to explicitly approve.

3. Only after explicit approval, call again with confirm=True:
   assign_investigation(investigation_id, user_email, confirm=True)
```

**Finding a valid `user_email`:**
1. Call `list_investigation_assignees()` first — it surfaces people already known from investigation history (fast, no guessing).
2. If the person the user wants isn't listed, that does NOT mean they're ineligible — this list is only "people assigned before," not "everyone who can be assigned." There is no InsightIDR API to enumerate all eligible platform users (that requires a separate account-management API this server's key does not have access to).
3. Ask the user for the exact email if it's not already in the conversation — do not guess at an email format (e.g. don't assume `firstname.lastname@company.com`) or infer one from a name alone.
4. Just call `assign_investigation` with the email — the API validates eligibility itself (400/403/404 for an invalid or ineligible email) and its error message is the authoritative answer, more reliable than trying to pre-check.

## Log Search

A genuinely separate API family from Investigations/Comments (different path prefix `/log_search`, no `Accept-version` header, async/pollable queries). See `references/log-search-api.md` for full endpoint details.

**Basic flow**:
```
list_logs() → find the log_id(s) relevant to the question (e.g. "O365", "Windows Event Log")
query_logs(log_ids="<id>", statement="<LEQL>", time_range="Last 7 Days")
  → returns matching events, or a "still processing" note for unusually large queries
```

**LEQL basics**: a query statement like `where(user)`, `where(result=FAILED) groupby(source_ip)`, `calculate(count) timeslice(1h)`. This skill does not attempt to teach LEQL syntax in depth — if unsure, start narrow (`where(...)` on a specific field) and widen only if needed, rather than guessing a complex statement. `time_range` accepts relative windows like `"Today"`, `"Last 1 Day"`, `"Last 7 Days"`; use `from_ms`/`to_ms` (Unix milliseconds) instead for a precise window.

**To match every event with no filter, use `statement=""` (empty string) — NOT `where()`.** `where()` with empty parentheses is invalid LEQL and the API rejects it with `400 "Invalid Query Syntax"`.

**PRIVACY — same treatment as `get_alert_evidence`**: `query_logs` and `run_saved_query` return raw log content that can include real personal data (usernames, emails, source IPs, actions like password resets). Only fetch what's needed, extract relevant fields when presenting results instead of dumping every raw event by default, and never forward this output externally without the user's explicit instruction.

**Async queries**: most queries resolve immediately. If `query_logs`/`run_saved_query` reports "still processing," that's unusual — consider narrowing the time range or statement rather than just retrying the same query, since retrying creates a brand-new query rather than resuming the pending one.

### Pagination — getting more than 50 events, or the real "last N"

**Empirically confirmed (live testing, not documented behavior)**: `query_logs` reliably returns at most ~50 events per call, regardless of `time_range` width or `max_events`. Those events are the **earliest matches chronologically within the requested window** — i.e. the *first* N events of the window, not the *last* N. Asking for `time_range="Last 1 Day"` and getting exactly 50 events back does NOT mean those are the day's most recent events; they're very likely a cluster from near the start of that window. Do not report a capped result as "the last N events" without pagination — say what it actually is instead.

**Do not try to work around this by shrinking `time_range` alone** (e.g. jumping straight to `"Last 5 Minutes"`) — a very narrow window combined with several `log_ids` at once can itself return `400 Bad Request`. Widen first, then paginate forward with `from_ms` as below.

**Algorithm — the real "last N events" (most recent first)**:
1. Call `query_logs` with a reasonably wide `time_range` (e.g. `"Last 1 Hour"` or `"Last 1 Day"` — not `"Last 5 Minutes"`).
2. If the response has exactly ~50 events, that's one page, not the whole window. Take the **maximum timestamp** in that page.
3. Call `query_logs` again with `from_ms = <that max timestamp> + 1000` and `to_ms` left at the original window's end (or omitted to mean "now").
4. Repeat step 2–3, accumulating pages, until either:
   - a page comes back with fewer than ~50 events (you've reached the end of available data), or
   - you've collected enough events / hit the safety limit below.
5. Sort everything accumulated by timestamp descending and take the tail (most recent N) — that's the real answer.

**Algorithm — "all events in period X" (period may contain >50 events)**:
Same loop as above, but the stopping condition is `to_ms` (the end of the requested period) being reached, not a target count.

**Safety limit**: stop and tell the user if the loop is approaching ~20 iterations / ~1000 accumulated events / ~30 seconds of wall time, rather than looping indefinitely — a busy log (e.g. a firewall log during a scan) can produce hundreds of events per second and never "run out" within a reasonable window.

**Worked example** (3 log_ids for one logical log, `statement=""` to match everything):
```
Call 1: query_logs(log_ids="id-a,id-b,id-c", statement="", time_range="Last 1 Hour")
  → 50 events, timestamps span 07:58:04–07:58:40 (NOT up to "now" — this is
    the window's first page)
  → max timestamp seen: 1787903920681 (07:58:40.681)

Call 2: query_logs(log_ids="id-a,id-b,id-c", statement="",
                    from_ms=1787903921681, to_ms=<end of original 1-hour window>)
  → 50 more events, timestamps span 07:58:40–07:59:12
  → max timestamp seen: 1787903952000

Call 3: query_logs(log_ids="id-a,id-b,id-c", statement="",
                    from_ms=1787903953000, to_ms=<end of original window>)
  → 12 events, timestamps span 07:59:12–07:59:24 — fewer than ~50, so this
    is genuinely the end of matching data in this window (or the point
    where activity actually goes quiet — worth double-checking the next
    time slice returns 0 before concluding "no more events" vs. "another
    page follows")

→ Accumulated 112 events total, sorted descending by timestamp — take
  however many the user actually asked for from the top (most recent).
```

**Aggregation (top-N by field, count-by-field, etc.)**: `groupby()`/`calculate()` LEQL statements reliably return `400 Bad Request` through `query_logs` — the backend aggregation path does not work through this tool. Do not retry these statements expecting a different result. Instead:
1. Pull the raw events with the pagination loop above (`statement=""` or a `where(...)` filter, never `groupby`/`calculate`).
2. Compute the aggregate client-side (e.g. count occurrences of `source_address` across the accumulated events with a short script).
3. **Explicitly tell the user this is an approximation bounded by however many raw events were actually collected**, not an exact count over the entire source — phrase it as "of the N events I pulled, the top source IPs were..." rather than presenting it as a complete statistical answer.

**Finding the right `log_ids`**: a single logical log (e.g. "O365", "Gigatrans") is often split across multiple `log_ids` (duplicate instances/collectors) — pass all of them together in one `log_ids` list, or you will silently miss part of the activity. Prefer `list_logsets()` over manually scanning `list_logs()` when the task maps to a named functional grouping (e.g. "Firewall Activity", "Ingress Authentication") — it's the intended starting point for "give me everything relevant to X" rather than eyeballing a flat log list.

### Log Search — Saved Queries (SAFETY FLOW)

`create_saved_query` and `delete_saved_query` are write operations and follow the same mandatory two-step pattern as closing/assigning an investigation:

```
1. Call without confirm=True — previews what will be created/deleted, makes no changes
2. WAIT for explicit user approval
3. Only then call again with confirm=True
```

`list_saved_queries`/`run_saved_query` are read-only, no confirm needed.

## Comments

Comments are the one write action in this skill that does NOT require a preview/confirm step. Rationale: unlike closing or assigning, adding a comment doesn't change the investigation's status, priority, disposition, or who's responsible for it — it's an additive note, visible and removable by an analyst in the InsightIDR UI, not a state transition with downstream effects (no notification email is sent, unlike assignment).

This is a narrower exception, not a precedent for skipping confirmation elsewhere — don't generalize "this write op is low-risk enough to skip confirm" to other actions without the same reasoning holding up.

**Still be deliberate about content**: only post a comment the user actually asked for, or clearly wants recorded (e.g. "note that I checked with the user and this is expected"). Don't narrate your own tool calls or routine analysis into the investigation's comment thread unprompted — a comment is a permanent-ish record other analysts will read, not a scratchpad.

**Visibility is not controllable at creation** — the API sets it (see `references/comments-api-v1.md`); there's no `visibility` parameter on `add_investigation_comment`. If a user needs a specific visibility, that requires a separate `updateComment` call this server doesn't implement yet.

**Finding the right `investigation_id`**: `list_comments`/`create_comment` use the investigation's RRN as `target` — pass the full `rrn` from `get_investigations`/`get_investigation_details`, not a short id (documented as an RRN in the API, unlike several Investigations v2 endpoints that accept either).

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

**Threat Command-linked investigations**: check `get_investigation_product_alerts` first if the investigation might be Threat Command-linked — it lists `applicable_close_reasons` (e.g. `ProblemSolved`, `FalsePositive`). `close_investigation` does NOT currently forward a Threat Command close reason to the API (a known gap — see `references/investigations-api-v2.md`); mention this limitation to the user rather than silently ignoring a close reason they explicitly want recorded.

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
  (get_investigations/listInvestigations has no free-text title search —
  filter after listing, or narrow with status/priorities/assignee_email
  first to keep result sets small. The underlying API DOES support
  title CONTAINS search via the _search endpoint — see
  references/investigations-api-v2.md — but that's not wired up as a
  tool yet, so this workaround is still necessary for now.)
```

### Reviewing an analyst's workload
```
get_investigations(assignee_email="analyst@example.com")
```

### Understanding who/what triggered an alert
```
get_investigation_details(investigation_id) → note the alert id(s)
get_alert_evidence(alert_id) → extract user/account, source_ip, geoip fields,
  service, result from the event data — present only what answers the
  user's question, not the full raw payload, unless asked for it
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

### Checking for cross-product context
```
get_investigation_details(id) → note the source field and title
get_investigation_product_alerts(id) → if non-empty, this investigation
  ties to Threat Command or Insight Agent — surface that context (e.g.
  applicable_close_reasons) before recommending a disposition or closing
```

### Recording a finding before closing
```
get_investigation_details(id) → analyze evidence, reach a conclusion
add_investigation_comment(id, "<summary of the finding and why>")
  → no confirm needed, but only if the user wants this recorded
close_investigation(id, disposition) → normal preview/confirm flow
```

### Corroborating alert evidence with raw log data
```
get_alert_evidence(alert_id) → note the log_id/source implied by the evidence
list_logs() → confirm the exact log_id to search
query_logs(log_ids="<id>", statement="where(<relevant field>)", time_range="Last 7 Days")
  → look for related activity around the same time/user/IP the alert flagged
```

### Reusing a recurring investigative query
```
list_saved_queries() → check if a relevant query already exists
run_saved_query(id) if found, else query_logs(...) directly
If the user wants to reuse this query regularly:
  create_saved_query(name, log_ids, statement, ...) → preview → confirm=True
```

## Known Alert Patterns

Documented patterns save re-deriving the same analysis from scratch each time. Each entry below is a recognition signature + a short recommendation — the goal is to reach a conclusion in one pass instead of reasoning through the underlying mechanism again. **A documented pattern is a starting hypothesis, not a verdict** — it tells you what to check and what to say, never a reason to skip `close_investigation`'s preview/confirm flow or to close anything without the user's explicit go-ahead. This applies with extra weight for organizations other than the one this pattern was first observed in — a "usually benign" signature here can be a real incident somewhere else with different infrastructure norms.

### Pattern: VPN/proxy-sourced authentication flagged as `NOVEL_ASN`

**Signature** (from `get_alert_evidence`):
- Alert title matches `Suspicious Authentication - <Provider>` where `<Provider>` (from `geoip_organization`) is a known VPN/proxy/hosting service — e.g. Cloudflare WARP, a corporate VPN provider, a commercial VPN service — rather than a residential/mobile ISP.
- `adaptive_trust_assessments` shows `detected_attack_detectors: ["NOVEL_ASN"]` with `trust_level: LOW`.
- Exactly **one** MFA event, `result: SUCCESS`, `reason: user_approved` (Duo push or equivalent) — not a string of denied/retried attempts.
- The account, groups, and device are otherwise unremarkable for that user (no new device fingerprint, no privilege escalation alongside it).

**Why this triggers**: `NOVEL_ASN` fires on the network (ASN) being new *for that specific user's history* — not on the login being anomalous in absolute terms. A VPN or proxy service routes traffic through infrastructure the user hasn't used before, which trips this detector even for completely routine logins. This is a known source of low-value noise in Rapid7's own alert set, not something specific to this codebase's interpretation of it.

**What distinguishes this from a real credential-based attack (push/MFA fatigue)**: a single, immediately-approved push is consistent with a user who initiated their own login and expected the prompt. Multiple pushes in short succession, especially with denials before an eventual approval, is the actual fatigue-attack signature — that pattern is NOT covered by this entry and should be treated as a real risk indicator, not dismissed as VPN noise.

**Recommended handling**:
1. State the conclusion tersely — you do not need to re-explain what WARP/VPN routing is or why `NOVEL_ASN` fires each time this pattern recurs: *"Likely a VPN/proxy-related NOVEL_ASN false positive — single approved MFA push, no fatigue pattern. Recommend confirming with the user before marking BENIGN."*
2. Still surface it to the user with the account/service/timestamp so they can confirm intent (e.g. "did you use \[VPN provider\] to log in on \[date\]?") — do not silently conclude benign on the pattern match alone.
3. Only proceed through the normal `close_investigation` preview → explicit user approval → `confirm=True` flow. This pattern shortens your reasoning, not the required human approval step.

**Red flags that override this pattern** — treat as a real investigation, not noise, if any are present:
- Multiple MFA prompts, especially any denied/timed-out ones, before a success
- The user reports they did NOT initiate the login
- A password reset, MFA re-enrollment, or privilege change occurred close in time
- The VPN/proxy provider is unfamiliar to the organization (not a sanctioned corporate VPN or a widely known consumer service)
- Any indicator beyond `NOVEL_ASN` (e.g. impossible travel, known-malicious IP reputation)

## Error Handling

If `get_investigations`/`get_investigation_details`/`get_alert_evidence`/`get_investigation_product_alerts`/`list_investigation_assignees`/`assign_investigation`/`list_investigation_comments`/`add_investigation_comment`/`close_investigation`/`list_logs`/`list_logsets`/`query_logs`/`list_saved_queries`/`run_saved_query`/`create_saved_query`/`delete_saved_query` are not available:
1. STOP immediately
2. Tell the user: "InsightIDR tools are not configured in this MCP connection."
3. DO NOT attempt to fabricate investigation data or guess at API responses

If `close_investigation` returns an error:
1. Show the raw error to the user — do not retry automatically, especially not with `confirm=True`
2. Common causes: invalid `disposition` value (must be exact: `BENIGN`, `MALICIOUS`, `NOT_APPLICABLE`), invalid/stale `investigation_id`, insufficient API key permissions
3. Re-fetch with `get_investigation_details` to check current state before retrying

If `assign_investigation` returns an error (400/403/404):
1. Show the raw error to the user — do not retry with a guessed alternate email
2. Most likely cause: the email is not a platform admin, product admin, or read/write InsightIDR/InsightUBA user — ask the user to confirm the person's exact platform login email and role, rather than trying nearby spellings

If `get_investigation_product_alerts` returns zero results:
1. That's the normal case for most investigations — do not treat it as an error or retry
2. Only worth investigating further if the user specifically expected a Threat Command/Insight Agent link and it's missing

If `add_investigation_comment`/`list_investigation_comments` returns an error:
1. Show the raw error to the user
2. Most likely cause: `investigation_id` is a short id rather than the full RRN — the Comments API's `target` param requires the RRN. Re-fetch with `get_investigations` and use its `rrn` field, don't guess an RRN format

If `get_investigations` returns zero results:
1. Check whether filters are too narrow (e.g. wrong `status` value, `assignee_email` typo)
2. Retry with no filters to confirm the connection itself is working
3. Zero open investigations is a valid, good outcome — don't assume it's an error

If `query_logs`/`run_saved_query` reports "still processing" after the internal poll window:
1. This is unusual for typical queries — mention it to the user rather than silently retrying in a loop
2. Consider narrowing the time range or statement; a fresh call starts a new query, it does not resume the pending one

If `query_logs` returns `400 Bad Request` on a `groupby()`/`calculate()` statement:
1. This is expected — aggregation LEQL does not work through this tool (see "Pagination — getting more than 50 events" above)
2. Do not retry the same aggregation statement. Switch to pulling raw events (`statement=""` or `where(...)`) and aggregate client-side instead

If `create_saved_query` fails with a JSON mapping error:
1. Check that at least one `log_id` is valid (from `list_logs`) and the LEQL `statement` is well-formed
2. See `references/log-search-api.md` for the confirmed request shape — the error message itself won't explain what's wrong

## Tips for Analysis

- **This is live data, not a snapshot** — re-fetch (`get_investigations`/`get_investigation_details`) rather than relying on results from earlier in a long conversation; another analyst or automation may have changed state since.
- **Evidence over titles** — investigation titles come from the triggering detection rule and can be generic; always check `get_investigation_details` alerts before recommending a disposition.
- **Preview mode caveat** — mention to the user if API behavior seems inconsistent with this document; the v2 API can change without notice.
- **Never bulk-close via filters** — this MCP server's `close_investigation` always targets one explicit investigation ID, by design, to avoid accidentally closing investigations that merely match a filter. Don't try to work around this by asking for a different bulk-by-filter behavior.
- **API key scope** — the same `RAPID7_API_KEY`/Organization Key used for Bulk Export works here; no separate credential setup needed.

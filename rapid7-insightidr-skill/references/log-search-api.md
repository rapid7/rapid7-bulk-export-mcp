# InsightIDR Log Search API — Reference

**No single authoritative OpenAPI JSON spec was found for this API.**
`docs.rapid7.com/insightidr/log-search-api/` renders its content via
client-side JS (Next.js) — the underlying spec could not be located by
inspecting the page source or its RSC payload. This reference was built
from three independent sources instead, cross-checked against each other
and verified live against the real API:

1. Rapid7's official blog post: "Analyzing Log Data Using the InsightIDR
   (Rapid7 SIEM) API" (2020) — confirms base URL and the core query flow.
2. Rapid7's **official** Postman collection:
   `github.com/rapid7/logentries-postman-collection` — confirms exact
   request/response shapes, including the `saved_query` body-wrapping that
   no other source documented.
3. A community-scraped OpenAPI YAML (Shuffle's `insightidr_log_api.yaml`)
   — used for path/parameter names, cross-checked rather than trusted
   blindly (it had an inconsistency: its declared `servers:` used
   `{region}.api.insight.rapid7.com/log_search`, but its embedded curl
   examples used `{region}.rest.logs.insight.rapid7.com` instead — both
   hosts were tested live and return identical data, so either works, but
   this server standardizes on the former for consistency with every
   other InsightIDR path in this codebase).

**Base URL**: `https://{region}.api.insight.rapid7.com/log_search` (same
region as `idr_base`, plus the `/log_search` prefix — see
`src/insightidr_log_search_manager.py::_LOG_SEARCH_BASE`).

**No `Accept-version` header** — unlike Investigations v2, this API is not
in preview mode.

## Endpoints

| Method | Path | Implemented as | Notes |
|---|---|---|---|
| GET | `/management/logs` | `list_logs` | All logs for the account |
| GET | `/management/logs/{logId}` | — not implemented | Single log detail; `list_logs` already returns enough detail for this server's needs |
| POST | `/management/logs/` | — not implemented (out of scope) | Create a new log source — a config change with broader blast radius than anything else in this skill; not requested |
| PATCH/DELETE | `/management/logs/{logId}` | — not implemented (out of scope) | Same reasoning |
| GET | `/management/logsets` | `list_logsets` | All logsets |
| GET | `/management/logsets/{logsetid}` | — not implemented | Same reasoning as single-log detail |
| POST | `/query/logs` | `query_logs` | **Body-based** query (not the `/query/logs/{log_id}` GET+query-string variant some sources show) — supports multiple `logs` in one call. Async: may return `202` with a polling link instead of immediate results. |
| GET | `/query/{query_id}` | (internal — `_poll_until_ready`) | Follow the `links[].rel == "self"` href from a `202` response. Expires ~20s after last poll — a stale `query_id` returns `404`. |
| GET | `/query/saved_queries` | `list_saved_queries` | All saved queries |
| POST | `/query/saved_queries` | `create_saved_query` | Body **must** be wrapped: `{"saved_query": {name, logs, leql: {statement, during}}}` — confirmed via the official Postman collection and live testing; a flat (unwrapped) body returns `400 "A Json Error occurred mapping the request received"` with no hint at the real shape. |
| DELETE | `/query/saved_queries/{id}` | `delete_saved_query` | Returns `204` empty body |
| PATCH / PUT | `/query/saved_queries/{id}` | — not implemented | Partial vs full update of a saved query; not requested this iteration |
| GET | `/query/saved_query/{id}` | `run_saved_query` | **Singular** `saved_query`, not `saved_queries` — a real, different path from the list/create endpoints. Same async 202+poll behavior as `/query/logs`. |

## Request/response shapes (all verified live)

**`POST /query/logs` body:**
```json
{
  "logs": ["<log_id>", "..."],
  "leql": {
    "statement": "where(user)",
    "during": {"time_range": "Last 1 Day"}
  }
}
```
`during` is either `{"time_range": "<relative window>"}` (e.g. `"Today"`,
`"Last 7 Days"`) OR `{"from": <unix_ms>, "to": <unix_ms>}` — not both.

`statement` of `""` (empty string) matches every event with no filter.
`"where()"` (empty parentheses) is **not** valid LEQL — the API rejects it
with `400 {"errorCode":101011,"message":"Invalid Query Syntax: the query
entered is invalid"}`; confirmed live. This was misread once as a
per-request `logs` array size limit (a 400 was seen with 11 log IDs and
assumed to be a count cap) — it was actually this same invalid-statement
issue; the count itself is not the problem, at least up to several dozen
log IDs, confirmed live.

**Pending-vs-final detection (real bug, fixed)**: a `202` (still
processing) response already includes an `"events": []` key — empty, not
absent. Checking for the key's presence to decide whether a query is done
is wrong and was, in fact, this codebase's first implementation, which
caused `query_logs`/`run_saved_query` to silently return the still-empty,
still-pending first response as if it were final — every query looked
like "0 matching events" regardless of real data, statement, or time
window. The correct signal is the `"links"` array: a `rel` of `"Self"`
(capital S in this API's real response, matched case-insensitively) means
still-pending/poll again; once `"links"` disappears and the status is
`200`, the result is final. See `_poll_until_ready` in
`src/insightidr_log_search_manager.py`.

**`202` response** (query still processing):
```json
{
  "logs": ["<log_id>"],
  "id": "<query_id>",
  "progress": 0,
  "query": {"statement": "...", "during": {...}},
  "links": [{"rel": "self", "href": "https://.../log_search/query/<query_id>"}]
}
```

**`200` response** (results ready — either immediately, or after polling):
```json
{
  "logs": ["<log_id>"],
  "events": [
    {
      "timestamp": 1583009183456,
      "sequence_number": 3234730521598636000,
      "log_id": "<log_id>",
      "message": "<JSON string — the actual log entry content>",
      "links": [{"rel": "Context", "href": "..."}]
    }
  ],
  "leql": {"statement": "...", "during": {...}}
}
```
`message` is itself a JSON string (parse it — this codebase's
`_format_log_events` in `mcp_server.py` does this the same way
`get_alert_evidence` handles its `data` field).

**`POST /query/saved_queries` body:**
```json
{
  "saved_query": {
    "name": "get bytes count for yesterday",
    "logs": ["<log_id>"],
    "leql": {"statement": "calculate(bytes) timeslice(1h)", "during": {"time_range": "Yesterday"}}
  }
}
```
Response on `201`: same shape, wrapped in `"saved_query"`, plus an assigned `"id"`.

## Privacy

Same treatment as `get_alert_evidence` (see `SKILL.md`): `query_logs` and
`run_saved_query` return raw log content that can include real personal
data (usernames, emails, IPs, actions like password resets) — confirmed
live against this org's actual O365 ingress-auth logs during
verification. Handle accordingly.

## Not implemented this iteration

- **Pre-Computed Queries** — a distinct tag in the official docs
  (`docs.rapid7.com/insightidr/insightidr-rest-api/`), but no verified
  endpoint details were found, and it appears to primarily be a UI/product
  concept (a saved query with a `groupby`/`calculate` clause, cached for
  faster repeated access) rather than confirmed to be a separate API
  resource. Explicitly excluded from this iteration's scope per the user's
  decision — revisit if a real need comes up, starting with a fresh
  search for its actual API surface rather than assuming it doesn't exist.
- Full log/logset management (create/rename/delete a log or logset) — a
  bigger, more destructive scope than "search your logs"; not requested.
- `PATCH`/`PUT` on an existing saved query (modify in place) — only
  create/list/delete were requested.

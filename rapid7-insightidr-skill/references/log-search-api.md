# InsightIDR Log Search API — Reference

No official OpenAPI JSON spec exists for this API
(`docs.rapid7.com/insightidr/log-search-api/` renders via client-side JS).
This reference was built from three sources, cross-checked against each
other and against the real API:

1. Rapid7's official blog post: "Analyzing Log Data Using the InsightIDR
   (Rapid7 SIEM) API" (2020) — base URL and core query flow.
2. Rapid7's official Postman collection:
   `github.com/rapid7/logentries-postman-collection` — exact
   request/response shapes, including the `saved_query` body-wrapping.
3. A community-scraped OpenAPI YAML (Shuffle's `insightidr_log_api.yaml`)
   — path/parameter names only.

**Base URL**: `https://{region}.api.insight.rapid7.com/log_search` (same
region as `idr_base`, plus the `/log_search` prefix — see
`src/insightidr_log_search_manager.py::_LOG_SEARCH_BASE`).
`{region}.rest.logs.insight.rapid7.com` also works (same backend); this
codebase standardizes on the former for consistency with other InsightIDR
paths.

**No `Accept-version` header** — unlike Investigations v2, this API is not
in preview mode.

## Endpoints

| Method | Path | Implemented as | Notes |
|---|---|---|---|
| GET | `/management/logs` | `list_logs` | All logs for the account |
| GET | `/management/logs/{logId}` | not implemented | `list_logs` covers this server's needs |
| POST | `/management/logs/` | not implemented (out of scope) | Creating a log source is a broader config change than this skill's scope |
| PATCH/DELETE | `/management/logs/{logId}` | not implemented (out of scope) | Same reasoning |
| GET | `/management/logsets` | `list_logsets` | All logsets |
| GET | `/management/logsets/{logsetid}` | not implemented | Same reasoning as single-log detail |
| POST | `/query/logs` | `query_logs` | Body-based query, supports multiple `logs` in one call. Async: may return `202` with a polling link instead of immediate results. |
| GET | `/query/{query_id}` | internal — `_poll_until_ready` | Follow the `links[].rel == "self"` href from a `202` response. Expires ~20s after last poll; a stale `query_id` returns `404`. |
| GET | `/query/saved_queries` | `list_saved_queries` | All saved queries |
| POST | `/query/saved_queries` | `create_saved_query` | Body must be wrapped: `{"saved_query": {name, logs, leql: {statement, during}}}`. A flat (unwrapped) body returns `400 "A Json Error occurred mapping the request received"`. |
| DELETE | `/query/saved_queries/{id}` | `delete_saved_query` | Returns `204` empty body |
| PATCH / PUT | `/query/saved_queries/{id}` | not implemented | Out of scope |
| GET | `/query/saved_query/{id}` | `run_saved_query` | Singular `saved_query`, distinct from the plural list/create path. Same async 202+poll behavior as `/query/logs`. |

## Request/response shapes

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
entered is invalid"}`.

**Pending vs. final**: a `202` (still processing) response already
includes an `"events": []` key — empty, not absent. The completion signal
is the `"links"` array: a `rel` of `"Self"` (capital S, matched
case-insensitively) means still-pending; once `"links"` disappears and the
status is `200`, the result is final. See `_poll_until_ready` in
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
`message` is itself a JSON string (parse it — `_format_log_events` in
`mcp_server.py` does this the same way `get_alert_evidence` handles its
`data` field).

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
data (usernames, emails, IPs, actions like password resets). Handle
accordingly.

## Not implemented

- **Pre-Computed Queries** — a distinct tag in the official docs
  (`docs.rapid7.com/insightidr/insightidr-rest-api/`), but no confirmed
  API resource was found under that name.
- Full log/logset management (create/rename/delete) — out of scope.
- `PATCH`/`PUT` on an existing saved query — only create/list/delete are
  implemented.

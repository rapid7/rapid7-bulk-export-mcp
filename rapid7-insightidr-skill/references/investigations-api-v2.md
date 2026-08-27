# InsightIDR Investigations API v2 — Reference

Condensed from the **official** OpenAPI spec (`investigations-api-v2.json` in this
same directory — authoritative source, fetched from
`help.rapid7.com/insightidr/en-us/api/v2/insightidr-api-v2.json`, the JSON
backing `docs.rapid7.com`'s ReDoc page). Consult this file before assuming an
endpoint doesn't exist or guessing a parameter name — check the JSON directly
for anything not covered here.

Base URL: `https://{region}.api.insight.rapid7.com` (see `src/config.py`'s
`IDR_BASE_ENDPOINTS`). All operations require header
`Accept-version: investigations-preview` (preview API).

**A caught bug**: `listInvestigations`'s status filter query param is
**`statuses`** (plural), not `status`. Sending `status=` is silently ignored
by the API (no error — it just returns unfiltered results). This actually
happened in this codebase; see `src/insightidr_manager.py::list_investigations`
and the fix noted in `WHATS_NEW_INSIGHTIDR.md`. Treat this as a general
lesson: **verify query param names against this spec, don't assume from a
"reasonable" guess** — a silently-ignored filter fails without any error to
notice.

## Endpoints

| Method | Path | operationId | Implemented as | Notes |
|---|---|---|---|---|
| GET | `/idr/v2/investigations` | `listInvestigations` | `list_investigations` | Query params below |
| POST | `/idr/v2/investigations` | `createInvestigation` | — not implemented | Body: `CreateInvestigationRequestV2` |
| POST | `/idr/v2/investigations/_search` | `searchInvestigations` | — not implemented | Richer filtering than `listInvestigations` — see Search fields below |
| POST | `/idr/v2/investigations/bulk_close` | `bulkCloseInvestigations` | — deliberately NOT implemented | Filter-based bulk close — see "Why bulk_close is intentionally unused" below |
| GET | `/idr/v2/investigations/{id}` | `getInvestigationById` | `get_investigation` | `id` accepts either short id or full rrn |
| PATCH | `/idr/v2/investigations/{id}` | `updateInvestigation` | — not implemented | Multi-field update in one call; could replace several single-field tools below |
| PUT | `/idr/v2/investigations/{id}/status/{status}` | `setStatus` | `close_investigation` (status hardcoded to `CLOSED`) | Body: `{disposition, threat_command_close_reason?, threat_command_free_text?}` |
| PUT | `/idr/v2/investigations/{id}/priority/{priority}` | `setPriority` | — not implemented | No request body |
| PUT | `/idr/v2/investigations/{id}/disposition/{disposition}` | `setDisposition` | — not implemented | No request body |
| PUT | `/idr/v2/investigations/{id}/assignee` | `assignUserToInvestigation` | `assign_investigation` | Body: `{user_email_address}` |
| GET | `/idr/v2/investigations/{identifier}/alerts` | `listInvestigationAlerts` | `list_investigation_alerts` | Alert metadata only, not event evidence |
| DELETE | `/idr/v2/investigations/{identifier}/alerts/{alertRrn}` | `removeAlertFromInvestigation` | — not implemented | Unlink an alert from an investigation |
| GET | `/idr/v2/investigations/{identifier}/rapid7-product-alerts` | `getInvestigationRapid7ProductAlertInfo` | — not implemented | Alerts from OTHER Rapid7 products (Threat Command, Insight Agent) tied to this investigation — separate from InsightIDR's own alerts |

Alert evidence (`get_alert_evidence`) uses a **different** API path entirely —
`/idr/at/alerts/{alert_rrn}/evidences` (alert-triage API) — not present in
this spec at all. That endpoint was found via community/forum research, not
an official spec; treat it as less stable than everything in this document.

## `listInvestigations` query parameters

| Param | Type | Notes |
|---|---|---|
| `index` | int, ≥0, default 0 | Page index |
| `size` | int, 1-100, default 20 | Page size |
| `statuses` | comma-separated string | **Not `status`** — see caught bug above |
| `sources` | comma-separated string | e.g. `USER,ALERT` (spec example) — actual enum per `InvestigationV2.source` is `MANUAL,HUNT,ALERT` |
| `priorities` | comma-separated string | `UNSPECIFIED,LOW,MEDIUM,HIGH,CRITICAL` |
| `assignee.email` | string | Exact match, single email |
| `start_time` / `end_time` | ISO timestamp | Filters on `created_time`; default window is the last 28 days if omitted |
| `sort` | string | `field,DIRECTION` — sortable: `created_time`, `priority`, `rrn`, `alerts_most_recent_created_time`, `alerts_most_recent_detection_created_time`. Default: `priority,DESC` |
| `tags` | comma-separated string | ALL given tags must be present |
| `multi-customer` | bool | MSP-style cross-org access; requires a user API key, not covered by this server |

**Default sort is by priority, not creation time** — if a user asks for "the newest" or "most recent" investigations, you likely need `sort=created_time,DESC` once this is exposed as a tool parameter, not just take the first page as-is.

## `_search` (searchInvestigations) — searchable/sortable fields

More powerful filtering than `listInvestigations`, not yet exposed as a tool. Useful to know exists for a future "find investigations mentioning host X" or "actor Y" kind of request:

| Field | Searchable | Sortable | Operator |
|---|---|---|---|
| `actor_asset_hostname` | Yes | No | CONTAINS |
| `actor_user_name` | Yes | No | CONTAINS |
| `alert_mitre_t_codes` | Yes | No | EQUALS |
| `alert_rule_rrn` | Yes | No | EQUALS |
| `alerts_most_recent_created_time` | No | Yes | — |
| `alerts_most_recent_detection_created_time` | No | Yes | — |
| `assignee_id` | Yes | No | EQUALS |
| `created_time` | No | Yes | — |
| `organization_id` | Yes | No | EQUALS |
| `priority` | Yes | Yes | EQUALS |
| `rrn` | Yes | Yes | EQUALS |
| `source` | Yes | No | EQUALS |
| `status` | Yes | No | EQUALS |
| `title` | Yes | No | CONTAINS |

`title` + CONTAINS is notable: `listInvestigations` has no free-text title search at all (per the skill's existing "Investigating a specific alert type" pattern, which works around this by filtering client-side) — `_search` actually can do this server-side. Worth implementing if title search becomes a frequent need.

## Enums (from `InvestigationV2` and request schemas)

| Field | Values |
|---|---|
| `status` | `OPEN`, `INVESTIGATING`, `WAITING`, `CLOSED` |
| `priority` | `UNSPECIFIED`, `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `disposition` | `UNDECIDED` (default on creation), `BENIGN`, `MALICIOUS`, `NOT_APPLICABLE` |
| `source` | `MANUAL`, `HUNT`, `ALERT` |

## Why `bulk_close` is intentionally unused

`bulkCloseInvestigations` closes **every investigation matching filter
criteria** (source, alert_type, detection_rule_rrn, a time range, an optional
max-count cap) — not a specific set of IDs. This server's `close_investigation`
deliberately uses the per-ID `setStatus` endpoint instead, so closing always
targets one explicit, already-reviewed investigation. Do not propose using
`bulk_close` as a shortcut for closing several investigations the user has
reviewed — loop `close_investigation` (with its confirm flow) per ID instead.
See `WHATS_NEW_INSIGHTIDR.md` (Phase 1 entry) for the original reasoning.

## `updateInvestigation` (PATCH) — a possible future consolidation

Can set `title`, `status`, `priority`, `disposition`, and `assignee`
(`{"email": "..."} ` or `{"email": null}` to unassign) in a single call, plus
Threat Command close-reason fields. This server currently uses separate
single-purpose calls (`close_investigation` for status+disposition,
`assign_investigation` for assignee). If a future request needs to change
multiple fields atomically (e.g. "reassign AND bump priority"), this endpoint
is the right one to reach for instead of chaining several single-field calls.

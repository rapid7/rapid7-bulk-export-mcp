# InsightIDR Comments API v1 — Reference

Condensed from the official OpenAPI spec, fetched via the same `spec-url`
trick as the v2 investigations spec: `help.rapid7.com/insightidr/en-us/api/v1/docs.html`
→ `spec-url="insightidr-api-v1.json"` → full spec at
`help.rapid7.com/insightidr/en-us/api/v1/insightidr-api-v1.json`. Saved
verbatim as `insightidr-api-v1.json` in this directory.

**Unlike Investigations v2, Comments is stable v1 — no `Accept-version`
header required.** Same base URL (`https://{region}.api.insight.rapid7.com`)
and `X-Api-Key` auth as everything else.

## Endpoints

| Method | Path | operationId | Implemented as | Notes |
|---|---|---|---|---|
| GET | `/idr/v1/comments` | `listComments` | `list_comments` | Requires `target` query param |
| POST | `/idr/v1/comments` | `createComment` | `create_comment` | Body: `{target, body, attachments?}` |
| GET | `/idr/v1/comments/{rrn}` | `getComment` | — not implemented | Fetch a single comment by its own rrn |
| DELETE | `/idr/v1/comments/{rrn}` | `deleteComment` | — not implemented (deliberately, this iteration — see WHATS_NEW_INSIGHTIDR.md) | |
| PUT | `/idr/v1/comments/{rrn}/{visibility}` | `updateComment` | — not implemented | `visibility` path param: `INTERNAL` or `PUBLIC` |

## `listComments` query parameters

| Param | Type | Notes |
|---|---|---|
| `target` | string, **required** | The RRN of the resource to list comments for — e.g. an investigation's rrn. Not optional, unlike most list endpoints elsewhere in this API family. |
| `index` | int, ≥0, default 0 | Page index |
| `size` | int, 1-100, default 20 | Page size |
| `sortDirection` | `ASC`/`DESC`, default `DESC` | By creation time; default is newest-first |

## `Comment` object (response shape)

| Field | Notes |
|---|---|
| `rrn` | The comment's own RRN — needed for get/delete/visibility-update, not the same as `target` |
| `target` | The RRN of the thing this comment is attached to |
| `creator` | `{type: "USER"\|"ORG_API_KEY"\|"SYSTEM", name}` — a comment created by this server's API key shows `type: "ORG_API_KEY"`, not a real user's name |
| `body` | Comment text |
| `visibility` | `INTERNAL` or `PUBLIC` — **not settable at creation time**, only via the separate `updateComment` (PUT .../{visibility}) call |
| `created_time` | ISO timestamp |
| `attachments` | Array of attached files, if any |

`CommentCreateRequest` (the POST body) only accepts `target`, `body`, and an
optional `attachments` array of attachment RRNs — there is no `visibility`
field on creation. If a future request needs to control visibility at
creation time, that requires a follow-up `updateComment` call right after
creating the comment, not a single combined call.

## Not yet cataloged in this reference

`insightidr-api-v1.json` also contains specs for **Accounts**, **Assets**
(including local accounts), **Attachments**, **Custom Threats**,
**Collectors**, **Cloud Webhooks**, and **Users** (`/idr/v1/users/*` —
confirmed to be **UBA-tracked network identities**, `rrn:uba:...`, e.g. a
monitored employee's activity profile — NOT Rapid7 platform login accounts.
This is a different "users" concept from `list_investigation_assignees`;
it does not solve the "who can be assigned an investigation" problem — see
`investigations-api-v2.md`'s note on that). None of these are condensed
here yet since this iteration only needed Comments — read the raw JSON
directly if one of these becomes relevant.

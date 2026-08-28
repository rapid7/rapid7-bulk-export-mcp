# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

MCP server exposing two Rapid7 product integrations to AI assistants (Claude Desktop, Claude Code, Kiro):

1. **InsightVM Bulk Export** — exports vulnerability data via the Bulk Export GraphQL API, loads Parquet into DuckDB for SQL analysis.
2. **InsightIDR** — SIEM/XDR investigations, alert evidence, comments, assignment, and Log Search (LEQL), via REST.

Both share one `RAPID7_API_KEY` / `RAPID7_REGION` credential pair and one Command Platform base URL pattern (`https://{region}.api.insight.rapid7.com`). See `AGENTS.md` for the full InsightVM architecture reference (data directory conventions, export lifecycle, DuckDB table routing, Docker sandbox details) — it is not repeated here.

## Commands

```bash
uv sync                          # install dependencies
uv run rapid7-mcp-server         # run the server (stdio)
.venv/bin/rapid7-mcp-server      # run via venv entry point (required for Claude Desktop config)

make test                        # unit tests + Docker integration test
uv run pytest tests/test_insightidr_manager.py -v   # run a single test file
uv run pytest tests/test_insightidr_manager.py::TestClassName::test_name -v  # single test

make lint                        # ruff check + format check
make lint-fix                    # auto-fix lint/format issues
make security                    # bandit scan
make check-idr-spec FORCE=1      # check InsightIDR OpenAPI spec for drift (run before touching InsightIDR server code)
```

Full command reference (versioning, packaging, release, Docker) is in `AGENTS.md`.

## Architecture: InsightIDR integration

| Module | Responsibility |
|---|---|
| `src/insightidr_client.py` | Authenticated REST HTTP client (JSON, analogous to `graphql_client.py` for GraphQL) |
| `src/insightidr_manager.py` | Investigations v2 + Comments v1 business logic (list/get/close investigations, assignment, comments, cross-product alerts, alert evidence) |
| `src/insightidr_log_search_manager.py` | Log Search API business logic (LEQL queries, saved queries, log/logset listing) |
| `src/mcp_server.py` | FastMCP tool definitions for both InsightVM and InsightIDR |

### Log Search async polling

`POST /query/logs` and `GET /query/saved_query/{id}` are asynchronous: a `202` response already contains `"events": []` — an empty list, not an absent key. Completion is signaled only by the disappearance of the `links` self-href, never by the presence of `"events"`. This is implemented in `_poll_until_ready()` in `insightidr_log_search_manager.py` and mirrored by the `_log_query_still_pending()` helper in `mcp_server.py`.

LEQL gotcha: `statement=""` (empty string) matches every event with no filter. `statement="where()"` (empty parentheses) is invalid syntax and returns `400`.

### Two-step write safety

Every InsightIDR write operation (`close_investigation`, `assign_investigation`, `create_saved_query`, `delete_saved_query`) requires a `confirm` parameter: called without it, the tool returns a preview of the change; the caller must re-call with `confirm=True` to execute. `add_investigation_comment` is the one exception — it doesn't change investigation state, so no confirm step. Investigations are never closed in bulk by filter — only by a single explicit ID.

### API reference docs

`rapid7-insightidr-skill/references/` holds the authoritative API specs consulted when adding InsightIDR tools:
- `investigations-api-v2.md` / `.json` — official Rapid7 OpenAPI spec (preview API, `Accept-version: investigations-preview` header required)
- `comments-api-v1.md` / `insightidr-api-v1.json` — Comments API (stable, no version header)
- `log-search-api.md` — hand-curated reference (no official OpenAPI spec exists for this API)

The JSON spec copy is kept in sync via `make check-idr-spec` (weekly gate, `FORCE=1` to bypass). The `.md` reference is hand-curated and not auto-regenerated. **Run `make check-idr-spec FORCE=1` before changing `src/insightidr_manager.py` or `src/insightidr_client.py`.**

A parallel copy of the skill lives at `.claude/skills/rapid7-insightidr-investigations-expert/` — keep it synced with `rapid7-insightidr-skill/` after editing skill docs (`rm -rf` + `cp -r`, then `diff -rq` to verify).

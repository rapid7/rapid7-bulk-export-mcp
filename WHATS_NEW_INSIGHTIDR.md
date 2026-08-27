# Що нового: інтеграція з InsightIDR / What's New: InsightIDR Integration

*Зміни, специфічні для розширення InsightIDR — не для форку в цілому. / Changes specific to the InsightIDR extension — not the fork as a whole.*

## Про проєкт / About

| Українською | English |
|---|---|
| Форк `rapid7/rapid7-bulk-export-mcp`, що додає підтримку **Rapid7 InsightIDR** (SIEM/XDR) поруч з наявним InsightVM Bulk Export функціоналом. Той самий MCP-сервер, той самий `RAPID7_API_KEY`/`RAPID7_REGION`. | Fork of `rapid7/rapid7-bulk-export-mcp` adding **Rapid7 InsightIDR** (SIEM/XDR) support alongside the existing InsightVM Bulk Export functionality. Same MCP server, same `RAPID7_API_KEY`/`RAPID7_REGION`. |
| InsightVM (vulnerability data, GraphQL) і InsightIDR (investigations/alerts/logs, REST) share the same Command Platform API key and region — one server, one credential, both products. | InsightVM (vulnerability data, GraphQL) and InsightIDR (investigations/alerts/logs, REST) share the same Command Platform API key and region — one server, one credential, both products. |

## Компоненти / Components

```
AI assistant → src/mcp_server.py (@mcp.tool)
             → src/insightidr_manager.py (Investigations + Comments)
             → src/insightidr_log_search_manager.py (Log Search / LEQL)
             → src/insightidr_client.py (REST transport: X-Api-Key, Accept-version)
             → Rapid7 InsightIDR REST API (/idr/v2/*, /idr/at/*, /idr/v1/*, /log_search/*)

src/config.py: load_config() → idr_base (regional base URL), shared with InsightVM's endpoint.
```

| Файл | Українською | English |
|---|---|---|
| `src/config.py` | `IDR_BASE_ENDPOINTS` — регіональна мапа base URL для Command Platform, окрема від GraphQL `REGION_ENDPOINTS`. | `IDR_BASE_ENDPOINTS` — regional base-URL map for the Command Platform, separate from GraphQL's `REGION_ENDPOINTS`. |
| `src/insightidr_client.py` | REST-транспорт (аналог `graphql_client.py`): заголовки, `Accept-version`. Не знає про бізнес-логіку. | REST transport (analogous to `graphql_client.py`): headers, `Accept-version`. No business logic. |
| `src/insightidr_manager.py` | Investigations v2 + Comments v1: список/деталі/закриття/призначення/коментарі. Валідація статусів/dispositions. | Investigations v2 + Comments v1: list/detail/close/assign/comments. Status/disposition validation. |
| `src/insightidr_log_search_manager.py` | Log Search API (LEQL): окрема API-родина, інший базовий шлях, async polling. | Log Search API (LEQL): a separate API family, different base path, async polling. |
| `src/mcp_server.py` | `@mcp.tool` визначення, форматування відповідей, safety-flow для write-операцій. | `@mcp.tool` definitions, response formatting, safety flow for write operations. |
| `rapid7-insightidr-skill/` | Agent skill: SKILL.md, README.md, `references/*` (офіційні OpenAPI-специ + конденсовані нотатки). | Agent skill: SKILL.md, README.md, `references/*` (official OpenAPI specs + condensed notes). |
| `scripts/check_idr_api_spec.py` | Dev-only: перевіряє офіційний spec на зміни. Не викликається сервером у рантаймі. | Dev-only: checks the official spec for changes. Not called by the server at runtime. |

## Можливості / Capabilities

| Область | MCP tools |
|---|---|
| Investigations | `get_investigations`, `get_investigation_details`, `close_investigation` (confirm required) |
| Alert evidence | `get_alert_evidence` — raw source event (user, IP, geolocation, service, result) |
| Assignment | `list_investigation_assignees`, `assign_investigation` (confirm required) |
| Comments | `list_investigation_comments`, `add_investigation_comment` |
| Cross-product alerts | `get_investigation_product_alerts` (Threat Command, Insight Agent) |
| Log Search | `list_logs`, `list_logsets`, `query_logs` (LEQL) |
| Saved queries | `list_saved_queries`, `run_saved_query`, `create_saved_query`/`delete_saved_query` (confirm required) |

Дизайнові принципи / Design principles:
- Investigations **ніколи** не закриваються автоматично — обов'язковий two-step preview/confirm для всіх write-операцій, окрім `add_investigation_comment` (не змінює стан). / Investigations are **never** closed automatically — mandatory two-step preview/confirm for all write operations except `add_investigation_comment` (doesn't change state).
- Close використовує точковий `/status/CLOSED` (один ID), а не `bulk_close` (за фільтром). / Close uses the targeted `/status/CLOSED` (one ID), not `bulk_close` (filter-based).
- `get_alert_evidence`/`query_logs`/`run_saved_query` повертають реальні персональні дані — застереження в docstring і SKILL.md. / `get_alert_evidence`/`query_logs`/`run_saved_query` return real personal data — flagged in docstrings and SKILL.md.

## Виправлені баги / Bugs found and fixed

| Баг | Причина | Виправлення |
|---|---|---|
| `list_investigations(status=...)` не фільтрував нічого | API-параметр — `statuses` (множина), не `status`; невідомий параметр мовчки ігнорується | Виправлено query-параметр, підтверджено наживо (7/7 → 3/7 для CLOSED) |
| `query_logs`/`run_saved_query` завжди повертали 0 подій | Pending (`202`) відповідь вже містить `"events": []`; поллінг зупинявся одразу | Поллінг тепер триває, доки не зникне `links`-self, а не за наявністю `events` |
| `400 Invalid Query Syntax` на LEQL-запитах | `statement="where()"` — невалідний синтаксис | Використовувати `statement=""` для "усі події без фільтра" |

| Bug | Cause | Fix |
|---|---|---|
| `list_investigations(status=...)` filtered nothing | API param is `statuses` (plural), not `status`; unknown params are silently ignored | Fixed query param, confirmed live (7/7 → 3/7 for CLOSED) |
| `query_logs`/`run_saved_query` always returned 0 events | A pending (`202`) response already contains `"events": []`; polling stopped immediately | Polling now continues until `links` self disappears, not until `events` appears |
| `400 Invalid Query Syntax` on LEQL queries | `statement="where()"` is invalid syntax | Use `statement=""` for "all events, no filter" |

## API references used / Використані API-специфікації

- Investigations v2 (preview, `Accept-version: investigations-preview`) — official OpenAPI spec, `rapid7-insightidr-skill/references/investigations-api-v2.json`.
- Comments v1 (stable) — official OpenAPI spec, `references/insightidr-api-v1.json`.
- Alert Evidence (`/idr/at/alerts/{id}/evidences`) — undocumented in any spec, found via Rapid7 community forum, verified live.
- Log Search — no official OpenAPI spec exists; built from Rapid7's blog post, Rapid7's official Postman collection (`rapid7/logentries-postman-collection`), and a community spec, cross-checked and verified live. Notably: `POST /query/saved_queries` requires the body wrapped in `{"saved_query": {...}}`, found only in the Postman collection.

## Тести / Tests

147 unit tests, `responses`-mocked (no live credentials required). `make lint` (ruff), `make security` (bandit), `make test` — all clean.

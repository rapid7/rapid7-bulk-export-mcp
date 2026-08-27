# Що нового: інтеграція з InsightIDR / What's New: InsightIDR Integration

*Цей файл відстежує зміни, повʼязані саме з розширенням сервера підтримкою Rapid7 InsightIDR — новий функціонал, а не форк в цілому. / This file tracks changes specific to the server's InsightIDR extension — the new functionality, not the fork as a whole.*

## Про проєкт / About the Project

| Українською | English |
|---|---|
| Цей репозиторій — форк офіційного `rapid7/rapid7-bulk-export-mcp`: гібридний MCP-сервер (Model Context Protocol), який дозволяє AI-асистентам (Claude Desktop, Claude Code, Kiro тощо) працювати з даними Rapid7 напряму через природну мову. | This repository is a fork of the official `rapid7/rapid7-bulk-export-mcp`: a hybrid MCP (Model Context Protocol) server that lets AI assistants (Claude Desktop, Claude Code, Kiro, etc.) work with Rapid7 data directly through natural language. |
| Базовий (upstream) функціонал: експорт даних вразливостей з **Rapid7 InsightVM** через Bulk Export API (GraphQL), завантаження в локальну DuckDB-базу та SQL-аналіз через MCP-інструменти. | Baseline (upstream) functionality: exporting vulnerability data from **Rapid7 InsightVM** via the Bulk Export API (GraphQL), loading it into a local DuckDB database, and SQL analysis through MCP tools. |
| Наше розширення: додаємо підтримку **Rapid7 InsightIDR** (SIEM/XDR) — роботу з investigations (розслідуваннями інцидентів) через REST Command Platform API, той самий сервер, той самий ключ доступу. | Our extension: adding support for **Rapid7 InsightIDR** (SIEM/XDR) — working with investigations (incident investigations) via the REST Command Platform API, same server, same access key. |
| Розгортання: локально, поруч із Claude Desktop / Claude Code, запуск через `uv`, транспорт `stdio`. | Deployment: local, alongside Claude Desktop / Claude Code, launched via `uv`, `stdio` transport. |
| Автентифікація: єдиний ключ `RAPID7_API_KEY` + `RAPID7_REGION` (Organization Key з Command Platform) — без окремих секретів для кожного продукту Rapid7. | Authentication: a single `RAPID7_API_KEY` + `RAPID7_REGION` (an Organization Key from the Command Platform) — no separate secrets per Rapid7 product. |

## Обґрунтування / Rationale

| Українською | English |
|---|---|
| Управління вразливостями (InsightVM) і виявлення/розслідування загроз (InsightIDR) — дві частини одного циклу безпеки, які часто аналізуються разом (наприклад: чи є критична вразливість на активі, де щойно зафіксовано підозрілу активність). | Vulnerability management (InsightVM) and threat detection/investigation (InsightIDR) are two parts of the same security lifecycle, often analyzed together (e.g. whether a critical vulnerability exists on an asset where suspicious activity was just detected). |
| Об'єднання обох продуктів в одному MCP-сервері дозволяє AI-асистенту відповідати на такі наскрізні питання без перемикання між інструментами чи вікнами. | Combining both products in a single MCP server lets the AI assistant answer such cross-cutting questions without switching between tools or windows. |
| Command Platform API вже надає єдиний ендпоінт, автентифікацію і дизайн для обох продуктів — розширення наявного клієнта природніше й дешевше, ніж створення окремого сервера з нуля. | The Command Platform API already provides a single endpoint, authentication, and design shared across both products — extending the existing client is more natural and cheaper than building a separate server from scratch. |
| Форк офіційного open-source репозиторію Rapid7 дає готову інфраструктуру (конфіг, тестування, пакування, DuckDB-шар) — розширюємо перевірений код, а не пишемо MCP-сервер з нуля. | Forking Rapid7's official open-source repository provides ready-made infrastructure (config, testing, packaging, DuckDB layer) — we extend proven code instead of writing an MCP server from scratch. |

## Структура компонентів та залежності / Component Structure & Dependencies

Потік виклику для будь-якого InsightIDR MCP-інструменту, зверху вниз (АІ-асистент → Rapid7 API) і назад: / The call flow for any InsightIDR MCP tool, top to bottom (AI assistant → Rapid7 API) and back:

```
AI-асистент (Claude Desktop / Claude Code)
    │  викликає MCP tool: get_investigations / get_investigation_details /
    │  get_alert_evidence / close_investigation
    ▼
src/mcp_server.py
    │  @mcp.tool функції: приймають аргументи від AI, форматують відповідь
    │  у людяний текст, ловлять помилки. Для close_investigation — тут
    │  живе обов'язковий preview/confirm safety-flow.
    ▼
src/insightidr_manager.py
    │  бізнес-логіка: list_investigations, get_investigation,
    │  list_investigation_alerts, get_alert_evidence, close_investigation.
    │  Валідує статуси (VALID_STATUSES) і dispositions (VALID_DISPOSITIONS)
    │  до того, як щось піде в мережу.
    ▼
src/insightidr_client.py
    │  send_idr_request(): REST-транспорт — X-Api-Key, Content-Type,
    │  User-Agent (з config.py), опційний Accept-version для preview API.
    ▼
Rapid7 InsightIDR REST API  (Command Platform, /idr/v2/*, /idr/at/*)
    ▲
    │  base URL (idr_base) + RAPID7_API_KEY
src/config.py
    load_config(): читає RAPID7_API_KEY (env → macOS Keychain fallback) і
    RAPID7_REGION, будує idr_base = IDR_BASE_ENDPOINTS[region].
```

| Компонент | Українською | English |
|---|---|---|
| `src/config.py` | Єдина точка входу для credentials. `IDR_BASE_ENDPOINTS` — регіональна мапа base URL, окрема від `REGION_ENDPOINTS` (GraphQL Bulk Export). Використовується і InsightVM-, і InsightIDR-частиною. | Single entry point for credentials. `IDR_BASE_ENDPOINTS` — a regional base-URL map, separate from `REGION_ENDPOINTS` (GraphQL Bulk Export). Used by both the InsightVM and InsightIDR parts. |
| `src/insightidr_client.py` | Не знає нічого про investigations/alerts — лише вміє відправити REST-запит з правильними заголовками. Аналог `graphql_client.py`, але для JSON REST замість GraphQL. Не залежить від `insightidr_manager.py`. | Knows nothing about investigations/alerts — it only knows how to send a REST request with the right headers. Analogous to `graphql_client.py`, but for JSON REST instead of GraphQL. Does not depend on `insightidr_manager.py`. |
| `src/insightidr_manager.py` | Тут живуть правила: які статуси/dispositions валідні, який шлях API для кожної дії (наприклад, закриття — через точковий `/status/CLOSED`, а не `bulk_close`). Залежить лише від `insightidr_client.py`. | This is where the rules live: which statuses/dispositions are valid, which API path each action uses (e.g. closing goes through the targeted `/status/CLOSED`, not `bulk_close`). Depends only on `insightidr_client.py`. |
| `src/mcp_server.py` | Єдиний файл, який АІ-асистент бачить напряму — оголошує tools через `@mcp.tool`. Залежить від `insightidr_manager.py` (бізнес-логіка) і `config.py` (credentials). | The only file the AI assistant sees directly — declares tools via `@mcp.tool`. Depends on `insightidr_manager.py` (business logic) and `config.py` (credentials). |
| `rapid7-insightidr-skill/SKILL.md` + `README.md` | Не код — документація для АІ-асистента: як і коли викликати tools з `mcp_server.py`, safety-flow, обробка помилок. Оновлюється вручну щоразу, коли зʼявляється новий tool. | Not code — documentation for the AI assistant: how and when to call the tools from `mcp_server.py`, the safety flow, error handling. Updated by hand whenever a new tool appears. |

**Ключова залежність**: `RAPID7_API_KEY` + `RAPID7_REGION` — спільні для InsightVM (GraphQL) і InsightIDR (REST) частин сервера; одна зміна в `config.py` (`load_config()`) впливає на обидві. / **Key dependency**: `RAPID7_API_KEY` + `RAPID7_REGION` are shared between the InsightVM (GraphQL) and InsightIDR (REST) parts of the server; one change in `config.py` (`load_config()`) affects both.

## Історія змін / Changelog

### 2026-08-27 — Розгортання та тестування базового сервера / Baseline server deployment and testing

| Українською | English |
|---|---|
| Встановлено `uv`, локальне (поза Google Drive) віртуальне середовище для запуску сервера — обхід проблеми `Operation not permitted` при виконанні бінарника з тому Google Drive Desktop. | Installed `uv`, a local (outside Google Drive) virtual environment to run the server — works around an `Operation not permitted` issue when executing a binary from a Google Drive Desktop mount. |
| Підключено MCP-сервер `rapid7-bulk-export` (наявний, немодифікований функціонал InsightVM) до Claude Desktop і до цієї сесії Claude Code. | Connected the `rapid7-bulk-export` MCP server (existing, unmodified InsightVM functionality) to Claude Desktop and to this Claude Code session. |
| Ключ `RAPID7_API_KEY` збережено в macOS Keychain (не в конфігах); `RAPID7_REGION=eu` — у конфігах MCP-клієнтів. | Stored `RAPID7_API_KEY` in macOS Keychain (not in config files); `RAPID7_REGION=eu` — in the MCP client configs. |
| Наскрізно протестовано наявний функціонал: створення export job, очікування завершення, завантаження Parquet у DuckDB, SQL-запити з JOIN — на реальних даних організації. | End-to-end tested the existing functionality: creating an export job, waiting for completion, loading Parquet into DuckDB, SQL queries with JOINs — against the organization's real data. |

### 2026-08-27 — Інтеграція з InsightIDR, Phase 1: Investigations (читання + закриття) / InsightIDR Integration, Phase 1: Investigations (read + close)

| Українською | English |
|---|---|
| Звірено реальну специфікацію InsightIDR Investigations API v2 (preview) з офіційної OpenAPI-схеми: base URL `https://{region}.api.insight.rapid7.com`, шлях `/idr/v2/investigations`, заголовок `Accept-version: investigations-preview`. | Verified the real InsightIDR Investigations API v2 (preview) spec against the official OpenAPI schema: base URL `https://{region}.api.insight.rapid7.com`, path `/idr/v2/investigations`, header `Accept-version: investigations-preview`. |
| `src/config.py`: додано `IDR_BASE_ENDPOINTS` і ключ `idr_base` у `load_config()` — регіональний base URL для Command Platform Capability APIs, окремо від GraphQL-ендпоінту Bulk Export. | `src/config.py`: added `IDR_BASE_ENDPOINTS` and an `idr_base` key in `load_config()` — the regional base URL for Command Platform Capability APIs, separate from the Bulk Export GraphQL endpoint. |
| Новий файл `src/insightidr_client.py`: REST-транспорт (`requests` + `X-Api-Key` + опційний `Accept-version`) — аналог `graphql_client.py`, але для JSON REST замість GraphQL. | New file `src/insightidr_client.py`: REST transport (`requests` + `X-Api-Key` + optional `Accept-version`) — analogous to `graphql_client.py`, but for JSON REST instead of GraphQL. |
| Новий файл `src/insightidr_manager.py`: бізнес-логіка `list_investigations`, `get_investigation`, `list_investigation_alerts`, `close_investigation`; валідація статусів (`OPEN/INVESTIGATING/WAITING/CLOSED`) і dispositions (`BENIGN/MALICIOUS/NOT_APPLICABLE`). | New file `src/insightidr_manager.py`: business logic for `list_investigations`, `get_investigation`, `list_investigation_alerts`, `close_investigation`; validation of statuses (`OPEN/INVESTIGATING/WAITING/CLOSED`) and dispositions (`BENIGN/MALICIOUS/NOT_APPLICABLE`). |
| Закриття investigation навмисно реалізовано через точковий ендпоінт `/investigations/{id}/status/CLOSED` (по одному явному ID), а не через `bulk_close` (закриває все за фільтром) — щоб унеможливити випадкове масове закриття. | Closing an investigation deliberately uses the targeted `/investigations/{id}/status/CLOSED` endpoint (one explicit ID at a time), not `bulk_close` (which closes everything matching a filter) — to make accidental mass-closing impossible. |
| Новий MCP-інструмент `get_investigations` — список/пошук investigations з фільтрами (status, priorities, assignee_email). | New MCP tool `get_investigations` — lists/searches investigations with filters (status, priorities, assignee_email). |
| Новий MCP-інструмент `get_investigation_details` — повний запис одного investigation + пов'язані alerts (докази). | New MCP tool `get_investigation_details` — the full record for one investigation plus its associated alerts (evidence). |
| Новий MCP-інструмент `close_investigation` — write-операція з обов'язковим двоетапним підтвердженням: перший виклик (`confirm=False`, за замовчуванням) лише показує превʼю й нічого не змінює; зміна відбувається тільки при явному `confirm=True`. | New MCP tool `close_investigation` — a write operation with mandatory two-step confirmation: the first call (`confirm=False`, the default) only shows a preview and changes nothing; the change happens only with an explicit `confirm=True`. |
| Тести: `tests/test_insightidr_client.py`, `tests/test_insightidr_manager.py` (табличні, за стилем наявних тестів), оновлено `tests/test_config.py` під новий ключ конфігу `idr_base`. | Tests: `tests/test_insightidr_client.py`, `tests/test_insightidr_manager.py` (table-driven, matching the existing test style), updated `tests/test_config.py` for the new `idr_base` config key. |
| `make lint`, `make security` (bandit), `make test` — усі чисті на момент цього запису. | `make lint`, `make security` (bandit), `make test` — all clean as of this entry. |
| Наскрізно протестовано на реальних даних організації: список 7 investigations, повна деталізація однієї (з alert-доказом), preview закриття без фактичної зміни стану. | End-to-end tested against the organization's real data: listing 7 investigations, full detail on one (with its alert evidence), a close preview with no actual state change. |
| Новий файл `rapid7-insightidr-skill/SKILL.md` — agent skill для аналітики InsightIDR: довідник статусів/dispositions, обов'язковий safety-flow для закриття, типові паттерни тріажу, error handling. | New file `rapid7-insightidr-skill/SKILL.md` — an agent skill for InsightIDR analysis: a reference for statuses/dispositions, the mandatory close safety flow, common triage patterns, error handling. |
| Авторство (`rozumeyroman@gmail.com`) вказано в `SKILL.md` та в докстрінгах нових файлів: `src/insightidr_client.py`, `src/insightidr_manager.py`, `tests/test_insightidr_client.py`, `tests/test_insightidr_manager.py`. | Authorship (`rozumeyroman@gmail.com`) noted in `SKILL.md` and in the docstrings of the new files: `src/insightidr_client.py`, `src/insightidr_manager.py`, `tests/test_insightidr_client.py`, `tests/test_insightidr_manager.py`. |
| Новий файл `rapid7-insightidr-skill/README.md` — за аналогією з README InsightVM-скіла: як встановити/використати скіл, приклади, обмеження. Явно перелічує, які MCP tools вже покриті (3), а які ще ні (Log Search, Detection Rules, Comments, assign/priority без закриття) — щоб документація не забігала наперед реалізації. | New file `rapid7-insightidr-skill/README.md` — modeled on the InsightVM skill's README: how to install/use the skill, examples, limitations. Explicitly lists which MCP tools are already covered (3) and which are not yet (Log Search, Detection Rules, Comments, assign/priority without closing) — so the documentation doesn't get ahead of the implementation. |
| Локальна тестова інфраструктура (не комітиться, у `.gitignore`): `.mcp.json` (MCP-конфіг для Claude Code) та `.claude/skills/` (встановлені скіли для цієї сесії). | Local test infrastructure (not committed, in `.gitignore`): `.mcp.json` (MCP config for Claude Code) and `.claude/skills/` (skills installed for this session). |

### 2026-08-27 — Деталізація подій: новий інструмент `get_alert_evidence` / Event-level detail: new `get_alert_evidence` tool

| Українською | English |
|---|---|
| Після тестування в Claude Desktop виявилось, що `get_investigation_details` повертає лише метадані алертів (тип, час, detection rule) — без IP, користувача чи результату автентифікації. | Testing in Claude Desktop showed that `get_investigation_details` only returns alert metadata (type, timestamp, detection rule) — no IP, user, or authentication result. |
| Досліджено й експериментально перевірено (одним безпечним GET-запитом з реальними даними) окремий ендпоінт alert-triage API: `GET /idr/at/alerts/{alert_rrn}/evidences` — інший шлях (`/idr/at/`), відмінний від Investigations v2 (`/idr/v2/`). | Researched and experimentally verified (one safe GET request against real data) a separate alert-triage API endpoint: `GET /idr/at/alerts/{alert_rrn}/evidences` — a different path (`/idr/at/`) from Investigations v2 (`/idr/v2/`). |
| Спільнота Rapid7 повідомляє, що ця частина API "недорозвинена" і для деяких org потребує окремого увімкнення від підтримки Rapid7 ("restricted evidence") — у нашому org це спрацювало без додаткового звернення. | Rapid7's own community reports this part of the API is "underdeveloped" and for some orgs requires separate enablement from Rapid7 support ("restricted evidence") — in our org it worked without any such request. |
| `src/insightidr_manager.py`: новий `get_alert_evidence(config, alert_id)`. | `src/insightidr_manager.py`: new `get_alert_evidence(config, alert_id)`. |
| Новий MCP-інструмент `get_alert_evidence(alert_id)` — повертає джерело події (user/account, source IP, геолокацію, сервіс, результат) для конкретного alert. | New MCP tool `get_alert_evidence(alert_id)` — returns the source event (user/account, source IP, geolocation, service, result) for a specific alert. |
| **Важливо**: цей виклик повертає реальні персональні дані співробітника (ім'я, email, IP, дані пристрою) — не тестові дані. Додано явне попередження в docstring інструменту та окремий розділ "PRIVACY" у SKILL.md з правилами поводження з такими даними. | **Important**: this call returns a real employee's personal data (name, email, IP, device info) — not test data. Added an explicit warning in the tool's docstring and a dedicated "PRIVACY" section in SKILL.md with rules for handling such data. |
| Тест: `tests/test_insightidr_manager.py::TestGetAlertEvidence`. `make lint`, `make security`, `make test` — усі чисті. | Test: `tests/test_insightidr_manager.py::TestGetAlertEvidence`. `make lint`, `make security`, `make test` — all clean. |
| Оновлено `rapid7-insightidr-skill/SKILL.md` і `README.md`: новий інструмент у списку, приклад використання, розділ про приватність даних. | Updated `rapid7-insightidr-skill/SKILL.md` and `README.md`: new tool listed, usage example, data-privacy section. |

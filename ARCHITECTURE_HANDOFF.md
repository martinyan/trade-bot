# Trade Bot Architecture Handoff

This document is the canonical architecture snapshot for continuing development across different AI models.

## 1) System Diagram

```mermaid
flowchart LR
    U[Discord User] -->|Slash Commands| D[discord-bot]
    D -->|HTTP JSON| S[strategy-engine]
    D -->|HTTP JSON| X[market-dashboard]
    S -->|HTTP JSON| M[market-data-service]
    M -->|REST API| F[FMP API]
    S -->|Redis Set ops| R[(Redis)]
    S -->|Persistence / DSN wired| P[(Postgres)]
    W[scheduler-worker] -->|HTTP scan| S
    W -->|Webhook alerts| H[Discord Webhook]
    P -. init.sql .-> I[infra/sql/init.sql]
```

## 2) Runtime Components

1. `discord-bot`
- Purpose: User-facing command interface in Discord.
- Transport: `discord.py` command events in, HTTP requests to `strategy-engine`, plus direct HTTP requests to `market-dashboard` for dashboard-backed commands.
- Source: `services/discord-bot/app/bot.py`.
- Includes `/13f_delta` for the rolling latest-two-quarter 13F comparison flow.
- Includes `/marketsnap` and `/bullbear`, which intentionally bypass `strategy-engine` and consume the dashboard's cached API directly.
- Can auto-broadcast the market snapshot to a configured Discord channel at 9:35 AM America/New_York on NYSE trading days only.

2. `strategy-engine`
- Purpose: Orchestration and business logic.
- Transport: FastAPI endpoints; upstream HTTP to `market-data-service`; Redis for watchlist state.
- Source: `services/strategy-engine/app/main.py`.

3. `market-data-service`
- Purpose: Integration wrapper over Financial Modeling Prep (FMP) API with light normalization.
- Transport: FastAPI endpoints; upstream HTTP to FMP.
- Source: `services/market-data-service/app/main.py`.

4. `scheduler-worker`
- Purpose: Periodically scans premarket movers and posts webhook alerts.
- Transport: HTTP polling to `strategy-engine`; webhook POST to Discord.
- Source: `services/scheduler-worker/app/main.py`.

5. `market-dashboard` (external service)
- Purpose: Serves the web dashboard plus cached market snapshot and bull/bear APIs.
- Transport: Express HTTP API on port 3000.
- Source: external repo or sibling folder at `/docker/dashboard`; not modified from this monorepo.

6. Datastores
- `Redis`: active storage for `watchlist:{user_id}` sets.
- `Postgres`: provisioned and initialized with `infra/sql/init.sql`.

## 3) Public API Surface

## `market-data-service` (port 8001)
- `GET /health`
- `GET /quote/{symbol}`
- `GET /profile/{symbol}`
- `GET /v1/quotes?symbols=AAPL,MSFT`
- `GET /v1/universe/quotes?exchanges=NASDAQ,NYSE`
- `GET /v1/scan/premarket?limit=10`
- `GET /v1/news/{symbol}?limit=5`
- `GET /news/{symbol}?limit=5` (legacy)
- `GET /v1/insider-trades/{symbol}?page=0&limit=50&include_stats=true`

## `strategy-engine` (port 8002)
- `GET /health`
- `GET /v1/brief?symbol=AAPL`
- `GET /v1/scan/premarket?limit=30`
- `POST /v1/watchlist/add` with body `{ "user_id": "123", "symbol": "TSLA" }`
- `GET /v1/quote-detail?symbol=NVDA`
- `GET /v1/news?symbol=NVDA&limit=5`
- `GET /v1/13f/holdings-delta?symbol=AAPL&limit=20`
- `GET /v1/13f/symbol-map?symbol=AAPL`
- `POST /v1/13f/symbol-map`

## `market-dashboard` (port 3000, external)
- `GET /api/status`
- `GET /api/market`
- `GET /api/bull-bear`

## 4) Command-to-Flow Mapping

1. `/brief`:
- Discord -> `strategy-engine /v1/brief` -> `market-data-service /v1/quotes` -> FMP `quote`.

2. `/quote_detail`:
- Discord -> `strategy-engine /v1/quote-detail` -> `market-data-service /quote/{symbol}` + `/profile/{symbol}` -> FMP.

3. `/scan_premarket`:
- Discord -> `strategy-engine /v1/scan/premarket` -> `market-data-service /v1/universe/quotes` -> FMP `batch-exchange-quote`.

4. `/watch_add`:
- Discord -> `strategy-engine /v1/watchlist/add` -> Redis `SADD watchlist:{user_id}`.

5. `/news`:
- Discord -> `strategy-engine /v1/news` -> `market-data-service /v1/news/{symbol}` -> FMP `news/stock`.

6. Scheduler alerts:
- `scheduler-worker` timer -> `strategy-engine /v1/scan/premarket` -> rank -> Discord webhook.

7. `/marketsnap`:
- Discord -> `market-dashboard /api/market` -> Discord-formatted ETF, sectors, and movers snapshot.

8. `/bullbear`:
- Discord -> `market-dashboard /api/status` -> `market-dashboard /api/bull-bear` -> Discord-formatted grade board + web link.

9. Scheduled `/marketsnap` broadcast:
- `discord-bot` timer -> NYSE trading-day calendar check -> `market-dashboard /api/market` -> channel post in Discord.

## 5) Data Ownership

1. Market quote/news/insider/profile data:
- System of record: FMP API.
- Local role: transient transformation/proxy only.

2. Watchlists:
- System of record: Redis (`watchlist:{user_id}` set).
- Persistence/backup strategy: not yet implemented.

3. Signals table (`strategy_signals`):
- Created in Postgres init script.
- Not currently written/read by running services.

4. Dashboard market snapshot / bull-bear cache:
- System of record: `market-dashboard` in-memory caches backed by Yahoo Finance and Finviz pulls.
- Local role inside this repo: read-only consumption from `discord-bot`.

## 6) Environment Contract (Required)

- `DISCORD_BOT_TOKEN`
- `FMP_API_KEY`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Also used:
- `DISCORD_GUILD_ID`
- `STRATEGY_ENGINE_URL`
- `DASHBOARD_BASE_URL`
- `DASHBOARD_PUBLIC_URL`
- `DASHBOARD_WARMUP_ENABLED`
- `DASHBOARD_WARMUP_POLL_SECONDS`
- `MARKETSNAP_BROADCAST_ENABLED`
- `MARKETSNAP_CHANNEL_ID`
- `MARKETSNAP_HOUR_ET`
- `MARKETSNAP_MINUTE_ET`
- `MARKETSNAP_WINDOW_MINUTES`
- `MARKET_DATA_SERVICE_URL`
- `REDIS_URL`
- `DISCORD_ALERT_WEBHOOK_URL`
- `ALERT_SCAN_INTERVAL_SECONDS`
- `ALERT_TOP_N`
- `ALERT_COOLDOWN_SECONDS`
- `FMP_BASE_URL`

## 7) Current Health Snapshot (2026-03-08 UTC)

1. Runtime sanity:
- `docker compose config` resolves successfully (with a warning that top-level `version` is obsolete in Compose spec).
- Python modules compile with `python3 -m compileall services`.

2. High-risk issues to address first:
- `POSTGRES_DSN` interpolation in Compose can break when password contains `@` because credentials are not URL-encoded.
- Secrets from `.env` are currently injected into all services via `env_file`; least-privilege env scoping is not enforced.
- Some docs are stale vs implemented endpoints/commands.

## 8) Immediate Continuation Backlog

1. Security hardening:
- Move bot tokens/webhook/API keys out of `.env` in repo-local dev workflows where possible.
- Reduce env var exposure per service (`env_file` + broad inherited vars currently over-shares).
- URL-encode database credentials or compose DSN from safe parts at runtime.

2. Reliability:
- Add request retries/backoff for upstream FMP calls.
- Add readiness checks for FastAPI services and explicit startup dependency validation.
- Persist scheduler cooldown state in Redis (not process memory).

3. Product consistency:
- Normalize `changePercentage` vs `changesPercentage` key usage end-to-end.
- Align README to actual command and endpoint surface.

4. Testing:
- Add unit tests for scanners/filters and normalization helpers.
- Add API integration smoke tests for key endpoints.

## 9) 13F Service Plan (Scoped)

This plan adds a limited institutional-holdings feature based on SEC 13F data.

Product scope:
- Input is a stock symbol.
- Output is a comparison of institutional holders across the latest two available 13F quarters.
- No historical browsing UI/API beyond the rolling last-two-quarter window.
- Only data needed for the rolling one-year support window is loaded. With `2026-03-31` now active, that means keeping report periods `2025-03-31`, `2025-06-30`, `2025-09-30`, `2025-12-31`, and `2026-03-31`, because `2025-03-31` is required as the comparison baseline for `2025-06-30`.

Why SEC bulk data instead of scraping `13f.info`:
- Free and official source.
- Better fit for symbol-to-manager reverse lookups than scraping HTML pages.
- Avoids fragility and rate-limit risk from third-party scraping.

### 9.1) Proposed Ownership

1. `strategy-engine`
- Owns the public read API for 13F comparisons.
- Owns the loader code that imports SEC 13F quarterly datasets into Postgres.
- Computes symbol-level deltas from stored normalized rows.

2. `postgres`
- System of record for 13F snapshots, symbol/CUSIP mapping, and precomputed or query-time comparison results.
- Also stores a bounded working-set table for holdings from only the latest two completed report periods.

3. `scheduler-worker`
- Optional future trigger for a lightweight refresh check after each 13F deadline.
- Not required for initial delivery because the dataset range is small and bounded.

No new microservice is required for the initial implementation.

### 9.2) External Data Inputs

1. SEC Form 13F structured datasets
- Source of holdings snapshots by filing/report period.
- Load only the datasets covering report periods needed for 2025 service behavior.

2. SEC official 13F securities list
- Reference set for eligible 13F securities and CUSIPs.

3. Ticker-to-CUSIP mapping
- Required because the user query starts with ticker, while 13F holdings are keyed by CUSIP.
- Preferred approach: maintain a local mapping table seeded from an internal symbol master if available, otherwise from a one-time curated import.
- The mapping must support one symbol mapping to multiple CUSIPs over time only if needed; otherwise start with one active CUSIP per symbol.

### 9.3) Data Retention Rules

Retention is intentionally narrow.

Keep only:
- the latest two report periods needed for the active response window
- one additional prior quarter when necessary to bridge a new quarter rollover inside 2025
- the symbol/CUSIP reference tables
- loader audit metadata

Operationally for the current bounded scope:
- Store `2025-03-31` through `2026-03-31`.
- The read API only returns one comparison pair at a time: latest period vs previous period.
- The API does not accept arbitrary historical quarter ranges.

### 9.4) Proposed Postgres Schema

Add these tables in `infra/sql/init.sql` or a migration layer if introduced later:

1. `sec_13f_dataset`
- `id BIGSERIAL PRIMARY KEY`
- `dataset_name TEXT UNIQUE NOT NULL`
- `report_period DATE NOT NULL`
- `source_url TEXT NOT NULL`
- `load_status TEXT NOT NULL`
- `loaded_at TIMESTAMPTZ`
- `row_counts JSONB NOT NULL DEFAULT '{}'::jsonb`
- `error_text TEXT`

Purpose:
- Tracks each SEC quarterly import and supports idempotent reloads.

2. `sec_13f_filing`
- `id BIGSERIAL PRIMARY KEY`
- `dataset_id BIGINT NOT NULL REFERENCES sec_13f_dataset(id)`
- `accession_number TEXT NOT NULL`
- `cik TEXT NOT NULL`
- `manager_name TEXT NOT NULL`
- `report_period DATE NOT NULL`
- `filed_at DATE`
- `submission_type TEXT NOT NULL`
- `is_amendment BOOLEAN NOT NULL DEFAULT FALSE`
- `other_manager_included BOOLEAN NOT NULL DEFAULT FALSE`

Indexes:
- unique on `(accession_number)`
- index on `(report_period, cik)`

Purpose:
- Stores manager-level filing metadata and supports amendment handling.

3. `sec_13f_holding`
- `id BIGSERIAL PRIMARY KEY`
- `filing_id BIGINT NOT NULL REFERENCES sec_13f_filing(id) ON DELETE CASCADE`
- `report_period DATE NOT NULL`
- `cik TEXT NOT NULL`
- `manager_name TEXT NOT NULL`
- `cusip TEXT NOT NULL`
- `issuer_name TEXT`
- `class_title TEXT`
- `value_thousands BIGINT`
- `shares BIGINT`
- `share_type TEXT`
- `put_call TEXT`
- `investment_discretion TEXT`
- `voting_sole BIGINT`
- `voting_shared BIGINT`
- `voting_none BIGINT`

Indexes:
- index on `(report_period, cusip)`
- index on `(cusip, cik, report_period)`
- index on `(report_period, manager_name)`

Purpose:
- Core holdings snapshot table used by the symbol lookup endpoint.

4. `sec_13f_symbol_map`
- `symbol TEXT PRIMARY KEY`
- `cusip TEXT NOT NULL`
- `issuer_name TEXT`
- `source TEXT NOT NULL`
- `is_active BOOLEAN NOT NULL DEFAULT TRUE`
- `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`

Indexes:
- unique on `(cusip, symbol)`

Purpose:
- Translates ticker input to the CUSIP needed for 13F lookups.

5. `sec_13f_recent_holding`
- Mirrors the lookup columns from `sec_13f_holding`
- Refreshed after each successful loader run
- Contains only rows from the latest two completed report periods

Purpose:
- Keeps the read path aligned to the rolling last-two-quarter product contract.

### 9.5) Loader Design

Add a one-shot loader script under `services/strategy-engine/` or a small module imported by `app/main.py`.

Inputs:
- SEC dataset URL
- report period
- Postgres DSN

Behavior:
- Download and unpack one quarterly dataset.
- Insert a dataset audit row.
- Normalize manager metadata into `sec_13f_filing`.
- Normalize position rows into `sec_13f_holding`.
- Refresh `sec_13f_recent_holding` so it only contains the latest two completed report periods.
- Deduplicate on accession number and reruns.
- Prefer the latest amended filing if multiple filings exist for the same manager and report period.

Initial load set for the current service window:
- `2025-03-31`
- `2025-06-30`
- `2025-09-30`
- `2025-12-31`
- `2026-03-31`

Operational rule:
- When a new quarter becomes available, load it and drop quarters older than the bounded support window if storage simplicity is preferred.

### 9.6) Read API Contract

Add one endpoint to `strategy-engine`:

- `GET /v1/13f/holdings-delta?symbol=AAPL&limit=20`

Response shape:
- `symbol`
- `cusip`
- `latest_report_period`
- `previous_report_period`
- `count`
- `data`

Each item in `data`:
- `cik`
- `manager_name`
- `latest_shares`
- `previous_shares`
- `share_delta`
- `latest_value_thousands`
- `previous_value_thousands`
- `value_delta_thousands`
- `change_type`

`change_type` enum:
- `new`
- `increased`
- `reduced`
- `exited`
- `unchanged`

Rules:
- No quarter parameters are exposed publicly.
- The endpoint always resolves to the latest available report period and its immediate predecessor.
- Default sorting should be by `latest_value_thousands DESC`, with `new` and large positive deltas surfacing naturally near the top.
- `limit` should be capped, for example at `100`.

### 9.7) Query Logic

For a given `symbol`:

1. Resolve `symbol -> cusip` from `sec_13f_symbol_map`.
2. Determine the latest loaded report period.
3. Determine the previous loaded report period.
4. Pull all holders for the symbol CUSIP in both periods from `sec_13f_recent_holding`.
5. Full outer join on manager identity, using `cik` as the primary key.
6. Compute:
- `latest_shares`
- `previous_shares`
- `share_delta`
- `latest_value_thousands`
- `previous_value_thousands`
- `value_delta_thousands`
- `change_type`
7. Return the bounded result set.

Manager identity rule:
- Use `cik` as canonical identity.
- Use `manager_name` only as display text.

### 9.8) Freshness Model

13F is delayed data.

Service behavior should explicitly state:
- data is reported quarterly
- filings can arrive up to 45 days after quarter end
- the endpoint returns the latest two loaded quarters, not intraday institutional ownership

The endpoint should include exact dates, for example:
- `latest_report_period: 2026-03-31`
- `previous_report_period: 2025-12-31`

### 9.9) UX / Product Constraints

This is a partial service, not a full institutional-ownership platform.

Out of scope:
- multi-year history
- arbitrary quarter selection
- institution detail pages
- portfolio reconstruction beyond the queried symbol
- scraping third-party sites
- real-time ownership estimates

Recommended Discord/API framing:
- "13F holders delta"
- "Latest quarter vs previous quarter"
- "SEC-reported holdings only"

### 9.10) Testing Plan

1. Loader tests
- dataset row parsing
- amendment precedence
- idempotent reruns
- symbol/CUSIP mapping misses

2. Query tests
- new holder
- exited holder
- increased position
- reduced position
- unchanged position
- unknown symbol
- known symbol with no mapped CUSIP

3. API smoke test
- `GET /v1/13f/holdings-delta?symbol=AAPL`

### 9.11) Immediate Build Order

1. Add Postgres tables for dataset audit, filing metadata, holdings, and symbol map.
2. Add a loader script that imports the bounded SEC quarterly datasets.
3. Seed `sec_13f_symbol_map` for the initial supported symbols.
4. Add `strategy-engine` endpoint `/v1/13f/holdings-delta`.
5. Add tests for loader normalization and delta classification.
6. Optionally add a Discord command only after the HTTP endpoint is stable.

## 10) Assumptions and Invariants

1. All inter-service calls are internal Docker network HTTP.
2. `market-data-service` is the only service that should call external FMP endpoints.
3. `strategy-engine` is the only service `discord-bot` should call.
4. Redis watchlist key format is stable: `watchlist:{discord_user_id}`.
5. Production-grade auth/rate-limiting/auditing is not yet implemented.
6. 13F support is intentionally bounded to the rolling support window, retaining the latest five completed report periods so the product can always compare the newest loaded quarter against its immediate predecessor.
7. The symbol lookup contract depends on a maintained `symbol -> cusip` mapping; incorrect mapping will produce incorrect holder comparisons.

# Trade Bot Architecture Handoff

This document is the canonical architecture snapshot for continuing development across different AI models.

## 1) System Diagram

```mermaid
flowchart LR
    U[Discord User] -->|Slash Commands| D[discord-bot]
    D -->|HTTP JSON| S[strategy-engine]
    S -->|HTTP JSON| M[market-data-service]
    M -->|REST API| F[FMP API]
    S -->|Redis Set ops| R[(Redis)]
    S -->|Future persistence / DSN wired| P[(Postgres)]
    W[scheduler-worker] -->|HTTP scan| S
    W -->|Webhook alerts| H[Discord Webhook]
    P -. init.sql .-> I[infra/sql/init.sql]
```

## 2) Runtime Components

1. `discord-bot`
- Purpose: User-facing command interface in Discord.
- Transport: `discord.py` command events in, HTTP requests to `strategy-engine`.
- Source: `services/discord-bot/app/bot.py`.

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

5. Datastores
- `Redis`: active storage for `watchlist:{user_id}` sets.
- `Postgres`: provisioned and initialized with `infra/sql/init.sql`; currently minimal runtime usage.

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

## 6) Environment Contract (Required)

- `DISCORD_BOT_TOKEN`
- `FMP_API_KEY`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Also used:
- `DISCORD_GUILD_ID`
- `STRATEGY_ENGINE_URL`
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

## 9) Assumptions and Invariants

1. All inter-service calls are internal Docker network HTTP.
2. `market-data-service` is the only service that should call external FMP endpoints.
3. `strategy-engine` is the only service `discord-bot` should call.
4. Redis watchlist key format is stable: `watchlist:{discord_user_id}`.
5. Production-grade auth/rate-limiting/auditing is not yet implemented.

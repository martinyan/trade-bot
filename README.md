# Discord Trading Bot Monorepo

Python monorepo scaffold for a Discord trading bot using Docker Compose with:
- `discord-bot`
- `market-data-service`
- `strategy-engine`
- `postgres`
- `redis`
- `adminer` (web UI for DB queries)

## Architecture

- `discord-bot`:
  - Registers slash commands:
    - `/ping`
    - `/quote`
    - `/quote_detail`
    - `/scan_premarket`
    - `/watch_add`
    - `/watch_remove`
    - `/watch_list`
    - `/news`
    - `/insider_trades`
    - `/earnings_risk`
    - `/catalyst_brief`
  - Calls `strategy-engine` over HTTP.

- `strategy-engine`:
  - Orchestrates business logic.
  - Calls `market-data-service` for market data.
  - Stores watchlists in Redis.
  - Has Postgres DSN wired for persistence extensions.

- `market-data-service`:
  - Wraps Financial Modeling Prep REST API.
  - Exposes quote and premarket scan endpoints.

- `postgres`:
  - Starts with `infra/sql/init.sql`.

- `redis`:
  - Used by strategy-engine watchlists.

## Repo Layout

- `docker-compose.yml`
- `.env.example`
- `requirements/`
  - `discord-bot.txt`
  - `market-data-service.txt`
  - `strategy-engine.txt`
- `services/`
  - `discord-bot/`
  - `market-data-service/`
  - `strategy-engine/`
- `infra/sql/init.sql`

## Environment Variables

Copy and edit environment values:

```bash
cp .env.example .env
```

Required values:
- `DISCORD_BOT_TOKEN`
- `FMP_API_KEY`
- `POSTGRES_PASSWORD`

Optional:
- `DISCORD_GUILD_ID` (recommended during development for fast command sync)
- `FMP_BASE_URL` (defaults to `https://financialmodelingprep.com/api/v3`)
- `SEC_USER_AGENT` (recommended for SEC downloads, e.g. `your-name your-email@example.com`)
- `DISCORD_PRIVATE_BY_DEFAULT` (defaults to `true`; slash command replies are ephemeral unless explicitly shared)
- `WATCHLIST_EOD_SUMMARY_ENABLED` (defaults to `true`; send end-of-day watchlist DM summaries)
- `WATCHLIST_EOD_HOUR_ET` (defaults to `16`)
- `WATCHLIST_EOD_MINUTE_ET` (defaults to `10`)
- `WATCHLIST_EOD_WINDOW_MINUTES` (defaults to `20`)

## Run

```bash
docker compose up --build
```

Services:
- market-data-service: `http://localhost:8001`
- strategy-engine: `http://localhost:8002`
- postgres: `localhost:5432`
- redis: `localhost:6379`
- adminer: `http://localhost:8080`

## Postgres Web UI (Adminer)

After `docker compose up --build`, open:

- `http://localhost:8080`

Login values:
- System: `PostgreSQL`
- Server: `postgres` (from inside Docker network)
- Username: `${POSTGRES_USER}` (from `.env`, currently `tradebot`)
- Password: `${POSTGRES_PASSWORD}` (from `.env`)
- Database: `${POSTGRES_DB}` (from `.env`, currently `tradebot`)

## Service Endpoints

### market-data-service
- `GET /health`
- `GET /v1/quotes?symbols=AAPL,MSFT`
- `GET /v1/scan/premarket?limit=10`
- `GET /v1/insider-trades/AAPL?page=0&limit=50&include_stats=true`
- `GET /v1/earnings/AAPL?limit=12`

### strategy-engine
- `GET /health`
- `GET /v1/brief?symbol=AAPL` (used by `/quote`)
- `GET /v1/scan/premarket?limit=10`
- `GET /v1/quote-detail?symbol=AAPL`
- `GET /v1/news?symbol=AAPL&limit=5`
- `GET /v1/insider-trades/latest?symbol=AAPL&limit=10`
- `GET /v1/earnings-risk?symbol=AAPL`
- `GET /v1/catalyst-brief?symbol=AAPL&news_limit=3`
- `POST /v1/watchlist/add`
  - JSON body: `{ "user_id": "123", "symbol": "TSLA" }`
- `POST /v1/watchlist/remove`
  - JSON body: `{ "user_id": "123", "symbol": "TSLA" }`
- `GET /v1/watchlist?user_id=123`
- `GET /v1/watchlist/all`

## Notes

- Secrets are loaded from environment variables; nothing is hardcoded.
- Premarket scanning currently uses FMP active movers as a starter proxy.
- Expand strategy logic and Postgres persistence in `strategy-engine` as needed.

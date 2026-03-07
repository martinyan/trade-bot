# Discord Trading Bot Monorepo

Python monorepo scaffold for a Discord trading bot using Docker Compose with:
- `discord-bot`
- `market-data-service`
- `strategy-engine`
- `postgres`
- `redis`

## Architecture

- `discord-bot`:
  - Registers slash commands:
    - `/ping`
    - `/brief`
    - `/scan_premarket`
    - `/watch_add`
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

## Run

```bash
docker compose up --build
```

Services:
- market-data-service: `http://localhost:8001`
- strategy-engine: `http://localhost:8002`
- postgres: `localhost:5432`
- redis: `localhost:6379`

## Service Endpoints

### market-data-service
- `GET /health`
- `GET /v1/quotes?symbols=AAPL,MSFT`
- `GET /v1/scan/premarket?limit=10`

### strategy-engine
- `GET /health`
- `GET /v1/brief?symbol=AAPL`
- `GET /v1/scan/premarket?limit=10`
- `POST /v1/watchlist/add`
  - JSON body: `{ "user_id": "123", "symbol": "TSLA" }`

## Notes

- Secrets are loaded from environment variables; nothing is hardcoded.
- Premarket scanning currently uses FMP active movers as a starter proxy.
- Expand strategy logic and Postgres persistence in `strategy-engine` as needed.

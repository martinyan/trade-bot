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
    - `/world_index`
    - `/13f_delta`
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
- `SEC_FORM4_SYNC_INTERVAL_SECONDS` (defaults to `86400`; scheduler refresh interval for SEC Form 4 ingest)
- `SEC_FORM4_SYNC_CRON` (optional; 5-field UTC cron expression for SEC Form 4 ingest, overrides the interval when set)
- `SEC_FORM4_SYNC_LOOKBACK_DAYS` (defaults to `2`; how many recent calendar days of SEC Form 4 filings to ingest on each scheduled run)
- `SEC_FORM4_SYNC_RETAIN_DAYS` (defaults to `10`; rolling retention window for board-market insider scan rows)
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
- `GET /v1/index-quotes?symbols=%5ESPX,%5EIXIC`
- `GET /v1/scan/premarket?limit=10`
- `GET /v1/insider-trades/AAPL?page=0&limit=50&include_stats=true`
- `GET /v1/earnings/AAPL?limit=12`

### strategy-engine
- `GET /health`
- `GET /v1/brief?symbol=AAPL` (used by `/quote`)
- `GET /v1/world-indexes`
- `GET /v1/scan/premarket?limit=10`
- `GET /v1/quote-detail?symbol=AAPL`
- `GET /v1/news?symbol=AAPL&limit=5`
- `GET /v1/insider-trades/latest?symbol=AAPL&limit=10`
- `GET /v1/insider-trades/latest?limit=20&days=5`
- `POST /v1/admin/sec-form4/sync?days_back=2&retain_days=10`
- `GET /v1/earnings-risk?symbol=AAPL`
- `GET /v1/catalyst-brief?symbol=AAPL&news_limit=3`
- `GET /v1/13f/holdings-delta?symbol=AAPL&limit=20`
- `GET /v1/13f/symbol-map?symbol=AAPL`
- `POST /v1/13f/symbol-map`
- `POST /v1/watchlist/add`
  - JSON body: `{ "user_id": "123", "symbol": "TSLA" }`
- `POST /v1/watchlist/remove`
  - JSON body: `{ "user_id": "123", "symbol": "TSLA" }`
- `GET /v1/watchlist?user_id=123`
- `GET /v1/watchlist/all`

## 13F Loader

The bounded 13F schema plus loader entrypoints live in:
- [sec_13f_loader.py](/docker/trade-bot/services/strategy-engine/app/sec_13f_loader.py) for one quarter
- [sec_13f_batch_loader.py](/docker/trade-bot/services/strategy-engine/app/sec_13f_batch_loader.py) for a quarterly batch with pruning

Example invocation from the `strategy-engine` container:

```bash
python sec_13f_loader.py \
  --dataset-url "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01sep2025-30nov2025_form13f.zip" \
  --report-period 2025-09-30 \
  --parse-only
```

Current behavior:
- downloads the SEC zip
- inspects archive members
- parses `SUBMISSION.tsv`, `COVERPAGE.tsv`, `SUMMARYPAGE.tsv`, and `INFOTABLE.tsv`
- supports `--parse-only` for archive validation without Postgres writes
- writes normalized filing and holding rows to Postgres on a full run
- refreshes `sec_13f_recent_holding` to the latest two loaded report periods
- upserts a `sec_13f_dataset` audit row with counts and load status

Batch load example:

```bash
python sec_13f_batch_loader.py \
  --retain-report-periods 5
```

Batch loader behavior:
- loads the configured SEC quarterly datasets in order
- refreshes the rolling `sec_13f_recent_holding` working set after each import
- prunes completed report periods older than the retention window
- deletes old dataset audit rows together with their filing and holding rows via cascade

Operational use when a new quarter arrives:
- add the new SEC dataset URL/report period to the batch config
- rerun `sec_13f_batch_loader.py`
- keep `--retain-report-periods 5` to preserve a one-year bounded raw history plus the extra comparison baseline

Example symbol-map seed:

```bash
curl -X POST http://localhost:8002/v1/13f/symbol-map \
  -H "Content-Type: application/json" \
  -d '{"symbol":"AAPL","cusip":"037833100","issuer_name":"Apple Inc.","source":"manual"}'
```

Curated bulk seed file:
- [seed_13f_symbol_map.sql](/docker/trade-bot/infra/sql/seed_13f_symbol_map.sql)

Discord usage:
- `/13f_delta symbol:AAPL limit:10`

## Notes

- Secrets are loaded from environment variables; nothing is hardcoded.
- Premarket scanning currently uses FMP active movers as a starter proxy.
- Expand strategy logic and Postgres persistence in `strategy-engine` as needed.

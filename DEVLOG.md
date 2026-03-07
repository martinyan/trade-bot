# Trade Bot Development Log

## Project goal
Build a Discord-based stock trading signal bot using Financial Modeling Prep (FMP), deployed on a Hostinger VPS with Docker Compose, and structured as a multi-service system that can evolve into a hedge-fund style scanner architecture.

---

## Current architecture

### Infrastructure
- Hostinger VPS
- Docker engine already in use for OpenClaw
- Existing OpenClaw path: `/docker/openclaw-rcpj`
- Trade bot project path: `/docker/trade-bot`

### Services
- `discord-bot`
- `market-data-service`
- `strategy-engine`
- `scheduler-worker`
- `postgres`
- `redis`

### Current high-level flow
Discord slash commands  
→ `discord-bot`  
→ `strategy-engine`  
→ `market-data-service`  
→ FMP API

Autonomous scan flow  
→ `scheduler-worker`  
→ `strategy-engine`  
→ `market-data-service`  
→ FMP API  
→ Discord webhook alerts

---

## Discord setup
- Discord Application created
- Bot token created
- Guild-scoped slash commands enabled
- Working guild/server ID updated to current server during setup
- Slash commands confirmed working after fixing sync and access issues

### Important
A bot token was previously exposed in chat and should be treated as compromised.
Action taken / required:
- rotate token in Discord Developer Portal
- update `.env` with new token

---

## Environment and developer workflow
### Local / editor workflow
- VS Code installed on Windows
- Remote SSH used to connect directly to VPS
- Continue extension installed in VS Code
- Development happening directly against VPS project files

### Repo location
- `/docker/trade-bot`

### Suggested workflow
- edit in VS Code over Remote SSH
- use Continue for code assistance inside repo
- use ChatGPT for architecture and debugging
- maintain `DEVLOG.md` as project memory

---

## Docker / repo structure decisions
### Key decision
Do not merge the trading bot into the same compose file as OpenClaw.

### Chosen structure
- keep OpenClaw separate at `/docker/openclaw-rcpj`
- keep trade bot separate at `/docker/trade-bot`
- same Docker host, separate Compose project

### Build context cleanup
Dockerfiles were updated to use repo-root build context rather than fragile `../../../` relative COPY paths.

### Requirements filenames currently in repo
- `requirements/discord-bot.txt`
- `requirements/market-data-service.txt`
- `requirements/strategy-engine.txt`
- `requirements/scheduler.txt`

---

## Implemented features so far

### Discord bot
Implemented slash commands:
- `/ping`
- `/brief`
- `/quote_detail`
- `/scan_premarket`
- `/watch_add`

### Market data service
Working endpoints:
- `/health`
- `/quote/{symbol}`
- `/profile/{symbol}`
- `/news/{symbol}`
- `/v1/quotes`
- `/v1/scan/premarket` (currently using quote-based watch universe instead of broken FMP movers endpoint)

### Strategy engine
Implemented:
- `/v1/brief`
- `/v1/quote-detail`
- `/v1/scan/premarket`

### Scheduler worker
Implemented:
- startup loop
- 5-minute scan interval
- scoring logic
- Discord webhook posting
- in-memory cooldown suppression

---

## Major issues encountered and fixes

### 1. Discord slash commands not appearing
Symptoms:
- bot connected to Discord
- no slash commands visible

Root causes:
- bot invite / command scope issues
- sync timing and visibility issues

Fixes:
- ensured bot invited with `bot` and `applications.commands`
- added sync logging
- used guild-scoped commands for immediate visibility

---

### 2. Discord bot `ModuleNotFoundError: No module named 'app'`
Symptoms:
- `discord-bot` container restarting
- commands timing out in Discord with "The application did not respond"

Cause:
- import path in `bot.py` used:
  `from app.formatters import ...`
- container runs `python /app/bot.py`, so package `app` does not exist

Fix:
- changed import to:
  `from formatters import fmt_compact, fmt_price, fmt_change, fmt_range`

Status:
- fixed

---

### 3. Shared formatting utilities
Goal:
- apply consistent K/M/B/T formatting and cleaner prices/ranges across commands

Action:
- created `formatters.py` in:
  `services/discord-bot/app/formatters.py`

Helpers added:
- `fmt_compact`
- `fmt_price`
- `fmt_change`
- `fmt_range`

Status:
- working

---

### 4. `/brief` and `/quote_detail` field mapping mismatch
Symptoms:
- some values returned as `None` / `n/a`
- differences between raw curl results and Discord output

Cause:
- strategy engine expected wrong FMP field names
- example:
  - expected `changesPercentage`
  - actual payload returned `changePercentage`
  - expected `avgVolume` from quote
  - actual payload had `averageVolume` in profile

Fixes:
- updated strategy-engine field mapping
- quote/profile merge now uses actual FMP payload fields
- removed unreliable fields temporarily:
  - EPS
  - PE
  - Shares Outstanding

Status:
- working, with some fields intentionally omitted until a new FMP endpoint is added later

---

### 5. Market-data scanner endpoint returning 404 from FMP
Symptoms:
- scheduler-worker got 502 from strategy-engine
- market-data-service returned:
  `FMP request failed with status 404: []`

Cause:
- FMP path `stock_market/actives` was not valid for current configured base URL / plan

Fix:
- replaced broken movers path with quote-based watch-universe scanner using:
  `/quote?symbol=AAPL,MSFT,NVDA,...`

Status:
- endpoint now runs, but current strategy-engine scan still returns zero results and needs further tuning

---

### 6. Docker build failures from bad COPY paths
Symptoms:
- Docker failed to copy requirements files
- mismatch between expected filenames and actual filenames

Fixes:
- standardized build context to repo root
- updated Dockerfiles to use actual filenames:
  - `discord-bot.txt`
  - `market-data-service.txt`
  - `strategy-engine.txt`
  - `scheduler.txt`

Status:
- fixed

---

## Current known issues

### 1. `scan_premarket` returns zero results
Current behavior:
- `scheduler-worker` runs successfully
- `strategy-engine /v1/scan/premarket` returns:
  `{"count":0,"data":[]}`

Likely cause:
- filter logic in strategy-engine is still too strict or not aligned with actual quote payload

Next step:
- debug and loosen scan filters
- inspect exact payload returned by `/v1/quotes`
- verify strategy-engine filtering and score logic

---

### 2. Autonomous alerts not yet posting meaningful signals
Current behavior:
- scheduler-worker starts correctly
- reaches strategy-engine
- no scan results returned

Next step:
- fix non-empty scanner output first
- then validate webhook posting path

---

## Current working commands
- `/ping`
- `/brief`
- `/quote_detail`

### Partially working / not final
- `/scan_premarket` exists but currently returns no results
- `/watch_add` depends on downstream watchlist implementation

---

## Strategic architecture direction
Target architecture is a hedge-fund style signal engine with clear separation of concerns:

- market data ingestion
- feature computation
- scanner engine
- signal ranking / suppression
- alert routing
- persistence

Planned service evolution:
- keep `discord-bot` as interface only
- keep scanner logic in `strategy-engine`
- keep autonomous timing in `scheduler-worker`
- later add:
  - persistent watchlists
  - richer scanner filters
  - breakout logic
  - relative volume features
  - news / catalyst scoring
  - user-specific alerts
  - possible risk / execution / backtest layers

---

## Immediate next priorities
1. fix `strategy-engine /v1/scan/premarket` so it returns non-zero signals
2. confirm `scheduler-worker` posts alerts to Discord webhook
3. tune scanner filters:
   - price floor
   - market cap floor
   - volume floor
   - absolute move threshold
4. then work on breakout signal refinement

---

## Notes for future self
- keep OpenClaw stack separate from trade bot stack
- keep imports simple in `discord-bot`
- prefer normalized field mapping in strategy-engine rather than passing provider quirks downstream
- commit frequently at working checkpoints
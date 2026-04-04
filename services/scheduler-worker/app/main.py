import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx
import psycopg

STRATEGY_ENGINE_URL = os.getenv("STRATEGY_ENGINE_URL", "http://strategy-engine:8002").rstrip("/")
DISCORD_ALERT_WEBHOOK_URL = os.getenv("DISCORD_ALERT_WEBHOOK_URL", "")
ALERT_SCAN_INTERVAL_SECONDS = int(os.getenv("ALERT_SCAN_INTERVAL_SECONDS", "300"))
ALERT_TOP_N = int(os.getenv("ALERT_TOP_N", "3"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
SEC_FORM4_SYNC_INTERVAL_SECONDS = int(os.getenv("SEC_FORM4_SYNC_INTERVAL_SECONDS", "86400"))
SEC_FORM4_SYNC_CRON = os.getenv("SEC_FORM4_SYNC_CRON", "").strip()
SEC_FORM4_SYNC_LOOKBACK_DAYS = int(os.getenv("SEC_FORM4_SYNC_LOOKBACK_DAYS", "2"))
SEC_FORM4_SYNC_RETAIN_DAYS = int(os.getenv("SEC_FORM4_SYNC_RETAIN_DAYS", "10"))
SEC_FORM4_SYNC_STATE_KEY = "sec_form4_last_successful_slot"
SEC_13F_BATCH_SYNC_CRON = os.getenv("SEC_13F_BATCH_SYNC_CRON", "").strip()
SEC_13F_BATCH_SYNC_RETAIN_REPORT_PERIODS = int(os.getenv("SEC_13F_BATCH_SYNC_RETAIN_REPORT_PERIODS", "5"))
SEC_13F_BATCH_SYNC_LAST_ALLOWED_SLOT = os.getenv("SEC_13F_BATCH_SYNC_LAST_ALLOWED_SLOT", "").strip()
SEC_13F_BATCH_SYNC_STATE_KEY = "sec_13f_batch_last_successful_slot"

# in-memory cooldown for v1
# later this should move to Postgres/Redis
LAST_ALERT_AT: dict[str, float] = {}
LAST_SEC_FORM4_SYNC_AT: float = 0.0
LAST_SEC_FORM4_SYNC_SLOT: str = ""
LAST_SEC_13F_BATCH_SYNC_SLOT: str = ""


def _postgres_conninfo() -> str:
    if POSTGRES_USER and POSTGRES_PASSWORD and POSTGRES_DB:
        encoded_password = quote(POSTGRES_PASSWORD, safe="")
        return (
            f"postgresql://{POSTGRES_USER}:{encoded_password}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
    return POSTGRES_DSN


def _scheduler_state_enabled() -> bool:
    return bool(_postgres_conninfo().strip())


def _ensure_scheduler_state_table() -> None:
    conninfo = _postgres_conninfo()
    if not conninfo:
        return

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS scheduler_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        conn.commit()


def _load_last_sec_form4_sync_slot() -> str:
    return _load_scheduler_slot(SEC_FORM4_SYNC_STATE_KEY)


def _load_scheduler_slot(state_key: str) -> str:
    conninfo = _postgres_conninfo()
    if not conninfo:
        return ""

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT state_value FROM scheduler_state WHERE state_key = %s",
                (state_key,),
            )
            row = cur.fetchone()
            return str(row[0]) if row else ""


def _persist_last_sec_form4_sync_slot(slot: str) -> None:
    _persist_scheduler_slot(SEC_FORM4_SYNC_STATE_KEY, slot)


def _persist_scheduler_slot(state_key: str, slot: str) -> None:
    conninfo = _postgres_conninfo()
    if not conninfo:
        return

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scheduler_state (state_key, state_value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (state_key)
                DO UPDATE SET
                    state_value = EXCLUDED.state_value,
                    updated_at = NOW()
                """,
                (state_key, slot),
            )
        conn.commit()


def is_in_cooldown(key: str) -> bool:
    last = LAST_ALERT_AT.get(key)
    if last is None:
        return False
    return (time.time() - last) < ALERT_COOLDOWN_SECONDS


def mark_alerted(key: str) -> None:
    LAST_ALERT_AT[key] = time.time()


def score_signal(item: dict) -> float:
    change_pct = abs(float(item.get("changesPercentage", item.get("changePercentage", 0)) or 0))
    volume = float(item.get("volume", 0) or 0)
    market_cap = float(item.get("marketCap", 0) or 0)

    volume_score = min(volume / 10_000_000, 20)
    market_cap_score = min(market_cap / 100_000_000_000, 20)

    return round(change_pct * 3 + volume_score + market_cap_score, 2)


def fmt_compact(value) -> str:
    if value is None:
        return "n/a"
    try:
        num = float(value)
        abs_num = abs(num)
        if abs_num >= 1_000_000_000_000:
            return f"{num/1_000_000_000_000:.2f}T"
        if abs_num >= 1_000_000_000:
            return f"{num/1_000_000_000:.2f}B"
        if abs_num >= 1_000_000:
            return f"{num/1_000_000:.2f}M"
        if abs_num >= 1_000:
            return f"{num/1_000:.2f}K"
        return f"{num:.0f}"
    except Exception:
        return str(value)


async def fetch_scan() -> list[dict]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{STRATEGY_ENGINE_URL}/v1/scan/premarket", params={"limit": 15})
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])


async def send_discord_alert(content: str) -> None:
    if not DISCORD_ALERT_WEBHOOK_URL:
        print("DISCORD_ALERT_WEBHOOK_URL is not configured; skipping alert")
        return

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(DISCORD_ALERT_WEBHOOK_URL, json={"content": content})
        resp.raise_for_status()


def sec_form4_sync_due() -> bool:
    if SEC_FORM4_SYNC_CRON:
        return _sec_form4_sync_due_cron()
    return (time.time() - LAST_SEC_FORM4_SYNC_AT) >= SEC_FORM4_SYNC_INTERVAL_SECONDS


def mark_sec_form4_synced(slot: str | None = None) -> None:
    global LAST_SEC_FORM4_SYNC_AT, LAST_SEC_FORM4_SYNC_SLOT
    LAST_SEC_FORM4_SYNC_AT = time.time()
    LAST_SEC_FORM4_SYNC_SLOT = slot or _cron_slot_key(datetime.now(timezone.utc))
    if SEC_FORM4_SYNC_CRON:
        _persist_last_sec_form4_sync_slot(LAST_SEC_FORM4_SYNC_SLOT)


def _parse_cron_field(field: str, *, minimum: int, maximum: int) -> set[int]:
    raw = field.strip()
    if raw == "*":
        return set(range(minimum, maximum + 1))
    values: set[int] = set()
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        if token.startswith("*/"):
            step = int(token[2:])
            if step <= 0:
                raise ValueError("cron step must be positive")
            values.update(range(minimum, maximum + 1, step))
            continue
        value = int(token)
        if value < minimum or value > maximum:
            raise ValueError(f"cron value {value} outside {minimum}-{maximum}")
        values.add(value)
    if not values:
        raise ValueError("cron field resolved to no values")
    return values


def _cron_slot_key(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M")


def _cron_matches(now: datetime, expr: str) -> bool:
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError("SEC_FORM4_SYNC_CRON must have 5 fields: minute hour day month weekday")
    minute, hour, day, month, weekday = parts
    utc_now = now.astimezone(timezone.utc)
    python_weekday = utc_now.weekday()
    cron_weekday = (python_weekday + 1) % 7
    return (
        utc_now.minute in _parse_cron_field(minute, minimum=0, maximum=59)
        and utc_now.hour in _parse_cron_field(hour, minimum=0, maximum=23)
        and utc_now.day in _parse_cron_field(day, minimum=1, maximum=31)
        and utc_now.month in _parse_cron_field(month, minimum=1, maximum=12)
        and cron_weekday in _parse_cron_field(weekday, minimum=0, maximum=6)
    )


def _sec_form4_sync_due_cron() -> bool:
    now = datetime.now(timezone.utc)
    slot = _latest_due_cron_slot(now, SEC_FORM4_SYNC_CRON)
    if not slot:
        return False
    return LAST_SEC_FORM4_SYNC_SLOT != slot


def _slot_is_allowed(slot: str, last_allowed_slot: str) -> bool:
    if not last_allowed_slot:
        return True
    return slot <= last_allowed_slot


def _sec_13f_batch_sync_due_cron() -> bool:
    if not SEC_13F_BATCH_SYNC_CRON:
        return False
    now = datetime.now(timezone.utc)
    slot = _latest_due_cron_slot(now, SEC_13F_BATCH_SYNC_CRON)
    if not slot or not _slot_is_allowed(slot, SEC_13F_BATCH_SYNC_LAST_ALLOWED_SLOT):
        return False
    return LAST_SEC_13F_BATCH_SYNC_SLOT != slot


def _latest_due_cron_slot(now: datetime, expr: str) -> str:
    # Allow a small grace window so a polling loop that wakes a bit late
    # still picks up the intended cron minute instead of skipping the day.
    lookback_seconds = max(ALERT_SCAN_INTERVAL_SECONDS + 60, 60)
    utc_now = now.astimezone(timezone.utc).replace(second=0, microsecond=0)
    minutes_back = max(lookback_seconds // 60, 1)
    for offset in range(minutes_back + 1):
        candidate = utc_now - timedelta(minutes=offset)
        if _cron_matches(candidate, expr):
            return _cron_slot_key(candidate)
    return ""


def _effective_sec_form4_lookback_days(now: datetime) -> int:
    configured_days = max(SEC_FORM4_SYNC_LOOKBACK_DAYS, 1)
    if not LAST_SEC_FORM4_SYNC_SLOT:
        return configured_days

    try:
        last_sync_date = datetime.strptime(LAST_SEC_FORM4_SYNC_SLOT, "%Y-%m-%dT%H:%M").date()
    except ValueError:
        return configured_days

    gap_days = max((now.astimezone(timezone.utc).date() - last_sync_date).days + 1, 1)
    return min(max(configured_days, gap_days), 30)


async def sync_sec_form4() -> None:
    now = datetime.now(timezone.utc)
    slot = _latest_due_cron_slot(now, SEC_FORM4_SYNC_CRON) if SEC_FORM4_SYNC_CRON else ""
    lookback_days = _effective_sec_form4_lookback_days(now)
    async with httpx.AsyncClient(timeout=300.0) as client:
        resp = await client.post(
            f"{STRATEGY_ENGINE_URL}/v1/admin/sec-form4/sync",
            params={
                "days_back": lookback_days,
                "retain_days": SEC_FORM4_SYNC_RETAIN_DAYS,
            },
        )
        resp.raise_for_status()
        payload = resp.json()
        mark_sec_form4_synced(slot or None)
        print(f"SEC Form 4 sync complete (lookback_days={lookback_days}): {payload}")


def mark_sec_13f_batch_synced(slot: str) -> None:
    global LAST_SEC_13F_BATCH_SYNC_SLOT
    LAST_SEC_13F_BATCH_SYNC_SLOT = slot
    _persist_scheduler_slot(SEC_13F_BATCH_SYNC_STATE_KEY, slot)


async def sync_sec_13f_batch() -> None:
    slot = _latest_due_cron_slot(datetime.now(timezone.utc), SEC_13F_BATCH_SYNC_CRON)
    if not slot:
        raise RuntimeError("no due SEC 13F batch cron slot found")

    async with httpx.AsyncClient(timeout=1200.0) as client:
        resp = await client.post(
            f"{STRATEGY_ENGINE_URL}/v1/admin/sec-13f/batch-sync",
            params={"retain_report_periods": SEC_13F_BATCH_SYNC_RETAIN_REPORT_PERIODS},
        )
        resp.raise_for_status()
        payload = resp.json()
        mark_sec_13f_batch_synced(slot)
        print(f"SEC 13F batch sync complete (slot={slot}): {payload}")


def build_alert_message(item: dict, score: float) -> str:
    symbol = item.get("symbol", "?")
    price = item.get("price", "?")
    change = item.get("changesPercentage", item.get("changePercentage", "?"))
    volume = fmt_compact(item.get("volume"))
    market_cap = fmt_compact(item.get("marketCap"))

    return (
        f"🚨 **PREMARKET SIGNAL**\n"
        f"**{symbol}** | Price: ${price}\n"
        f"Move: {change}%\n"
        f"Volume: {volume}\n"
        f"Market Cap: {market_cap}\n"
        f"Score: {score}"
    )


async def run_once() -> None:
    try:
        rows = await fetch_scan()
        if not rows:
            print("No scan results returned")
            return

        scored = []
        for item in rows:
            score = score_signal(item)
            scored.append((score, item))

        scored.sort(key=lambda x: x[0], reverse=True)

        sent = 0
        for score, item in scored:
            if sent >= ALERT_TOP_N:
                break

            symbol = item.get("symbol", "?")
            scanner_name = "premarket"
            cooldown_key = f"{scanner_name}:{symbol}"

            if is_in_cooldown(cooldown_key):
                continue

            msg = build_alert_message(item, score)
            await send_discord_alert(msg)
            mark_alerted(cooldown_key)
            sent += 1
            print(f"Alert sent for {symbol} with score {score}")

        if sent == 0:
            print("No new alerts sent (likely cooldown or empty results)")

    except Exception as e:
        print(f"run_once failed against {STRATEGY_ENGINE_URL}: {e}", flush=True)


async def main_loop() -> None:
    global LAST_SEC_FORM4_SYNC_SLOT, LAST_SEC_13F_BATCH_SYNC_SLOT
    print("scheduler-worker started")
    print(f"Strategy engine: {STRATEGY_ENGINE_URL}")
    print(f"Scan interval: {ALERT_SCAN_INTERVAL_SECONDS}s")
    print(f"Top N alerts: {ALERT_TOP_N}")
    print(f"Cooldown: {ALERT_COOLDOWN_SECONDS}s")
    if _scheduler_state_enabled():
        try:
            _ensure_scheduler_state_table()
            LAST_SEC_FORM4_SYNC_SLOT = _load_last_sec_form4_sync_slot()
            if LAST_SEC_FORM4_SYNC_SLOT:
                print(f"Loaded persisted SEC Form 4 sync slot (UTC): {LAST_SEC_FORM4_SYNC_SLOT}")
            else:
                print("No persisted SEC Form 4 sync slot found")
            LAST_SEC_13F_BATCH_SYNC_SLOT = _load_scheduler_slot(SEC_13F_BATCH_SYNC_STATE_KEY)
            if LAST_SEC_13F_BATCH_SYNC_SLOT:
                print(f"Loaded persisted SEC 13F batch sync slot (UTC): {LAST_SEC_13F_BATCH_SYNC_SLOT}")
            else:
                print("No persisted SEC 13F batch sync slot found")
        except Exception as e:
            print(f"Failed to initialize scheduler state store: {e}", flush=True)
    else:
        print("Postgres scheduler state store is not configured; SEC Form 4 cron will not survive reboot")
    if SEC_FORM4_SYNC_CRON:
        print(f"SEC Form 4 sync cron (UTC): {SEC_FORM4_SYNC_CRON}")
    else:
        print(f"SEC Form 4 sync interval: {SEC_FORM4_SYNC_INTERVAL_SECONDS}s")
    if SEC_13F_BATCH_SYNC_CRON:
        print(f"SEC 13F batch sync cron (UTC): {SEC_13F_BATCH_SYNC_CRON}")
        if SEC_13F_BATCH_SYNC_LAST_ALLOWED_SLOT:
            print(f"SEC 13F batch sync last allowed slot (UTC): {SEC_13F_BATCH_SYNC_LAST_ALLOWED_SLOT}")
    print("Waiting 10 seconds for dependencies to become ready...", flush=True)
    await asyncio.sleep(10)
    while True:
        if sec_form4_sync_due():
            try:
                await sync_sec_form4()
            except Exception as e:
                print(f"sec_form4 sync failed against {STRATEGY_ENGINE_URL}: {e}", flush=True)
        if _sec_13f_batch_sync_due_cron():
            try:
                await sync_sec_13f_batch()
            except Exception as e:
                print(f"sec_13f batch sync failed against {STRATEGY_ENGINE_URL}: {e}", flush=True)
        await run_once()
        await asyncio.sleep(ALERT_SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main_loop())

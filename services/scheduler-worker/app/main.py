import asyncio
import os
import time
from datetime import datetime, timezone

import httpx

STRATEGY_ENGINE_URL = os.getenv("STRATEGY_ENGINE_URL", "http://strategy-engine:8002").rstrip("/")
DISCORD_ALERT_WEBHOOK_URL = os.getenv("DISCORD_ALERT_WEBHOOK_URL", "")
ALERT_SCAN_INTERVAL_SECONDS = int(os.getenv("ALERT_SCAN_INTERVAL_SECONDS", "300"))
ALERT_TOP_N = int(os.getenv("ALERT_TOP_N", "3"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
SEC_FORM4_SYNC_INTERVAL_SECONDS = int(os.getenv("SEC_FORM4_SYNC_INTERVAL_SECONDS", "86400"))
SEC_FORM4_SYNC_CRON = os.getenv("SEC_FORM4_SYNC_CRON", "").strip()
SEC_FORM4_SYNC_LOOKBACK_DAYS = int(os.getenv("SEC_FORM4_SYNC_LOOKBACK_DAYS", "2"))
SEC_FORM4_SYNC_RETAIN_DAYS = int(os.getenv("SEC_FORM4_SYNC_RETAIN_DAYS", "10"))

# in-memory cooldown for v1
# later this should move to Postgres/Redis
LAST_ALERT_AT: dict[str, float] = {}
LAST_SEC_FORM4_SYNC_AT: float = 0.0
LAST_SEC_FORM4_SYNC_SLOT: str = ""


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


def mark_sec_form4_synced() -> None:
    global LAST_SEC_FORM4_SYNC_AT, LAST_SEC_FORM4_SYNC_SLOT
    LAST_SEC_FORM4_SYNC_AT = time.time()
    LAST_SEC_FORM4_SYNC_SLOT = _cron_slot_key(datetime.now(timezone.utc))


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
    if not _cron_matches(now, SEC_FORM4_SYNC_CRON):
        return False
    return LAST_SEC_FORM4_SYNC_SLOT != _cron_slot_key(now)


async def sync_sec_form4() -> None:
    async with httpx.AsyncClient(timeout=300.0) as client:
        resp = await client.post(
            f"{STRATEGY_ENGINE_URL}/v1/admin/sec-form4/sync",
            params={
                "days_back": SEC_FORM4_SYNC_LOOKBACK_DAYS,
                "retain_days": SEC_FORM4_SYNC_RETAIN_DAYS,
            },
        )
        resp.raise_for_status()
        payload = resp.json()
        mark_sec_form4_synced()
        print(f"SEC Form 4 sync complete: {payload}")


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
    print("scheduler-worker started")
    print(f"Strategy engine: {STRATEGY_ENGINE_URL}")
    print(f"Scan interval: {ALERT_SCAN_INTERVAL_SECONDS}s")
    print(f"Top N alerts: {ALERT_TOP_N}")
    print(f"Cooldown: {ALERT_COOLDOWN_SECONDS}s")
    if SEC_FORM4_SYNC_CRON:
        print(f"SEC Form 4 sync cron (UTC): {SEC_FORM4_SYNC_CRON}")
    else:
        print(f"SEC Form 4 sync interval: {SEC_FORM4_SYNC_INTERVAL_SECONDS}s")
    print("Waiting 10 seconds for dependencies to become ready...", flush=True)
    await asyncio.sleep(10)
    while True:
        if sec_form4_sync_due():
            try:
                await sync_sec_form4()
            except Exception as e:
                print(f"sec_form4 sync failed against {STRATEGY_ENGINE_URL}: {e}", flush=True)
        await run_once()
        await asyncio.sleep(ALERT_SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main_loop())

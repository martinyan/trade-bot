import asyncio
import os
import time

import httpx

STRATEGY_ENGINE_URL = os.getenv("STRATEGY_ENGINE_URL", "http://strategy-engine:8002").rstrip("/")
DISCORD_ALERT_WEBHOOK_URL = os.getenv("DISCORD_ALERT_WEBHOOK_URL", "")
ALERT_SCAN_INTERVAL_SECONDS = int(os.getenv("ALERT_SCAN_INTERVAL_SECONDS", "300"))
ALERT_TOP_N = int(os.getenv("ALERT_TOP_N", "3"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))

# in-memory cooldown for v1
# later this should move to Postgres/Redis
LAST_ALERT_AT: dict[str, float] = {}


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
    print("Waiting 10 seconds for dependencies to become ready...", flush=True)
    await asyncio.sleep(10)
    while True:
        await run_once()
        await asyncio.sleep(ALERT_SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main_loop())
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis

app = FastAPI(title="strategy-engine", version="0.1.0")

MARKET_DATA_SERVICE_URL = os.getenv("MARKET_DATA_SERVICE_URL", "http://market-data-service:8001").rstrip("/")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")

redis_client = Redis.from_url(REDIS_URL, decode_responses=True)


class WatchAddRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    symbol: str = Field(..., min_length=1, max_length=10)


async def _market_data_get(path: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{MARKET_DATA_SERVICE_URL}/{path.lstrip('/')}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url, params=params)

    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="market-data-service request failed")

    return response.json()


@app.get("/health")
async def health() -> dict[str, str]:
    redis_status = "ok"
    try:
        await redis_client.ping()
    except Exception:
        redis_status = "error"

    postgres_status = "configured" if POSTGRES_DSN else "not_configured"
    return {"status": "ok", "redis": redis_status, "postgres": postgres_status}


@app.get("/v1/brief")
async def brief(symbol: str) -> dict[str, Any]:
    normalized = symbol.strip().upper()
    quote_result = await _market_data_get("/v1/quotes", {"symbols": normalized})

    items = quote_result.get("data", [])
    if not items:
        raise HTTPException(status_code=404, detail=f"No quote found for {normalized}")

    q = items[0]
    return {
        "symbol": normalized,
        "price": q.get("price"),
        "change": q.get("change"),
        "changePercentage": q.get("changePercentage"),
        "dayLow": q.get("dayLow"),
        "dayHigh": q.get("dayHigh"),
        "volume": q.get("volume"),
    }


@app.get("/v1/scan/premarket")
async def scan_premarket(limit: int = 30):
    try:
        payload = await _market_data_get("/v1/universe/quotes", {"exchanges": "NASDAQ,NYSE"})
        rows = payload.get("data", [])

        if not isinstance(rows, list):
            return {"count": 0, "data": []}

        filtered = []

        for item in rows:
            symbol = item.get("symbol")
            exchange = str(item.get("exchange", "") or "").upper()
            price = float(item.get("price", 0) or 0)
            volume = float(item.get("volume", 0) or 0)
            market_cap = float(item.get("marketCap", 0) or 0)
            change_pct = float(item.get("changePercentage", 0) or 0)
            avg10d_volume = float(item.get("avg10dVolume", 0) or 0)

            if exchange not in {"NASDAQ", "NYSE"}:
                continue
            if market_cap <= 2_000_000_000:
                continue
            if change_pct <= 8:
                continue

            dollar_volume = price * volume
            relative_volume = (volume / avg10d_volume) if avg10d_volume > 0 else 0

            if not (dollar_volume > 100_000_000 or relative_volume > 1.5):
                continue

            filtered.append({
                "symbol": symbol,
                "exchange": exchange,
                "price": round(price, 2),
                "changePercentage": round(change_pct, 2),
                "volume": int(volume),
                "marketCap": int(market_cap),
                "dollarVolume": round(dollar_volume, 2),
                "avg10dVolume": int(avg10d_volume) if avg10d_volume > 0 else None,
                "relativeVolume": round(relative_volume, 2) if avg10d_volume > 0 else None,
            })

        filtered.sort(
            key=lambda x: (
                x["dollarVolume"],
                x["relativeVolume"] or 0,
                x["changePercentage"],
            ),
            reverse=True,
        )

        return {
            "count": min(limit, len(filtered)),
            "data": filtered[:limit],
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"market-data-service request failed: {e}")

@app.post("/v1/watchlist/add")
async def watch_add(payload: WatchAddRequest) -> dict[str, Any]:
    symbol = payload.symbol.strip().upper()
    key = f"watchlist:{payload.user_id}"
    await redis_client.sadd(key, symbol)
    watchlist = sorted(await redis_client.smembers(key))
    return {"user_id": payload.user_id, "watchlist": watchlist}

@app.get("/v1/quote-detail")
async def quote_detail(symbol: str):
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            quote_resp = await client.get(
                f"{MARKET_DATA_SERVICE_URL}/quote/{symbol}"
            )
            quote_resp.raise_for_status()

            profile_resp = await client.get(
                f"{MARKET_DATA_SERVICE_URL}/profile/{symbol}"
            )
            profile_resp.raise_for_status()

        quote_data = quote_resp.json()
        profile_data = profile_resp.json()

        q = quote_data[0] if isinstance(quote_data, list) and quote_data else {}
        p = profile_data[0] if isinstance(profile_data, list) and profile_data else {}

        return {
            "symbol": q.get("symbol", symbol.upper()),
            "companyName": p.get("companyName"),
            "exchangeShortName": p.get("exchange"),
            "price": q.get("price"),
            "change": q.get("change"),
            "changePercentage": q.get("changePercentage"),
            "open": q.get("open"),
            "previousClose": q.get("previousClose"),
            "dayLow": q.get("dayLow"),
            "dayHigh": q.get("dayHigh"),
            "yearLow": q.get("yearLow"),
            "yearHigh": q.get("yearHigh"),
            "volume": q.get("volume"),
            "avgVolume": p.get("averageVolume"),
            "marketCap": q.get("marketCap") or p.get("marketCap"),
            "earningsAnnouncement": q.get("earningsAnnouncement"),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/v1/news")
async def news(symbol: str, limit: int = 5) -> dict[str, Any]:
    normalized = symbol.strip().upper()

    try:
        payload = await _market_data_get(f"/v1/news/{normalized}", {"limit": limit})
        items = payload.get("data", []) if isinstance(payload, dict) else []

        return {
            "symbol": normalized,
            "count": len(items),
            "data": items,
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"news request failed: {e}")
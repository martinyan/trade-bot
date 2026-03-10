import os
import asyncio
from datetime import datetime, timedelta
from statistics import mean
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
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
    quote_result = await _market_data_get("/v1/quotes", {"symbols": symbol})
    quote_rows = quote_result.get("data", []) if isinstance(quote_result, dict) else []
    if not isinstance(quote_rows, list) or not quote_rows:
        raise HTTPException(status_code=404, detail=f"No quote found for {symbol}")

    key = f"watchlist:{payload.user_id}"
    current = set(await redis_client.smembers(key))
    if symbol not in current and len(current) >= 5:
        raise HTTPException(status_code=400, detail="Watchlist limit reached (max 5 symbols)")

    await redis_client.sadd(key, symbol)
    watchlist = sorted(await redis_client.smembers(key))
    return {"user_id": payload.user_id, "watchlist": watchlist, "max_size": 5}


@app.post("/v1/watchlist/remove")
async def watch_remove(payload: WatchAddRequest) -> dict[str, Any]:
    symbol = payload.symbol.strip().upper()
    key = f"watchlist:{payload.user_id}"
    removed = await redis_client.srem(key, symbol)
    watchlist = sorted(await redis_client.smembers(key))
    return {
        "user_id": payload.user_id,
        "removed": bool(removed),
        "symbol": symbol,
        "watchlist": watchlist,
        "max_size": 5,
    }


@app.get("/v1/watchlist")
async def watch_get(user_id: str) -> dict[str, Any]:
    if not user_id.strip():
        raise HTTPException(status_code=400, detail="user_id is required")
    key = f"watchlist:{user_id}"
    watchlist = sorted(await redis_client.smembers(key))
    return {"user_id": user_id, "watchlist": watchlist, "max_size": 5}


@app.get("/v1/watchlist/all")
async def watch_all() -> dict[str, Any]:
    cursor = 0
    out: dict[str, list[str]] = {}
    pattern = "watchlist:*"

    while True:
        cursor, keys = await redis_client.scan(cursor=cursor, match=pattern, count=100)
        for key in keys:
            user_id = key.split("watchlist:", 1)[-1]
            symbols = sorted(await redis_client.smembers(key))
            if symbols:
                out[user_id] = symbols
        if cursor == 0:
            break

    return {"count": len(out), "data": out, "max_size": 5}

@app.get("/v1/quote-detail")
async def quote_detail(symbol: str):
    try:
        normalized = symbol.strip().upper()
        if not normalized:
            raise HTTPException(status_code=400, detail="symbol is required")

        async with httpx.AsyncClient(timeout=20.0) as client:
            quote_resp = await client.get(
                f"{MARKET_DATA_SERVICE_URL}/quote/{normalized}"
            )
            quote_resp.raise_for_status()

            profile_resp = await client.get(
                f"{MARKET_DATA_SERVICE_URL}/profile/{normalized}"
            )
            profile_resp.raise_for_status()

        quote_data = quote_resp.json()
        profile_data = profile_resp.json()

        q = quote_data[0] if isinstance(quote_data, list) and quote_data else {}
        p = profile_data[0] if isinstance(profile_data, list) and profile_data else {}

        if not isinstance(q, dict):
            q = {}
        if not isinstance(p, dict):
            p = {}

        # FMP can return HTTP 200 with empty arrays for unknown symbols.
        if not q and not p:
            raise HTTPException(status_code=404, detail=f"No quote found for {normalized}")

        return {
            "symbol": q.get("symbol", normalized),
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
    except HTTPException:
        raise
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


def _parse_insider_dt(value: Any) -> datetime:
    if not value or not isinstance(value, str):
        return datetime.min

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return datetime.min


def _parse_iso_date(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d")
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _calculate_earnings_risk_payload(
    symbol: str,
    quote: dict[str, Any],
    earnings_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    now = datetime.utcnow()
    parsed_rows: list[tuple[datetime, dict[str, Any]]] = []
    for row in earnings_rows:
        if not isinstance(row, dict):
            continue
        event_date = _parse_iso_date(row.get("date"))
        if event_date is None:
            continue
        parsed_rows.append((event_date, row))

    if not parsed_rows:
        raise HTTPException(status_code=404, detail=f"No valid earnings dates found for {symbol}")

    future_rows = sorted((item for item in parsed_rows if item[0] >= now), key=lambda x: x[0])
    next_event_date, next_event_row = (
        future_rows[0] if future_rows else sorted(parsed_rows, key=lambda x: x[0], reverse=True)[0]
    )
    days_to_event = (next_event_date.date() - now.date()).days

    historical_rows = [row for dt, row in sorted(parsed_rows, key=lambda x: x[0], reverse=True) if dt < now]
    recent_hist = historical_rows[:4]

    eps_surprises: list[float] = []
    miss_count = 0
    for row in recent_hist:
        actual = _to_float(row.get("epsActual"))
        estimate = _to_float(row.get("epsEstimated"))
        if actual is None or estimate is None or estimate == 0:
            continue
        surprise_pct = ((actual - estimate) / abs(estimate)) * 100
        eps_surprises.append(surprise_pct)
        if surprise_pct < 0:
            miss_count += 1

    if days_to_event <= 1:
        proximity_score = 35.0
    elif days_to_event <= 3:
        proximity_score = 30.0
    elif days_to_event <= 7:
        proximity_score = 24.0
    elif days_to_event <= 14:
        proximity_score = 16.0
    elif days_to_event <= 30:
        proximity_score = 8.0
    else:
        proximity_score = 2.0

    avg_abs_surprise = mean([abs(x) for x in eps_surprises]) if eps_surprises else 0.0
    surprise_variability_score = min(25.0, avg_abs_surprise * 2.0)

    quarters_scored = len(eps_surprises)
    miss_ratio = (miss_count / quarters_scored) if quarters_scored else 0.0
    miss_history_score = miss_ratio * 20.0

    price = _to_float(quote.get("price")) or 0.0
    day_low = _to_float(quote.get("dayLow"))
    day_high = _to_float(quote.get("dayHigh"))
    range_pct = 0.0
    if price > 0 and day_low is not None and day_high is not None and day_high >= day_low:
        range_pct = ((day_high - day_low) / price) * 100
    intraday_vol_score = min(20.0, range_pct * 4.0)

    change_pct = abs(_to_float(quote.get("changePercentage")) or 0.0)
    momentum_shock_score = min(10.0, change_pct * 2.0)

    total_score = round(
        proximity_score
        + surprise_variability_score
        + miss_history_score
        + intraday_vol_score
        + momentum_shock_score,
        1,
    )

    if total_score >= 70:
        label = "High"
    elif total_score >= 40:
        label = "Moderate"
    else:
        label = "Low"

    return {
        "symbol": symbol,
        "score": total_score,
        "label": label,
        "next_earnings": {
            "date": next_event_row.get("date"),
            "days_to_event": days_to_event,
            "eps_estimated": _to_float(next_event_row.get("epsEstimated")),
            "revenue_estimated": _to_float(next_event_row.get("revenueEstimated")),
        },
        "history": {
            "quarters_used": quarters_scored,
            "miss_count": miss_count,
            "beat_count": max(quarters_scored - miss_count, 0),
            "avg_abs_eps_surprise_pct": round(avg_abs_surprise, 2),
        },
        "market_context": {
            "price": _to_float(quote.get("price")),
            "change_percentage": _to_float(quote.get("changePercentage")),
            "day_range_pct": round(range_pct, 2),
        },
        "components": {
            "proximity_0_35": round(proximity_score, 1),
            "surprise_variability_0_25": round(surprise_variability_score, 1),
            "miss_history_0_20": round(miss_history_score, 1),
            "intraday_volatility_0_20": round(intraday_vol_score, 1),
            "momentum_shock_0_10": round(momentum_shock_score, 1),
        },
    }


@app.get("/v1/insider-trades/latest")
async def latest_insider_trade(
    symbol: str,
    limit: int = Query(10, ge=1, le=20),
    days: int = Query(60, ge=1, le=365),
) -> dict[str, Any]:
    normalized = symbol.strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="symbol is required")

    try:
        payload = await _market_data_get(
            f"/v1/insider-trades/{normalized}",
            {"page": 0, "limit": 100, "include_stats": True},
        )
        items = payload.get("data", []) if isinstance(payload, dict) else []

        if not items:
            raise HTTPException(status_code=404, detail=f"No insider trades found for {normalized}")

        def _row_trade_dt(row: dict[str, Any]) -> datetime:
            transaction_dt = _parse_insider_dt(row.get("transaction_date"))
            if transaction_dt != datetime.min:
                return transaction_dt
            return _parse_insider_dt(row.get("filing_date"))

        cutoff = datetime.utcnow() - timedelta(days=days)
        recent = [row for row in items if _row_trade_dt(row) >= cutoff]

        if not recent:
            raise HTTPException(
                status_code=404,
                detail=f"No insider trades found for {normalized} in the last {days} days",
            )

        ranked = sorted(
            recent,
            key=_row_trade_dt,
            reverse=True,
        )
        top = ranked[:limit]

        fmp_stats: Any = None
        if isinstance(payload, dict):
            raw_stats = payload.get("stats")
            if isinstance(raw_stats, dict):
                fmp_stats = raw_stats
            elif isinstance(raw_stats, list):
                first = raw_stats[0] if raw_stats else None
                if isinstance(first, dict):
                    fmp_stats = first

        return {
            "symbol": normalized,
            "window_days": days,
            "total_recent": len(recent),
            "count": len(top),
            "fmp_statistics": fmp_stats,
            "data": top,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"insider trade request failed: {e}")


@app.get("/v1/earnings-risk")
async def earnings_risk(symbol: str) -> dict[str, Any]:
    normalized = symbol.strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="symbol is required")

    try:
        quote_result = await _market_data_get("/v1/quotes", {"symbols": normalized})
        quote_rows = quote_result.get("data", []) if isinstance(quote_result, dict) else []
        quote = quote_rows[0] if isinstance(quote_rows, list) and quote_rows else {}
        if not isinstance(quote, dict) or not quote:
            raise HTTPException(status_code=404, detail=f"No quote found for {normalized}")

        earnings_result = await _market_data_get(f"/v1/earnings/{normalized}", {"limit": 12})
        earnings_rows = earnings_result.get("data", []) if isinstance(earnings_result, dict) else []
        if not isinstance(earnings_rows, list) or not earnings_rows:
            raise HTTPException(status_code=404, detail=f"No earnings data found for {normalized}")

        return _calculate_earnings_risk_payload(normalized, quote, earnings_rows)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"earnings risk request failed: {e}")


@app.get("/v1/catalyst-brief")
async def catalyst_brief(
    symbol: str,
    news_limit: int = Query(3, ge=1, le=10),
    insider_days: int = Query(60, ge=1, le=365),
    insider_limit: int = Query(10, ge=1, le=50),
) -> dict[str, Any]:
    normalized = symbol.strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="symbol is required")

    quote_res, earnings_res, insider_res, news_res = await asyncio.gather(
        quote_detail(normalized),
        earnings_risk(normalized),
        latest_insider_trade(
            normalized,
            limit=insider_limit,
            days=insider_days,
        ),
        news(normalized, limit=news_limit),
        return_exceptions=True,
    )

    errors: dict[str, str] = {}
    quote_data = quote_res if isinstance(quote_res, dict) else None
    earnings_data = earnings_res if isinstance(earnings_res, dict) else None
    insider_data = insider_res if isinstance(insider_res, dict) else None
    news_data = news_res if isinstance(news_res, dict) else None

    if quote_data is None and isinstance(quote_res, Exception):
        errors["quote"] = str(quote_res)
    if earnings_data is None and isinstance(earnings_res, Exception):
        errors["earnings_risk"] = str(earnings_res)
    if insider_data is None and isinstance(insider_res, Exception):
        errors["insider"] = str(insider_res)
    if news_data is None and isinstance(news_res, Exception):
        errors["news"] = str(news_res)

    if quote_data is None and earnings_data is None and insider_data is None and news_data is None:
        raise HTTPException(status_code=502, detail=f"catalyst brief failed for {normalized}")

    insider_summary: dict[str, Any] | None = None
    if insider_data:
        fmp_stats = (
            insider_data.get("fmp_statistics")
            if isinstance(insider_data.get("fmp_statistics"), dict)
            else {}
        )
        rows = insider_data.get("data", []) if isinstance(insider_data.get("data"), list) else []
        latest = rows[0] if rows and isinstance(rows[0], dict) else None
        insider_summary = {
            "window_days": insider_data.get("window_days"),
            "total_recent": insider_data.get("total_recent"),
            "statistics": fmp_stats,
            "latest_trade": latest,
        }

    top_news: list[dict[str, Any]] = []
    if news_data:
        for item in news_data.get("data", []):
            if not isinstance(item, dict):
                continue
            top_news.append(
                {
                    "title": item.get("title"),
                    "site": item.get("site"),
                    "published_date": item.get("published_date"),
                    "url": item.get("url"),
                }
            )

    # A valid symbol must resolve to quote data; otherwise return a not-found error.
    if quote_data is None:
        raise HTTPException(status_code=404, detail=f"No quote found for {normalized}")

    return {
        "symbol": normalized,
        "quote": quote_data,
        "earnings_risk": earnings_data,
        "insider_summary": insider_summary,
        "news": top_news[:news_limit],
        "errors": errors,
    }

import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="market-data-service", version="0.1.0")

FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FMP_BASE_URL = os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com/stable").rstrip("/")


async def _fmp_get(path: str, params: dict[str, Any] | None = None) -> Any:
    if not FMP_API_KEY:
        raise HTTPException(status_code=500, detail="FMP_API_KEY is not configured")

    qp = dict(params or {})
    qp["apikey"] = FMP_API_KEY

    url = f"{FMP_BASE_URL}/{path.lstrip('/')}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url, params=qp)

    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"FMP request failed with status {response.status_code}: {response.text}",
        )

    return response.json()

def _normalize_quote_row(item: dict[str, Any]) -> dict[str, Any]:
    symbol = item.get("symbol")
    exchange = (
        item.get("exchange")
        or item.get("exchangeShortName")
        or item.get("exchangeName")
        or ""
    )

    price = float(item.get("price", 0) or 0)
    volume = float(item.get("volume", 0) or 0)
    market_cap = float(item.get("marketCap", 0) or 0)

    raw_change_pct = item.get("changePercentage")
    if raw_change_pct is None:
        raw_change_pct = item.get("changesPercentage")
    change_pct = float(raw_change_pct or 0)

    return {
        "symbol": symbol,
        "exchange": str(exchange).upper(),
        "price": price,
        "changePercentage": change_pct,
        "volume": volume,
        "marketCap": market_cap,
        "avg10dVolume": 0,  # placeholder for now
    }

@app.get("/profile/{symbol}")
async def profile(symbol: str) -> Any:
    return await _fmp_get("profile", {"symbol": symbol.upper()})

@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/quote/{symbol}")
async def quote(symbol: str) -> Any:
    return await _fmp_get("quote", {"symbol": symbol.upper()})


def _normalize_news_row(row: dict[str, Any]) -> dict[str, Any]:
    symbols = row.get("symbol") or row.get("symbols") or []
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]

    return {
        "published_date": row.get("publishedDate") or row.get("date") or row.get("published_date"),
        "title": row.get("title"),
        "site": row.get("site") or row.get("source"),
        "text": row.get("text") or row.get("snippet"),
        "url": row.get("url"),
        "image": row.get("image"),
        "symbols": symbols,
        "raw": row,
    }


def _normalize_search_variant_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": row.get("symbol"),
        "companyName": row.get("companyName"),
        "cusip": row.get("cusip"),
        "isin": row.get("isin"),
        "cik": row.get("cik"),
        "exchange": row.get("exchange"),
        "exchangeShortName": row.get("exchangeShortName"),
        "isEtf": bool(row.get("isEtf")),
        "isFund": bool(row.get("isFund")),
        "isAdr": bool(row.get("isAdr")),
        "isActivelyTrading": bool(row.get("isActivelyTrading")),
        "country": row.get("country"),
    }


@app.get("/v1/news/{symbol}")
async def news(symbol: str, limit: int = Query(5, ge=1, le=20)) -> dict[str, Any]:
    data = await _fmp_get("news/stock", {"symbols": symbol.upper(), "limit": limit})

    if not isinstance(data, list):
        raise HTTPException(status_code=502, detail=f"Unexpected FMP response: {data}")

    normalized = [_normalize_news_row(item) for item in data if isinstance(item, dict)]

    return {
        "symbol": symbol.upper(),
        "limit": limit,
        "count": len(normalized),
        "data": normalized,
    }


@app.get("/v1/search-exchange-variants/{symbol}")
async def search_exchange_variants(symbol: str) -> dict[str, Any]:
    data = await _fmp_get("search-exchange-variants", {"symbol": symbol.upper()})

    if not isinstance(data, list):
        raise HTTPException(status_code=502, detail=f"Unexpected FMP response: {data}")

    rows = [_normalize_search_variant_row(item) for item in data if isinstance(item, dict)]
    return {
        "symbol": symbol.upper(),
        "count": len(rows),
        "data": rows,
    }


@app.get("/v1/earnings/{symbol}")
async def earnings(symbol: str, limit: int = Query(12, ge=1, le=40)) -> dict[str, Any]:
    data = await _fmp_get("earnings", {"symbol": symbol.upper(), "limit": limit})

    if not isinstance(data, list):
        raise HTTPException(status_code=502, detail=f"Unexpected FMP response: {data}")

    rows = [item for item in data if isinstance(item, dict)]
    return {
        "symbol": symbol.upper(),
        "limit": limit,
        "count": len(rows),
        "data": rows,
    }


@app.get("/news/{symbol}")
async def news_legacy(symbol: str, limit: int = Query(5, ge=1, le=20)) -> Any:
    return await _fmp_get("stock-news", {"symbols": symbol.upper(), "limit": limit})


@app.get("/v1/quotes")
async def quotes(symbols: str = Query(..., description="Comma-separated symbols")) -> dict[str, Any]:
    data = await _fmp_get("quote", {"symbol": symbols})
    return {"count": len(data), "data": data}
    
@app.get("/v1/universe/quotes")
async def universe_quotes(
    exchanges: str = Query("NASDAQ,NYSE", description="Comma-separated exchanges")
) -> dict[str, Any]:
    try:
        exchange_list = [ex.strip().upper() for ex in exchanges.split(",") if ex.strip()]
        if not exchange_list:
            raise HTTPException(status_code=400, detail="No exchanges provided")

        all_rows: list[dict[str, Any]] = []
        failed_exchanges: list[str] = []

        for exchange in exchange_list:
            try:
                data = await _fmp_get("batch-exchange-quote", {"exchange": exchange})
            except HTTPException:
                failed_exchanges.append(exchange)
                continue

            if not isinstance(data, list):
                failed_exchanges.append(exchange)
                continue

            for item in data:
                if not isinstance(item, dict):
                    continue
                normalized = _normalize_quote_row(item)
                if normalized.get("symbol"):
                    all_rows.append(normalized)

        if not all_rows:
            raise HTTPException(
                status_code=502,
                detail=f"No exchange quote data available. Failed exchanges: {failed_exchanges}",
            )

        return {
            "count": len(all_rows),
            "data": all_rows,
            "failedExchanges": failed_exchanges,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"universe_quotes failed: {e}")

@app.get("/v1/scan/premarket")
async def scan_premarket(limit: int = Query(10, ge=1, le=50)) -> dict[str, Any]:
    # Starter universe for v1. Later we can expand this or load from DB/watchlists.
    symbols = [
        "AAPL", "MSFT", "NVDA", "TSLA", "META",
        "AMZN", "AMD", "PLTR", "GOOGL", "NFLX",
        "AVGO", "SMCI", "MU", "CRWD", "PANW",
    ]

    try:
        data = await _fmp_get("quote", {"symbol": ",".join(symbols)})

        if not isinstance(data, list):
            raise HTTPException(status_code=502, detail=f"Unexpected FMP response: {data}")

        # basic sort by absolute move %
        ranked = sorted(
            data,
            key=lambda x: abs(float(x.get("changePercentage", 0) or 0)),
            reverse=True,
        )

        return {"count": min(limit, len(ranked)), "data": ranked[:limit]}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"scan_premarket failed: {e}")
def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def _normalize_insider_row(row: dict[str, Any]) -> dict[str, Any]:
    qty = _safe_int(
        row.get("securitiesTransacted")
        or row.get("securities_transacted")
        or row.get("transactionShares")
    )
    price = _safe_float(row.get("price"))
    value = qty * price if qty is not None and price is not None else None

    return {
        "filing_date": row.get("filingDate") or row.get("filing_date"),
        "transaction_date": row.get("transactionDate") or row.get("transaction_date"),
        "reporting_name": row.get("reportingName") or row.get("reporting_name"),
        "reporting_cik": row.get("reportingCik") or row.get("reporting_cik"),
        "company_cik": row.get("companyCik") or row.get("company_cik"),
        "company_name": row.get("companyName") or row.get("issuerName"),
        "type": row.get("transactionType") or row.get("type"),
        "security_name": row.get("securityName") or row.get("security_name"),
        "securities_transacted": qty,
        "price": price,
        "value": value,
        "shares_owned_after": _safe_int(
            row.get("securitiesOwned") or row.get("sharesOwnedFollowingTransaction")
        ),
        "acquisition_or_disposition": (
            row.get("acquistionOrDisposition")
            or row.get("acquisitionOrDisposition")
            or row.get("acquisition_or_disposition")
        ),
        "form_type": row.get("formType") or row.get("form_type"),
        "filing_url": (
            row.get("url")
            or row.get("link")
            or row.get("filingUrl")
            or row.get("filing_url")
        ),
        "raw": row,
    }
@app.get("/v1/insider-trades/{symbol}")
async def insider_trades(
    symbol: str,
    page: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    include_stats: bool = Query(True),
) -> dict[str, Any]:
    try:
        trades_data = await _fmp_get(
            "insider-trading/search",
            {
                "symbol": symbol.upper(),
                "page": page,
                "limit": limit,
            },
        )

        if not isinstance(trades_data, list):
            raise HTTPException(
                status_code=502,
                detail=f"Unexpected insider trades response: {trades_data}",
            )

        stats: Any = None
        if include_stats:
            try:
                stats = await _fmp_get(
                    "insider-trading/statistics",
                    {"symbol": symbol.upper()},
                )
            except Exception:
                stats = {"available": False}

        normalized = [
            _normalize_insider_row(item)
            for item in trades_data
            if isinstance(item, dict)
        ]

        return {
            "symbol": symbol.upper(),
            "page": page,
            "limit": limit,
            "count": len(normalized),
            "stats": stats,
            "data": normalized,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"insider_trades failed: {e}")

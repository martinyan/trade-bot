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


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/quote/{symbol}")
async def quote(symbol: str) -> Any:
    return await _fmp_get("quote", {"symbol": symbol.upper()})


@app.get("/profile/{symbol}")
async def profile(symbol: str) -> Any:
    return await _fmp_get("profile", {"symbol": symbol.upper()})


@app.get("/news/{symbol}")
async def news(symbol: str, limit: int = Query(5, ge=1, le=20)) -> Any:
    return await _fmp_get("stock-news", {"symbols": symbol.upper(), "limit": limit})


@app.get("/v1/quotes")
async def quotes(symbols: str = Query(..., description="Comma-separated symbols")) -> dict[str, Any]:
    data = await _fmp_get("quote", {"symbol": symbols})
    return {"count": len(data), "data": data}


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
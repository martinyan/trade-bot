import asyncio
import json
import os
import re
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="market-data-service", version="0.1.0")

FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FMP_BASE_URL = os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com/stable").rstrip("/")
ETNET_BASE_URL = os.getenv("ETNET_BASE_URL", "https://www.etnet.com.hk").rstrip("/")


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


async def _etnet_get(path: str, params: dict[str, Any] | None = None) -> str:
    url = f"{ETNET_BASE_URL}/{path.lstrip('/')}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        response = await client.get(url, params=params, headers=headers)

    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"ET Net request failed with status {response.status_code}: {response.text[:300]}",
        )

    # ET Net uses a meta refresh redirect to quote_form.php?error=1 for invalid HK codes.
    if "quote_form.php?error=1" in response.text:
        code_value = str((params or {}).get("code") or "").strip()
        raise HTTPException(
            status_code=404,
            detail=f"No quote found for {code_value}" if code_value else "No quote found",
        )

    return response.text


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", "", value)
    text = text.replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", text).strip()


def _as_float_or_none(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = value.replace(",", "").replace("%", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_etnet_quote_html(html: str, code: str) -> dict[str, Any]:
    name_match = re.search(r'<div id="QuoteNameA">\s*([0-9]{5})\s+([^<]+?)\s*</div>', html, re.IGNORECASE)
    quote_match = re.search(
        r'<div id="QuoteNameD">.*?Nominal.*?<span class="HeaderTxt[^"]*">\s*([0-9,]+\.[0-9]+)\s*</span>'
        r'\s*<span class="boldTxt">\s*([+-]?[0-9,]+\.[0-9]+)\s*\(([+-]?[0-9,]+\.[0-9]+)%\)\s*</span>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    updated_match = re.search(
        r"Real time quote last updated:\s*([0-9]{2}/[0-9]{2}/[0-9]{4}\s+[0-9]{2}:[0-9]{2})",
        html,
        re.IGNORECASE,
    )
    vwap_match = re.search(
        r'<li class="Title boldTxt" style="font-size:12px">Today</li>.*?<li class="VWAP boldTxt">([0-9,]+\.[0-9]+)</li>',
        html,
        re.IGNORECASE | re.DOTALL,
    )

    if not name_match or not quote_match:
        raise HTTPException(status_code=502, detail="ET Net quote page format was not recognized")

    padded_code = name_match.group(1)
    company_name = _strip_html(name_match.group(2))
    price = _as_float_or_none(quote_match.group(1))
    change = _as_float_or_none(quote_match.group(2))
    change_pct = _as_float_or_none(quote_match.group(3))
    vwap = _as_float_or_none(vwap_match.group(1)) if vwap_match else None

    return {
        "ticker": str(int(code)),
        "code": padded_code,
        "symbol": f"{padded_code}.HK",
        "name": company_name,
        "price": price,
        "change": change,
        "changePercentage": change_pct,
        "vwap": vwap,
        "lastUpdated": updated_match.group(1) if updated_match else None,
        "source": "etnet",
        "sourceUrl": f"{ETNET_BASE_URL}/www/eng/stocks/realtime/quote_transaction.php?code={int(code)}",
    }


def _parse_etnet_chart_html(html: str) -> dict[str, Any]:
    quote_match = re.search(
        r'<div id="icQuote">.*?<div class="quoteName"[^>]*>\s*([0-9]{5})\s+([^<]+)\s*</div>.*?'
        r'<div class="quotePrice [^"]*">\s*([0-9,]+\.[0-9]+)\s*<span class="icquoteChange">\s*'
        r'([+-]?[0-9,]+\.[0-9]+)\s*\(([+-]?[0-9,]+\.[0-9]+)%\)\s*</span>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    low_52_match = re.search(r'<div class="lowPrice">\s*([0-9,]+\.[0-9]+)\s*</div>', html, re.IGNORECASE)
    high_52_match = re.search(r'<div class="highPrice">\s*([0-9,]+\.[0-9]+)\s*</div>', html, re.IGNORECASE)
    if not quote_match:
        raise HTTPException(status_code=502, detail="ET Net chart page format was not recognized")

    marker = "var testData_1_Daily = "
    marker_idx = html.find(marker)
    if marker_idx < 0:
        raise HTTPException(status_code=502, detail="ET Net chart page did not include daily series data")

    start_idx = html.find("{", marker_idx)
    if start_idx < 0:
        raise HTTPException(status_code=502, detail="ET Net chart series object start was not found")

    depth = 0
    end_idx = -1
    for idx in range(start_idx, len(html)):
        ch = html[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end_idx = idx
                break

    if end_idx < 0:
        raise HTTPException(status_code=502, detail="ET Net chart series object end was not found")

    try:
        series_payload = json.loads(html[start_idx : end_idx + 1])
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to parse ET Net chart data: {exc}") from exc

    rows = series_payload.get("result", []) if isinstance(series_payload, dict) else []
    daily_rows = [row for row in rows if isinstance(row, list) and len(row) >= 6]
    if not daily_rows:
        raise HTTPException(status_code=502, detail="ET Net chart page returned no daily series data")

    latest = daily_rows[-1]
    previous = daily_rows[-2] if len(daily_rows) >= 2 else None
    trailing_avg_rows = daily_rows[-30:] if len(daily_rows) >= 30 else daily_rows

    latest_ts = int(latest[0])
    latest_dt = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
    trailing_start = latest_dt - timedelta(days=365)
    trailing_rows = [
        row for row in daily_rows
        if datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc) >= trailing_start
    ]
    if not trailing_rows:
        trailing_rows = daily_rows

    derived_year_low = min(float(row[3]) for row in trailing_rows)
    derived_year_high = max(float(row[2]) for row in trailing_rows)

    return {
        "code": quote_match.group(1),
        "name": _strip_html(quote_match.group(2)),
        "chineseName": series_payload.get("tc") or series_payload.get("sc"),
        "price": _as_float_or_none(quote_match.group(3)),
        "change": _as_float_or_none(quote_match.group(4)),
        "changePercentage": _as_float_or_none(quote_match.group(5)),
        "yearLow": _as_float_or_none(low_52_match.group(1)) if low_52_match else derived_year_low,
        "yearHigh": _as_float_or_none(high_52_match.group(1)) if high_52_match else derived_year_high,
        "open": float(latest[1]),
        "dayHigh": float(latest[2]),
        "dayLow": float(latest[3]),
        "close": float(latest[4]),
        "volume": float(latest[5]),
        "avgVolume": float(mean(float(row[5]) for row in trailing_avg_rows)),
        "previousClose": float(previous[4]) if previous else None,
    }


def _parse_etnet_news_html(html: str, code: str, limit: int) -> dict[str, Any]:
    name_match = re.search(r'<div id="QuoteNameA">\s*([0-9]{5})\s+([^<]+?)\s*</div>', html, re.IGNORECASE)
    if not name_match:
        raise HTTPException(status_code=502, detail="ET Net news page format was not recognized")

    rows: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    pattern = re.compile(
        r'<div class="DivArticleList[^"]*">.*?<span class="date">\s*([^<]+?)\s*</span>.*?<a href="([^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        published = _strip_html(match.group(1))
        href = match.group(2).strip()
        title = _strip_html(match.group(3))
        if not href or not title:
            continue

        absolute_url = href if href.startswith("http") else f"{ETNET_BASE_URL}/www/eng/stocks/realtime/{href.lstrip('./')}"
        if absolute_url in seen_urls:
            continue
        seen_urls.add(absolute_url)

        rows.append(
            {
                "published_date": published,
                "title": title,
                "site": "ET Net",
                "text": None,
                "url": absolute_url,
                "image": None,
                "symbols": [f"{name_match.group(1)}.HK"],
                "raw": {
                    "ticker": str(int(code)),
                    "code": name_match.group(1),
                    "source": "etnet",
                },
            }
        )
        if len(rows) >= limit:
            break

    return {
        "symbol": f"{name_match.group(1)}.HK",
        "chineseName": None,
        "limit": limit,
        "count": len(rows),
        "data": rows,
    }


def _parse_etnet_company_info_html(html: str) -> dict[str, Any]:
    issued_match = re.search(
        r'Issued Share Capital</td><td[^>]*align="right"[^>]*>\s*([0-9,]+)\s*</td>',
        html,
        re.IGNORECASE,
    )
    if not issued_match:
        return {}

    issued_shares = _as_float_or_none(issued_match.group(1))
    if issued_shares is None:
        return {}

    return {
        "issuedShares": issued_shares,
    }

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
        "change": float(item.get("change", 0) or 0),
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


@app.get("/v1/index-quotes")
async def index_quotes(symbols: str = Query(..., description="Comma-separated index symbols")) -> dict[str, Any]:
    requested = [sym.strip().upper() for sym in symbols.split(",") if sym.strip()]
    rows: list[dict[str, Any]] = []
    failed: list[str] = []

    for symbol in requested:
        try:
            data = await _fmp_get("quote", {"symbol": symbol})
        except HTTPException:
            failed.append(symbol)
            continue

        if not isinstance(data, list) or not data:
            failed.append(symbol)
            continue

        item = data[0]
        if not isinstance(item, dict):
            failed.append(symbol)
            continue

        normalized = _normalize_quote_row(item)
        if normalized.get("symbol"):
            rows.append(normalized)
        else:
            failed.append(symbol)

    return {"count": len(rows), "data": rows}


@app.get("/v1/index-history-latest/{symbol}")
async def index_history_latest(symbol: str) -> dict[str, Any]:
    data = await _fmp_get("historical-price-eod/full", {"symbol": symbol.upper()})
    if not isinstance(data, list) or not data:
        raise HTTPException(status_code=404, detail=f"No index history found for {symbol.upper()}")

    first = data[0]
    if not isinstance(first, dict):
        raise HTTPException(status_code=502, detail=f"Unexpected FMP response: {data}")

    return {
        "symbol": symbol.upper(),
        "date": first.get("date"),
        "price": first.get("close") if first.get("close") is not None else first.get("price"),
        "change": first.get("change"),
        "changePercentage": first.get("changePercent"),
    }


@app.get("/v1/hk/etnet-quote/{ticker}")
async def hk_etnet_quote(ticker: str) -> dict[str, Any]:
    normalized = ticker.strip()
    if not normalized.isdigit():
        raise HTTPException(status_code=400, detail="HK ticker must be numeric, for example 941")

    code = str(int(normalized))
    transaction_html, chart_html, company_html = await asyncio.gather(
        _etnet_get("/www/eng/stocks/realtime/quote_transaction.php", {"code": code}),
        _etnet_get("/www/eng/stocks/realtime/quote_chart_interactive.php", {"code": code}),
        _etnet_get("/www/eng/stocks/realtime/quote_ci_brief.php", {"code": code}),
    )

    quote_data = _parse_etnet_quote_html(transaction_html, normalized)
    chart_data = _parse_etnet_chart_html(chart_html)
    company_data = _parse_etnet_company_info_html(company_html)
    price_for_market_cap = quote_data.get("price") if quote_data.get("price") is not None else chart_data.get("price")
    issued_shares = company_data.get("issuedShares")
    market_cap = (price_for_market_cap * issued_shares) if price_for_market_cap is not None and issued_shares is not None else None

    quote_data.update(
        {
            "name": chart_data.get("name") or quote_data.get("name"),
            "chineseName": chart_data.get("chineseName"),
            "price": quote_data.get("price") if quote_data.get("price") is not None else chart_data.get("price"),
            "change": quote_data.get("change") if quote_data.get("change") is not None else chart_data.get("change"),
            "changePercentage": quote_data.get("changePercentage")
            if quote_data.get("changePercentage") is not None
            else chart_data.get("changePercentage"),
            "open": chart_data.get("open"),
            "dayHigh": chart_data.get("dayHigh"),
            "dayLow": chart_data.get("dayLow"),
            "previousClose": chart_data.get("previousClose"),
            "yearHigh": chart_data.get("yearHigh"),
            "yearLow": chart_data.get("yearLow"),
            "close": chart_data.get("close"),
            "volume": chart_data.get("volume"),
            "avgVolume": chart_data.get("avgVolume"),
            "marketCap": market_cap,
            "issuedShares": issued_shares,
            "chartSourceUrl": f"{ETNET_BASE_URL}/www/eng/stocks/realtime/quote_chart_interactive.php?code={code}",
            "companyInfoSourceUrl": f"{ETNET_BASE_URL}/www/eng/stocks/realtime/quote_ci_brief.php?code={code}",
        }
    )
    return quote_data


@app.get("/v1/hk/etnet-news/{ticker}")
async def hk_etnet_news(ticker: str, limit: int = Query(5, ge=1, le=20)) -> dict[str, Any]:
    normalized = ticker.strip()
    if not normalized.isdigit():
        raise HTTPException(status_code=400, detail="HK ticker must be numeric, for example 941")

    html = await _etnet_get("/www/eng/stocks/realtime/quote_news.php", {"code": str(int(normalized))})
    return _parse_etnet_news_html(html, normalized, limit)


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

import argparse
import json
import os
from typing import Any
from urllib.parse import quote

import httpx
import psycopg


DEFAULT_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG", "META", "TSLA", "AVGO", "AMD",
    "NFLX", "JPM", "V", "MA", "COST", "PLTR", "MU", "BRK-B", "CRM", "ORCL",
    "ADBE", "CSCO", "QCOM", "TXN", "IBM", "INTU", "AMAT", "NOW", "SHOP", "UBER",
    "ABNB", "BKNG", "PANW", "CRWD", "SNOW", "LLY", "UNH", "JNJ", "ABBV", "MRK",
    "PFE", "TMO", "DHR", "WMT", "HD", "PG", "KO", "PEP", "MCD", "DIS",
    "NKE", "XOM", "CVX", "COP", "BA", "CAT", "GE", "HON", "RTX", "LMT",
    "GS", "MS", "BAC", "WFC", "C", "AXP", "BLK", "SPGI", "AMGN", "GILD",
    "ISRG", "ADP", "SCHW", "UPS", "PM", "LOW", "TJX", "MDT", "VRTX", "SBUX",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed sec_13f_symbol_map from live market-data-service profiles.")
    parser.add_argument("--dsn", default=os.getenv("POSTGRES_DSN", ""), help="Postgres DSN")
    parser.add_argument(
        "--market-data-url",
        default=os.getenv("MARKET_DATA_SERVICE_URL", "http://market-data-service:8001").rstrip("/"),
        help="Base URL for market-data-service",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated ticker list to seed",
    )
    parser.add_argument(
        "--source",
        default="fmp_profile_seed",
        help="Source label stored in sec_13f_symbol_map",
    )
    return parser.parse_args()


def postgres_conninfo(raw_dsn: str) -> str:
    user = os.getenv("POSTGRES_USER", "")
    password = os.getenv("POSTGRES_PASSWORD", "")
    database = os.getenv("POSTGRES_DB", "")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    if user and password and database:
        return f"postgresql://{user}:{quote(password, safe='')}@{host}:{port}/{database}"
    if raw_dsn.strip():
        return raw_dsn
    raise ValueError("POSTGRES_DSN or POSTGRES_* env vars are required")


def load_profile(base_url: str, symbol: str) -> dict[str, Any] | None:
    url = f"{base_url}/profile/{symbol}"
    with httpx.Client(timeout=20.0) as client:
        response = client.get(url)
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, list) or not payload:
        return None
    row = payload[0]
    if not isinstance(row, dict):
        return None
    return row


def main() -> int:
    args = parse_args()
    dsn = postgres_conninfo(args.dsn)
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    upserted: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for symbol in symbols:
                try:
                    profile = load_profile(args.market_data_url, symbol)
                except Exception as exc:
                    skipped.append({"symbol": symbol, "reason": f"profile lookup failed: {exc}"})
                    continue

                if not profile:
                    skipped.append({"symbol": symbol, "reason": "no profile payload"})
                    continue

                cusip = str(profile.get("cusip") or "").strip().upper()
                issuer_name = str(profile.get("companyName") or "").strip() or None
                if not cusip:
                    skipped.append({"symbol": symbol, "reason": "missing cusip"})
                    continue

                cur.execute(
                    """
                    INSERT INTO sec_13f_symbol_map (symbol, cusip, issuer_name, source, is_active, updated_at)
                    VALUES (%s, %s, %s, %s, TRUE, NOW())
                    ON CONFLICT (symbol)
                    DO UPDATE SET
                        cusip = EXCLUDED.cusip,
                        issuer_name = EXCLUDED.issuer_name,
                        source = EXCLUDED.source,
                        is_active = TRUE,
                        updated_at = NOW()
                    """,
                    (symbol, cusip, issuer_name, args.source),
                )
                upserted.append({"symbol": symbol, "cusip": cusip, "issuer_name": issuer_name or ""})
        conn.commit()

    print(json.dumps({"upserted": upserted, "skipped": skipped, "requested": len(symbols)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

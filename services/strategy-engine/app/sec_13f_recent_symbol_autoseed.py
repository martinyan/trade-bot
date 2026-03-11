import argparse
import asyncio
import json
import os
from urllib.parse import quote

import httpx
import psycopg


PROFILE_BULK_PARTS = (0, 1, 2, 3)
DEFAULT_FMP_BASE_URL = "https://financialmodelingprep.com/stable"

ALLOWED_CLASS_PATTERNS = (
    "COM",
    "COMMON",
    "ORD",
    "SHS",
    "SHARE",
    "ADR",
    "ADS",
    "SPONSORED ADR",
    "ETF",
    "TR ETF",
    "INDEX FD",
)

EXCLUDED_CLASS_PATTERNS = (
    "NOTE",
    "NOTES",
    "BOND",
    "BONDS",
    "DEBT",
    "DBCV",
    "PFD",
    "PREF",
    "PREFERRED",
    "UNIT",
    "UNITS",
    "RIGHT",
    "RIGHTS",
    "WARRANT",
    "WARRANTS",
    "WT",
    "PUT",
    "CALL",
)

ALLOWED_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "ARCA", "CBOE", "BATS"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto-seed sec_13f_symbol_map from recent 13F CUSIPs using FMP profile bulk data."
    )
    parser.add_argument("--dsn", default=os.getenv("POSTGRES_DSN", ""), help="Postgres DSN")
    parser.add_argument("--fmp-api-key", default=os.getenv("FMP_API_KEY", ""), help="FMP API key")
    parser.add_argument(
        "--fmp-base-url",
        default=os.getenv("FMP_BASE_URL", DEFAULT_FMP_BASE_URL).rstrip("/"),
        help="FMP base URL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write symbol-map rows; print summary only",
    )
    parser.add_argument(
        "--market-data-url",
        default=os.getenv("MARKET_DATA_SERVICE_URL", "http://market-data-service:8001").rstrip("/"),
        help="Base URL for market-data-service",
    )
    parser.add_argument(
        "--profile-concurrency",
        type=int,
        default=20,
        help="Concurrent profile fetches for fallback mode",
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


def class_allowed(class_title: str | None) -> bool:
    raw = (class_title or "").upper().strip()
    if not raw:
        return False
    if any(token in raw for token in EXCLUDED_CLASS_PATTERNS):
        return False
    return any(token in raw for token in ALLOWED_CLASS_PATTERNS)


def load_recent_13f_targets(conn: psycopg.Connection) -> dict[str, dict[str, str]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                cusip,
                MAX(issuer_name) AS issuer_name,
                MAX(class_title) AS class_title
            FROM sec_13f_recent_holding
            WHERE cusip IS NOT NULL
              AND btrim(cusip) <> ''
              AND (put_call IS NULL OR btrim(put_call) = '')
            GROUP BY cusip
            """
        )
        rows = list(cur.fetchall())

    targets: dict[str, dict[str, str]] = {}
    for row in rows:
        cusip = str(row["cusip"]).strip().upper()
        class_title = row.get("class_title")
        if not class_allowed(class_title):
            continue
        targets[cusip] = {
            "issuer_name": str(row.get("issuer_name") or "").strip(),
            "class_title": str(class_title or "").strip(),
        }
    return targets


def fetch_profile_bulk(fmp_base_url: str, api_key: str, part: int) -> list[dict]:
    if not api_key:
        raise ValueError("FMP_API_KEY is required")
    url = f"{fmp_base_url}/profile-bulk"
    with httpx.Client(timeout=120.0) as client:
        response = client.get(url, params={"part": part, "apikey": api_key})
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"unexpected profile-bulk payload for part={part}")
    return [row for row in payload if isinstance(row, dict)]


def fetch_universe_symbols(market_data_url: str) -> list[str]:
    rows: list[dict] = []
    url = f"{market_data_url}/v1/universe/quotes"
    with httpx.Client(timeout=120.0) as client:
        for exchange in ("NASDAQ", "NYSE", "AMEX"):
            try:
                response = client.get(url, params={"exchanges": exchange})
                response.raise_for_status()
            except Exception:
                continue
            payload = response.json()
            data = payload.get("data", []) if isinstance(payload, dict) else []
            if isinstance(data, list):
                rows.extend(item for item in data if isinstance(item, dict))

    symbols: list[str] = []
    seen: set[str] = set()
    for row in rows:
        symbol = normalize_symbol(row.get("symbol"))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


async def fetch_profile(session: httpx.AsyncClient, market_data_url: str, symbol: str) -> dict | None:
    response = await session.get(f"{market_data_url}/profile/{symbol}")
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        return None
    row = payload[0]
    return row if isinstance(row, dict) else None


async def build_candidate_rows_from_market_data(
    targets: dict[str, dict[str, str]],
    market_data_url: str,
    profile_concurrency: int,
) -> tuple[list[dict[str, str]], dict[str, list[str]]]:
    symbols = fetch_universe_symbols(market_data_url)
    matched: dict[str, dict[str, str]] = {}
    unresolved = {cusip: [] for cusip in targets}
    semaphore = asyncio.Semaphore(max(1, profile_concurrency))

    async def process_symbol(session: httpx.AsyncClient, symbol: str) -> None:
        async with semaphore:
            try:
                profile = await fetch_profile(session, market_data_url, symbol)
            except Exception as exc:
                return
            if not profile:
                return

            cusip = str(profile.get("cusip") or "").strip().upper()
            if not cusip or cusip not in targets:
                return

            exchange = str(profile.get("exchange") or profile.get("exchangeShortName") or "").strip().upper()
            if exchange and exchange not in ALLOWED_EXCHANGES:
                unresolved[cusip].append(f"excluded exchange {exchange}")
                return

            matched[symbol] = {
                "symbol": symbol,
                "cusip": cusip,
                "issuer_name": str(profile.get("companyName") or targets[cusip]["issuer_name"]).strip(),
                "source": "market_data_profile_recent_13f",
            }
            unresolved.pop(cusip, None)

    async with httpx.AsyncClient(timeout=30.0) as session:
        await asyncio.gather(*(process_symbol(session, symbol) for symbol in symbols))

    return list(matched.values()), unresolved


def normalize_symbol(symbol: str | None) -> str:
    return str(symbol or "").strip().upper()


def build_candidate_rows(
    targets: dict[str, dict[str, str]],
    fmp_base_url: str,
    api_key: str,
) -> tuple[list[dict[str, str]], dict[str, list[str]]]:
    matched: list[dict[str, str]] = []
    unresolved = {cusip: [] for cusip in targets}

    for part in PROFILE_BULK_PARTS:
        rows = fetch_profile_bulk(fmp_base_url, api_key, part)
        for row in rows:
            cusip = str(row.get("cusip") or "").strip().upper()
            if not cusip or cusip not in targets:
                continue

            symbol = normalize_symbol(row.get("symbol"))
            if not symbol:
                unresolved[cusip].append("missing symbol")
                continue

            exchange = str(row.get("exchangeShortName") or row.get("exchange") or "").strip().upper()
            if exchange and exchange not in ALLOWED_EXCHANGES:
                unresolved[cusip].append(f"excluded exchange {exchange}")
                continue

            matched.append(
                {
                    "symbol": symbol,
                    "cusip": cusip,
                    "issuer_name": str(row.get("companyName") or targets[cusip]["issuer_name"]).strip(),
                    "source": "fmp_profile_bulk_recent_13f",
                }
            )
            unresolved.pop(cusip, None)

    # Deduplicate by symbol so aliases remain but exact duplicates are removed.
    deduped: dict[str, dict[str, str]] = {}
    for row in matched:
        deduped[row["symbol"]] = row
    return list(deduped.values()), unresolved


def upsert_symbol_rows(conn: psycopg.Connection, rows: list[dict[str, str]]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
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
            [
                (row["symbol"], row["cusip"], row["issuer_name"], row["source"])
                for row in rows
            ],
        )
    conn.commit()


def main() -> int:
    args = parse_args()
    dsn = postgres_conninfo(args.dsn)

    with psycopg.connect(dsn) as conn:
        targets = load_recent_13f_targets(conn)
        try:
            matched_rows, unresolved = build_candidate_rows(targets, args.fmp_base_url, args.fmp_api_key)
            source_mode = "fmp_profile_bulk"
        except httpx.HTTPStatusError as exc:
            if exc.response is None or exc.response.status_code != 402:
                raise
            matched_rows, unresolved = asyncio.run(
                build_candidate_rows_from_market_data(
                    targets,
                    args.market_data_url,
                    args.profile_concurrency,
                )
            )
            source_mode = "market_data_profile_fallback"
        if not args.dry_run:
            upsert_symbol_rows(conn, matched_rows)

    print(
        json.dumps(
            {
                "source_mode": source_mode,
                "recent_target_cusips": len(targets),
                "matched_symbols": len(matched_rows),
                "unresolved_cusips": len(unresolved),
                "sample_unresolved": dict(list(unresolved.items())[:20]),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

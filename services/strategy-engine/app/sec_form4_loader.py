import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from html import unescape
from typing import Any
from urllib.parse import urlparse

import httpx
import psycopg


DEFAULT_SEC_USER_AGENT = "trade-bot/form4-loader contact@example.com"
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_FEED_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 50
SEC_REQUEST_PAUSE_SECONDS = 0.15
SEC_MAX_RETRIES = 4

FORM4_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sec_form4_filing (
    id BIGSERIAL PRIMARY KEY,
    accession_number TEXT NOT NULL UNIQUE,
    form_type TEXT NOT NULL,
    issuer_cik TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    issuer_symbol TEXT,
    reporter_cik TEXT,
    reporter_name TEXT NOT NULL,
    period_of_report DATE,
    filed_at DATE NOT NULL,
    accepted_at TIMESTAMPTZ,
    source_url TEXT NOT NULL,
    raw_path TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS sec_form4_filing_filed_at_idx
    ON sec_form4_filing (filed_at);

CREATE INDEX IF NOT EXISTS sec_form4_filing_issuer_symbol_filed_at_idx
    ON sec_form4_filing (issuer_symbol, filed_at);

CREATE TABLE IF NOT EXISTS sec_form4_transaction (
    id BIGSERIAL PRIMARY KEY,
    filing_id BIGINT NOT NULL REFERENCES sec_form4_filing(id) ON DELETE CASCADE,
    issuer_symbol TEXT,
    issuer_name TEXT NOT NULL,
    reporter_name TEXT NOT NULL,
    security_title TEXT,
    transaction_date DATE,
    transaction_code TEXT,
    acquired_disposed_code TEXT,
    shares NUMERIC(20, 4),
    price NUMERIC(20, 6),
    value NUMERIC(24, 6),
    shares_owned_following NUMERIC(20, 4),
    direct_or_indirect TEXT,
    ownership_nature TEXT,
    is_derivative BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS sec_form4_transaction_filing_id_idx
    ON sec_form4_transaction (filing_id);

CREATE INDEX IF NOT EXISTS sec_form4_transaction_symbol_date_idx
    ON sec_form4_transaction (issuer_symbol, transaction_date);

CREATE INDEX IF NOT EXISTS sec_form4_transaction_code_idx
    ON sec_form4_transaction (transaction_code, is_derivative);
"""


@dataclass(frozen=True)
class Form4FilingRecord:
    accession_number: str
    form_type: str
    issuer_cik: str
    issuer_name: str
    issuer_symbol: str | None
    reporter_cik: str | None
    reporter_name: str
    period_of_report: date | None
    filed_at: date
    accepted_at: datetime | None
    source_url: str
    raw_path: str


@dataclass(frozen=True)
class Form4TransactionRecord:
    security_title: str | None
    transaction_date: date | None
    transaction_code: str | None
    acquired_disposed_code: str | None
    shares: Decimal | None
    price: Decimal | None
    value: Decimal | None
    shares_owned_following: Decimal | None
    direct_or_indirect: str | None
    ownership_nature: str | None
    is_derivative: bool


@dataclass(frozen=True)
class ParsedForm4Filing:
    filing: Form4FilingRecord
    transactions: list[Form4TransactionRecord]


def _is_common_equity_title(title: str | None) -> bool:
    value = str(title or "").strip().lower()
    if not value:
        return False

    excluded_markers = (
        "option",
        "swap",
        "unit",
        "warrant",
        "rsu",
        "restricted",
        "phantom",
        "preferred",
        "depositary",
        "derivative",
        "note",
        "bond",
        "debenture",
    )
    if any(marker in value for marker in excluded_markers):
        return False

    included_markers = ("common stock", "common shares", "ordinary shares")
    return any(marker in value for marker in included_markers)


def _filter_purchase_transactions(
    transactions: list[Form4TransactionRecord],
) -> list[Form4TransactionRecord]:
    kept: list[Form4TransactionRecord] = []
    for item in transactions:
        if (item.transaction_code or "").strip().upper() != "P":
            continue
        if item.is_derivative:
            continue
        if not _is_common_equity_title(item.security_title):
            continue
        kept.append(item)
    return kept


def ensure_form4_tables(conn: psycopg.Connection[Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(FORM4_SCHEMA_SQL)
    conn.commit()


def _build_headers(user_agent: str, url: str) -> dict[str, str]:
    host = urlparse(url).netloc or "www.sec.gov"
    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": host,
    }


def _quarter_for_day(day_value: date) -> int:
    return ((day_value.month - 1) // 3) + 1


def _master_index_url(day_value: date) -> str:
    return (
        "https://www.sec.gov/Archives/edgar/daily-index/"
        f"{day_value:%Y}/QTR{_quarter_for_day(day_value)}/master.{day_value:%Y%m%d}.idx"
    )


def _iter_recent_days(days_back: int) -> list[date]:
    today = datetime.now(timezone.utc).date()
    return [today - timedelta(days=offset) for offset in range(max(days_back, 1))]


def _current_feed_url(start: int, count: int = 100) -> str:
    return (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcurrent&type=4&owner=only&count={count}&start={start}&output=atom"
    )


def _parse_master_index(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line in text.splitlines():
        if line.count("|") != 4:
            continue
        cik, company_name, form_type, filed_on, file_name = [part.strip() for part in line.split("|", 4)]
        if not cik.isdigit():
            continue
        rows.append(
            {
                "cik": cik,
                "company_name": company_name,
                "form_type": form_type,
                "filed_on": filed_on,
                "file_name": file_name,
            }
        )
    return rows


def _parse_current_feed_entries(text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(text)
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    out: list[dict[str, Any]] = []
    for entry in root.findall("atom:entry", ns):
        link_node = entry.find("atom:link", ns)
        summary_text = entry.findtext("atom:summary", default="", namespaces=ns)
        summary_clean = re.sub(r"<[^>]+>", " ", unescape(summary_text))
        summary_clean = re.sub(r"\s+", " ", summary_clean).strip()
        filed_match = re.search(r"Filed:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", summary_clean)
        acc_match = re.search(r"AccNo:\s*([0-9-]+)", summary_clean)
        if link_node is None or filed_match is None or acc_match is None:
            continue
        href = link_node.attrib.get("href") or ""
        if not href:
            continue
        out.append(
            {
                "filed_at": _parse_iso_date(filed_match.group(1)),
                "accession_number": acc_match.group(1),
                "index_url": href,
            }
        )
    return out


def _request_with_backoff(
    client: httpx.Client,
    url: str,
    *,
    user_agent: str,
) -> httpx.Response:
    last_error: Exception | None = None
    for attempt in range(SEC_MAX_RETRIES):
        if attempt:
            time.sleep(min(2**attempt, 8))
        response = client.get(url, headers=_build_headers(user_agent, url))
        if response.status_code != 429:
            response.raise_for_status()
            return response
        last_error = httpx.HTTPStatusError(
            "SEC rate limit hit",
            request=response.request,
            response=response,
        )
    if last_error is not None:
        raise last_error
    raise RuntimeError("SEC request failed without a response")


def _is_rate_limited(exc: Exception) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response is not None
        and exc.response.status_code == 429
    )


def _is_forbidden(exc: Exception) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response is not None
        and exc.response.status_code == 403
    )


def _parse_iso_date(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    for fmt, candidate in (("%Y-%m-%d", raw[:10]), ("%Y%m%d", raw[:8])):
        try:
            return datetime.strptime(candidate, fmt).date()
        except ValueError:
            continue
    return None


def _parse_acceptance_dt(text: str) -> datetime | None:
    match = re.search(r"<ACCEPTANCE-DATETIME>(\d{14})", text)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_filed_at(text: str, fallback: date | None) -> date | None:
    match = re.search(r"FILED AS OF DATE:\s+(\d{8})", text)
    if not match:
        return fallback
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return fallback


def _extract_ownership_xml(text: str) -> str:
    for match in re.finditer(r"<XML>\s*(.*?)\s*</XML>", text, flags=re.DOTALL | re.IGNORECASE):
        candidate = match.group(1).strip()
        if "<ownershipDocument" in candidate:
            return candidate
    raise ValueError("ownershipDocument XML not found in SEC filing body")


def _xml_text(node: ET.Element | None, path: str) -> str | None:
    if node is None:
        return None
    current = node.find(path)
    if current is None or current.text is None:
        return None
    value = current.text.strip()
    return value or None


def _to_decimal(value: str | None) -> Decimal | None:
    raw = (value or "").strip().replace(",", "")
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _parse_transaction(node: ET.Element, *, is_derivative: bool) -> Form4TransactionRecord:
    shares = _to_decimal(_xml_text(node, "./transactionAmounts/transactionShares/value"))
    price = _to_decimal(_xml_text(node, "./transactionAmounts/transactionPricePerShare/value"))
    value = (shares * price) if shares is not None and price is not None else None
    return Form4TransactionRecord(
        security_title=_xml_text(node, "./securityTitle/value"),
        transaction_date=_parse_iso_date(_xml_text(node, "./transactionDate/value")),
        transaction_code=_xml_text(node, "./transactionCoding/transactionCode"),
        acquired_disposed_code=_xml_text(node, "./transactionAmounts/transactionAcquiredDisposedCode/value"),
        shares=shares,
        price=price,
        value=value,
        shares_owned_following=_to_decimal(
            _xml_text(node, "./postTransactionAmounts/sharesOwnedFollowingTransaction/value")
        ),
        direct_or_indirect=_xml_text(node, "./ownershipNature/directOrIndirectOwnership/value"),
        ownership_nature=_xml_text(node, "./ownershipNature/natureOfOwnership/value"),
        is_derivative=is_derivative,
    )


def parse_form4_filing(text: str, source_url: str, raw_path: str, fallback_filed_at: date | None) -> ParsedForm4Filing:
    xml_text = _extract_ownership_xml(text)
    root = ET.fromstring(xml_text)

    issuer_name = _xml_text(root, "./issuer/issuerName")
    reporter_names = [
        value.text.strip()
        for value in root.findall("./reportingOwner/reportingOwnerId/rptOwnerName")
        if value.text and value.text.strip()
    ]
    filed_at = _parse_filed_at(text, fallback_filed_at)

    if not issuer_name or not reporter_names or filed_at is None:
        raise ValueError("SEC Form 4 filing missing issuer/reporter/filed-at metadata")

    filing = Form4FilingRecord(
        accession_number=_xml_text(root, "./documentType") and (
            re.search(r"ACCESSION NUMBER:\s+([0-9-]+)", text).group(1)  # type: ignore[union-attr]
        ) or "",
        form_type=_xml_text(root, "./documentType") or "4",
        issuer_cik=_xml_text(root, "./issuer/issuerCik") or "",
        issuer_name=issuer_name,
        issuer_symbol=_xml_text(root, "./issuer/issuerTradingSymbol"),
        reporter_cik=_xml_text(root, "./reportingOwner/reportingOwnerId/rptOwnerCik"),
        reporter_name="; ".join(reporter_names),
        period_of_report=_parse_iso_date(_xml_text(root, "./periodOfReport")),
        filed_at=filed_at,
        accepted_at=_parse_acceptance_dt(text),
        source_url=source_url,
        raw_path=raw_path,
    )

    accession_match = re.search(r"ACCESSION NUMBER:\s+([0-9-]+)", text)
    if accession_match:
        filing = Form4FilingRecord(
            accession_number=accession_match.group(1),
            form_type=filing.form_type,
            issuer_cik=filing.issuer_cik,
            issuer_name=filing.issuer_name,
            issuer_symbol=filing.issuer_symbol,
            reporter_cik=filing.reporter_cik,
            reporter_name=filing.reporter_name,
            period_of_report=filing.period_of_report,
            filed_at=filing.filed_at,
            accepted_at=filing.accepted_at,
            source_url=filing.source_url,
            raw_path=filing.raw_path,
        )

    transactions: list[Form4TransactionRecord] = []
    for node in root.findall("./nonDerivativeTable/nonDerivativeTransaction"):
        transactions.append(_parse_transaction(node, is_derivative=False))
    for node in root.findall("./derivativeTable/derivativeTransaction"):
        transactions.append(_parse_transaction(node, is_derivative=True))

    return ParsedForm4Filing(filing=filing, transactions=_filter_purchase_transactions(transactions))


def _upsert_filing(conn: psycopg.Connection[Any], filing: Form4FilingRecord) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_form4_filing (
                accession_number,
                form_type,
                issuer_cik,
                issuer_name,
                issuer_symbol,
                reporter_cik,
                reporter_name,
                period_of_report,
                filed_at,
                accepted_at,
                source_url,
                raw_path,
                loaded_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (accession_number)
            DO UPDATE SET
                form_type = EXCLUDED.form_type,
                issuer_cik = EXCLUDED.issuer_cik,
                issuer_name = EXCLUDED.issuer_name,
                issuer_symbol = EXCLUDED.issuer_symbol,
                reporter_cik = EXCLUDED.reporter_cik,
                reporter_name = EXCLUDED.reporter_name,
                period_of_report = EXCLUDED.period_of_report,
                filed_at = EXCLUDED.filed_at,
                accepted_at = EXCLUDED.accepted_at,
                source_url = EXCLUDED.source_url,
                raw_path = EXCLUDED.raw_path,
                loaded_at = NOW()
            RETURNING id
            """,
            (
                filing.accession_number,
                filing.form_type,
                filing.issuer_cik,
                filing.issuer_name,
                filing.issuer_symbol,
                filing.reporter_cik,
                filing.reporter_name,
                filing.period_of_report,
                filing.filed_at,
                filing.accepted_at,
                filing.source_url,
                filing.raw_path,
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("failed to upsert sec_form4_filing row")
        return int(row[0])


def _replace_transactions(
    conn: psycopg.Connection[Any],
    filing_id: int,
    filing: Form4FilingRecord,
    transactions: list[Form4TransactionRecord],
) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sec_form4_transaction WHERE filing_id = %s", (filing_id,))
        insert_rows = [
            (
                filing_id,
                filing.issuer_symbol,
                filing.issuer_name,
                filing.reporter_name,
                item.security_title,
                item.transaction_date,
                item.transaction_code,
                item.acquired_disposed_code,
                item.shares,
                item.price,
                item.value,
                item.shares_owned_following,
                item.direct_or_indirect,
                item.ownership_nature,
                item.is_derivative,
            )
            for item in transactions
        ]
        if insert_rows:
            cur.executemany(
                """
                INSERT INTO sec_form4_transaction (
                    filing_id,
                    issuer_symbol,
                    issuer_name,
                    reporter_name,
                    security_title,
                    transaction_date,
                    transaction_code,
                    acquired_disposed_code,
                    shares,
                    price,
                    value,
                    shares_owned_following,
                    direct_or_indirect,
                    ownership_nature,
                    is_derivative,
                    loaded_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                insert_rows,
            )
    return len(insert_rows)


def _prune_old_rows(conn: psycopg.Connection[Any], retain_days: int) -> int:
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=max(retain_days - 1, 0))
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sec_form4_filing WHERE filed_at < %s", (cutoff,))
        removed = int(cur.fetchone()[0])
        cur.execute("DELETE FROM sec_form4_filing WHERE filed_at < %s", (cutoff,))
    return removed


def purge_non_purchase_form4_rows(conn: psycopg.Connection[Any]) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM sec_form4_transaction
            WHERE transaction_code IS DISTINCT FROM 'P'
               OR is_derivative = TRUE
               OR security_title IS NULL
               OR (
                    lower(security_title) NOT LIKE '%common stock%'
                AND lower(security_title) NOT LIKE '%common shares%'
                AND lower(security_title) NOT LIKE '%ordinary shares%'
               )
               OR lower(security_title) LIKE '%option%'
               OR lower(security_title) LIKE '%swap%'
               OR lower(security_title) LIKE '%unit%'
               OR lower(security_title) LIKE '%warrant%'
               OR lower(security_title) LIKE '%rsu%'
               OR lower(security_title) LIKE '%restricted%'
               OR lower(security_title) LIKE '%phantom%'
               OR lower(security_title) LIKE '%preferred%'
               OR lower(security_title) LIKE '%depositary%'
               OR lower(security_title) LIKE '%derivative%'
               OR lower(security_title) LIKE '%note%'
               OR lower(security_title) LIKE '%bond%'
               OR lower(security_title) LIKE '%debenture%'
            """
        )
        removed_transactions = int(cur.fetchone()[0])
        cur.execute(
            """
            DELETE FROM sec_form4_transaction
            WHERE transaction_code IS DISTINCT FROM 'P'
               OR is_derivative = TRUE
               OR security_title IS NULL
               OR (
                    lower(security_title) NOT LIKE '%common stock%'
                AND lower(security_title) NOT LIKE '%common shares%'
                AND lower(security_title) NOT LIKE '%ordinary shares%'
               )
               OR lower(security_title) LIKE '%option%'
               OR lower(security_title) LIKE '%swap%'
               OR lower(security_title) LIKE '%unit%'
               OR lower(security_title) LIKE '%warrant%'
               OR lower(security_title) LIKE '%rsu%'
               OR lower(security_title) LIKE '%restricted%'
               OR lower(security_title) LIKE '%phantom%'
               OR lower(security_title) LIKE '%preferred%'
               OR lower(security_title) LIKE '%depositary%'
               OR lower(security_title) LIKE '%derivative%'
               OR lower(security_title) LIKE '%note%'
               OR lower(security_title) LIKE '%bond%'
               OR lower(security_title) LIKE '%debenture%'
            """
        )
        cur.execute(
            """
            SELECT COUNT(*)
            FROM sec_form4_filing f
            WHERE NOT EXISTS (
                SELECT 1
                FROM sec_form4_transaction t
                WHERE t.filing_id = f.id
            )
            """
        )
        removed_filings = int(cur.fetchone()[0])
        cur.execute(
            """
            DELETE FROM sec_form4_filing f
            WHERE NOT EXISTS (
                SELECT 1
                FROM sec_form4_transaction t
                WHERE t.filing_id = f.id
            )
            """
        )
    return {
        "removed_transactions": removed_transactions,
        "removed_filings": removed_filings,
    }


def _load_recent_accessions(
    conn: psycopg.Connection[Any],
    *,
    filed_at_cutoff: date,
) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT accession_number FROM sec_form4_filing WHERE filed_at >= %s",
            (filed_at_cutoff,),
        )
        return {str(row[0]) for row in cur.fetchall() if row and row[0]}


def sync_recent_form4_filings(
    dsn: str,
    sec_user_agent: str,
    *,
    days_back: int = 14,
    retain_days: int = 10,
) -> dict[str, Any]:
    if not dsn.strip():
        raise ValueError("POSTGRES_DSN is required")
    if not sec_user_agent.strip():
        raise ValueError("SEC_USER_AGENT is required")

    fetch_cutoff = datetime.now(timezone.utc).date() - timedelta(days=max(days_back - 1, 0))
    filings_seen = 0
    filings_saved = 0
    transactions_saved = 0
    days_loaded: list[str] = []
    skipped_existing = 0
    failed_filings = 0
    failure_samples: list[dict[str, str]] = []
    rate_limited = False

    with psycopg.connect(dsn) as conn:
        ensure_form4_tables(conn)
        existing_accessions = _load_recent_accessions(conn, filed_at_cutoff=fetch_cutoff)

        with httpx.Client(timeout=DEFAULT_TIMEOUT_SECONDS, follow_redirects=True) as client:
            seen_accessions: set[str] = set()
            for day_value in _iter_recent_days(days_back):
                if day_value < fetch_cutoff:
                    continue
                feed_url = _master_index_url(day_value)
                try:
                    response = _request_with_backoff(client, feed_url, user_agent=sec_user_agent)
                except Exception as exc:
                    if _is_rate_limited(exc):
                        rate_limited = True
                        if len(failure_samples) < 5:
                            failure_samples.append(
                                {
                                    "accession_number": "",
                                    "source_url": feed_url,
                                    "error": "SEC rate limit hit while loading SEC daily index",
                                }
                            )
                        conn.commit()
                        break
                    if _is_forbidden(exc):
                        if len(failure_samples) < 5:
                            failure_samples.append(
                                {
                                    "accession_number": "",
                                    "source_url": feed_url,
                                    "error": "SEC daily index not available for this day",
                                }
                            )
                        conn.commit()
                        continue
                    raise
                day_entries = []
                for row in _parse_master_index(response.text):
                    form_type = str(row.get("form_type") or "").strip().upper()
                    if form_type not in {"4", "4/A"}:
                        continue
                    day_entries.append(row)
                if not day_entries:
                    break

                days_loaded.append(day_value.isoformat())
                day_new_accessions = 0
                for entry in day_entries:
                    filed_at = _parse_iso_date(str(entry.get("filed_on") or ""))
                    raw_path = str(entry.get("file_name") or "").strip().lstrip("/")
                    accession_match = re.search(r"([0-9]{10}-[0-9]{2}-[0-9]{6})\.txt$", raw_path)
                    accession_number = accession_match.group(1) if accession_match else ""
                    if filed_at is None or not accession_number or not raw_path:
                        continue
                    if accession_number in seen_accessions:
                        continue
                    seen_accessions.add(accession_number)
                    if filed_at < fetch_cutoff:
                        continue
                    if accession_number in existing_accessions:
                        skipped_existing += 1
                        continue

                    raw_path = raw_path.replace("-index.htm", ".txt")
                    source_url = f"https://www.sec.gov/Archives/{raw_path}"
                    try:
                        filing_text = _request_with_backoff(client, source_url, user_agent=sec_user_agent)
                        parsed = parse_form4_filing(
                            filing_text.text,
                            source_url=source_url,
                            raw_path=raw_path,
                            fallback_filed_at=filed_at,
                        )
                        if not parsed.transactions:
                            continue
                        filings_seen += 1
                        filing_id = _upsert_filing(conn, parsed.filing)
                        transactions_saved += _replace_transactions(conn, filing_id, parsed.filing, parsed.transactions)
                        filings_saved += 1
                        day_new_accessions += 1
                        existing_accessions.add(accession_number)
                    except Exception as exc:
                        failed_filings += 1
                        if _is_rate_limited(exc):
                            rate_limited = True
                        if len(failure_samples) < 5:
                            failure_samples.append(
                                {
                                    "accession_number": accession_number,
                                    "source_url": source_url,
                                    "error": str(exc),
                                }
                            )
                    finally:
                        time.sleep(SEC_REQUEST_PAUSE_SECONDS)

                conn.commit()
                if rate_limited:
                    break

        pruned = _prune_old_rows(conn, retain_days)
        conn.commit()

    return {
        "days_requested": days_back,
        "retain_days": retain_days,
        "days_loaded": days_loaded,
        "filings_seen": filings_seen,
        "filings_saved": filings_saved,
        "transactions_saved": transactions_saved,
        "skipped_existing": skipped_existing,
        "failed_filings": failed_filings,
        "failure_samples": failure_samples,
        "rate_limited": rate_limited,
        "filings_pruned": pruned,
    }

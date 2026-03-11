import argparse
import csv
import io
import json
import os
import sys
import tempfile
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import psycopg


DEFAULT_TIMEOUT_SECONDS = 120.0
DEFAULT_SEC_USER_AGENT = "trade-bot/13f-loader contact@example.com"


@dataclass(frozen=True)
class DatasetManifest:
    dataset_name: str
    report_period: date
    source_url: str
    archive_path: Path
    archive_members: list[str]
    tabular_members: list[str]


@dataclass(frozen=True)
class FilingRecord:
    accession_number: str
    cik: str
    manager_name: str
    report_period: date
    filed_at: date | None
    submission_type: str
    is_amendment: bool
    other_manager_included: bool


@dataclass(frozen=True)
class HoldingRecord:
    accession_number: str
    report_period: date
    cik: str
    manager_name: str
    cusip: str
    issuer_name: str | None
    class_title: str | None
    value_thousands: int | None
    shares: int | None
    share_type: str | None
    put_call: str | None
    investment_discretion: str | None
    voting_sole: int | None
    voting_shared: int | None
    voting_none: int | None


@dataclass(frozen=True)
class ParsedDataset:
    filings: list[FilingRecord]
    holdings: list[HoldingRecord]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a bounded SEC 13F quarterly dataset and register it in Postgres."
    )
    parser.add_argument("--dsn", default=os.getenv("POSTGRES_DSN", ""), help="Postgres DSN")
    parser.add_argument("--dataset-url", required=True, help="SEC dataset zip URL")
    parser.add_argument(
        "--report-period",
        required=True,
        help="Report period in YYYY-MM-DD form, for example 2025-09-30",
    )
    parser.add_argument(
        "--dataset-name",
        default="",
        help="Override dataset name; defaults to the zip file basename",
    )
    parser.add_argument(
        "--download-dir",
        default="",
        help="Optional directory for the downloaded zip; defaults to a temp directory",
    )
    parser.add_argument(
        "--sec-user-agent",
        default=os.getenv("SEC_USER_AGENT", DEFAULT_SEC_USER_AGENT),
        help="User-Agent for SEC requests",
    )
    parser.add_argument(
        "--manifest-only",
        action="store_true",
        help="Stop after archive inspection and dataset audit row updates",
    )
    parser.add_argument(
        "--allow-placeholder-complete",
        action="store_true",
        help="Mark the dataset as completed after manifest inspection even though row parsing is not implemented yet",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Parse the archive and print counts without writing filing and holding rows",
    )
    return parser.parse_args()


def derive_dataset_name(dataset_url: str, override: str) -> str:
    if override:
        return override
    parsed = urlparse(dataset_url)
    name = Path(parsed.path).name
    if not name:
        raise ValueError("unable to derive dataset name from dataset URL")
    return name


def parse_report_period(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_sec_date(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    return datetime.strptime(raw, "%d-%b-%Y").date()


def normalize_text(value: str | None) -> str | None:
    raw = (value or "").strip()
    return raw or None


def normalize_flag(value: str | None) -> bool:
    raw = (value or "").strip().upper()
    return raw == "Y"


def parse_int(value: str | None) -> int | None:
    raw = (value or "").strip().replace(",", "")
    if not raw:
        return None
    return int(raw)


def _build_headers(user_agent: str, url: str) -> dict[str, str]:
    host = urlparse(url).netloc or "www.sec.gov"
    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": host,
    }


def download_dataset(url: str, user_agent: str, download_dir: str) -> Path:
    if not user_agent.strip():
        raise ValueError("SEC_USER_AGENT is required for SEC downloads")

    target_dir = Path(download_dir) if download_dir else Path(tempfile.mkdtemp(prefix="sec13f-"))
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(urlparse(url).path).name or "dataset.zip"
    target_path = target_dir / filename

    with httpx.stream(
        "GET",
        url,
        headers=_build_headers(user_agent, url),
        timeout=DEFAULT_TIMEOUT_SECONDS,
        follow_redirects=True,
    ) as response:
        response.raise_for_status()
        with target_path.open("wb") as handle:
            for chunk in response.iter_bytes():
                handle.write(chunk)

    return target_path


def inspect_archive(dataset_name: str, report_period: date, source_url: str, archive_path: Path) -> DatasetManifest:
    with zipfile.ZipFile(archive_path) as archive:
        members = sorted(archive.namelist())

    tabular_members = [
        name for name in members if name.lower().endswith((".tsv", ".txt", ".csv"))
    ]
    return DatasetManifest(
        dataset_name=dataset_name,
        report_period=report_period,
        source_url=source_url,
        archive_path=archive_path,
        archive_members=members,
        tabular_members=tabular_members,
    )


def _read_tsv_rows(archive: zipfile.ZipFile, member_name: str) -> list[dict[str, str]]:
    with archive.open(member_name) as handle:
        wrapper = io.TextIOWrapper(handle, encoding="utf-8", errors="replace", newline="")
        reader = csv.DictReader(wrapper, delimiter="\t")
        return [dict(row) for row in reader]


def resolve_member_name(available_members: set[str], required_name: str) -> str:
    if required_name in available_members:
        return required_name

    suffix = f"/{required_name}"
    matches = sorted(name for name in available_members if name.endswith(suffix))
    if not matches:
        raise ValueError(f"dataset archive missing required member: {required_name}")
    if len(matches) > 1:
        raise ValueError(f"dataset archive has multiple matches for member: {required_name}")
    return matches[0]


def parse_archive(manifest: DatasetManifest) -> ParsedDataset:
    required_members = {"SUBMISSION.tsv", "COVERPAGE.tsv", "INFOTABLE.tsv"}
    available_members = set(manifest.archive_members)
    resolved_required = {
        member_name: resolve_member_name(available_members, member_name)
        for member_name in required_members
    }
    summary_member = None
    try:
        summary_member = resolve_member_name(available_members, "SUMMARYPAGE.tsv")
    except ValueError:
        summary_member = None

    with zipfile.ZipFile(manifest.archive_path) as archive:
        submission_rows = _read_tsv_rows(archive, resolved_required["SUBMISSION.tsv"])
        cover_rows = _read_tsv_rows(archive, resolved_required["COVERPAGE.tsv"])
        info_rows = _read_tsv_rows(archive, resolved_required["INFOTABLE.tsv"])
        summary_rows = _read_tsv_rows(archive, summary_member) if summary_member else []

    cover_by_accession = {
        row["ACCESSION_NUMBER"]: row
        for row in cover_rows
        if normalize_text(row.get("ACCESSION_NUMBER"))
    }
    summary_by_accession = {
        row["ACCESSION_NUMBER"]: row
        for row in summary_rows
        if normalize_text(row.get("ACCESSION_NUMBER"))
    }

    submissions_by_accession: dict[str, dict[str, str]] = {}
    for row in submission_rows:
        accession = normalize_text(row.get("ACCESSION_NUMBER"))
        if not accession:
            continue
        submissions_by_accession[accession] = row

    holdings_by_accession: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in info_rows:
        accession = normalize_text(row.get("ACCESSION_NUMBER"))
        if accession:
            holdings_by_accession[accession].append(row)

    filings: list[FilingRecord] = []
    holdings: list[HoldingRecord] = []

    for accession, submission in submissions_by_accession.items():
        cover = cover_by_accession.get(accession, {})
        summary = summary_by_accession.get(accession, {})
        submission_type = normalize_text(submission.get("SUBMISSIONTYPE")) or ""
        report_period = parse_sec_date(submission.get("PERIODOFREPORT")) or manifest.report_period
        cik = normalize_text(submission.get("CIK")) or ""
        manager_name = normalize_text(cover.get("FILINGMANAGER_NAME")) or cik
        other_manager_count = parse_int(summary.get("OTHERINCLUDEDMANAGERSCOUNT")) or 0

        filings.append(
            FilingRecord(
                accession_number=accession,
                cik=cik,
                manager_name=manager_name,
                report_period=report_period,
                filed_at=parse_sec_date(submission.get("FILING_DATE")),
                submission_type=submission_type,
                is_amendment=normalize_flag(cover.get("ISAMENDMENT")),
                other_manager_included=other_manager_count > 0,
            )
        )

        if submission_type not in {"13F-HR", "13F-HR/A"}:
            continue

        for row in holdings_by_accession.get(accession, []):
            cusip = normalize_text(row.get("CUSIP"))
            if not cusip:
                continue
            holdings.append(
                HoldingRecord(
                    accession_number=accession,
                    report_period=report_period,
                    cik=cik,
                    manager_name=manager_name,
                    cusip=cusip,
                    issuer_name=normalize_text(row.get("NAMEOFISSUER")),
                    class_title=normalize_text(row.get("TITLEOFCLASS")),
                    value_thousands=parse_int(row.get("VALUE")),
                    shares=parse_int(row.get("SSHPRNAMT")),
                    share_type=normalize_text(row.get("SSHPRNAMTTYPE")),
                    put_call=normalize_text(row.get("PUTCALL")),
                    investment_discretion=normalize_text(row.get("INVESTMENTDISCRETION")),
                    voting_sole=parse_int(row.get("VOTING_AUTH_SOLE")),
                    voting_shared=parse_int(row.get("VOTING_AUTH_SHARED")),
                    voting_none=parse_int(row.get("VOTING_AUTH_NONE")),
                )
            )

    return ParsedDataset(filings=filings, holdings=holdings)


def upsert_dataset_row(conn: psycopg.Connection, manifest: DatasetManifest, status: str, error_text: str | None = None) -> int:
    row_counts = {
        "archive_members": len(manifest.archive_members),
        "tabular_members": len(manifest.tabular_members),
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_13f_dataset (
                dataset_name,
                report_period,
                source_url,
                load_status,
                loaded_at,
                row_counts,
                error_text
            )
            VALUES (%s, %s, %s, %s, CASE WHEN %s = 'completed' THEN NOW() ELSE NULL END, %s::jsonb, %s)
            ON CONFLICT (dataset_name)
            DO UPDATE SET
                report_period = EXCLUDED.report_period,
                source_url = EXCLUDED.source_url,
                load_status = EXCLUDED.load_status,
                loaded_at = CASE
                    WHEN EXCLUDED.load_status = 'completed' THEN NOW()
                    ELSE sec_13f_dataset.loaded_at
                END,
                row_counts = EXCLUDED.row_counts,
                error_text = EXCLUDED.error_text
            RETURNING id
            """,
            (
                manifest.dataset_name,
                manifest.report_period,
                manifest.source_url,
                status,
                status,
                json.dumps(row_counts),
                error_text,
            ),
        )
        dataset_id = cur.fetchone()[0]
    return dataset_id


def set_dataset_status(
    conn: psycopg.Connection,
    manifest: DatasetManifest,
    status: str,
    row_counts: dict[str, int],
    error_text: str | None = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_13f_dataset (
                dataset_name,
                report_period,
                source_url,
                load_status,
                loaded_at,
                row_counts,
                error_text
            )
            VALUES (%s, %s, %s, %s, CASE WHEN %s = 'completed' THEN NOW() ELSE NULL END, %s::jsonb, %s)
            ON CONFLICT (dataset_name)
            DO UPDATE SET
                report_period = EXCLUDED.report_period,
                source_url = EXCLUDED.source_url,
                load_status = EXCLUDED.load_status,
                loaded_at = CASE
                    WHEN EXCLUDED.load_status = 'completed' THEN NOW()
                    ELSE sec_13f_dataset.loaded_at
                END,
                row_counts = EXCLUDED.row_counts,
                error_text = EXCLUDED.error_text
            RETURNING id
            """,
            (
                manifest.dataset_name,
                manifest.report_period,
                manifest.source_url,
                status,
                status,
                json.dumps(row_counts),
                error_text,
            ),
        )
        dataset_id = cur.fetchone()[0]
    return dataset_id


def register_manifest(conn: psycopg.Connection, manifest: DatasetManifest) -> int:
    dataset_id = upsert_dataset_row(conn, manifest, status="manifest_inspected")
    conn.commit()
    return dataset_id


def mark_dataset_failed(conn: psycopg.Connection, manifest: DatasetManifest, exc: Exception) -> None:
    conn.rollback()
    row_counts = {
        "archive_members": len(manifest.archive_members),
        "tabular_members": len(manifest.tabular_members),
    }
    set_dataset_status(conn, manifest, status="failed", row_counts=row_counts, error_text=str(exc))
    conn.commit()


def mark_dataset_completed(conn: psycopg.Connection, manifest: DatasetManifest) -> None:
    row_counts = {
        "archive_members": len(manifest.archive_members),
        "tabular_members": len(manifest.tabular_members),
    }
    set_dataset_status(conn, manifest, status="completed", row_counts=row_counts)
    conn.commit()


def refresh_recent_holdings(cur: psycopg.Cursor) -> int:
    cur.execute("TRUNCATE TABLE sec_13f_recent_holding")
    cur.execute(
        """
        WITH latest_periods AS (
            SELECT report_period
            FROM sec_13f_dataset
            WHERE load_status = 'completed'
            GROUP BY report_period
            ORDER BY report_period DESC
            LIMIT 2
        )
        INSERT INTO sec_13f_recent_holding (
            filing_id,
            report_period,
            cik,
            manager_name,
            cusip,
            issuer_name,
            class_title,
            value_thousands,
            shares,
            share_type,
            put_call,
            investment_discretion,
            voting_sole,
            voting_shared,
            voting_none,
            refreshed_at
        )
        SELECT
            h.filing_id,
            h.report_period,
            h.cik,
            h.manager_name,
            h.cusip,
            h.issuer_name,
            h.class_title,
            h.value_thousands,
            h.shares,
            h.share_type,
            h.put_call,
            h.investment_discretion,
            h.voting_sole,
            h.voting_shared,
            h.voting_none,
            NOW()
        FROM sec_13f_holding h
        JOIN latest_periods lp
            ON lp.report_period = h.report_period
        """
    )
    return cur.rowcount


def replace_dataset_rows(conn: psycopg.Connection, dataset_id: int, parsed: ParsedDataset, manifest: DatasetManifest) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sec_13f_filing WHERE dataset_id = %s", (dataset_id,))
        cur.executemany(
            """
            INSERT INTO sec_13f_filing (
                dataset_id,
                accession_number,
                cik,
                manager_name,
                report_period,
                filed_at,
                submission_type,
                is_amendment,
                other_manager_included
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (accession_number)
            DO UPDATE SET
                dataset_id = EXCLUDED.dataset_id,
                cik = EXCLUDED.cik,
                manager_name = EXCLUDED.manager_name,
                report_period = EXCLUDED.report_period,
                filed_at = EXCLUDED.filed_at,
                submission_type = EXCLUDED.submission_type,
                is_amendment = EXCLUDED.is_amendment,
                other_manager_included = EXCLUDED.other_manager_included
            """,
            [
                (
                    dataset_id,
                    filing.accession_number,
                    filing.cik,
                    filing.manager_name,
                    filing.report_period,
                    filing.filed_at,
                    filing.submission_type,
                    filing.is_amendment,
                    filing.other_manager_included,
                )
                for filing in parsed.filings
            ],
        )

        cur.execute(
            "SELECT accession_number, id FROM sec_13f_filing WHERE dataset_id = %s",
            (dataset_id,),
        )
        filing_ids = dict(cur.fetchall())
        cur.execute(
            """
            DELETE FROM sec_13f_holding
            WHERE filing_id IN (
                SELECT id FROM sec_13f_filing WHERE dataset_id = %s
            )
            """,
            (dataset_id,),
        )
        cur.executemany(
            """
            INSERT INTO sec_13f_holding (
                filing_id,
                report_period,
                cik,
                manager_name,
                cusip,
                issuer_name,
                class_title,
                value_thousands,
                shares,
                share_type,
                put_call,
                investment_discretion,
                voting_sole,
                voting_shared,
                voting_none
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    filing_ids[holding.accession_number],
                    holding.report_period,
                    holding.cik,
                    holding.manager_name,
                    holding.cusip,
                    holding.issuer_name,
                    holding.class_title,
                    holding.value_thousands,
                    holding.shares,
                    holding.share_type,
                    holding.put_call,
                    holding.investment_discretion,
                    holding.voting_sole,
                    holding.voting_shared,
                    holding.voting_none,
                )
                for holding in parsed.holdings
                if holding.accession_number in filing_ids
            ],
        )
        row_counts = {
            "archive_members": len(manifest.archive_members),
            "tabular_members": len(manifest.tabular_members),
            "filings": len(parsed.filings),
            "holdings": len(parsed.holdings),
        }
        set_dataset_status(conn, manifest, status="completed", row_counts=row_counts)
        row_counts["recent_holdings"] = refresh_recent_holdings(cur)
        set_dataset_status(conn, manifest, status="completed", row_counts=row_counts)
    conn.commit()


def prune_old_report_periods(conn: psycopg.Connection, retain_report_periods: int) -> list[str]:
    if retain_report_periods <= 0:
        raise ValueError("retain_report_periods must be greater than 0")

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ranked_periods AS (
                SELECT
                    report_period,
                    ROW_NUMBER() OVER (ORDER BY report_period DESC) AS rn
                FROM (
                    SELECT DISTINCT report_period
                    FROM sec_13f_dataset
                    WHERE load_status = 'completed'
                ) periods
            )
            SELECT report_period::text
            FROM ranked_periods
            WHERE rn > %s
            ORDER BY report_period
            """,
            (retain_report_periods,),
        )
        stale_periods = [row[0] for row in cur.fetchall()]

        if stale_periods:
            cur.execute(
                """
                DELETE FROM sec_13f_dataset
                WHERE report_period = ANY(%s::date[])
                """,
                (stale_periods,),
            )
            refresh_recent_holdings(cur)

    conn.commit()
    return stale_periods


def load_dataset(
    *,
    dsn: str,
    dataset_url: str,
    report_period: date,
    dataset_name: str = "",
    download_dir: str = "",
    sec_user_agent: str = DEFAULT_SEC_USER_AGENT,
    manifest_only: bool = False,
    parse_only: bool = False,
) -> dict[str, object]:
    if not dsn.strip() and not parse_only:
        raise ValueError("POSTGRES_DSN or --dsn is required")

    resolved_dataset_name = derive_dataset_name(dataset_url, dataset_name)
    archive_path = download_dataset(dataset_url, sec_user_agent, download_dir)
    manifest = inspect_archive(resolved_dataset_name, report_period, dataset_url, archive_path)
    parsed = parse_archive(manifest)

    summary: dict[str, object] = {
        "dataset_name": manifest.dataset_name,
        "report_period": manifest.report_period.isoformat(),
        "source_url": manifest.source_url,
        "archive_path": str(manifest.archive_path),
        "archive_members": len(manifest.archive_members),
        "tabular_members": len(manifest.tabular_members),
        "filings": len(parsed.filings),
        "holdings": len(parsed.holdings),
    }

    if parse_only:
        return summary

    with psycopg.connect(dsn) as conn:
        dataset_id = register_manifest(conn, manifest)
        summary["dataset_id"] = dataset_id

        if manifest_only:
            return summary

        try:
            replace_dataset_rows(conn, dataset_id, parsed, manifest)
        except Exception as exc:
            mark_dataset_failed(conn, manifest, exc)
            raise

    return summary


def emit_manifest(manifest: DatasetManifest, dataset_id: int) -> None:
    summary = {
        "dataset_id": dataset_id,
        "dataset_name": manifest.dataset_name,
        "report_period": manifest.report_period.isoformat(),
        "source_url": manifest.source_url,
        "archive_path": str(manifest.archive_path),
        "archive_members": manifest.archive_members,
        "tabular_members": manifest.tabular_members,
    }
    print(json.dumps(summary, indent=2))


def validate_environment(args: argparse.Namespace) -> None:
    if args.parse_only:
        return
    if not args.dsn.strip():
        raise ValueError("POSTGRES_DSN or --dsn is required")


def main() -> int:
    args = parse_args()
    validate_environment(args)

    try:
        summary = load_dataset(
            dsn=args.dsn,
            dataset_url=args.dataset_url,
            report_period=parse_report_period(args.report_period),
            dataset_name=args.dataset_name,
            download_dir=args.download_dir,
            sec_user_agent=args.sec_user_agent,
            manifest_only=args.manifest_only,
            parse_only=args.parse_only,
        )
        print(json.dumps(summary, indent=2))
    except Exception as exc:
        if args.allow_placeholder_complete and not args.parse_only:
            report_period = parse_report_period(args.report_period)
            dataset_name = derive_dataset_name(args.dataset_url, args.dataset_name)
            archive_path = download_dataset(args.dataset_url, args.sec_user_agent, args.download_dir)
            manifest = inspect_archive(dataset_name, report_period, args.dataset_url, archive_path)
            with psycopg.connect(args.dsn) as conn:
                mark_dataset_completed(conn, manifest)
            return 0
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"sec_13f_loader error: {exc}", file=sys.stderr)
        raise

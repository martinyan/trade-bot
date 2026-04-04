import argparse
import json
import os
from datetime import datetime

import psycopg

from sec_13f_loader import (
    DEFAULT_SEC_USER_AGENT,
    load_dataset,
    prune_old_report_periods,
)


DEFAULT_BATCH = [
    {
        "dataset_url": "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01mar2025-31may2025_form13f.zip",
        "report_period": "2025-03-31",
    },
    {
        "dataset_url": "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01jun2025-31aug2025_form13f.zip",
        "report_period": "2025-06-30",
    },
    {
        "dataset_url": "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01sep2025-30nov2025_form13f.zip",
        "report_period": "2025-09-30",
    },
    {
        "dataset_url": "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01dec2025-28feb2026_form13f.zip",
        "report_period": "2025-12-31",
    },
    {
        "dataset_url": "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01mar2026-31may2026_form13f.zip",
        "report_period": "2026-03-31",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load a batch of SEC 13F quarterly datasets and prune old quarters."
    )
    parser.add_argument("--dsn", default=os.getenv("POSTGRES_DSN", ""), help="Postgres DSN")
    parser.add_argument(
        "--batch-file",
        default="",
        help="Optional JSON file containing an array of {dataset_url, report_period, dataset_name?}",
    )
    parser.add_argument(
        "--download-dir",
        default="",
        help="Optional directory for downloaded SEC ZIP files",
    )
    parser.add_argument(
        "--sec-user-agent",
        default=os.getenv("SEC_USER_AGENT", DEFAULT_SEC_USER_AGENT),
        help="User-Agent for SEC requests",
    )
    parser.add_argument(
        "--retain-report-periods",
        type=int,
        default=5,
        help="How many completed report periods to keep after the batch load",
    )
    parser.add_argument(
        "--manifest-only",
        action="store_true",
        help="Inspect/register manifests only without writing filing and holding rows",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Parse each archive and print summaries without DB writes",
    )
    return parser.parse_args()


def load_batch_config(batch_file: str) -> list[dict[str, str]]:
    if not batch_file:
        return DEFAULT_BATCH
    with open(batch_file, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise ValueError("batch file must contain a list of dataset objects")
    batch: list[dict[str, str]] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("batch items must be objects")
        dataset_url = str(item.get("dataset_url") or "").strip()
        report_period = str(item.get("report_period") or "").strip()
        dataset_name = str(item.get("dataset_name") or "").strip()
        if not dataset_url or not report_period:
            raise ValueError("each batch item requires dataset_url and report_period")
        record = {"dataset_url": dataset_url, "report_period": report_period}
        if dataset_name:
            record["dataset_name"] = dataset_name
        batch.append(record)
    return batch


def run_batch(
    *,
    dsn: str,
    batch: list[dict[str, str]],
    download_dir: str = "",
    sec_user_agent: str = DEFAULT_SEC_USER_AGENT,
    retain_report_periods: int = 5,
    manifest_only: bool = False,
    parse_only: bool = False,
) -> dict[str, object]:
    if not parse_only and not dsn.strip():
        raise ValueError("POSTGRES_DSN or --dsn is required")

    results: list[dict[str, object]] = []
    for item in batch:
        summary = load_dataset(
            dsn=dsn,
            dataset_url=item["dataset_url"],
            report_period=datetime.strptime(item["report_period"], "%Y-%m-%d").date(),
            dataset_name=item.get("dataset_name", ""),
            download_dir=download_dir,
            sec_user_agent=sec_user_agent,
            manifest_only=manifest_only,
            parse_only=parse_only,
        )
        results.append(summary)

    pruned_periods: list[str] = []
    if not parse_only and not manifest_only:
        with psycopg.connect(dsn) as conn:
            pruned_periods = prune_old_report_periods(conn, retain_report_periods)

    return {
        "loaded_datasets": results,
        "retain_report_periods": retain_report_periods,
        "pruned_report_periods": pruned_periods,
    }


def main() -> int:
    args = parse_args()
    batch = load_batch_config(args.batch_file)
    result = run_batch(
        dsn=args.dsn,
        batch=batch,
        download_dir=args.download_dir,
        sec_user_agent=args.sec_user_agent,
        retain_report_periods=args.retain_report_periods,
        manifest_only=args.manifest_only,
        parse_only=args.parse_only,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

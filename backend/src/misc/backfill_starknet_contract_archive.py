"""
Backfill Starknet contract active-address archive partitions from Postgres to GCS.

Writes files using the same layout consumed by the external BigQuery table:

    gs://gtp-archive/db_other/fact_active_addresses_contract/
      origin_key=starknet/date=YYYY-MM-DD/part_1.parquet

Example:
    python src/misc/backfill_starknet_contract_archive.py --dry-run
    python src/misc/backfill_starknet_contract_archive.py --replace-existing
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account


BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.append(str(BACKEND_DIR))

from src.db_connector import DbConnector  # noqa: E402


DEFAULT_START_DATE = date(2023, 8, 21)
DEFAULT_END_DATE = date(2023, 10, 30)
DEFAULT_BUCKET = "gtp-archive"
DEFAULT_TABLE = "fact_active_addresses_contract"
DEFAULT_ORIGIN_KEY = "starknet"


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}', expected YYYY-MM-DD") from exc


def iter_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def get_credentials_info() -> dict:
    load_dotenv()
    credentials_raw = os.getenv("GOOGLE_CREDENTIALS")
    if not credentials_raw:
        raise ValueError("Missing GOOGLE_CREDENTIALS")
    return json.loads(credentials_raw)


def partition_prefix(table_name: str, origin_key: str, archive_date: date) -> str:
    return (
        f"db_other/{table_name}/"
        f"origin_key={origin_key}/date={archive_date.isoformat()}"
    )


def partition_dir(bucket: str, table_name: str, origin_key: str, archive_date: date) -> str:
    return f"gs://{bucket}/{partition_prefix(table_name, origin_key, archive_date)}"


def get_day_count(db_connector: DbConnector, table_name: str, origin_key: str, archive_date: date) -> int:
    query = f"""
        SELECT COUNT(*) AS row_count
        FROM {table_name}
        WHERE origin_key = '{origin_key}'
          AND date = '{archive_date.isoformat()}'
    """
    df = pl.read_database_uri(query=query, uri=db_connector.uri)
    if df.is_empty():
        return 0
    return int(df["row_count"][0] or 0)


def load_day(db_connector: DbConnector, table_name: str, origin_key: str, archive_date: date) -> pl.DataFrame:
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE origin_key = '{origin_key}'
          AND date = '{archive_date.isoformat()}'
    """
    return pl.read_database_uri(query=query, uri=db_connector.uri)


def get_storage_client(credentials_info: dict) -> storage.Client:
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return storage.Client(credentials=credentials, project=credentials.project_id)


def partition_has_files(client: storage.Client, bucket_name: str, prefix: str) -> bool:
    blobs = client.list_blobs(bucket_name, prefix=f"{prefix}/", max_results=1)
    return next(blobs, None) is not None


def remove_partition(client: storage.Client, bucket_name: str, prefix: str) -> None:
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket_name, prefix=f"{prefix}/"))
    for blob in blobs:
        bucket.delete_blob(blob.name)


def write_partition(
    df: pl.DataFrame,
    client: storage.Client,
    bucket_name: str,
    prefix: str,
) -> str:
    object_name = f"{prefix}/part_1.parquet"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    with tempfile.NamedTemporaryFile(suffix=".parquet") as file:
        df.write_parquet(file.name, compression="snappy")
        blob.upload_from_filename(file.name)

    return f"gs://{bucket_name}/{object_name}"


def backfill(
    start_date: date,
    end_date: date,
    bucket: str,
    table_name: str,
    origin_key: str,
    dry_run: bool,
    replace_existing: bool,
) -> None:
    if end_date < start_date:
        raise ValueError("end-date must be >= start-date")

    credentials_info = get_credentials_info()
    storage_client = get_storage_client(credentials_info)
    db_connector = DbConnector()

    total_rows = 0
    written_dates = 0
    skipped_empty = 0
    skipped_existing = 0

    print(
        f"Backfilling {table_name} ({origin_key}) "
        f"from {start_date.isoformat()} to {end_date.isoformat()} into gs://{bucket}"
    )

    for archive_date in iter_dates(start_date, end_date):
        target_dir = partition_dir(bucket, table_name, origin_key, archive_date)
        target_prefix = partition_prefix(table_name, origin_key, archive_date)
        exists = partition_has_files(storage_client, bucket, target_prefix)

        if dry_run:
            row_count = get_day_count(db_connector, table_name, origin_key, archive_date)
            status = "exists" if exists else "missing"
            print(f"{archive_date}: {row_count} rows, GCS partition {status}")
            total_rows += row_count
            if row_count == 0:
                skipped_empty += 1
            continue

        if exists and not replace_existing:
            print(f"{archive_date}: partition exists, skipping. Use --replace-existing to overwrite it.")
            skipped_existing += 1
            continue

        df = load_day(db_connector, table_name, origin_key, archive_date)
        if df.is_empty():
            print(f"{archive_date}: no rows in Postgres, skipping")
            skipped_empty += 1
            continue

        if exists and replace_existing:
            remove_partition(storage_client, bucket, target_prefix)

        output_path = write_partition(df, storage_client, bucket, target_prefix)
        row_count = df.height
        total_rows += row_count
        written_dates += 1
        print(f"{archive_date}: wrote {row_count} rows to {output_path}")

    action = "would process" if dry_run else "wrote"
    print(
        f"Done: {action} {total_rows} rows. "
        f"written_dates={written_dates}, skipped_empty={skipped_empty}, "
        f"skipped_existing={skipped_existing}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill Starknet fact_active_addresses_contract archive partitions to GCS."
    )
    parser.add_argument("--start-date", type=parse_date, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=parse_date, default=DEFAULT_END_DATE)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--origin-key", default=DEFAULT_ORIGIN_KEY)
    parser.add_argument("--dry-run", action="store_true", help="Only print Postgres counts and GCS status.")
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Delete an existing GCS date partition before writing the replacement parquet.",
    )
    args = parser.parse_args()

    backfill(
        start_date=args.start_date,
        end_date=args.end_date,
        bucket=args.bucket,
        table_name=args.table,
        origin_key=args.origin_key,
        dry_run=args.dry_run,
        replace_existing=args.replace_existing,
    )


if __name__ == "__main__":
    main()

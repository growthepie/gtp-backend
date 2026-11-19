import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import gcsfs
import polars as pl
from airflow.sdk import dag, task
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import inspect

from src.db_connector import DbConnector
from src.main_config import get_main_config
from src.misc.airflow_utils import alert_via_webhook
from src.misc.helper_functions import send_discord_message

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def save_to_gcs(df: pl.DataFrame, path: str, fs: Optional[gcsfs.GCSFileSystem]) -> None:
    """Write a Polars DataFrame to GCS."""
    assert fs is not None, "GCS filesystem not initialised"
    with fs.open(path, "wb") as f:
        df.write_parquet(f, compression="snappy")
    logger.info("Saved to GCS: %s", path)


def gcs_prefix_has_files(fs: gcsfs.GCSFileSystem, prefix: str) -> bool:
    """Return True if any object exists under the given GCS prefix (bucket/path, no gs://)."""
    try:
        return len(fs.ls(prefix)) > 0
    except FileNotFoundError:
        return False


def handle_missing_bq_table(fs: gcsfs.GCSFileSystem, table_name: str, gcs_prefix: str) -> None:
    """Handle a 404 on a gtp_archive external table.

    - No files under the GCS prefix yet: expected for a newly onboarded chain/table
      (the BQ external table can only be created once data exists). Log, no alert.
    - Files already exist: the external table was never created. Starting from scratch
      would re-archive everything, so fail loudly instead.
    """
    if gcs_prefix_has_files(fs, gcs_prefix):
        raise RuntimeError(
            f"BQ external table growthepie.gtp_archive.{table_name} is missing but archived files "
            f"exist under gs://{gcs_prefix}/. Create the external table (copy the definition of an "
            f"existing gtp_archive table) before re-running, otherwise data gets re-archived."
        )
    logger.info(
        "BQ table gtp_archive.%s does not exist yet and gs://%s/ is empty - first archive run, "
        "starting from scratch. Create the BQ external table once files have been written.",
        table_name,
        gcs_prefix,
    )


def get_bq_max_block(
    client: bigquery.Client,
    table_name: str,
    fs: gcsfs.GCSFileSystem,
    bucket_name: str,
) -> Optional[int]:
    """Fetch the current max block in BQ for the table.

    Returns None only if nothing has been archived yet (table missing and GCS empty, or table empty).
    Any other BQ error is raised: returning None would restart from block 0 and duplicate files.
    """
    query = f"""
    SELECT
        MAX(block_number) AS max_block
    FROM `growthepie.gtp_archive.{table_name}`
    WHERE date > '2026-01-01'
    """
    try:
        df_bq = client.query(query).result().to_dataframe()
    except NotFound:
        handle_missing_bq_table(fs, table_name, f"{bucket_name}/db_tx/{table_name}")
        return None
    if df_bq.empty or df_bq["max_block"].isnull().all():
        return None
    return int(df_bq["max_block"][0])


def get_bq_max_date(
    client: bigquery.Client,
    table_name: str,
    origin_key: str,
    archival_date: date,
    fs: gcsfs.GCSFileSystem,
    bucket_name: str,
) -> Optional[date]:
    """Fetch the current max date in BQ for the table and origin_key.

    Returns None only if nothing has been archived yet. Other BQ errors are raised.
    """
    query = f"""
    SELECT
        MAX(date) AS max_date
    FROM `growthepie.gtp_archive.{table_name}`
    WHERE origin_key = '{origin_key}'
      AND date <= '{archival_date}'
    """
    try:
        df_bq = client.query(query).result().to_dataframe()
    except NotFound:
        handle_missing_bq_table(fs, table_name, f"{bucket_name}/db_other/{table_name}")
        return None
    if df_bq.empty or df_bq["max_date"].isnull().all():
        return None
    max_date = df_bq["max_date"][0]
    if isinstance(max_date, datetime):
        return max_date.date()
    if isinstance(max_date, date):
        return max_date
    if isinstance(max_date, str):
        return datetime.strptime(max_date, "%Y-%m-%d").date()
    return None


def get_eligible_chains() -> List[Dict[str, str]]:
    """Return chains flagged for archiving and with an existing *_tx table."""
    main_conf = get_main_config()
    db_connector = DbConnector()
    inspector = inspect(db_connector.engine)
    existing_tables = set(inspector.get_table_names())
    #skip_tables = {"rhino_tx"}  # safeguard against wrong start block noted in script

    eligible = []
    for chain in main_conf:
        if not chain.runs_archive_raw_tx:
            continue
        table_name = f"{chain.origin_key}_tx"
        # if table_name in skip_tables:
        #     logger.info("Skipping %s (listed in skip tables)", table_name)
        #     continue
        if table_name not in existing_tables:
            logger.info("Skipping %s: table %s not found in DB", chain.origin_key, table_name)
            continue
        eligible.append({"origin_key": chain.origin_key, "table_name": table_name})
    return eligible


def get_eligible_archive_tables() -> List[Dict[str, str]]:
    """Return chain/table pairs flagged in archive_tables and present in the DB."""
    main_conf = get_main_config()
    db_connector = DbConnector()
    inspector = inspect(db_connector.engine)
    existing_tables = set(inspector.get_table_names())

    eligible = []
    for chain in main_conf:
        if not chain.archive_tables:
            continue
        for table_name in chain.archive_tables:
            if table_name not in existing_tables:
                logger.info("Skipping %s: table %s not found in DB", chain.origin_key, table_name)
                continue
            eligible.append({"origin_key": chain.origin_key, "table_name": table_name})
    return eligible


def get_archive_dates(
    db_connector: DbConnector,
    table_name: str,
    origin_key: str,
    archival_date: date,
    min_date_override: Optional[date] = None,
) -> List[date]:
    
    if table_name.endswith("_hourly") or table_name.endswith("_hourly_hll"):
        query = f"""
            SELECT MIN(date_trunc('day', hour)) AS min_date
            FROM {table_name}
            WHERE origin_key = '{origin_key}'
            AND hour <= '{archival_date}'
        """
    else:
        query = f"""
            SELECT MIN(date) AS min_date
            FROM {table_name}
            WHERE origin_key = '{origin_key}'
            AND date <= '{archival_date}'
        """
    df = pl.read_database_uri(query=query, uri=db_connector.uri)
    if df.is_empty() or df["min_date"].is_null().all():
        return []
    min_date = min_date_override or df["min_date"][0]
    if isinstance(min_date, datetime):
        min_date = min_date.date()
    elif isinstance(min_date, str):
        min_date = datetime.strptime(min_date, "%Y-%m-%d").date()
    return [min_date + timedelta(days=offset) for offset in range((archival_date - min_date).days + 1)]


def archive_chain(table_name: str, bucket_name: str, keep_postgres_days: int, chunk_size: int) -> None:
    """Archive a single chain's *_tx table to GCS/BQ."""
    credentials_info = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    bq_client = bigquery.Client(credentials=credentials)

    db_connector = DbConnector()
    fs = gcsfs.GCSFileSystem(token=credentials_info)

    archival_date = datetime.now().date() - timedelta(days=keep_postgres_days)
    logger.info("Archival date cutoff: %s", archival_date)

    bq_max_block = get_bq_max_block(bq_client, table_name, fs, bucket_name)
    start_block = (bq_max_block + 1) if bq_max_block is not None else 0
    logger.info("Starting from block %s for table %s", start_block, table_name)

    part = 0
    date_part_counter: Dict[str, int] = {}
    reached_end = False

    while not reached_end:
        query = f"""
            SELECT * 
            FROM {table_name} 
            WHERE block_number >= {start_block} 
            ORDER BY block_number ASC 
            LIMIT {chunk_size}
        """

        df = pl.read_database_uri(query=query, uri=db_connector.uri)

        if df.is_empty():
            logger.info("No more rows to fetch for %s.", table_name)
            break

        logger.info(
            "Loaded %s rows. Min block: %s, Max block: %s",
            df.shape[0],
            df["block_number"].min(),
            df["block_number"].max(),
        )

        # Ensure timestamp is datetime
        if df.schema["block_timestamp"] == pl.Utf8:
            df = df.with_columns(pl.col("block_timestamp").str.strptime(pl.Datetime, strict=False))

        df = df.with_columns(pl.col("block_timestamp").dt.date().alias("block_date"))

        unique_dates = [x for x in df.select("block_date").unique().to_series().to_list() if x is not None]

        if len(unique_dates) == 1:
            date = unique_dates[0]
            logger.info("Single date batch: %s", date)
            max_block = df["block_number"].max()
            df = df.filter(df["block_number"] < max_block)
            logger.info("Filtered rows after removing max block: %s", df.shape[0])

            if df.is_empty():
                logger.info("%s is done. Max block was %s.", table_name, max_block)
                break

            year, month, day = date.year, f"{date.month:02}", f"{date.day:02}"

            if date > archival_date:
                logger.info("DONE: date %s is greater than archival date %s.", date, archival_date)
                reached_end = True
                break

            date_key = str(date)
            date_part_counter[date_key] = date_part_counter.get(date_key, 0) + 1
            part_id = date_part_counter[date_key]

            block_range = f"{df['block_number'].min()}_{df['block_number'].max()}"
            filename = f"part_{part_id}_{block_range}.parquet"
            gcs_path = f"{bucket_name}/db_tx/{table_name}/date={year}-{month}-{day}/{filename}"

            save_to_gcs(df, f"gs://{gcs_path}", fs)

        else:
            logger.info("Multiple dates in batch %s, dropping latest date.", unique_dates)
            latest_date = max(unique_dates)
            df = df.filter(df["block_date"] < latest_date)
            max_block = df["block_number"].max() + 1
            logger.info("Filtered rows: %s", df.shape[0])

            if df.is_empty():
                logger.info("%s is done. Max block was %s.", table_name, max_block)
                break

            for date in sorted(set(df["block_date"].to_list())):
                group = df.filter(df["block_date"] == date)
                year, month, day = date.year, f"{date.month:02}", f"{date.day:02}"

                if date > archival_date:
                    logger.info("DONE: date %s is greater than archival date %s.", date, archival_date)
                    reached_end = True
                    break

                date_key = str(date)
                date_part_counter[date_key] = date_part_counter.get(date_key, 0) + 1
                part_id = date_part_counter[date_key]

                block_range = f"{group['block_number'].min()}_{group['block_number'].max()}"
                filename = f"part_{part_id}_{block_range}.parquet"
                gcs_path = f"{bucket_name}/db_tx/{table_name}/date={year}-{month}-{day}/{filename}"

                save_to_gcs(group, f"gs://{gcs_path}", fs)

        start_block = max_block
        part += 1


def archive_table_by_date(
    table_name: str,
    origin_key: str,
    bucket_name: str,
    keep_postgres_days: int,
    keep_postgres_days_hourly: int,
) -> None:
    """Archive non-tx tables by date for a single origin_key."""
    credentials_info = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    fs = gcsfs.GCSFileSystem(token=credentials_info)
    bq_client = bigquery.Client(
        credentials=service_account.Credentials.from_service_account_info(credentials_info)
    )
    db_connector = DbConnector()

    if table_name.endswith("_hourly") or table_name.endswith("_hourly_hll"):
        archival_date = datetime.now().date() - timedelta(days=keep_postgres_days_hourly)
    else:
        archival_date = datetime.now().date() - timedelta(days=keep_postgres_days)
    logger.info("Archival date cutoff: %s", archival_date)

    bq_max_date = get_bq_max_date(bq_client, table_name, origin_key, archival_date, fs, bucket_name)
    if bq_max_date:
        logger.info("BQ max date for %s (%s): %s", table_name, origin_key, bq_max_date)
        if bq_max_date >= archival_date:
            logger.info("No new dates to archive for %s (%s).", table_name, origin_key)
            return
        start_date = bq_max_date + timedelta(days=1)
    else:
        start_date = None

    dates = get_archive_dates(
        db_connector,
        table_name,
        origin_key,
        archival_date,
        min_date_override=start_date,
    )
    if not dates:
        logger.info("No dates to archive for %s (%s).", table_name, origin_key)
        return

    for archive_date in dates:
        if table_name.endswith("_hourly") or table_name.endswith("_hourly_hll"):
            query = f"""
                SELECT *,  date_trunc('day', hour) AS date
                FROM {table_name}
                WHERE origin_key = '{origin_key}'
                AND date_trunc('day', hour) = '{archive_date}'
            """
        else:
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE origin_key = '{origin_key}'
                AND date = '{archive_date}'
            """
        df = pl.read_database_uri(query=query, uri=db_connector.uri)
        if df.is_empty():
            logger.info("No data for %s %s on %s.", table_name, origin_key, archive_date)
            continue

        year, month, day = archive_date.year, f"{archive_date.month:02}", f"{archive_date.day:02}"
        filename = "part_1.parquet"
        gcs_path = (
            f"{bucket_name}/db_other/{table_name}/origin_key={origin_key}/date={year}-{month}-{day}/{filename}"
        )
        save_to_gcs(df, f"gs://{gcs_path}", fs)


def build_task_id(prefix: str, origin_key: str, table_name: str) -> str:
    safe_name = re.sub(r"[^a-zA-Z0-9_]+", "_", f"{origin_key}_{table_name}")
    return f"{prefix}_{safe_name}"


def build_table_task_id(prefix: str, table_name: str) -> str:
    safe_name = re.sub(r"[^a-zA-Z0-9_]+", "_", table_name)
    return f"{prefix}_{safe_name}"


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #
eligible_chains = get_eligible_chains()
eligible_archive_tables = get_eligible_archive_tables()
archive_tables_by_name: Dict[str, List[str]] = {}
for chain_table in eligible_archive_tables:
    table_name = chain_table["table_name"]
    origin_key = chain_table["origin_key"]
    archive_tables_by_name.setdefault(table_name, []).append(origin_key)
archive_tables_by_name = {
    table_name: sorted(origin_keys)
    for table_name, origin_keys in sorted(archive_tables_by_name.items())
}
bucket_default = "gtp-archive"
keep_days_default = 14
keep_days_hourly_default = 7
chunk_size_default = 1_000_000


@dag(
    default_args={
        "owner": "mseidl",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="utility_archive",
    description="Archive raw *_tx tables and configured archive_tables to GCS/BQ.",
    tags=["utility", "archive"],
    start_date=datetime(2024, 1, 1),
    schedule="15 16 * * *",
)
def utility_archive():
    for chain in eligible_chains:
        origin_key = chain["origin_key"]
        table_name = chain["table_name"]

        @task(task_id=f"archive_{origin_key}", execution_timeout=timedelta(hours=2))
        def run_archive(
            table: str = table_name,
            bucket: str = bucket_default,
            keep_days: int = keep_days_default,
            chunk_size: int = chunk_size_default
        ):
            logger.info("Starting archive task for %s (table: %s)", origin_key, table)
            archive_chain(table, bucket, keep_days, chunk_size)
        run_archive()

    for table_name, origin_keys in archive_tables_by_name.items():
        @task(task_id=build_table_task_id("archive_table", table_name), execution_timeout=timedelta(hours=10))
        def run_archive_table(
            table: str = table_name,
            origins: List[str] = origin_keys,
            bucket: str = bucket_default,
            keep_days: int = keep_days_default,
            keep_days_hourly: int = keep_days_hourly_default,
        ):
            logger.info("Starting archive task for table %s across %s chains", table, len(origins))
            for origin in origins:
                try:
                    logger.info("Starting archive task for %s (table: %s)", origin, table)
                    archive_table_by_date(table, origin, bucket, keep_days, keep_days_hourly)
                except Exception as exc:
                    logger.exception("Archive failed for %s (table: %s): %s", origin, table, exc)
                    send_discord_message(
                        f"Archival DAG: Archive failed for {origin} (table: {table}): {exc}"
                    )

        run_archive_table()


utility_archive_dag = utility_archive()

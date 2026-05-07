import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict

import gcsfs
import polars as pl
from airflow.sdk import dag, task
from google.cloud import bigquery
from google.oauth2 import service_account

from src.db_connector import DbConnector
from src.misc.airflow_utils import alert_via_webhook

logger = logging.getLogger(__name__)


EXPORT_CONFIGS = {
    "attestations": {
        "destination_table": "attestations",
        "cursor_column": "last_updated_time",
        "cursor_fields": ["last_updated_time", "id"],
        "merge_keys": ["id"],
        "cluster_fields": ["chain_id", "attester", "is_offchain"],
        "schema": [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("time_created", "TIMESTAMP"),
            bigquery.SchemaField("chain_id", "STRING"),
            bigquery.SchemaField("attester", "STRING"),
            bigquery.SchemaField("recipient", "STRING"),
            bigquery.SchemaField("revoked", "BOOL"),
            bigquery.SchemaField("is_offchain", "BOOL"),
            bigquery.SchemaField("tx_hash", "STRING"),
            bigquery.SchemaField("ipfs_hash", "STRING"),
            bigquery.SchemaField("schema_info", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("tags_json", "STRING"),
            bigquery.SchemaField("raw", "STRING"),
            bigquery.SchemaField("last_updated_time", "TIMESTAMP"),
            bigquery.SchemaField("revocation_time", "TIMESTAMP"),
        ],
        "query": """
            SELECT
                '0x' || encode(uid, 'hex') AS id,
                "time" AS time_created,
                chain_id,
                '0x' || encode(attester, 'hex') AS attester,
                recipient,
                revoked,
                is_offchain,
                CASE WHEN tx_hash IS NULL THEN NULL ELSE '0x' || encode(tx_hash, 'hex') END AS tx_hash,
                ipfs_hash,
                schema_info,
                source,
                tags_json::text AS tags_json,
                raw::text AS raw,
                last_updated_time,
                revocation_time
            FROM public.attestations
            WHERE {cursor_predicate}
            ORDER BY last_updated_time ASC, uid ASC
            LIMIT {chunk_size}
        """,
    },
    "labels": {
        "destination_table": "labels",
        "cursor_column": "last_updated_time",
        "cursor_fields": ["last_updated_time", "id", "tag_id"],
        "merge_keys": ["id", "tag_id"],
        "cluster_fields": ["chain_id", "tag_id", "attester"],
        "schema": [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("chain_id", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("tag_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("tag_value", "STRING"),
            bigquery.SchemaField("attester", "STRING"),
            bigquery.SchemaField("time_created", "TIMESTAMP"),
            bigquery.SchemaField("is_offchain", "BOOL"),
            bigquery.SchemaField("last_updated_time", "TIMESTAMP"),
        ],
        "query": """
            SELECT
                '0x' || encode(uid, 'hex') AS id,
                chain_id,
                address,
                tag_id,
                tag_value,
                '0x' || encode(attester, 'hex') AS attester,
                "time" AS time_created,
                is_offchain,
                last_updated_time
            FROM public.labels
            WHERE {cursor_predicate}
            ORDER BY last_updated_time ASC, uid ASC, tag_id ASC
            LIMIT {chunk_size}
        """,
    },
}


def get_google_credentials():
    credentials_raw = os.getenv("GOOGLE_CREDENTIALS")
    if not credentials_raw:
        raise ValueError("Missing GOOGLE_CREDENTIALS")
    credentials_info = json.loads(credentials_raw)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return credentials_info, credentials


def get_bq_client(credentials: service_account.Credentials) -> bigquery.Client:
    project = os.getenv("OLI_PUBLIC_BQ_PROJECT", credentials.project_id)
    location = "US"
    return bigquery.Client(credentials=credentials, project=project, location=location)


def get_dataset_id(client: bigquery.Client) -> str:
    dataset_name = "oli_public"
    return f"{client.project}.{dataset_name}"


def ensure_public_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset.description = "Public Open Labels Initiative exports."
    dataset = client.create_dataset(dataset, exists_ok=True)

    public_entry = bigquery.AccessEntry("READER", "specialGroup", "allAuthenticatedUsers")
    if public_entry not in dataset.access_entries:
        dataset.access_entries = list(dataset.access_entries) + [public_entry]
        client.update_dataset(dataset, ["access_entries"])
        logger.info("Made BigQuery dataset public for all authenticated users: %s", dataset_id)


def ensure_table(client: bigquery.Client, table_id: str, config: Dict) -> None:
    table = bigquery.Table(table_id, schema=config["schema"])
    table.description = f"Public OLI {config['destination_table']} export."
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="time_created",
    )
    table.clustering_fields = config["cluster_fields"]
    client.create_table(table, exists_ok=True)


def get_bq_cursor(client: bigquery.Client, table_id: str, cursor_column: str) -> str:
    lookback_hours = int(os.getenv("OLI_PUBLIC_EXPORT_CURSOR_LOOKBACK_HOURS", "1"))
    query = f"""
        SELECT TIMESTAMP_SUB(MAX({cursor_column}), INTERVAL {lookback_hours} HOUR) AS cursor_value
        FROM `{table_id}`
    """
    try:
        result = client.query(query).result().to_dataframe()
    except Exception as exc:
        logger.warning("Could not read cursor from %s: %s", table_id, exc)
        return "1970-01-01 00:00:00+00"

    if result.empty or result["cursor_value"].isnull().all():
        return "1970-01-01 00:00:00+00"
    return str(result["cursor_value"][0])


def build_cursor_predicate(config_key: str, cursor: Dict[str, str]) -> str:
    timestamp = cursor["last_updated_time"]
    uid_hex = cursor.get("id", "").removeprefix("0x")
    tag_id = cursor.get("tag_id", "").replace("'", "''")

    if not uid_hex:
        return f"last_updated_time > '{timestamp}'"

    uid_after = f"uid > decode('{uid_hex}', 'hex')"
    if config_key == "attestations":
        tie_breaker = uid_after
    elif tag_id:
        tie_breaker = (
            f"uid > decode('{uid_hex}', 'hex') "
            f"OR (uid = decode('{uid_hex}', 'hex') AND tag_id > '{tag_id}')"
        )
    else:
        tie_breaker = uid_after

    return (
        f"last_updated_time > '{timestamp}' "
        f"OR (last_updated_time = '{timestamp}' AND ({tie_breaker}))"
    )


def save_to_gcs(df: pl.DataFrame, gcs_uri: str, fs: gcsfs.GCSFileSystem) -> None:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_file:
        df.write_parquet(tmp_file.name, compression="snappy")
        fs.put(tmp_file.name, gcs_uri)
    logger.info("Saved %s rows to %s", df.height, gcs_uri)


def load_parquet_to_staging(
    client: bigquery.Client,
    gcs_uri: str,
    staging_table_id: str,
    config: Dict,
) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=config["schema"],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    client.load_table_from_uri(gcs_uri, staging_table_id, job_config=job_config).result()
    logger.info("Loaded %s into staging table %s", gcs_uri, staging_table_id)


def merge_staging_to_destination(
    client: bigquery.Client,
    staging_table_id: str,
    destination_table_id: str,
    config: Dict,
) -> None:
    columns = [field.name for field in config["schema"]]
    merge_keys = config["merge_keys"]
    on_clause = " AND ".join([f"T.{key} = S.{key}" for key in merge_keys])
    update_clause = ", ".join([f"{col} = S.{col}" for col in columns if col not in merge_keys])
    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"S.{col}" for col in columns])

    query = f"""
        MERGE `{destination_table_id}` T
        USING `{staging_table_id}` S
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    client.query(query).result()
    logger.info("Merged staging table %s into %s", staging_table_id, destination_table_id)


def export_table(config_key: str) -> int:
    config = EXPORT_CONFIGS[config_key]
    credentials_info, credentials = get_google_credentials()
    bq_client = get_bq_client(credentials)
    dataset_id = get_dataset_id(bq_client)
    destination_table_id = f"{dataset_id}.{config['destination_table']}"
    staging_table_id = f"{dataset_id}._staging_{config['destination_table']}"

    ensure_public_dataset(bq_client, dataset_id)
    ensure_table(bq_client, destination_table_id, config)

    cursor_value = get_bq_cursor(bq_client, destination_table_id, config["cursor_column"])
    cursor = {"last_updated_time": cursor_value, "id": "", "tag_id": ""}
    logger.info("Exporting OLI %s rows after cursor %s", config_key, cursor_value)

    db_connector = DbConnector(db_name="oli")
    bucket = "gtp-public"
    prefix = "oli"
    export_date = datetime.utcnow().date().isoformat()
    chunk_size = int(os.getenv("OLI_PUBLIC_EXPORT_CHUNK_SIZE", "100000"))
    fs = gcsfs.GCSFileSystem(token=credentials_info)
    total_rows = 0
    part_id = 0

    while True:
        cursor_predicate = build_cursor_predicate(config_key, cursor)
        query = config["query"].format(
            cursor_predicate=cursor_predicate,
            chunk_size=chunk_size,
        )
        df = pl.read_database_uri(query=query, uri=db_connector.uri)
        if df.is_empty():
            logger.info("No more OLI %s rows to export.", config_key)
            break

        part_id += 1
        filename = f"part_{part_id:06}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.parquet"
        gcs_uri = (
            f"gs://{bucket}/{prefix}/{config['destination_table']}/"
            f"export_date={export_date}/{filename}"
        )

        save_to_gcs(df, gcs_uri, fs)
        load_parquet_to_staging(bq_client, gcs_uri, staging_table_id, config)
        merge_staging_to_destination(bq_client, staging_table_id, destination_table_id, config)

        last_row = df.tail(1).to_dicts()[0]
        cursor["last_updated_time"] = str(last_row["last_updated_time"])
        cursor["id"] = last_row["id"]
        cursor["tag_id"] = last_row.get("tag_id", "")
        total_rows += df.height
        logger.info(
            "Exported %s %s rows through cursor %s.",
            total_rows,
            config_key,
            cursor,
        )

        if df.height < chunk_size:
            break

    bq_client.delete_table(staging_table_id, not_found_ok=True)
    return total_rows


@dag(
    default_args={
        "owner": "lorenz",
        "retries": 2,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": lambda context: alert_via_webhook(context, user="lorenz"),
    },
    dag_id="oli_public_exports",
    description="Export new OLI labels and attestations to public BigQuery tables and GCS parquet files.",
    tags=["oli", "export", "bigquery", "gcs"],
    start_date=datetime(2026, 5, 7),
    schedule="30 2 * * *",
    catchup=False,
)
def main():
    @task(execution_timeout=timedelta(hours=12))
    def export_attestations() -> int:
        return export_table("attestations")

    @task(execution_timeout=timedelta(hours=12))
    def export_labels() -> int:
        return export_table("labels")

    attestations = export_attestations()
    labels = export_labels()
    attestations >> labels


main()

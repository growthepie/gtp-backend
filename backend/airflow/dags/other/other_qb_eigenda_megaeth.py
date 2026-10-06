from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        "owner": "lorenz",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="other_qb_eigenda_megaeth",
    description="Quick Bite on EigenDA & MegaETH blob throughput",
    tags=["other"],
    start_date=datetime(2026, 3, 31),
    schedule="52 6 * * *",  # At 06:52 every day, needs to run after metrics_eigenda
)
def run_dag():

    @task
    def create_jsons():
        import os
        import pandas as pd
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        # Query 1: Ethereum blob throughput (MiB/s) since Dencun
        query_eth_blobs = """
            WITH data AS (
                SELECT
                    block_date,
                    blob_gas_used,
                    target_blob_gas
                FROM public.ethereum_blocks
                WHERE
                    block_date >= '2025-04-01'
                    AND block_date < CURRENT_DATE
            )
            SELECT
                block_date                                          AS time,
                SUM(blob_gas_used)/24/60/60/1024/1024               AS blob_mib_per_second,
                SUM(target_blob_gas)/24/60/60/1024/1024             AS target_blob_mib_per_second,
                SUM(target_blob_gas)/24/60/60/1024/1024 * 4000      AS max_tps_megaeth,
                t.value / 86400.0                                   AS tps
            FROM data
            LEFT JOIN public.fact_kpis t
                ON  t."date"      = data.block_date
                AND t.origin_key  in ('megaeth', 'megaeth_pre_mainnet')
                AND t.metric_key  = 'txcount'
            GROUP BY block_date, t.value
            ORDER BY time DESC;
        """
        df_eth = db_connector.execute_query(query_eth_blobs, load_df=True)
        df_eth['unix_timestamp'] = df_eth['time'].apply(lambda x: int(pd.Timestamp(x).timestamp() * 1000))

        eth_values = [
            [
                row['unix_timestamp'],
                float(row['blob_mib_per_second']),
                float(row['target_blob_mib_per_second']),
                float(row['max_tps_megaeth']),
                float(row['tps']) if pd.notna(row['tps']) else None
            ]
            for _, row in df_eth.iterrows()
        ]
        eth_dict = {
            "data": {
                "timeseries": {
                    "types": [
                        "unix",
                        "blob_mib_per_second",
                        "target_blob_mib_per_second",
                        "max_tps_megaeth",
                        "tps"
                    ],
                    "values": eth_values
                }
            }
        }
        eth_dict = fix_dict_nan(eth_dict, 'eigenda_megaeth_eth_blobs')
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/eigenda-megaeth/eth_blob_throughput', eth_dict, cf_distribution_id, invalidate=False)

        # Query 2: MegaETH EigenDA blob size, count, and txcount
        query_megaeth = """
            SELECT
                b."date",
                b.value                         AS bytes,
                b.value/1024/1024               AS MiB,
                b.value/1024/1024/24/60/60      AS MiB_per_second,
                c.value                         AS eigenda_blob_count,
                t.value                         AS txcount,
                t.value / 86400.0               AS tps,
                (t.value / 86400.0)
                    / NULLIF(b.value/1024/1024/24/60/60, 0) AS tx_per_MiB
            FROM public.fact_kpis b
            LEFT JOIN public.fact_kpis c
                ON  c."date"       = b."date"
                AND c.origin_key   in ('megaeth', 'megaeth_pre_mainnet')
                AND c.metric_key   = 'eigenda_blob_count'
            LEFT JOIN public.fact_kpis t
                ON  t."date"       = b."date"
                AND t.origin_key   in ('megaeth', 'megaeth_pre_mainnet')
                AND t.metric_key   = 'txcount'
            WHERE
                b.origin_key  in ('megaeth', 'megaeth_pre_mainnet')
                AND b.metric_key  = 'eigenda_blob_size_bytes'
            ORDER BY b."date" DESC;
        """
        df_megaeth = db_connector.execute_query(query_megaeth, load_df=True)
        df_megaeth['unix_timestamp'] = df_megaeth['date'].apply(lambda x: int(pd.Timestamp(x).timestamp() * 1000))

        megaeth_values = [
            [
                row['unix_timestamp'],
                float(row['bytes']),
                float(row['mib']),
                float(row['mib_per_second']),
                float(row['eigenda_blob_count']) if row['eigenda_blob_count'] is not None else None,
                float(row['txcount']) if row['txcount'] is not None else None,
                float(row['tps']) if row['tps'] is not None else None,
                float(row['tx_per_mib']) if row['tx_per_mib'] is not None else None
            ]
            for _, row in df_megaeth.iterrows()
        ]
        megaeth_dict = {
            "data": {
                "timeseries": {
                    "types": [
                        "unix",
                        "bytes",
                        "MiB",
                        "MiB_per_second",
                        "eigenda_blob_count",
                        "txcount",
                        "tps",
                        "tx_per_MiB"
                    ],
                    "values": megaeth_values
                }
            }
        }
        megaeth_dict = fix_dict_nan(megaeth_dict, 'eigenda_megaeth_megaeth_blobs')
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/eigenda-megaeth/megaeth_eigenda_throughput', megaeth_dict, cf_distribution_id, invalidate=False)

        # Query 3: EigenDA max capacity over time
        query_eigenda_capacity = """
            WITH limits AS (
                SELECT
                    generate_series::date AS time,
                    CASE
                        WHEN generate_series::date < '2025-07-30' THEN 15
                        ELSE 100
                    END AS max_mib_per_second
                FROM generate_series(
                    '2024-04-09'::date,
                    CURRENT_DATE - INTERVAL '1 day',
                    INTERVAL '1 day'
                )
            )

            SELECT
                l.time,
                l.max_mib_per_second,
                l.max_mib_per_second * 4000     AS max_tps_megaeth,
                t.value / 86400.0               AS tps
            FROM limits l
            LEFT JOIN public.fact_kpis t
                ON  t."date"      = l.time
                AND t.origin_key  = 'megaeth'
                AND t.metric_key  = 'txcount'
            WHERE l.time >= '2025-04-01'
            ORDER BY l.time DESC;
        """
        df_capacity = db_connector.execute_query(query_eigenda_capacity, load_df=True)
        df_capacity['unix_timestamp'] = df_capacity['time'].apply(lambda x: int(pd.Timestamp(x).timestamp() * 1000))

        capacity_values = [
            [
                row['unix_timestamp'],
                float(row['max_mib_per_second']),
                float(row['max_tps_megaeth']),
                float(row['tps']) if pd.notna(row['tps']) else None
            ]
            for _, row in df_capacity.iterrows()
        ]
        capacity_dict = {
            "data": {
                "timeseries": {
                    "types": [
                        "unix",
                        "max_mib_per_second",
                        "max_tps_megaeth",
                        "tps"
                    ],
                    "values": capacity_values
                }
            }
        }
        capacity_dict = fix_dict_nan(capacity_dict, 'eigenda_max_capacity', send_notification=False)
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/eigenda-megaeth/eigenda_max_capacity', capacity_dict, cf_distribution_id, invalidate=False)

        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/eigenda-megaeth/*')

    create_jsons()

run_dag()

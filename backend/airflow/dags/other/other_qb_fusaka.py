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
    dag_id="other_qb_fusaka",
    description="Quick Bite on Fusaka + blob_base_fee backfilling",
    tags=["other"],
    start_date=datetime(2025, 12, 11),
    schedule="7 */8 * * *",  # At minute 7 past every 8th hour
)
def run_dag():

    @task
    def backfill_blob_base_fee():
        """
        Backfills target_blob_gas, blob_base_fee_update_fraction & blob_base_fee in public.ethereum_blocks table
        """
        from src.misc.jinja_helper import execute_jinja_query
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        query_parameters = {
            "from_block": db_connector.execute_query("SELECT MIN(number) FROM public.ethereum_blocks WHERE blob_base_fee IS NULL;", load_df=True).iloc[0,0],
            "to_block": db_connector.execute_query("SELECT MAX(number) FROM public.ethereum_blocks;", load_df=True).iloc[0,0]
        }
        if query_parameters["from_block"] != None:
            df = execute_jinja_query(db_connector, "da_metrics/ethereum_blob_base_fee.sql.j2", query_parameters)
            print(f"Updated blob_base_fee, target_blob_gas and blob_base_fee_update_fraction for blocks {query_parameters['from_block']} to {query_parameters['to_block']}.")

    @task
    def create_jsons():
        """
        Create JSON files for Fusaka Quick Bite
        """
        import os
        import json
        import pandas as pd
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        # options are '1h', '4h', 'daily'
        rel_block_ranges = [
            # (block_from, label, interval, date) - no spaces in label!
            (24179383, 'Fusaka-BPO2', '4h', '2026-01-07'), # Fusaka BPO2
            (23975778, 'Fusaka-BPO1', '4h', '2025-12-09'), # Fusaka BPO1
            (23935694, 'Fusaka', 'daily', '2025-12-03'), # Fusaka
            (22431084, 'Pectra', 'daily', '2025-05-07'), # Pectra
            (19426587, 'Dencun', 'daily', '2024-03-13') # Dencun
        ]

        # Blob data
        for block_from, label, interval, _ in rel_block_ranges:
            ### DAILY AGGREGATES
            if interval == 'daily':
                query = f"""
                    WITH data AS (
                        SELECT
                            block_date,
                            base_fee_per_gas,
                            blob_gas_used,
                            blob_gas_used/1024/128 AS blob_count,
                            target_blob_gas/1024/128 AS target_blob_count,
                            blob_base_fee,
                            -- blob fees in ETH for each block
                            (blob_base_fee * blob_gas_used) / 1e18 AS blob_fees_eth
                        FROM public.ethereum_blocks
                        WHERE 
                            number >= {block_from}  -- Filter for blocks >= specified block number
                            AND block_date < CURRENT_DATE  -- Exclude current date
                    ),

                    agg AS (
                        SELECT
                            block_date                              AS time,
                            --AVG(base_fee_per_gas)                   AS avg_base_fee_per_gas,
                            AVG(blob_count)                         AS avg_blob_count,
                            AVG(target_blob_count)                  AS avg_target_blob_count,
                            AVG(blob_base_fee)                      AS avg_blob_base_fee,
                            -- total blob fees in ETH per day
                            SUM(blob_fees_eth)                      AS blob_fees_eth
                        FROM data
                        GROUP BY block_date
                    )

                    SELECT
                        *,
                        SUM(blob_fees_eth) OVER (ORDER BY time) AS cumulative_blob_fees_eth
                    FROM agg
                    ORDER BY time;
                """

            ### 4H AGGREGATES
            elif interval == '4h':
                query = f"""
                    WITH data AS (
                        SELECT
                            -- 4-hour time bucket (14400 seconds)
                            to_timestamp(
                                floor(extract(epoch FROM "timestamp") / 14400) * 14400
                            ) AS ts_4h,
                            base_fee_per_gas,
                            blob_gas_used,
                            blob_gas_used/1024/128 AS blob_count,
                            target_blob_gas/1024/128 AS target_blob_count,
                            blob_base_fee,
                            -- blob fees in ETH for each block
                            (blob_base_fee * blob_gas_used) / 1e18 AS blob_fees_eth_block
                        FROM public.ethereum_blocks
                        WHERE 
                            "number" >= {block_from}  -- Filter for blocks >= specified block number
                            AND to_timestamp(floor(extract(epoch FROM "timestamp") / 14400) * 14400) < to_timestamp(floor(extract(epoch FROM NOW()) / 14400) * 14400)
                    ),

                    agg AS (
                        SELECT
                            ts_4h                                   AS time,
                            --AVG(base_fee_per_gas)                   AS avg_base_fee_per_gas,
                            AVG(blob_count)                         AS avg_blob_count,
                            AVG(target_blob_count)                  AS avg_target_blob_count,
                            AVG(blob_base_fee)                      AS avg_blob_base_fee,
                            -- total blob fees in ETH per 4h bucket
                            SUM(blob_fees_eth_block)                AS blob_fees_eth
                        FROM data
                        GROUP BY ts_4h
                    )

                    SELECT
                        *,
                        SUM(blob_fees_eth) OVER (ORDER BY time) AS cumulative_blob_fees_eth
                    FROM agg
                    ORDER BY time;
                """

            ### 1H AGGREGATES
            elif interval == '1h':
                query = f"""
                    WITH data AS (
                        SELECT
                            -- 1-hour time bucket (3600 seconds)
                            to_timestamp(
                                floor(extract(epoch FROM "timestamp") / 3600) * 3600
                            ) AS ts_1h,
                            base_fee_per_gas,
                            blob_gas_used,
                            blob_gas_used/1024/128 AS blob_count,
                            target_blob_gas/1024/128 AS target_blob_count,
                            blob_base_fee,
                            -- blob fees in ETH for each block
                            (blob_base_fee * blob_gas_used) / 1e18 AS blob_fees_eth_block
                        FROM public.ethereum_blocks
                        WHERE 
                            "number" >= {block_from}  -- Filter for blocks >= specified block number
                            AND to_timestamp(floor(extract(epoch FROM "timestamp") / 3600) * 3600) < to_timestamp(floor(extract(epoch FROM NOW()) / 3600) * 3600)  -- Exclude current hour
                    ),

                    agg AS (
                        SELECT
                            ts_1h                                   AS time,
                            --AVG(base_fee_per_gas)                   AS avg_base_fee_per_gas,
                            AVG(blob_count)                         AS avg_blob_count,
                            AVG(target_blob_count)                  AS avg_target_blob_count,
                            AVG(blob_base_fee)                      AS avg_blob_base_fee,
                            -- total blob fees in ETH per 1h bucket
                            SUM(blob_fees_eth_block)                AS blob_fees_eth
                        FROM data
                        GROUP BY ts_1h
                    )

                    SELECT
                        *,
                        SUM(blob_fees_eth) OVER (ORDER BY time) AS cumulative_blob_fees_eth
                    FROM agg
                    ORDER BY time;
                """

            # execute query and get dataframe
            df = db_connector.execute_query(query, load_df=True)

            # turn avg_target_blob_count into integer & name target_blob_count
            df['target_blob_count'] = df['avg_target_blob_count'].astype(int)
            # round avg_blob_count to 2 decimal places
            df['avg_blob_count'] = df['avg_blob_count'].round(2)
            # devide avg_base_fee_per_gas & avg_blob_base_fee by 1e9 to get gwei
            df['avg_blob_base_fee'] = (df['avg_blob_base_fee'] / 1e9).round(9)
            # rename time into unix_timestamp & convert from timestamp to int unix timestamp
            df['unix_timestamp'] = df['time'].apply(lambda x: pd.Timestamp(x).to_pydatetime()).apply(lambda x: int(x.timestamp() * 1000))
            df = df.drop(columns=['time'])
            # drop avg_target_blob_count column
            df = df.drop(columns=['avg_target_blob_count'])

            # Create combined values array with all 6 columns
            values = [
                [
                    row['unix_timestamp'],
                    row['avg_blob_count'],
                    row['avg_blob_base_fee'],
                    row['blob_fees_eth'],
                    row['cumulative_blob_fees_eth'],
                    row['target_blob_count']
                ] 
                for _, row in df.iterrows()
            ]

            # Create the data dictionary
            data_dict = {
                "data": {
                    "timeseries": {
                        "types": [
                            "unix",
                            "avg_blob_count",
                            "avg_blob_base_fee",
                            "blob_fees_eth",
                            "cumulative_blob_fees_eth",
                            "target_blob_count"
                        ],
                        "values": values
                    }
                }
            }

            # If you have a fix_dict_nan function, apply it here
            data_dict = fix_dict_nan(data_dict, f'blob_data_{label}')

            # Or if you want to upload to S3 directly:
            upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/fusaka/timeseries_blobs/{label}', data_dict, cf_distribution_id, invalidate=False)

        # Gas used, gas limit, eth burnt
        for block_from, label, interval, _ in rel_block_ranges:
            ### DAILY AGGREGATES
            if interval == 'daily':
                query = f"""
                    SELECT 
                        block_date AS time,
                        SUM(gas_used)/COUNT(*) AS gas_used,
                        SUM(gas_limit)/COUNT(*) AS gas_limit,
                        SUM((base_fee_per_gas::NUMERIC * gas_used) / 1e18) AS eth_burnt,
                        SUM(SUM((base_fee_per_gas::NUMERIC * gas_used) / 1e18)) OVER (ORDER BY block_date) AS cum_eth_burnt
                    FROM public.ethereum_blocks
                    WHERE 
                        "number" >= {block_from}
                        AND block_date < CURRENT_DATE  -- Exclude current date
                    GROUP BY block_date
                    ORDER BY 1
                """
            
            ### 4H AGGREGATES
            elif interval == '4h':
                query = f"""
                    SELECT 
                        to_timestamp(
                            floor(extract(epoch FROM "timestamp") / 14400) * 14400
                        ) AS time,
                        SUM(gas_used)/COUNT(*) AS gas_used,
                        SUM(gas_limit)/COUNT(*) AS gas_limit,
                        SUM((base_fee_per_gas::NUMERIC * gas_used) / 1e18) AS eth_burnt,
                        SUM(SUM((base_fee_per_gas::NUMERIC * gas_used) / 1e18)) OVER (ORDER BY to_timestamp(floor(extract(epoch FROM "timestamp") / 14400) * 14400)) AS cum_eth_burnt
                    FROM public.ethereum_blocks
                    WHERE 
                        "number" >= {block_from}
                        AND to_timestamp(floor(extract(epoch FROM "timestamp") / 14400) * 14400) < to_timestamp(floor(extract(epoch FROM NOW()) / 14400) * 14400)  -- Exclude current 4h bucket
                    GROUP BY time
                    ORDER BY time DESC
                """
            
            ### 1H AGGREGATES
            elif interval == '1h':
                query = f"""
                    SELECT 
                        to_timestamp(
                            floor(extract(epoch FROM "timestamp") / 3600) * 3600
                        ) AS time,
                        SUM(gas_used)/COUNT(*) AS gas_used,
                        SUM(gas_limit)/COUNT(*) AS gas_limit,
                        SUM((base_fee_per_gas::NUMERIC * gas_used) / 1e18) AS eth_burnt,
                        SUM(SUM((base_fee_per_gas::NUMERIC * gas_used) / 1e18)) OVER (ORDER BY to_timestamp(floor(extract(epoch FROM "timestamp") / 3600) * 3600)) AS cum_eth_burnt
                    FROM public.ethereum_blocks
                    WHERE 
                        "number" >= {block_from}
                        AND to_timestamp(floor(extract(epoch FROM "timestamp") / 3600) * 3600) < to_timestamp(floor(extract(epoch FROM NOW()) / 3600) * 3600)  -- Exclude current hour
                    GROUP BY time
                    ORDER BY time DESC
                """
            
            df_gas = db_connector.execute_query(query, load_df=True)
            # convert time (e.g. '2025-12-12') to unix timestamp int
            df_gas['time'] = df_gas['time'].apply(lambda x: pd.Timestamp(x).to_pydatetime())
            gas_values = [
                [
                    int(row['time'].timestamp() * 1000),
                    int(row['gas_used']),
                    int(row['gas_limit']),
                    float(row['eth_burnt']),
                    float(row['cum_eth_burnt'])
                ]
                for _, row in df_gas.iterrows()
            ]
            gas_dict = {
                "data": {
                    "timeseries": {
                        "types": [
                            "unix",
                            "gas_used",
                            "gas_limit",
                            "eth_burnt",
                            "cum_eth_burnt"
                        ],
                        "values": gas_values
                    }
                }
            }
            # Fix NaN values in the gas_dict
            gas_dict = fix_dict_nan(gas_dict, f'blob_data_{label}')
            # Upload to S3
            upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/fusaka/timeseries_gas/{label}', gas_dict, cf_distribution_id, invalidate=False)


        # KPI totals
        query = f"""
            SELECT
                COUNT(*) AS total_blocks,
                FLOOR(SUM(blob_gas_used)/128/1024)::INT AS total_blobs,
                SUM((blob_base_fee * blob_gas_used) / 1e18) AS total_blob_fees_eth
            FROM public.ethereum_blocks;
        """
        df_kpis = db_connector.execute_query(query, load_df=True)
        kpi_dict = {
            "data": {
                "fusaka_total_blocks": int(df_kpis.iloc[0]['total_blocks']),
                "fusaka_total_blobs": int(df_kpis.iloc[0]['total_blobs']),
                "fusaka_total_blob_fees_eth": float(df_kpis.iloc[0]['total_blob_fees_eth'])
            }
        }
        # Fix NaN values in the kpi_dict
        kpi_dict = fix_dict_nan(kpi_dict, 'quick_bites_fusaka_kpis')
        # Upload to S3
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/fusaka/totals', kpi_dict, cf_distribution_id, invalidate=False)


        # EIP-7918
        query = """
            WITH RECURSIVE cum_gas_calc AS (
                -- Base case: starting block with cum_gas = 0
                SELECT
                    "number",
                    "timestamp",
                    blob_base_fee,
                    blob_gas_used,
                    blob_base_fee_update_fraction,
                    (SELECT excess_blob_gas FROM public.ethereum_blocks WHERE "number" = 23935694) as old_excess_blob_gas
                FROM public.ethereum_blocks
                WHERE "number" = 23935693 -- Fusaka upgrade - 1 block

                UNION ALL

                -- Recursive case: each subsequent row
                SELECT
                    eb."number",
                    eb."timestamp",
                    eb.blob_base_fee,
                    eb.blob_gas_used,
                    eb.blob_base_fee_update_fraction,
                    CASE
                        WHEN (eb.blob_gas_used - (SELECT target_blob_gas FROM public.ethereum_blocks WHERE "number" = eb."number") + cgc.old_excess_blob_gas) < 0 THEN 0
                        ELSE (eb.blob_gas_used - (SELECT target_blob_gas FROM public.ethereum_blocks WHERE "number" = eb."number") + cgc.old_excess_blob_gas)
                    END as old_excess_blob_gas
                FROM public.ethereum_blocks eb
                INNER JOIN cum_gas_calc cgc ON eb."number" = cgc."number" + 1
            ),
            block_calcs AS (
                SELECT
                    DATE(cgc."timestamp") as block_date,
                    cgc.blob_base_fee * cgc.blob_gas_used / 1e18 as blob_fee_eth,
                    CAST(
                        TRUNC(
                            EXP(
                                CAST(LAG(cgc.old_excess_blob_gas) OVER (ORDER BY cgc."number") AS NUMERIC) /
                                CAST(cgc.blob_base_fee_update_fraction AS NUMERIC)
                            )
                        )
                    AS BIGINT) * cgc.blob_gas_used / 1e18 as old_blob_fee_eth
                FROM cum_gas_calc cgc
            ),
            block_calcs_with_price AS (
                SELECT
                    bc.blob_fee_eth,
                    bc.old_blob_fee_eth,
                    COALESCE(fk.value, 0) as eth_price_usd
                FROM block_calcs bc
                LEFT JOIN public.fact_kpis fk 
                    ON fk."date" = bc.block_date
                    AND fk.metric_key = 'price_usd'
                    AND fk.origin_key = 'ethereum'
            )
            SELECT
                SUM(blob_fee_eth) as total_blob_fee_eth_with7918,
                SUM(old_blob_fee_eth) as total_blob_fee_eth_without7918,
                SUM(blob_fee_eth * eth_price_usd) as total_blob_fee_usd_with7918,
                SUM(old_blob_fee_eth * eth_price_usd) as total_blob_fee_usd_without7918
            FROM block_calcs_with_price;
        """
        df_eip7918_kpis = db_connector.execute_query(query, load_df=True)
        eip7918_dict = {
            "data": {
                "fusaka_total_blob_fee_eth_with7918": float(df_eip7918_kpis.iloc[0]['total_blob_fee_eth_with7918']),
                "fusaka_total_blob_fee_eth_without7918": float(df_eip7918_kpis.iloc[0]['total_blob_fee_eth_without7918']),
                "fusaka_total_blob_fee_usd_with7918": float(df_eip7918_kpis.iloc[0]['total_blob_fee_usd_with7918']),
                "fusaka_total_blob_fee_usd_without7918": float(df_eip7918_kpis.iloc[0]['total_blob_fee_usd_without7918'])
            }
        }
        # Fix NaN values in the eip7918_dict
        eip7918_dict = fix_dict_nan(eip7918_dict, 'quick_bites_fusaka_eip7918_kpis')
        # Upload to S3
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/fusaka/eip7918_kpis', eip7918_dict, cf_distribution_id, invalidate=False)

        query = """
            WITH RECURSIVE cum_gas_calc AS (
                -- Base case: starting block with cum_gas = 0
                SELECT
                    "number",
                    "timestamp",
                    blob_base_fee,
                    blob_gas_used,
                    blob_base_fee_update_fraction,
                    (SELECT excess_blob_gas FROM public.ethereum_blocks WHERE "number" = 23921294) as old_excess_blob_gas
                FROM public.ethereum_blocks
                WHERE "number" = 23921293 -- Fusaka upgrade - 1 block - 2 days

                UNION ALL

                -- Recursive case: each subsequent row
                SELECT
                    eb."number",
                    eb."timestamp",
                    eb.blob_base_fee,
                    eb.blob_gas_used,
                    eb.blob_base_fee_update_fraction,
                    CASE
                        WHEN (eb.blob_gas_used - (SELECT target_blob_gas FROM public.ethereum_blocks WHERE "number" = eb."number") + cgc.old_excess_blob_gas) < 0 THEN 0
                        ELSE (eb.blob_gas_used - (SELECT target_blob_gas FROM public.ethereum_blocks WHERE "number" = eb."number") + cgc.old_excess_blob_gas)
                    END as old_excess_blob_gas
                FROM public.ethereum_blocks eb
                INNER JOIN cum_gas_calc cgc ON eb."number" = cgc."number" + 1
            ),
            block_calcs AS (
                SELECT
                    blob_base_fee,
                    blob_base_fee * blob_gas_used / 1e18 as blob_fee_eth,
                    CAST(
                        TRUNC(
                            EXP(
                                CAST(LAG(old_excess_blob_gas) OVER (ORDER BY "number") AS NUMERIC) /
                                CAST(blob_base_fee_update_fraction AS NUMERIC)
                            )
                        )
                    AS BIGINT) as old_blob_base_fee,
                    CAST(
                        TRUNC(
                            EXP(
                                CAST(LAG(old_excess_blob_gas) OVER (ORDER BY "number") AS NUMERIC) /
                                CAST(blob_base_fee_update_fraction AS NUMERIC)
                            )
                        )
                    AS BIGINT) * blob_gas_used / 1e18 as old_blob_fee_eth,
                    -- daily time bucket (86400 seconds)
                    to_timestamp(
                        floor(extract(epoch FROM "timestamp") / 86400) * 86400
                    ) AS ts_4h
                FROM cum_gas_calc
            )
            SELECT
                ts_4h,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY blob_base_fee) / 1e9 as median_blob_base_fee,
                SUM(blob_fee_eth) as total_blob_fee_eth,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY old_blob_base_fee) / 1e9 as median_old_blob_base_fee,
                SUM(old_blob_fee_eth) as total_old_blob_fee_eth
            FROM block_calcs
            GROUP BY ts_4h
            ORDER BY ts_4h;
        """
        df_eip7918_timeseries = db_connector.execute_query(query, load_df=True)
        eip7918_values = [
            [
                int(row['ts_4h'].timestamp() * 1000), # to milliseconds
                float(row['median_blob_base_fee']),
                float(row['total_blob_fee_eth']),
                float(row['median_old_blob_base_fee']),
                float(row['total_old_blob_fee_eth'])
            ]
            for _, row in df_eip7918_timeseries.iterrows()
        ]
        eip7918_dict = {
            "data": {
                "timeseries": {
                    "types": [
                        "unix",
                        "median_blob_base_fee",
                        "total_blob_fee_eth",
                        "median_old_blob_base_fee",
                        "total_old_blob_fee_eth"
                    ],
                    "values": eip7918_values
                }
            }
        }
        # Fix NaN values in the eip7918_dict
        eip7918_dict = fix_dict_nan(eip7918_dict, 'quick_bites_fusaka_eip7918_timeseries')
        # Upload to S3
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/fusaka/eip7918_timeseries', eip7918_dict, cf_distribution_id, invalidate=False)

        ### empty_cloudfront_cache
        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/fusaka/*')
    # DAG TASK DEPENDENCIES
    backfill_data = backfill_blob_base_fee()
    create_jsons_task = create_jsons()
    backfill_data >> create_jsons_task

run_dag()

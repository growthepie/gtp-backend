from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='other_bq_to_postres_aa',
    description='Load active addresses from BigQuery to Postgres',
    tags=['other'],
    start_date=datetime(2025,12,21),
     schedule=None ## was '50 1 * * *'. Polygon PoS active addresses come from our own raw data (polygon_pos_tx) since 2026-11-17; kept for manual backfills
)

def run_dag():
    @task()
    def run_polygon_aa():      
        #!/usr/bin/env python3
        """
        Write active addresses from BQ to Postgres.
        """

        import os
        import sys
        import logging
        import json
        from datetime import date
        from google.oauth2 import service_account

        import pandas as pd
        from google.cloud import bigquery
        from src.db_connector import DbConnector

        db_connector = DbConnector()

        # ---------------- Config ----------------

        ## yesterday
        DEFAULT_START = date.today() - timedelta(days=2)
        DEFAULT_END   = date.today()
        DEFAULT_PG_TABLE = "fact_active_addresses"
        DEFAULT_CHAIN = "polygon_pos"

        # Parameterized SQL (BigQuery Standard SQL)
        # Using DATE(block_timestamp) both for filter and grouping (truncated to day).
        BQ_SQL = """
            DECLARE d DATE DEFAULT @the_day;

            SELECT
                from_address as address,
                DATE(block_timestamp) AS date,
                COUNT(*) AS txcount
            FROM `bigquery-public-data.goog_blockchain_polygon_mainnet_us.transactions`
            WHERE DATE(block_timestamp) = @the_day
            GROUP BY 1,2
        """

        # --------------- Helpers ----------------
        def daterange(start: date, end_exclusive: date):
            d = start
            while d < end_exclusive:
                yield d
                d += timedelta(days=1)

        def run_one_day(client: bigquery.Client, one_day: date) -> pd.DataFrame:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("the_day", "DATE", one_day.isoformat())]
            )
            # Run query
            job = client.query(BQ_SQL, job_config=job_config)
            df = job.result().to_dataframe(create_bqstorage_client=False)  # BYTES -> Python bytes
            # Ensure columns and dtypes are as expected
            if not df.empty:
                # Safeguard column ordering and types
                df = df[["address", "date", "txcount"]]
                # Ensure 'date' is date (not datetime) and 'address' stays as bytes
                df['date'] = df['date'].apply(pd.to_datetime).dt.date
                # BigQuery returns BYTES as Python bytes; just ensure dtype=object with bytes inside
                # Null safety
                df["address"] = df["address"].astype(object)
                # Make sure txcount is int
                df["txcount"] = df["txcount"].astype("int64")
            return df

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )

        # Instantiate clients
        logging.info("Creating BigQuery client…")
        credentials_json = os.getenv("GOOGLE_CREDENTIALS")
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)

        # Create a BigQuery client
        bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        start_date = DEFAULT_START
        end_date = DEFAULT_END
        logging.info("Begin daily loop from %s to %s (exclusive).", start_date, end_date)

        total_rows = 0
        for d in daterange(start_date, end_date):
            logging.info("Querying day %s", d)
            df = run_one_day(bq_client, d)
            if df.empty:
                logging.info("No rows for %s.", d)
            else:
                logging.info("Fetched %d rows for %s. Writing to Postgres…", len(df), d)
                df['origin_key'] = DEFAULT_CHAIN
                df['address'] = df['address'].apply(lambda x: bytes.fromhex(x[2:]))
                df.set_index(['address', 'date', 'origin_key'], inplace=True)
                
                db_connector.upsert_table(DEFAULT_PG_TABLE, df)
                total_rows += len(df)
                logging.info("Wrote %d rows for %s.", len(df), d)  # success

        logging.info("Done. Total rows written: %d", total_rows)
        
    @task()
    def run_polygon_contract_aa():
        import os
        import sys
        import logging
        import json
        from datetime import timedelta, date
        from google.oauth2 import service_account

        import pandas as pd
        from google.cloud import bigquery
        from src.db_connector import DbConnector

        db_connector = DbConnector()

        # ---------------- Config ----------------

        ## yesterday
        DEFAULT_START = date.today() - timedelta(days=2)
        DEFAULT_END   = date.today()
        DEFAULT_PG_TABLE = "fact_active_addresses_contract"
        DEFAULT_CHAIN = "polygon_pos"

        # Parameterized SQL (BigQuery Standard SQL)
        # Using DATE(block_timestamp) both for filter and grouping (truncated to day).
        BQ_SQL = """
            DECLARE d DATE DEFAULT @the_day;

            SELECT
                `to_address` AS address,
                DATE(block_timestamp) AS date,
                `from_address` AS from_address
            FROM `bigquery-public-data.goog_blockchain_polygon_mainnet_us.transactions` t
            WHERE DATE(block_timestamp) = @the_day
                and `to_address` is not null
            GROUP BY 1,2,3
        """

        # --------------- Helpers ----------------
        def daterange(start: date, end_exclusive: date):
            d = start
            while d < end_exclusive:
                yield d
                d += timedelta(days=1)

        def run_one_day(client: bigquery.Client, one_day: date) -> pd.DataFrame:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("the_day", "DATE", one_day.isoformat())]
            )
            # Run query
            job = client.query(BQ_SQL, job_config=job_config)
            df = job.result().to_dataframe(create_bqstorage_client=False)  # BYTES -> Python bytes
            # Ensure columns and dtypes are as expected
            if not df.empty:
                # Safeguard column ordering and types
                df = df[["address", "date", "from_address"]]
                # Ensure 'date' is date (not datetime) and 'address' stays as bytes
                df['date'] = df['date'].apply(pd.to_datetime).dt.date
                # BigQuery returns BYTES as Python bytes; just ensure dtype=object with bytes inside
                # Null safety
                df["address"] = df["address"].astype(object)
                df["from_address"] = df["from_address"].astype(object)
            return df

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )

        # Instantiate clients
        logging.info("Creating BigQuery client…")
        credentials_json = os.getenv("GOOGLE_CREDENTIALS")
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)

        # Create a BigQuery client
        bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        start_date = DEFAULT_START
        end_date = DEFAULT_END
        logging.info("Begin daily loop from %s to %s (exclusive).", start_date, end_date)

        total_rows = 0
        for d in daterange(start_date, end_date):
            logging.info("Querying day %s", d)
            df = run_one_day(bq_client, d)
            if df.empty:
                logging.info("No rows for %s.", d)
            else:
                logging.info("Fetched %d rows for %s. Writing to Postgres…", len(df), d)
                df['origin_key'] = DEFAULT_CHAIN
                df['address'] = df['address'].apply(lambda x: bytes.fromhex(x[2:]))
                df['from_address'] = df['from_address'].apply(lambda x: bytes.fromhex(x[2:]))
                df.set_index(['address', 'origin_key', 'date', 'from_address'], inplace=True)
                
                db_connector.upsert_table(DEFAULT_PG_TABLE, df)
                total_rows += len(df)
                logging.info("Wrote %d rows for %s.", len(df), d)  # success

        logging.info("Done. Total rows written: %d", total_rows)     

    run_polygon_aa()
    run_polygon_contract_aa()
run_dag()
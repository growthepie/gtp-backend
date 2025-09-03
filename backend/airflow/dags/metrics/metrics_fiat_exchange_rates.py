import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta, date
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
import os

@dag(
    default_args={
        'owner': 'nader',
        'retries': 3,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_fiat_exchange_rates',
    description='Load fiat exchange rates (e.g., EUR, BRL) into fact_kpis daily',
    tags=['metrics', 'daily', 'fiat'],
    start_date=datetime(2023, 9, 1),
    schedule='15 00 * * *'
)
def etl():
    @task()
    def load_rates():
        from src.db_connector import DbConnector
        from src.adapters.adapter_currency_conversion import AdapterCurrencyConversion
        from src.currency_config import get_supported_currencies
        import pandas as pd

        db_connector = DbConnector()
        currencies = get_supported_currencies()
        adapter_params = {
            'currencies': currencies,
            'force_refresh': False
        }
        ad = AdapterCurrencyConversion(adapter_params, db_connector)

        # Check if first run using marker file
        marker_file = '/tmp/fiat_rates_initialized'
        is_first_run = not os.path.exists(marker_file)

        if is_first_run:
            print("First run detected. Backfilling 365 days of historical fiat rates...")
            # Backfill 365 days
            end = date.today()
            start = end - timedelta(days=365)
            frames = []
            for offset in range(366):  # 365 days + today
                d = start + timedelta(days=offset)
                load_params = {
                    'load_type': 'historical_rates',
                    'date': d.strftime('%Y-%m-%d'),
                    'currencies': currencies
                }
                df = ad.extract(load_params)
                if not df.empty:
                    frames.append(df)
            
            if frames:
                ad.load(pd.concat(frames))
                print(f"Backfilled {len(pd.concat(frames))} rows of historical fiat rates.")
            
            # Create marker file
            with open(marker_file, 'w') as f:
                f.write(str(datetime.now()))
            print("First run complete. Future runs will load daily rates only.")
        else:
            print("Regular daily run. Loading current fiat rates...")
            # Regular daily load
            load_params = {
                'load_type': 'current_rates',
                'currencies': currencies
            }
            df = ad.extract(load_params)
            ad.load(df)
            print(f"Loaded {len(df)} rows of current fiat rates.")

    load_rates()

etl()



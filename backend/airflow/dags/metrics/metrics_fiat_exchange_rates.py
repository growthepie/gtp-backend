from datetime import datetime, timedelta, date
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook
import os

@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 3,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_fiat_exchange_rates',
    description='Load fiat exchange rates (e.g., EUR, BRL) into fact_kpis daily',
    tags=['metrics', 'daily'],
    start_date=datetime(2023, 9, 1),
    schedule='15 01 * * *' # before metrics_stables DAG
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

        # Check if all currencies already in db
        days = 3 ##default backfill
        end = date.today()
        start = end - timedelta(days=days)

        for curr in currencies:
            query = f"""select max(date) as value from fact_kpis where origin_key = 'fiat_{curr}' and metric_key = 'price_usd' """
            result = db_connector.execute_query(query, load_df=True)
            
            if result.empty:
                print(f"Currency {curr} has no data in DB")
                start = date.today() - timedelta(days=(365*3)) #3 years of data backfilling
            else:
                max_date = result.value.values[0]
                if max_date is not None:
                    print(f"Currency {curr} already has data in DB from {max_date}")
                    if max_date < start:
                        start = max_date
                else:
                    print(f"Currency {curr} has no data in DB")
                    start = date.today() - timedelta(days=(365*3)) #3 years of data backfilling

        frames = []

        for offset in range((date.today() - start).days + 1):  # Include today
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
            df = pd.concat(frames)
            ad.load(pd.concat(frames))

            print(f"Loaded {len(df)} rows of current fiat rates.")

        # Fetch ISK/USD daily from Danmarks Nationalbank
        # Use today's date so weekends/holidays get covered with the latest available rate
        df_isk = ad._fetch_isk_from_nationalbanken()
        if not df_isk.empty:
            df_isk = df_isk.reset_index()
            df_isk['date'] = end
            df_isk = df_isk.set_index(['metric_key', 'origin_key', 'date'])
            ad.load(df_isk)
            print(f"Loaded ISK/USD rate from Nationalbanken.")

    load_rates()

etl()



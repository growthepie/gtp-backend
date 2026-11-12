from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 5,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_coingecko_hourly',
    description='Load price, volume, and market_cap from coingecko API for specific tokens.',
    tags=['metrics', 'hourly'],
    start_date=datetime(2023,4,24),
    schedule='20 * * * *'
)

def etl():
    @task()
    def run_market_chart_hourly():
        from src.db_connector import DbConnector
        from src.adapters.adapter_coingecko import AdapterCoingecko
        import os
        adapter_params = {
            'api_key' : os.getenv("COINGECKO_API")
        }
        
        load_params = {
            'load_type' : 'project',
            'granularity' : 'hourly', # 'daily' or 'hourly
            'metric_keys' : ['price', 'volume', 'market_cap'],
            'origin_keys' : ['ethereum', 'starknet', 'mantle', 'celestia'], # list of tokens that is required on hourly granularity
            'days' : '3', # auto, max, or a number (as string)
            'vs_currencies' : ['usd', 'eth']
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterCoingecko(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_market_chart_daily():
        from src.db_connector import DbConnector
        from src.adapters.adapter_coingecko import AdapterCoingecko
        import os
        adapter_params = {
            'api_key' : os.getenv("COINGECKO_API")
        }

        load_params = {
            'load_type' : 'project',
            'metric_keys' : ['price', 'volume', 'market_cap'],
            'origin_keys' : None, # could also be a list
            'days' : 'auto', # auto, max, or a number (as string)
            'vs_currencies' : ['usd', 'eth']
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterCoingecko(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df) 
        
    @task()
    def run_usd_to_eth():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL

        adapter_params = {
        }
        load_params = {
            'load_type' : 'usd_to_eth',
            'days' : 3, ## days as int
            'origin_keys' : None, ## origin_keys as list or None
            'metric_keys' : ['fdv_usd'], ## metric_keys as list or None
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # # load
        ad.load(df)

    run_market_chart_hourly()
    
    daily_vals = run_market_chart_daily()
    conversion = run_usd_to_eth() 

    # Define execution order
    # daily_vals >> fdv >> conversion >> jsons
    daily_vals >> conversion
etl()
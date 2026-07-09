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
    dag_id='metrics_sql_hourly',
    description='Load blockspace and other SQL-based metrics on an hourly basis.',
    tags=['metrics', 'hourly'],
    start_date=datetime(2026,2,10),
    schedule='10 * * * *'
)

def etl():
    @task()
    def run_blockspace_hourly():
        from src.db_connector import DbConnector
        db_connector = DbConnector()

        from src.main_config import get_main_config
        main_config = get_main_config(db_connector)
        
        hours = 3

        for chain in main_config:
            if chain.runs_aggregate_blockspace and chain.origin_key not in ['starknet', 'polygon_pos', 'megaeth', 'ronin', 'robinhood']:
                print(f"Updating blockspace data for {chain.origin_key} and last {hours} hours...")
                db_connector.get_blockspace_contracts_hourly(chain.origin_key, hours)
                
    @task()
    def run_active_addresses_agg_hourly():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        
        ad = AdapterSQL({}, DbConnector())
        df = ad.extract({
            "load_type": "active_addresses_agg_hourly",
            "hours": 5,
            "origin_keys": None,
            "metric_keys": None,
        })
                
    @task()
    def run_fundamentals_hourly():
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        from src.adapters.adapter_sql import AdapterSQL

        adapter_params = {
        }
        load_params = {
            'load_type' : 'metrics_hourly', ## load metrics such as imx txcount, daa, fees paid and user_base metric
            'hours' : (3), ## hours as int
            'origin_keys' : None, ## origin_keys as list or None
            'metric_keys' : None,
            'currency_dependent' : False,
            'upsert' : True, ## upsert after each query run
        }

        # initialize adapter
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        ad.extract(load_params)
        
    @task()
    def run_fundamentals_hourly_dependent():
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        from src.adapters.adapter_sql import AdapterSQL

        adapter_params = {
        }
        load_params = {
            'load_type' : 'metrics_hourly', ## load metrics such as imx txcount, daa, fees paid and user_base metric
            'hours' : (3), ## hours as int
            'origin_keys' : None, ## origin_keys as list or None
            'metric_keys' : None,
            'currency_dependent' : True,
            'upsert' : True, ## upsert after each query run
        }

        # initialize adapter
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        ad.extract(load_params)
        
    @task()
    def run_eth_to_usd():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        
        ad = AdapterSQL({}, DbConnector())
        df = ad.extract({
            "load_type": "eth_to_usd_hourly",
            "days": 7,
            "origin_keys": None,
            "metric_keys": None,
        })
        ad.load(df)
        
    run_fundamentals_hourly_dependent() >> run_eth_to_usd()        
    run_blockspace_hourly()
    run_fundamentals_hourly()
    
    run_active_addresses_agg_hourly()

etl()

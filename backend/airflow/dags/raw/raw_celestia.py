from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='mseidl')
    },
    dag_id='raw_celestia',
    description='Load raw tx data from Celestia',
    tags=['raw', 'near-real-time', 'rpc'],
    start_date=datetime(2023, 9, 1),
    schedule='1/10 * * * *'
)
def adapter_rpc():
    @task(execution_timeout=timedelta(minutes=45))
    def run_celestia():
        from src.adapters.adapter_raw_celestia import AdapterCelestia
        from src.db_connector import DbConnector
        from src.adapters.rpc_funcs.utils import MaxWaitTimeExceededException, get_chain_config

        # Initialize DbConnector
        db_connector = DbConnector()
        
        chain_name = 'celestia'

        rpc_list, batch_size = get_chain_config(db_connector, chain_name)
        rpc_urls = [rpc['url'] for rpc in rpc_list]

        print(f"TIA_CONFIG={rpc_list}")

        adapter_params = {
            'rpc': 'local_node',
            'chain': chain_name,
            'rpc_list': rpc_urls,
        }

        # Initialize AdapterCelestia
        adapter = AdapterCelestia(adapter_params, db_connector)

        # Test run method parameters
        load_params = {
            'block_start': 'auto',
            'batch_size': batch_size,
        }

        try:
            adapter.extract_raw(load_params)
        except MaxWaitTimeExceededException as e:
            print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
            raise e

    run_celestia()

adapter_rpc()

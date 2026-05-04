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
    dag_id='backfill_celestia',
    description='Backfill potentially missing Celestia data',
    tags=['backfill', 'daily'],
    start_date=datetime(2023, 9, 1),
    schedule='30 09 * * *'
)

def backfill_tia():
    @task(execution_timeout=timedelta(minutes=30))
    def run_backfill_tia():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_raw_celestia import AdapterCelestia
        from src.adapters.rpc_funcs.utils import get_chain_config
        
        adapter_params = {
            'chain': 'celestia',
            'rpc_url': os.getenv("TIA_RPC"),
        }

        # Initialize DbConnector
        db_connector = DbConnector()

        chain_name = 'celestia'
        days_back = 5

        rpc_list, batch_size = get_chain_config(db_connector, chain_name)
        rpc_urls = [rpc['url'] for rpc in rpc_list]

        adapter_params = {
            'rpc': 'local_node',
            'chain': chain_name,
            'rpc_list': rpc_urls,
        }

        # Initialize AdapterCelestia
        adapter = AdapterCelestia(adapter_params, db_connector)
        
        table_name = adapter_params['chain'] + "_tx"
        end_block = db_connector.get_max_block(table_name)
        
        # Calculate the date seven days ago
        date = datetime.now() - timedelta(days=days_back)
        start_block = db_connector.get_block_by_date(table_name, date)
        
        # Call the backfill_missing_blocks method
        backfill_params = {
            'batch_size': batch_size,
        }

        try:
            adapter.backfill_missing_blocks(start_block, end_block, backfill_params['batch_size'])
            print("Backfilling completed successfully.")
        except Exception as e:
            print(f"Backfilling stopped due to an exception: {e}")
            raise e

    run_backfill_tia()
backfill_tia()
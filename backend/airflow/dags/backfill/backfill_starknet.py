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
    dag_id='backfill_starknet',
    description='Backfill potentially missing Starknet data',
    tags=['backfill', 'daily'],
    start_date=datetime(2023, 9, 1),
    schedule='30 09 * * *'
)
def backfill_strk():
    @task()
    def run_backfill_strk():
        from src.db_connector import DbConnector
        from src.adapters.adapter_raw_starknet import AdapterStarknet
        from src.adapters.rpc_funcs.utils import get_chain_config
        
        # Initialize DbConnector
        db_connector = DbConnector()

        chain_name = 'starknet'

        # Obtain RPC configurations and batch size
        active_rpc_configs, batch_size = get_chain_config(db_connector, chain_name)
        print(f"STARKNET_CONFIG={active_rpc_configs}")

        adapter_params = {
            'chain': chain_name,
            'rpc_configs': active_rpc_configs,
        }

        # Initialize AdapterStarknet
        adapter = AdapterStarknet(adapter_params, db_connector)

        table_name = adapter_params['chain'] + "_tx"
        end_block = db_connector.get_max_block(table_name)
        start_block = end_block - 25000  # Backfill 25,000 blocks (~7 days of data)

        try:
            adapter.backfill_missing_blocks(start_block, end_block, batch_size)
            print("Backfilling completed successfully.")
        except Exception as e:
            print(f"Backfilling stopped due to an exception: {e}")
            raise e

    run_backfill_strk()
backfill_strk()
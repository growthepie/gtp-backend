from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.main_config import get_main_config
## DAG Configuration Variables
# batch_size: Number of blocks to process in a single task run
# config: Environment variable containing the RPC node configuration
# backfiller_on: Whether the chain is backfiller_on and should be backfilled


main_conf = get_main_config()

chain_settings = {
    chain.origin_key: {
        'batch_size': chain.backfiller_batch_size
    }
    for chain in main_conf if chain.backfiller_on == True
}

@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'on_failure_callback': lambda context: alert_via_webhook(context, user='mseidl')
    },
    dag_id='backfill_rpc',
    description='DAG for backfilling missing blockchain data',
    tags=['backfill', 'daily'],
    start_date=datetime(2023, 12, 1),
    schedule='20 11 * * *'
)

def backfiller_dag():
    import sys
    from src.adapters.adapter_raw_rpc import NodeAdapter
    from src.adapters.rpc_funcs.utils import Web3CC, get_chain_config
    from src.adapters.rpc_funcs.funcs_backfill import date_to_unix_timestamp, find_first_block_of_day, find_last_block_of_day
    

    for chain, settings in chain_settings.items():
        if chain == 'polygon_zkevm':
            continue  # skip zkevm as it contains many empty blocks and is slow to backfill
        
        @task(task_id=f'new_backfill_{chain}', execution_timeout=timedelta(minutes=90))
        def run_backfill_task(chain_name, backfill_start_date, backfill_end_date, batch_size):
            db_connector = DbConnector()
            active_rpc_configs, batch_size = get_chain_config(db_connector, chain_name)
            w3 = None

            for rpc_config in active_rpc_configs:
                try:
                    w3 = Web3CC(rpc_config)
                    break
                except Exception as e:
                    print(f"Failed to connect to RPC URL: {rpc_config['url']} with error: {e}")

            if not w3:
                raise ConnectionError("Failed to connect to any provided RPC node.")

            start_date_obj = datetime.strptime(backfill_start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(backfill_end_date, "%Y-%m-%d")
            start_timestamp = date_to_unix_timestamp(start_date_obj.year, start_date_obj.month, start_date_obj.day)
            end_timestamp = date_to_unix_timestamp(end_date_obj.year, end_date_obj.month, end_date_obj.day)
            start_block = find_first_block_of_day(w3, start_timestamp)
            end_block = find_last_block_of_day(w3, end_timestamp)

            adapter_params = {'chain': chain_name, 'rpc_configs': active_rpc_configs}
            node_adapter = NodeAdapter(adapter_params, db_connector)

            try:
                node_adapter.backfill_missing_blocks(start_block, end_block, batch_size)
                print("Backfill process completed successfully.")
            except Exception as e:
                print(f"Backfiller: An error occurred: {e}", file=sys.stderr)
                raise

        batch_size = settings['batch_size']

        start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        run_backfill_task(chain_name=chain, backfill_start_date=start_date, backfill_end_date=end_date, batch_size=batch_size)

backfiller_dag_instance = backfiller_dag()

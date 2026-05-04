import time
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
    dag_id='raw_loopring',
    description='Load raw tx data from Loopring',
    tags=['raw', 'near-real-time'],
    start_date=datetime(2023, 9, 1),
    schedule='4/10 * * * *'
)

def adapter_loopring_api():
    @task(execution_timeout=timedelta(minutes=45))
    def run_loopring():
        import os
        from src.adapters.adapter_raw_loopring import AdapterLoopring
        from src.adapters.rpc_funcs.utils import MaxWaitTimeExceededException
        from src.db_connector import DbConnector

        adapter_params = {
            'chain': 'loopring',
            'api_url': "https://api3.loopring.io/api/v3",
        }

        # Initialize DbConnector
        db_connector = DbConnector()

        # Initialize NodeAdapter
        adapter = AdapterLoopring(adapter_params, db_connector)

        # Initial load parameters
        load_params = {
            'block_start': 'auto',
            'batch_size': 5,
            'threads': 5,
        }

        while load_params['threads'] > 0:
            try:
                adapter.extract_raw(load_params)
                break  # Break out of the loop on successful execution
            except MaxWaitTimeExceededException as e:
                print(str(e))
                
                # Reduce threads if possible, stop if it reaches 1
                if load_params['threads'] > 1:
                    load_params['threads'] -= 1
                    print(f"Reducing threads to {load_params['threads']} and retrying.")
                else:
                    print("Reached minimum thread count (1)")
                    raise e 

                # Wait for 5 minutes before retrying
                time.sleep(300)

    run_loopring()
adapter_loopring_api()
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from src.misc.airflow_utils import alert_via_webhook


@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        'email_on_failure': False,
        'on_failure_callback': alert_via_webhook
    },
    dag_id='utility_rpc_sync_check',
    description='DAG to check if chain nodes are synchronized',
    tags=['utility', 'hourly',],
    start_date=datetime(2023, 12, 1),
    schedule='35 * * * *'
)

def blockchain_sync_dag():
    @task
    def sync_checker():
        from src.adapters.rpc_funcs.rpc_sync_checker import sync_check
        sync_check()

    sync_checker()

sync_dag_instance = blockchain_sync_dag()
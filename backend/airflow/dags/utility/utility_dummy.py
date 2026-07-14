from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(seconds=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='utility_dummy',
    description='This is a dummy DAG that is supposed to fail.',
    tags=['utility'],
    start_date=datetime(2023,4,24),
    schedule='*/15 * * * *'
)

def etl():
    @task()
    def run_dummy_task():
        import getpass
        import os

        print("User:", getpass.getuser())
        print("UID:", os.getuid())
        print("CWD:", os.getcwd())
        raise Exception("This is a dummy task that is supposed to fail.")
    
    run_dummy_task()
etl()

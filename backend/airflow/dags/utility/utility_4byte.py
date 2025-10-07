from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")
from airflow.decorators import dag, task 

from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 5,
        'email_on_failure': False,
        'retry_delay' : timedelta(seconds=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='utility_4byte',
    description='This DAG create a 4byte parquet export from relevant smart contract function signatures for our UserOp script.',
    tags=['utility'],
    start_date=datetime(2025,9,16),
    schedule_interval='11 01 * * 0'  # Run weekly on Sunday at 01:05 AM
)

def etl():
    @task()
    def run_4byte_export():
        from src.adapters.adapter_4bytes import Adapter4Bytes
        import os

        adapter = Adapter4Bytes()

        adapter.extract({
            'save_path': 'backend/', # save path of four_byte_lookup.pkl & 4bytes.parquet file inside ec2 instance
            'provider': 'sourcify' # options: "sourcify" or "verifieralliance"
        })

        adapter.load({
            's3_path_parquet': 'v1/export/4bytes.parquet', # save path inside S3 bucket
            's3_path_lookup': 'v1/export/four_byte_lookup.pkl', # save path inside S3 bucket
            'S3_CF_BUCKET': os.getenv("S3_CF_BUCKET"),
            'CF_DISTRIBUTION_ID': os.getenv("CF_DISTRIBUTION_ID")
        })        

    run_4byte_export()
etl()
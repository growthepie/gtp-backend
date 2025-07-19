import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 1,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='metrics_eigenda',
    description='Load data from EigenDA API.',
    tags=['EigenDA', 'fact_kpi'],
    start_date=datetime(2024, 7, 22),
    schedule='30 3 * * *'  # Run daily at 3:30 AM. Needs to be before metrics_sql dag
)
def run_dag():
    @task()
    def run_eigendata_extract_load():
        from src.adapters.adapter_eigenDA import AdapterEigenDA
        from src.db_connector import DbConnector

        # Initialize the adapter
        db_connector = DbConnector()
        adapter_params = {}
        eigen = AdapterEigenDA(adapter_params, db_connector)

        load_params = {
            'days': 7,  # Look back 7 days
            'endpoint': 'https://eigenda-mainnet-ethereum-blobmetadata-usage.s3.amazonaws.com/stats',
            'table': 'fact_kpis'  # Example table name
        }
        
        df = eigen.extract(load_params)
        df = df.set_index(['date', 'origin_key', 'metric_key'])
        eigen.load(df)

        # How to find out new namespaces
        # df = eigen.call_api_endpoint()
        # df.groupby(['account_name']).sum().reset_index()

    run_eigendata_extract_load()

run_dag()
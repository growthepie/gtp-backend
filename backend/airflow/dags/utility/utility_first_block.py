from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(seconds=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='utility_first_block',
    description='Pulls in the first_block_of_day metric into fact_kpis.',
    tags=['utility'],
    start_date=datetime(2026,2,12),
    schedule='5 0 * * *'  # Run daily at 00:05 AM UTC
)

def etl():

    @task()
    def get_first_blocks():
        from src.adapters.adapter_first_block_of_day import AdapterFirstBlockOfDay
        from src.db_connector import DbConnector

        db_connector = DbConnector()
        ad = AdapterFirstBlockOfDay({}, db_connector)
        df = ad.extract({
            'origin_keys': ['*']
        })
        ad.load(df)

    get_first_blocks()

etl()
import getpass
sys_user = getpass.getuser()

from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_og_images',
    description='Create and store og images',
    tags=['api', 'daily'],
    start_date = datetime(2023,4,24),
    schedule='0 7 * * SUN'
)

def etl():
    @task()
    def run_og_images():
        import os
        from src.api.screenshots_to_s3 import run_template_generation
        
        run_template_generation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), 'v1', sys_user)
    
    run_og_images()
etl()
import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='other_octant',
    description='Load data from Octant API for tracker.',
    tags=['other', 'near-real-time'],
    start_date=datetime(2024,7,22),
    schedule='*/05 * * * *'
)

def run_dag():
    @task()
    def run_octant_v2():      
        import os
        import dotenv
        from src.db_connector import DbConnector
        from src.misc.octant_v2 import OctantV2

        dotenv.load_dotenv()
        api_version = "v1"
        db_connector = DbConnector(db_name="fun")

        octantv2 = OctantV2(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        #octantv2.run_load_octant_data_for_all_epochs()
        print('### LOAD DATA FOR LATEST EPOCH ###')
        octantv2.run_load_epoch_data(8)
        
        print('### CREATE ALL OCTANT JSONS ###')
        octantv2.run_create_all_octant_jsons()


    run_octant_v2()
run_dag()
import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=1),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_oss_directory',
    description='Loads project data from the OSS Directory API',
    tags=['oli', 'daily'],
    start_date=datetime(2025,7,25),
    schedule='*/5 * * * *'  # Run every 5 minutes
)

def etl():
    @task()
    def update_OSS():

        import requests
        from datetime import datetime, timezone

        ### Check if there's been a commit in the last 5 minutes to the OSS directory
        url = 'https://api.github.com/repos/opensource-observer/oss-directory/commits'
        params = {'path': 'data/projects', 'per_page': 1}
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        latest_commit = response.json()[0]
        commit_time = datetime.fromisoformat(latest_commit['commit']['committer']['date'].replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        delta = now - commit_time


        ### If the latest commit was within the last 5 minutes (300s), proceed with the update
        if delta.total_seconds() < 301:  

            import os
            from src.adapters.adapter_oso import AdapterOSO
            from src.api.json_creation import JSONCreation
            from src.db_connector import DbConnector
            db_connector = DbConnector()

            ### Load new data from the OSS Directory API
            adapter_params = {
                'webhook' : os.getenv('DISCORD_CONTRACTS')
            }
            load_params = {}

            # initialize adapter
            ad = AdapterOSO(adapter_params, db_connector)
            # extract
            df = ad.extract(load_params)
            # load
            ad.load(df)

            ### Recreate JSON files
            api_version = "v1"
            json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
            json_creator.create_projects_json()
    
    update_OSS()

etl()
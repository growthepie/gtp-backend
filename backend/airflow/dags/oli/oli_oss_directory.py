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
    schedule='*/2 * * * *'  # Run every 2 minutes, make sure to also change line 46!
)

def etl():
    @task()
    def update_OSS():

        import requests
        from datetime import datetime, timezone

        ### Check if there's been a commit in the last x minutes to the OSS directory
        url = 'https://api.github.com/repos/opensource-observer/oss-directory/commits'
        params = {'path': 'data/projects', 'per_page': 1}
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        latest_commit = response.json()[0]
        commit_time = datetime.fromisoformat(latest_commit['commit']['committer']['date'].replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        delta = now - commit_time

        print(f"Time since last commit: {delta.total_seconds()} seconds")

        ### If the latest commit was within the last 2 minutes (60s * 2 + 10s), proceed with the update
        if delta.total_seconds() < 180: # takes around 1 minute to propagate changes  
            print("New commit detected in the last 2 minutes. Proceeding with data update...")
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


            ### Update Airtable projects (writes OSS projects from the DB to Airtable)
            from pyairtable import Api
            import src.misc.airtable_functions as at

            # initialize Airtable instance
            AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
            AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
            api = Api(AIRTABLE_API_KEY)
            table = api.table(AIRTABLE_BASE_ID, 'OSS Projects')

            # get active projects from db
            df = db_connector.get_projects_for_airtable()

            # merge current table with the new data
            table = api.table(AIRTABLE_BASE_ID, 'OSS Projects')
            df_air = at.read_airtable(table)[['Name', 'id']]
            df = df.merge(df_air, how='left', left_on='Name', right_on='Name')

            # update existing records (primary key is the id)
            at.update_airtable(table, df)

            # add any new records/chains if present
            df_new = df[df['id'].isnull()]
            if df_new.empty == False:
                at.push_to_airtable(table, df_new.drop(columns=['id']))

            # remove old records
            mask = ~df_air['Name'].isin(df['Name'])
            df_remove = df_air[mask]
            if df_remove.empty == False:
                at.delete_airtable_ids(table, df_remove['id'].tolist())
    
    update_OSS()

etl()
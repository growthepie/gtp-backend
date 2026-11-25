from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=1),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_master_json_creation',
    description='Create master json file.',
    tags=['api', 'daily'],
    start_date=datetime(2023,4,24),
    schedule=None,
    params={"api_version": "v1"}
)

def json_creation():
    @task()
    def run_create_master(**kwargs):
        import os
        import pickle
        import pandas as pd
        from src.main_config import get_main_config
        from src.da_config import get_da_config
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation
        from src.misc.helper_functions import upload_file_to_s3

        # Get the api_version from DAG parameters
        api_version = kwargs['params'].get('api_version', 'v1')
        db_connector = DbConnector()

        ## Create new main_conf pickle in S3
        main_conf = get_main_config(db_connector=db_connector, source='github')
        with open("main_conf.pkl", "wb") as file:
            pickle.dump(main_conf, file)
        upload_file_to_s3(os.getenv("S3_CF_BUCKET"), f"{api_version}/main_conf.pkl", "main_conf.pkl", os.getenv("CF_DISTRIBUTION_ID"))
        from src.misc.conf_cache import prime_conf_cache
        prime_conf_cache("main_conf.pkl", "main_conf.pkl", api_version)  # local conf cache gets the new pickle right away

        ## Update sys_main_conf table in database
        df_main_conf = pd.DataFrame([vars(project) for project in main_conf])
        df_main_conf.set_index(['origin_key'], inplace=True)
        db_connector.upsert_table('sys_main_conf', df_main_conf)
        
        ## Create new DA conf pickle in S3
        da_conf = get_da_config(source='github')
        with open("da_conf.pkl", "wb") as file:
            pickle.dump(da_conf, file)
        upload_file_to_s3(os.getenv("S3_CF_BUCKET"), f"{api_version}/da_conf.pkl", "da_conf.pkl", os.getenv("CF_DISTRIBUTION_ID"))
        from src.misc.conf_cache import prime_conf_cache
        prime_conf_cache("da_conf.pkl", "da_conf.pkl", api_version)  # local conf cache gets the new pickle right away

        ## Create master json
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        df = json_creator.get_all_data()
        json_creator.create_master_json(df)

    # Main
    run_create_master()    
   
json_creation()
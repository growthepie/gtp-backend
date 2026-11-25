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
    dag_id='api_conf_pickles',
    description='Create main and DA conf pickle file.',
    tags=['api', 'daily'],
    start_date=datetime(2024,12,17),
    schedule='30 00 * * *'
)

def json_creation():
    @task()
    def run_create_main_conf():
        import dotenv
        dotenv.load_dotenv()

        import os
        import pickle
        import pandas as pd
        from src.main_config import get_main_config
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_file_to_s3

        db_connector = DbConnector()
        api_version = "v1"

        ## Upload new main_conf to S3
        main_conf = get_main_config(db_connector=db_connector, source='github')
        with open("main_conf.pkl", "wb") as file:
            pickle.dump(main_conf, file)

        upload_file_to_s3(os.getenv("S3_CF_BUCKET"), f"{api_version}/main_conf.pkl", "main_conf.pkl", os.getenv("CF_DISTRIBUTION_ID"))
        from src.misc.conf_cache import prime_conf_cache
        prime_conf_cache("main_conf.pkl", "main_conf.pkl", api_version)  # local conf cache gets the new pickle right away
        print("main_conf.pkl uploaded to S3")

        df_main_conf = pd.DataFrame([vars(project) for project in main_conf])
        df_main_conf.set_index(['origin_key'], inplace=True)
        db_connector.upsert_table('sys_main_conf', df_main_conf)
        print(f"sys_main_conf table updated in database: rows {len(df_main_conf)}")

    @task()
    def run_create_da_conf():
        import dotenv
        dotenv.load_dotenv()
        
        import os
        import pickle
        from src.da_config import get_da_config
        from src.misc.helper_functions import upload_file_to_s3

        api_version = "v1"

        ## Upload new main_conf to S3
        da_conf = get_da_config(source='github')
        with open("da_conf.pkl", "wb") as file:
            pickle.dump(da_conf, file)

        upload_file_to_s3(os.getenv("S3_CF_BUCKET"), f"{api_version}/da_conf.pkl", "da_conf.pkl", os.getenv("CF_DISTRIBUTION_ID"))
        from src.misc.conf_cache import prime_conf_cache
        prime_conf_cache("da_conf.pkl", "da_conf.pkl", api_version)  # local conf cache gets the new pickle right away

    # Main
    run_create_main_conf()    
    run_create_da_conf()
   
json_creation()
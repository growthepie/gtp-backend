from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(seconds=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='mseidl')
    },
    dag_id='utility_app_logos',
    description='This DAG is supposed to download app logos from the github repo and upload them to s3.',
    tags=['utility'],
    start_date=datetime(2023,4,24),
    schedule='05 01 * * *'
)

def etl():
    @task()
    def run_app_logos():
        from github import Github
        import os
        from src.misc.helper_functions import get_app_logo_files, upload_app_logo_files_to_s3, get_current_file_list
        import dotenv
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation

        dotenv.load_dotenv()
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")
        cf_bucket_name = os.getenv("S3_CF_BUCKET")
        github_token = os.getenv("GITHUB_TOKEN")

        days = 3 # check if anything changed in the last d*24 hours
        repo_name = f"opensource-observer/oss-directory"
        file_path = "data/logos/"
        g = Github(github_token)
        repo = g.get_repo(repo_name)
        db_connector = DbConnector()

        ## retrieve the latest files (new files and modified files) from the github repo
        files_to_upload = get_app_logo_files(repo, file_path, days)[0]
        if len(files_to_upload) > 0:
            print(f"Found {len(files_to_upload)} files to upload")
            ## upload the files to s3
            upload_app_logo_files_to_s3(repo, files_to_upload, cf_bucket_name, cf_distribution_id)
            ## update the db with the latest file list (oli_oss_directory logo_path column)
            df = get_current_file_list(repo_name, file_path, github_token)
            db_connector.upsert_table('oli_oss_directory', df)
            ## create new project.json file
            json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector)
            json_creator.create_projects_json()
            json_creator.create_projects_filtered_json()
    
    run_app_logos()
etl()
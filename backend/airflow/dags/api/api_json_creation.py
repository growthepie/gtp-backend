from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

import os

api_version = "v1"

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=1),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_json_creation',
    description='Create json files that are necessary to power the frontend.',
    tags=['api', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='30 05 * * *'
)

def etl():
    @task()
    def run_create_various():
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation
        from src.misc.helper_functions import send_discord_message
        
        db_connector = DbConnector()
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        df = json_creator.get_all_data()
        
        try:
            json_creator.create_master_json(df)
            json_creator.create_master_json(df, 'zircuit')  # private version for zircuit
        except Exception as e:
            print(f"Error creating master JSON: {e}")
            send_discord_message(f"Error creating master JSON: {e}")

        try:
            json_creator.create_landingpage_json(df)
        except Exception as e:
            print(f"Error creating landing page JSON: {e}")
            send_discord_message(f"Error creating landing page JSON: {e}")
        
        try:
            json_creator.create_economics_json(df)
        except Exception as e:
            print(f"Error creating economics JSON: {e}")
            send_discord_message(f"Error creating economics JSON: {e}")
        
        try:
            json_creator.create_da_overview_json(df)
            json_creator.create_da_timeseries_json()
        except Exception as e:
            print(f"Error creating data availability JSONs: {e}")
            send_discord_message(f"Error creating data availability JSONs: {e}")
            
        try:
            json_creator.create_fundamentals_json(df)
            json_creator.create_fundamentals_internal_json(df)
            json_creator.create_metrics_export_json(df)
            json_creator.create_custom_metrics_json(['contract_deployment_count'])
            json_creator.create_da_fundamentals_json()
            json_creator.run_top_contracts_jsons()
        except Exception as e:
            print(f"Error creating fundamentals JSONs: {e}")
            send_discord_message(f"Error creating fundamentals JSONs: {e}")
        
    @task()
    def run_create_eco_overview():
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation
        db_connector = DbConnector()
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        json_creator.create_eco_overview_json()

    @task()
    def run_create_app_level_jsons():
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation
        db_connector = DbConnector()
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        json_creator.create_app_overview_json()
        json_creator.run_app_details_jsons_all()
        json_creator.create_projects_filtered_json()
        json_creator.clean_app_files()

    @task()
    def run_create_labels():
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation
        db_connector = DbConnector()
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        json_creator.create_labels_json('full')
        json_creator.create_labels_json('quick')
        json_creator.create_labels_sparkline_json()
        json_creator.create_projects_json() # also lives in backend/airflow/dags/oli/oli_oss_directory.py

        json_creator.create_export_labels_parquet('top50k')

    @task()
    def run_oli_s3_export():
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation
        db_connector = DbConnector()
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        #json_creator.create_export_oli_parquet()
        json_creator.create_export_project_labels_parquet()

    @task()
    def run_create_blockspace_overview():
        from src.db_connector import DbConnector
        from src.api.blockspace_json_creation import BlockspaceJSONCreation
        db_connector = DbConnector()
        blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        blockspace_json_creator.create_blockspace_overview_json()

    @task()
    def run_create_blockspace_category_comparison():
        from src.db_connector import DbConnector
        from src.api.blockspace_json_creation import BlockspaceJSONCreation
        db_connector = DbConnector()
        blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        blockspace_json_creator.create_blockspace_comparison_json()    

    @task()
    def run_create_chain_blockspace():
        from src.db_connector import DbConnector
        from src.api.blockspace_json_creation import BlockspaceJSONCreation
        db_connector = DbConnector()
        blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        blockspace_json_creator.create_blockspace_single_chain_json()

    @task()
    def run_create_eim():
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation
        db_connector = DbConnector()
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        df = json_creator.get_data_eim()

        json_creator.create_eth_exported_json(df)
        json_creator.create_eth_supply_json(df)
        json_creator.create_eth_holders_json()

    ## Main 
    run_create_various()

    ## Eco Overview
    run_create_eco_overview()

    ## Blockspace
    run_create_blockspace_overview()
    run_create_blockspace_category_comparison()
    run_create_chain_blockspace()

    ## App Level
    run_create_app_level_jsons()

    ## Labels
    run_create_labels()

    ## OLI
    run_oli_s3_export()

    ## Misc
    run_create_eim()
etl()
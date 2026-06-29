from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook    

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(seconds=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='utility_raw_data_check',
    description='This DAG checks if our raw data load is behind',
    tags=['utility'],
    start_date=datetime(2025,8,14),
    schedule='*/30 * * * *'
)

def check():
    @task()
    def run_check():
        from src.db_connector import DbConnector
        from src.main_config import get_main_config
        from src.misc.helper_functions import send_discord_message
        from datetime import datetime
        from zoneinfo import ZoneInfo

        main_config = get_main_config()
        db_connector = DbConnector()

        for chain in main_config:
            if chain.api_deployment_flag != 'PROD' or chain.origin_key in ['loopring', 'megaeth', 'polygon_pos', 'ronin', 'taiko']: ## TODO: remove Taiko once it is back online
                continue
            
            test_time = 60
            
            print(f"Processing chain: {chain.origin_key}")
            max_val = db_connector.get_max_value(f'{chain.origin_key}_tx', 'block_timestamp')
            
            # Ensure max_val is timezone aware in UTC
            max_val = max_val.replace(tzinfo=ZoneInfo("UTC"))

            # Get current UTC time as aware datetime
            current_time = datetime.now(ZoneInfo("UTC"))

            # Calculate diff in minutes
            time_diff = (current_time - max_val).total_seconds() / 60
            time_diff = round(time_diff,2)
            
            print(f"- Time difference to current UTC: {time_diff} minutes")
            if time_diff > test_time:
                print(f"ISSSUE: {chain.origin_key} is {time_diff} minutes behind")
                send_discord_message(f"RAW DATA SYNC ISSUE: {chain.origin_key} is {time_diff} minutes behind.")
    
    run_check()
check()
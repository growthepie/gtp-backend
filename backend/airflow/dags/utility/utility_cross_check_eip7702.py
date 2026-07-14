from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 0,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='utility_cross_check_eip7702',
    description='Cross-check EIP-7702 authorization_list for chains from Dune to our db',
    tags=['utility'],
    start_date=datetime(2025,9,16),
    schedule='22 02 * * 0'  # Run weekly on Sunday at 02:22 AM
)

def cross_check_data():
    @task(execution_timeout=timedelta(minutes=60))
    def run_cross_check():
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune
        from src.misc.helper_functions import send_discord_message
        import pandas as pd
        import os

        db = DbConnector()
        dune = AdapterDune({'api_key': os.getenv("DUNE_API")}, db)

        query = """
                SELECT
                    origin_key,
                    block_date,
                    count(*)
                FROM public.authorizations_7702
                WHERE block_date < CURRENT_DATE
                GROUP BY origin_key, block_date
                ORDER BY 2 DESC
        """
        df_db = db.execute_query(query, load_df=True)

        query = {
            'queries': [
                {
                    'name': 'EIP-7702 Authorization Count',
                    'query_id': 5786848
                }
            ]
        }
        df_dune = dune.extract(query)

        # Convert block_date to datetime in both dataframes
        df_db['block_date'] = pd.to_datetime(df_db['block_date'])
        df_dune['block_date'] = pd.to_datetime(df_dune['block_date'])

        # Left join df_dune onto df_db
        df = df_db.merge(df_dune, on=['origin_key', 'block_date'], how='left')
        # get the difference between count and total_auth_elements
        df['diff'] = df['count'] - df['total_auth_elements']
        # remove everything where diff is 0 or NaN
        df = df[df['diff'] != 0]
        df = df.dropna(subset=['diff'])
        # rename count to count_db and total_auth_elements to count_dune
        df = df.rename(columns={'count': 'count_db', 'total_auth_elements': 'count_dune'})

        # sort by diff column descending
        df = df.sort_values(by='diff', ascending=False)
        
        print(f"Found {df.shape[0]} discrepancies between Dune and DB")

        if len(df) > 0:
            for i, row in df.iterrows():
                print(abs(row['diff']))
                if abs(row['diff']) > 10:
                    message = f"""
                    ⚠️ **Discrepancy detected in EIP-7702 Authorization List for {row['origin_key']} on {row['block_date'].date()}**
                    
                    There is a discrepancy between our database and Dune Analytics regarding the number of authorization elements.

                    - Our Database Count: {row['count_db']}
                    - Dune Analytics Count: {row['count_dune']}
                    - Difference: {int(abs(row['diff']))}

                    """
                    message = message.replace("            ", "")
                    send_discord_message(message, os.getenv('DISCORD_ALERTS'))



    run_cross_check()
cross_check_data()
    
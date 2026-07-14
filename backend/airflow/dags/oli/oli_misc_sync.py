from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Define the DAG and task using decorators
@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 5,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='oli_misc_sync',
    description='Sync various OLI related data such as categories to our web3 db.',
    tags=['oli'],
    start_date=datetime(2025, 12, 9),
    schedule='45 0 * * *',  # Runs daily at 0:45 AM
    catchup=False  # Prevents backfilling
)

def main():

    @task()
    def sync_tags_from_OLI_github():
        """
        This task adds new oli tags to oli_tags table, only addative, doesn't remove tags!
        """
        from src.db_connector import DbConnector
        from src.misc.helper_functions import get_all_oli_tags_from_github
        from src.misc.helper_functions import send_discord_message
        db_connector = DbConnector()

        # get tags from gtp-dna Github
        df = get_all_oli_tags_from_github()

        # get tags from oli_tags table to compare
        df_existing = db_connector.get_table('oli_tags')

        # upsert/update all oli_tags table
        df = df.set_index('tag_id')
        db_connector.upsert_table('oli_tags', df, if_exists='update')

        # send discord message for new tags
        df_new = df[~df.index.isin(df_existing['tag_id'])]
        for row in df_new.itertuples():
            import os
            send_discord_message(f'New OLI tag found and synced to oli_tags table: {row.name}', os.getenv('DISCORD_CONTRACTS'))
    
    @task()
    def sync_categories_from_OLI_github():
        """
        This task adds new oli categories to oli_categories table, only addative, doesn't remove categories!
        """
        from src.db_connector import DbConnector
        from src.misc.helper_functions import get_all_oli_categories_from_github
        from src.misc.helper_functions import send_discord_message
        import pandas as pd
        import os

        db_connector = DbConnector()

        # get tags from gtp-dna Github
        df = get_all_oli_categories_from_github()

        # drop the column examples
        df = df.drop(columns=['examples'])
        df = df.set_index('category_id')

        # get tags from oli_categories table to compare
        df_existing = db_connector.get_table('oli_categories') # we have 4 more in our db than on Github: 'unlabeled', 'total_usage', 'contract_deployment', 'native_transfer'

        # find new tags - add .copy() to avoid SettingWithCopyWarning
        df_new = df[~df.index.isin(df_existing['category_id'])].copy()
        df_new['main_category_id'] = 'unlabeled' # temporarily set all new categories to 'unlabeled' main category

        # upsert/update all oli_categories table
        db_connector.upsert_table('oli_categories', df_new)

        # send discord message for new categories
        for row in df_new.itertuples():
            send_discord_message(f'New OLI usage_category found and synced to oli_categories table: `{row.name}`. Please assign a main category to this new usage_category in the database under the table oli_categories!', os.getenv('DISCORD_CONTRACTS'))


    @task()
    def sync_oli_trust_table():
        """
        This task syncs the OLI trust table (based on the source node of OLI_gtp_pk = "0xA725646c05e6Bb813d98C5aBB4E72DF4bcF00B56") from OLI Label Pool to our web3 database.
        To change the list of trusted entities of us, send PR here: https://github.com/growthepie/gtp-dna/blob/main/oli/trusted_entities_v2.yml
        """
        from oli import OLI
        import os
        import pandas as pd
        from src.db_connector import DbConnector

        oli = OLI(private_key=os.getenv("OLI_gtp_pk"), api_key=os.getenv("OLI_API_KEY"))
        df = pd.DataFrame([
            {'address': k[0], 'tag_id': k[1], 'chain_id': k[2], 'confidence': v}
            for k, v in oli.trust.trust_table.items()
        ])
        df['updated_at'] = pd.Timestamp.now()
        df = df.set_index(['address', 'tag_id', 'chain_id'])
        db_connector = DbConnector()
        db_connector.upsert_table('oli_trust_table', df)

    tags = sync_tags_from_OLI_github()
    categories = sync_categories_from_OLI_github()
    trust_table = sync_oli_trust_table()

    tags >> categories >> trust_table 

main()

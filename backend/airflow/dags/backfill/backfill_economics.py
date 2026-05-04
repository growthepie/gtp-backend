from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='martin')
    },
    dag_id='backfill_economics',
    description='Backfill economics and da values for new entries or changes in gtp_dna economics mapping',
    tags=['backfill', 'daily'],
    start_date=datetime(2023, 9, 1),
    schedule='30 09 * * *'
)

def main():

    @task()
    def check_and_backfill():
        from src.misc.helper_functions import convert_economics_mapping_into_df
        from github import Github
        from datetime import datetime, timedelta, timezone
        import pandas as pd
        import yaml
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        # check if a new commit for the file economics_mapping.yml was made in the last 24 hours
        d = 1 # number of days to look back for commits
        repo_name = "growthepie/gtp-dna"
        file_path = "economics_da/economics_mapping.yml"
        branch = "main"
        g = Github()
        repo = g.get_repo(repo_name)

        commits = repo.get_commits(path=file_path, sha=branch)

        if commits[0].commit.author.date < datetime.now(timezone.utc) - timedelta(days=d):
            print(f"No new commit found in the last {24*d} hours.")
            return

        print(f"New commit found in the last {24*d} hours.")

        commit_24h_ago = None
        for commit in commits:
            if commit.commit.author.date < datetime.now(timezone.utc) - timedelta(days=d):
                commit_24h_ago = commit
                break

        f_now = repo.get_contents(file_path, ref=branch).decoded_content.decode()
        f_24h_ago = repo.get_contents(file_path, ref=commit_24h_ago.sha).decoded_content.decode()
        df_now = convert_economics_mapping_into_df(yaml.safe_load(f_now))
        df_24h_ago = convert_economics_mapping_into_df(yaml.safe_load(f_24h_ago))

        new_rows = df_now.merge(df_24h_ago, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)
        depreciated_rows = df_24h_ago.merge(df_now, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)
        df = pd.concat([new_rows, depreciated_rows])

        if df.empty:
            print("No new or depreciated rows in the economics mapping file.")
            return

        print("There are new or depreciated rows in the economics mapping file.")

        df = df.groupby(['name', 'origin_key', 'da_layer']).size().reset_index(name='count')
        print(df)

        db_connector = DbConnector()
        adapter_params = {
            'api_key': os.getenv("DUNE_API")
        }
        print("Initializing AdapterDune...")
        ad = AdapterDune(adapter_params, db_connector)
        print("AdapterDune initialized.")

        for _, row in df.iterrows():

            if row['da_layer'] == 'l1':
                print(f"Backfilling {row['name']} for Ethereum L1")
                load_params = {
                    'queries': [
                        {
                            'name': 'l1-values-per-chain-chain-filter',
                            'query_id': 4561949, # BACKFILLS ONLY FROM 2024-01-01 ONWARDS, CHANGE DUNE HERE LINE 15: https://dune.com/queries/4561893
                            'params': {
                                'chain': row['origin_key'],
                            }
                        }
                    ],
                    'prepare_df': 'prepare_df_metric_daily',
                    'load_type': 'fact_kpis'
                }
                df_dune = ad.extract(load_params)
                ad.load(df_dune)
                # ... TODO recalculating metrics in fact_kpis

            elif row['da_layer'] == 'beacon':
                print(f"Backfilling {row['name']} for Ethereum Beacon Chain")
                load_params = {
                    'queries': [
                        {
                            'name': 'beacon-values-per-chain-chain-filter',
                            'query_id': 4561837,
                            'params': {
                                'chain': row['origin_key'],
                            }
                        }
                    ],
                    'prepare_df': 'prepare_df_metric_daily',
                    'load_type': 'fact_kpis'
                }
                df_dune = ad.extract(load_params)
                ad.load(df_dune)
                # ... TODO recalculating metrics in fact_kpis

            elif row['da_layer'] == 'celestia':
                print(f"Backfilling {row['name']} for Celestia")
                # ... TODO recalculating raw metric in fact_kpis for celestia
                # ... TODO recalculating metrics in fact_kpis

            print(f"Completed backfilling: {row['name']}.")

    check_and_backfill()

main()
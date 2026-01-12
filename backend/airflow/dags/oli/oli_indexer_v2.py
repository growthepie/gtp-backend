from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Define the DAG and task using decorators
@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_indexer',
    description='Indexes the onchain OLI Label Pool v2. This includes attestations of labels (v2) and trust_lists (v1), aswell as revocations (v.any).',
    tags=['indexing', 'oli', 'raw'],
    start_date=datetime(2026, 1, 8),
    schedule='*/30 * * * *',  # Runs every 30 minutes
    catchup=False  # Prevents backfilling
)

def main():

    @task()
    def sync():

        from src.adapters.adapter_oli_onchain import AdapterOLIOnchain # onchain attestations & revocations
        from src.adapters.adapter_oli_offchain import AdapterOLIOffchain # onchain revocations of offchain attestations
        from src.db_connector import DbConnector
        import pandas as pd

        adapter_params = {'rpc_url': 'https://base.gateway.tenderly.co'}
        db_connector = DbConnector(db_name='oli')
        ad_onchain = AdapterOLIOnchain(adapter_params, db_connector)
        ad_offchain = AdapterOLIOffchain(adapter_params, db_connector)

        ## tracking time
        import time
        start_time = time.time()

        while True:

            ## Onchain, attestations of labels & revocations of labels ##
            extract_params_onchain = {
                'from_block': 'last_run_block',  # add last run block
                'to_block': 'latest',
                'chunk_size': 100000
            }

            attest_topics = [
                '0x8bf46bf4cfd674fa735a3d63ec1c9ad4153f033c290341f3a588b75685141b35', # Attest topic
                None,
                None,
                '0xcff83309b59685fdae9dad7c63d969150676d51d8eeda66799d1c4898b84556a' # Schema topic
            ]
            revoke_topics = [
                '0xf930a6e2523c9cc298691873087a740550b8fc85a0680830414c148ed927f615', # Revoke topic of onchain attestations
                None,
                None,
                '0xcff83309b59685fdae9dad7c63d969150676d51d8eeda66799d1c4898b84556a' # Schema topic
            ]

            # extract onchain attestations
            extract_params_onchain['topics'] = attest_topics
            df_attest = ad_onchain.extract(extract_params_onchain)

            # extract onchain revocations of onchain attestations
            extract_params_onchain['topics'] = revoke_topics
            df_revokes = ad_onchain.extract(extract_params_onchain)

            # merge df_attest & df_revokes, if there are duplicate uid, then keep the one in df_revokes (overwrites tx_id)
            df = pd.concat([df_revokes, df_attest]).drop_duplicates(subset=['uid']).reset_index(drop=True)

            # load onchain attestations & revocations
            ad_onchain.load(df, 'attestations')


            ## Onchain, attestations of trust lists & revocations of trust lists ##
            extract_params_onchain = {
                'from_block': 'last_run_block',  # add last run block
                'to_block': 'latest',
                'chunk_size': 100000
            }
            attest_topics = [
                '0x8bf46bf4cfd674fa735a3d63ec1c9ad4153f033c290341f3a588b75685141b35', # Attest topic
                None,
                None,
                '0x6d780a85bfad501090cd82868a0c773c09beafda609d54888a65c106898c363d' # Schema topic
            ]
            revoke_topics = [
                '0xf930a6e2523c9cc298691873087a740550b8fc85a0680830414c148ed927f615', # Revoke topic of onchain attestations
                None,
                None,
                '0x6d780a85bfad501090cd82868a0c773c09beafda609d54888a65c106898c363d' # Schema topic
            ]

            # extract onchain attestations
            extract_params_onchain['topics'] = attest_topics
            df_attest = ad_onchain.extract(extract_params_onchain)

            # extract onchain revocations of onchain attestations
            extract_params_onchain['topics'] = revoke_topics
            df_revokes = ad_onchain.extract(extract_params_onchain)

            # merge df_attest & df_revokes, if there are duplicate uid, then keep the one in df_revokes (overwrites tx_id)
            df = pd.concat([df_revokes, df_attest]).drop_duplicates(subset=['uid']).reset_index(drop=True)

            # load onchain attestations & revocations
            ad_onchain.load(df, 'trust_lists')


            ## Offchain, revocations of trust lists and labels ##
            extract_params_offchain = {
                'from_block': 'last_run_block',  # add last run block
                'to_block': 'latest',
                'chunk_size': 100000,
                'topics': [
                    '0x92a1f7a41a7c585a8b09e25b195e225b1d43248daca46b0faf9e0792777a2229', # Revoke topic of offchain attestations (schema independent)
                    None,
                    None,
                    None
                ]
            }

            # extract onchain revocations of offchain attestations
            df_revokes = ad_offchain.extract(extract_params_offchain)

            # load offchain revocations
            ad_offchain.load(df_revokes, table_names=['attestations', 'trust_lists'])


            ## break the loop if time reached ##
            if start_time + (29 * 60) < time.time():  # 29 minutes
                print("⏰ Approaching task timeout, exiting loop to allow for graceful restart.")
                break

            time.sleep(30)

    sync()

main()

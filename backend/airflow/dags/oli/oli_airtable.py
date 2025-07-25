import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook


@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_airtable',
    description='Update Airtable for contracts labelling',
    tags=['oli', 'daily'],
    start_date=datetime(2023,9,10),
    schedule='50 00 * * *' #after coingecko, after oli_oss_directory, and before metrics_sql_blockspace
)

def etl():
    """
    This DAG is responsible for reading and writing data to and from Airtable as well as attesting new labels and syncing the label pool with the database.
    """
    @task()
    def airtable_read_contracts():
        """
        This task reads the contracts from Airtable and attests them to the OLI label pool.
        """
        import os
        import pandas as pd
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from oli import OLI

        # initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)
        # initialize db connection
        db_connector = DbConnector()
        # initialize OLI instance
        oli = OLI(private_key=os.getenv("OLI_gtp_pk"))

        # read current airtable labels
        table = api.table(AIRTABLE_BASE_ID, 'Unlabeled Contracts')
        df = at.read_all_labeled_contracts_airtable(api, AIRTABLE_BASE_ID, table)
        if df is None:
            print("No new labels detected")
        else:
            # remove duplicates address, chain_id
            df.drop_duplicates(subset=['address', 'chain_id'], inplace=True)
            # keep columns address, chain_id, labelling type and unpivot the other columns
            df = df.melt(id_vars=['address', 'chain_id'], var_name='tag_id', value_name='value')
            # filter out rows with empty values & make address lower
            df = df[df['value'].notnull()]
            df['address'] = df['address'].str.lower() # important, as db addresses are all lower

            # go one by one and attest the new labels
            for group in df.groupby(['address', 'chain_id']):
                # add source column to keep track of origin
                df_air = group[1]
                df_air['source'] = 'airtable'
                # get label from gold table to fill in missing tags
                df_gold = db_connector.get_oli_trusted_label_gold(group[0][0], group[0][1])
                df_gold = df_gold[['address', 'caip2', 'tag_id', 'value']]
                df_gold = df_gold.rename(columns={'caip2': 'chain_id'})
                df_gold['source'] = 'gold_table'
                # merge (if not empty) and drop duplicates, keep airtable over gold tags in case of duplicates
                if df_gold.empty:
                    df_merged = df_air
                else:
                    df_merged = pd.concat([df_air, df_gold], ignore_index=True)
                df_merged = df_merged.drop_duplicates(subset=['address', 'chain_id', 'tag_id'], keep='first')
                df_merged = df_merged.drop(columns=['source'])
                # attest the label
                tags = df_merged.set_index('tag_id')['value'].to_dict() 
                address = group[0][0].replace('\\x', '0x')
                chain_id = group[0][1]
                # submit offchain attestation, try 10 times
                response = oli.submit_offchain_label(address, chain_id, tags, retry=10)
                print(f"Successfully attested label for {address} on {chain_id}: {response.json()}")

    @task()
    def refresh_oli_tags():
        """
        This task adds new oli tags to oli_tags table, only addative, doesn't remove tags!
        """
        from src.db_connector import DbConnector
        from src.misc.helper_functions import get_all_oli_tags_from_github

        # get tags from gtp-dna Github
        df = get_all_oli_tags_from_github()

        # upsert/update oli_tags table 
        db_connector = DbConnector()
        df = df.set_index('tag_id')
        db_connector.upsert_table('oli_tags', df, if_exists='update')
    
    
    @task()
    def refresh_trusted_entities():
        """
        This task gets the trusted entities from the gtp-dna Github and upserts them to the oli_trusted_entities table.
        """
        # TODO: change the trusted entities to also allow for chain filtering!

        from src.misc.helper_functions import get_trusted_entities
        from src.db_connector import DbConnector
        db_connector = DbConnector()

        # get trusted entities from gtp-dna Github, rows with '*' are expanded based on public.oli_tags
        df = get_trusted_entities(db_connector)

        # turn attester into bytea
        df['attester'] = df['attester'].apply(lambda x: '\\x' + x[2:])

        # set attester & tag_id as index
        df = df.set_index(['attester', 'tag_id'])

        # upsert to oli_trusted_entities table, making sure to delete first for full refresh
        db_connector.delete_all_rows('oli_trusted_entities')
        db_connector.upsert_table('oli_trusted_entities', df)
    
    @task()
    def run_refresh_materialized_view():
        """
        This task refreshes the materialized view for the label pool.
        It also refreshes the materialized view for the pivoted view.
        """
        from src.db_connector import DbConnector
        db_connector = DbConnector()

        # also refresh the materialized view for the oli_label_pool_gold and the pivoted view
        db_connector.refresh_materialized_view('vw_oli_label_pool_gold')
        db_connector.refresh_materialized_view('vw_oli_label_pool_gold_pivoted')

    @task()
    def airtable_write_oss_projects():
        """
        This task writes OSS projects from the DB to Airtable.
        """
        import os
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at

        # initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

        # db connection and airtable connection
        db_connector = DbConnector()
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

    @task()
    def airtable_write_chain_info():
        """
        This task writes the chain info from the main config to Airtable.
        """
        import pandas as pd
        import os
        import src.misc.airtable_functions as at
        from pyairtable import Api
        from src.main_config import get_main_config

        # initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

        # get all chain info from main config
        config = get_main_config()
        df = pd.DataFrame(data={'origin_key': [chain.origin_key for chain in config],
                                'chain_type': [chain.chain_type for chain in config],
                                'l2beat_stage': [chain.l2beat_stage for chain in config],
                                'caip2': [chain.caip2 for chain in config],
                                'name': [chain.name for chain in config],
                                'name_short': [chain.name_short for chain in config],
                                'bucket': [chain.bucket for chain in config],
                                'block_explorers': [None if chain.block_explorers is None else next(iter(chain.block_explorers.values())) for chain in config],
                                'socials_website': [chain.socials_website for chain in config],
                                'socials_twitter': [chain.socials_twitter for chain in config],
                                'runs_aggregate_blockspace': [chain.runs_aggregate_blockspace for chain in config]
                                })

        # merge current table with the new data
        table = api.table(AIRTABLE_BASE_ID, 'Chain List')
        df_air = at.read_airtable(table)[['origin_key', 'id']]
        df = df.merge(df_air, how='left', left_on='origin_key', right_on='origin_key')

        # update existing records (primary key is the id)
        at.update_airtable(table, df)

        # add any new records/chains if present
        df_new = df[df['id'].isnull()]
        if df_new.empty == False:
            at.push_to_airtable(table, df_new.drop(columns=['id']))

        # remove old records
        mask = ~df_air['origin_key'].isin(df['origin_key'])
        df_remove = df_air[mask]
        if df_remove.empty == False:
            at.delete_airtable_ids(table, df_remove['id'].tolist())

    @task()
    def airtable_write_contracts():
        """
        This task writes the top unlabeled contracts from the DB to Airtable.
        It also removes any duplicates that are already in the airtable due to temp_owner_project.
        """
        import os
        import pandas as pd
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from eth_utils import to_checksum_address

        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

        # db connection and airtable connection
        db_connector = DbConnector()
        table = api.table(AIRTABLE_BASE_ID, 'Unlabeled Contracts')

        # clears all records in the airtable table except for the ones that has the column temp_owner_project filled out
        at.clear_all_airtable(table)
        
        # get top unlabelled contracts, short and long term and also inactive contracts
        df0 = db_connector.get_unlabelled_contracts('20', '720') # top 20 contracts per chain from last 720 days
        df1 = db_connector.get_unlabelled_contracts('20', '180') # top 20 contracts per chain from last 3 months
        df2 = db_connector.get_unlabelled_contracts('20', '30') # top 20 contracts per chain from last month
        df3 = db_connector.get_unlabelled_contracts('20', '7') # top 20 contracts per chain from last week
        
        # merge the all dataframes & reset index
        df = pd.concat([df0, df1, df2, df3])
        df = df.reset_index(drop=True)

        # remove duplicates
        df = df.drop_duplicates(subset=['address', 'origin_key'])

        # checksum the addresses
        df['address'] = df['address'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))

        # remove all duplicates that are still in the airtable due to temp_owner_project
        df_remove = at.read_airtable(table)
        if df_remove.empty == False:
            # replace id with actual origin_key
            chains = api.table(AIRTABLE_BASE_ID, 'Chain List')
            df_chains = at.read_airtable(chains)
            df_remove['origin_key'] = df_remove['origin_key'].apply(lambda x: x[0])
            df_remove = df_remove.replace({'origin_key': df_chains.set_index('id')['origin_key']})
            # remove duplicates from df
            df_remove = df_remove[['address', 'origin_key']]
            df = df.merge(df_remove, on=['address', 'origin_key'], how='left', indicator=True)
            df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])

        # exchange the category with the id & make it a list
        cat = api.table(AIRTABLE_BASE_ID, 'Usage Categories')
        df_cat = at.read_airtable(cat)
        df = df.replace({'usage_category': df_cat.set_index('Category')['id']})
        df['usage_category'] = df['usage_category'].apply(lambda x: [x])

        # exchange the project with the id & make it a list
        proj = api.table(AIRTABLE_BASE_ID, 'OSS Projects')
        df_proj = at.read_airtable(proj)
        df = df.replace({'owner_project': df_proj.set_index('Name')['id']})
        df['owner_project'] = df['owner_project'].apply(lambda x: [x])

        # exchange the chain origin_key with the id & make it a list
        chains = api.table(AIRTABLE_BASE_ID, 'Chain List')
        df_chains = at.read_airtable(chains)
        df = df.replace({'origin_key': df_chains.set_index('origin_key')['id']})
        df['origin_key'] = df['origin_key'].apply(lambda x: [x])

        # write to airtable
        at.push_to_airtable(table, df)

    @task()
    def airtable_write_depreciated_owner_project():
        """
        This task writes the remap owner project table to Airtable.
        """
        import os
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from src.misc.helper_functions import send_discord_message

        #initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

        # clear the whole table
        table = api.table(AIRTABLE_BASE_ID, 'Remap Owner Project')
        at.clear_all_airtable(table)

        # db connection
        db_connector = DbConnector()

        # get inactive projects
        df = db_connector.get_tags_inactive_projects()

        # send alert to discord
        if df.shape[0] > 0:
            print(f"Inactive contracts found: {df['tag_value'].unique().tolist()}")
            send_discord_message(f"<@874921624720257037> Inactive projects with assigned contracts: {df['tag_value'].unique().tolist()}", os.getenv('DISCORD_CONTRACTS'))
        else:
            print("No inactive projects with contracts assigned found")

        # group by owner_project and then write to airtable
        df = df.rename(columns={'tag_value': 'old_owner_project'})
        df = df.groupby('old_owner_project').size()
        df = df.reset_index(name='count')
        at.push_to_airtable(table, df)
    
    @task()
    def airtable_read_label_pool_reattest():
        """
        This task reads other attestations from the label pool from Airtable and attests the approved labels to the label pool as growthepie.
        It also reads the remap owner project table and reattests the labels with the new owner project.
        """
        # read in approved labels & delete approved labels from airtable
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from pyairtable import Api
        from oli import OLI
        import pandas as pd
        import os
        # airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, 'Label Pool Reattest')
        # read all approved labels in 'Label Pool Reattest'
        df = at.read_all_approved_label_pool_reattest(api, AIRTABLE_BASE_ID, table)
        if df is not None and not df.empty:
            # initialize db connection
            db_connector = DbConnector()
            # OLI instance
            oli = OLI(private_key=os.getenv("OLI_gtp_pk"))
            # remove duplicates address, origin_key
            df = df.drop_duplicates(subset=['address', 'chain_id'])
            # keep track of ids
            ids = df['id'].tolist()
            ### capitalize the first letter for contract_name
            ###df['contract_name'] = df['contract_name'].str[0].str.upper() + df['contract_name'].str[1:]
            # keep columns address, origin_key and unpivot the other columns
            df = df[['address', 'chain_id', 'contract_name', 'owner_project', 'usage_category']]
            df = df.melt(id_vars=['address', 'chain_id'], var_name='tag_id', value_name='value')
            # turn address into lower case (IMPORTANT for later concatenation)
            df['address'] = df['address'].str.lower()
            # filter out rows with empty values
            df = df[df['value'].notnull()]
            # go one by one and attest the new labels
            for group in df.groupby(['address', 'chain_id']):
                # add source column to keep track of origin
                df_air = group[1]
                df_air['source'] = 'airtable'
                # get label from gold table to fill in missing tags
                df_gold = db_connector.get_oli_trusted_label_gold(group[0][0], group[0][1])
                df_gold = df_gold[['address', 'caip2', 'tag_id', 'value']]
                df_gold = df_gold.rename(columns={'caip2': 'chain_id'})
                df_gold['source'] = 'gold_table'
                # merge (if not empty) and drop duplicates, keep airtable over gold tags in case of duplicates
                if df_gold.empty:
                    df_merged = df_air
                else:
                    df_merged = pd.concat([df_air, df_gold], ignore_index=True)
                df_merged = df_merged.drop_duplicates(subset=['address', 'chain_id', 'tag_id'], keep='first')
                df_merged = df_merged.drop(columns=['source'])
                # attest the label
                tags = df_merged.set_index('tag_id')['value'].to_dict()
                address = group[0][0].replace('\\x', '0x')
                chain_id = group[0][1]
                # submit offchain attestation, try 10 times
                response = oli.submit_offchain_label(address, chain_id, tags, retry=10)
                print(f"Successfully attested label for {address} on {chain_id}: {response.json()}")
            # at the end delete just uploaded rows from airtable
            at.delete_airtable_ids(table, ids)

    @task()
    def airtable_read_depreciated_owner_project():
        """
        This task reads the remap owner project table and reattests the labels with the new owner project.
        """
        import os
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from oli import OLI

        #initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

        # read the whole table
        table = api.table(AIRTABLE_BASE_ID, 'Remap Owner Project')
        df = at.read_all_remap_owner_project(api, AIRTABLE_BASE_ID, table)

        if df is not None and not df.empty:
            # db connection
            db_connector = DbConnector()
            # initialize OLI instance
            oli = OLI(private_key=os.getenv("OLI_gtp_pk"))
            # iterate over each owner_project that was changed
            for i, row in df.iterrows():
                # get the old and new owner project
                old_owner_project = row['old_owner_project']
                new_owner_project = row['owner_project']
                # get all labels with the old owner project
                df_labels = db_connector.get_oli_labels_gold_by_owner_project(old_owner_project)
                # reattest the labels with the new owner project, going one by one and attest the new labels
                for group in df_labels.groupby(['address', 'caip2']):
                    tags = group[1].set_index('tag_id')['tag_value'].to_dict()
                    address = group[0][0].replace('\\x', '0x')
                    chain_id = group[0][1]
                    # replace with new owner project
                    tags['owner_project'] = new_owner_project
                    # post the label to EAS API
                    response = oli.submit_offchain_label(address, chain_id, tags, retry=10)
                    print(f"Successfully attested label for {address} on {chain_id}: {response.json()}")
                    # deleting the rows is triggered in airtable_write_depreciated_owner_project

    @task()
    def revoke_old_attestations():
        """
        This task revokes old attestations from the label pool.
        It reads the labels from the DB and revokes them in batches of 500.
        """
        from src.db_connector import DbConnector
        from oli import OLI
        import os

        db_connector = DbConnector()

        df = db_connector.get_oli_to_be_revoked()
        uids_offchain = df[df['is_offchain'] == True]['id_hex'].tolist()
        uids_onchain = df[df['is_offchain'] == False]['id_hex'].tolist()

        ### remove toxic uids (e.g. GraphQL indexing errors, message https://t.me/stevedakh to get fixed)
        #toxic_uids = ['0x10ad46514c42ba85efd1b2893904093257cf2aea73bedd992d52c3921ac8f27d']
        #uids_offchain = [uid for uid in uids_offchain if uid not in toxic_uids]
        #uids_onchain = [uid for uid in uids_onchain if uid not in toxic_uids]

        if uids_offchain == [] and uids_onchain == []:
            print("No labels to be revoked")
        else:
            oli = OLI(private_key=os.getenv("OLI_gtp_pk"))
            # revoke with max 500 uids at once
            for i in range(0, len(uids_offchain), 500):
                tx_hash, count = oli.multi_revoke_attestations(uids_offchain[i:i + 500], onchain=False, gas_limit=15000000)
                print(f"Revoked {count} offchain labels with tx_hash {tx_hash}")
            for i in range(0, len(uids_onchain), 500):
                tx_hash, count = oli.multi_revoke_attestations(uids_onchain[i:i + 500], onchain=True, gas_limit=15000000)
                print(f"Revoked {count} onchain labels with tx_hash {tx_hash}")

    @task()
    def sync_attestations():
        """
        This task syncs the attestations from the label pool to the DB.
        It's the same as in the oli_label_pool DAG
        """
        from src.db_connector import DbConnector
        from src.adapters.adapter_oli_label_pool import AdapterLabelPool

        db_connector = DbConnector()
        ad = AdapterLabelPool({}, db_connector)

        # get new labels from the GraphQL endpoint
        df = ad.extract()
        
        # load labels into bronze table, then also increment to silver and lastly pushes untrusted owner_project labels to airtable
        ad.load(df)

    # all tasks
    read_contracts = airtable_read_contracts()  ## read in contracts from airtable and attest
    read_pool = airtable_read_label_pool_reattest() ## read in approved labels from airtable and attest 
    read_remap = airtable_read_depreciated_owner_project() ## read in remap owner project from airtable and attest
    
    refresh_tags = refresh_oli_tags() ## read in oli tags from oli Github and upsert to DB table oli_tags
    trusted_entities = refresh_trusted_entities() ## read in trusted entities from gtp-dna Github and upsert to DB
    sync_to_db = sync_attestations() ## updates oli bronze and silver tables with new labels from the label pool
    refresh = run_refresh_materialized_view() ## refresh materialized views vw_oli_label_pool_gold and vw_oli_label_pool_gold_pivoted
    
    write_oss = airtable_write_oss_projects() ## write oss projects from DB to airtable
    write_chain = airtable_write_chain_info() ## write chain info from main config to airtable
    write_contracts = airtable_write_contracts()  ## write contracts from DB to airtable
    write_owner_project = airtable_write_depreciated_owner_project() ## write remap owner project from DB to airtable
    
    revoke_onchain = revoke_old_attestations() ## revoke old attestations from the label pool

    # Define execution order
    read_contracts >> read_pool >> read_remap >> refresh_tags >> trusted_entities >> sync_to_db >> refresh >> write_oss >> write_chain >> write_contracts >> write_owner_project >> revoke_onchain
    
etl()


## When writing to DB in general? when bronze and silver getting filled?
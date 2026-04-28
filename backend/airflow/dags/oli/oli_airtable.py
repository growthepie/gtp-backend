from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook, claude_fix_on_failure


@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': [alert_via_webhook, claude_fix_on_failure]
    },
    dag_id='oli_airtable',
    description='Update Airtable for contracts labelling',
    tags=['oli', 'daily'],
    start_date=datetime(2023,9,10),
    schedule='50 00 * * *' # 0:50am. After coingecko, after oli_oss_directory, oli_misc_sync and before metrics_sql_blockspace
)

def etl():

    """
    This DAG is responsible for reading and writing data to and from Airtable.
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
        from oli import OLI # v2.0.4

        # initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)
        # initialize db connection
        db_connector = DbConnector()
        # initialize OLI instance
        oli = OLI(private_key=os.getenv("OLI_gtp_pk"), api_key=os.getenv("OLI_api_key"))

        # read current airtable labels
        table = api.table(AIRTABLE_BASE_ID, 'Unlabelled')
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
                df_gold = db_connector.get_all_prior_attested_labels(group[0][0], group[0][1], "0xA725646C05E6BB813D98C5ABB4E72DF4BCF00B56")
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
                # submit offchain attestation
                response = oli.submit_label(address, chain_id, tags)
                print(f"Successfully attested label for {address} on {chain_id}: {response}")
                
    @task()
    def sync_chains_to_airtable():
        """
        This task syncs the chain info from the main config to Airtable.
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
                                'block_explorers': [next(iter(chain.links["block_explorers"].values())) if chain.links and chain.links["block_explorers"] else None for chain in config],
                                'socials_website': [chain.links["website"] if chain.links and chain.links["website"] else None for chain in config],
                                'socials_twitter': [chain.links["socials"]["Twitter"] if chain.links and chain.links["socials"] and "Twitter" in chain.links["socials"] else None for chain in config],
                                'runs_aggregate_blockspace': [chain.runs_aggregate_blockspace for chain in config]
                                })

        # merge current table with the new data
        table = api.table(AIRTABLE_BASE_ID, 'Chains')
        df_air = at.read_airtable(table)[['origin_key', 'id']]
        df = df.merge(df_air, how='left', left_on='origin_key', right_on='origin_key')

        # update existing records (primary key is the id)
        at.update_airtable(table, df)

        # add any new records/chains if present
        df_new = df[df['id'].isnull()]
        if df_new.empty == False:
            at.push_to_airtable(table, df_new.drop(columns=['id']))

        # remove old records (rare occurrence, commented out as of Dec.9th 2025)
        #mask = ~df_air['origin_key'].isin(df['origin_key'])
        #df_remove = df_air[mask]
        #if df_remove.empty == False:
        #    at.delete_airtable_ids(table, df_remove['id'].tolist())

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
        table = api.table(AIRTABLE_BASE_ID, 'Unlabelled')

        # clears all records in the airtable table except for the ones that has the column temp_owner_project filled out
        at.clear_all_airtable(table)

        # set 'approve' column to False for all records with temp_owner_project filled out
        df_unapprove = at.read_airtable(table)
        df_unapprove['approve'] = False
        df_unapprove = df_unapprove[['id', 'approve']]
        at.update_airtable(table, df_unapprove)

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

        # normalize addresses to checksum format
        def normalize_address(row):
            if row['caip2'].startswith('eip155'):
                return to_checksum_address(row['address'])
            addr = row['address']
            if isinstance(addr, memoryview):
                addr = addr.tobytes()
            if isinstance(addr, (bytes, bytearray)):
                return '0x' + addr.hex()
            return str(addr)

        df['address'] = df.apply(normalize_address, axis=1)
        
        ## drop column caip2
        df = df.drop(columns=['caip2'])

        # remove all duplicates that are still in the airtable due to temp_owner_project
        df_remove = at.read_airtable(table)
        if df_remove.empty == False:
            # replace id with actual origin_key
            chains = api.table(AIRTABLE_BASE_ID, 'Chains')
            df_chains = at.read_airtable(chains)
            df_remove['origin_key'] = df_remove['origin_key'].apply(lambda x: x[0])
            df_remove = df_remove.replace({'origin_key': df_chains.set_index('id')['origin_key']})
            # remove duplicates from df
            df_remove = df_remove[['address', 'origin_key']]
            df = df.merge(df_remove, on=['address', 'origin_key'], how='left', indicator=True)
            df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])

        # exchange the category with the id & make it a list
        cat = api.table(AIRTABLE_BASE_ID, 'Sub Categories')
        df_cat = at.read_airtable(cat)
        df = df.replace({'usage_category': df_cat.set_index('category_id')['id']})
        df['usage_category'] = df['usage_category'].apply(lambda x: [x] if pd.notna(x) else None)

        # exchange the project with the id & make it a list
        proj = api.table(AIRTABLE_BASE_ID, 'OSS Projects')
        df_proj = at.read_airtable(proj)
        df = df.replace({'owner_project': df_proj.set_index('Name')['id']})
        df['owner_project'] = df['owner_project'].apply(lambda x: [x] if pd.notna(x) else None)

        # exchange the chain origin_key with the id & make it a list
        chains = api.table(AIRTABLE_BASE_ID, 'Chains')
        df_chains = at.read_airtable(chains)
        df = df.replace({'origin_key': df_chains.set_index('origin_key')['id']})
        df['origin_key'] = df['origin_key'].apply(lambda x: [x] if pd.notna(x) else None)

        # write to airtable
        at.push_to_airtable(table, df)

    @task()
    def airtable_write_label_pool_reattest():
        """
        This task writes the approved label pool reattest table to Airtable.
        """
        from eth_utils import to_checksum_address
        from src.db_connector import DbConnector
        from src.misc.helper_functions import send_discord_message
        import src.misc.airtable_functions as at
        from pyairtable import Api
        import os

        db_connector = DbConnector()
        hours = 24

        # from our db, get the labels that were attested in the last 24 hours
        yesterday = datetime.now() - timedelta(hours=hours)
        load_params = {'date': yesterday.strftime('%Y-%m-%d %H:00:00')}
        df_attested = db_connector.execute_jinja('/oli/extract_labels_for_review.sql.j2', load_params, load_into_df=True)

        if df_attested.empty == False:
            df_attested['address'] = df_attested.apply(
                lambda row: to_checksum_address(row['address']) if row['chain_id'].startswith('eip155') else row['address'], axis=1
            )
            df_attested = df_attested[['address', 'attester', 'chain_id', 'contract_name', 'owner_project', 'usage_category']]

            # from airtable, get the labels that are in the Label Pool Reattest table
            AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
            AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
            api = Api(AIRTABLE_API_KEY)
            table = api.table(AIRTABLE_BASE_ID, 'Label Pool Reattest')

            # read all approved labels in 'Label Pool Reattest'
            df_in_airtable = at.read_all_label_pool_reattest(api, AIRTABLE_BASE_ID, table, approved=False)

            ## create df_new with rows that are in df_air but not in df based on all columns
            if df_in_airtable is None:
                df_new = df_attested
            else:
                df_in_airtable = df_in_airtable[['address', 'chain_id', 'contract_name', 'owner_project', 'usage_category']]
                df_in_airtable['address'] = df_in_airtable['address'].str.replace('\\x', '0x')
                df_new = df_attested.merge(df_in_airtable, on=['address', 'chain_id', 'contract_name', 'owner_project', 'usage_category'], how='left', indicator=True)
                df_new = df_new[df_new['_merge'] == 'left_only']
                df_new = df_new.drop(columns=['_merge'])
            
            if df_new.empty == False:
                ## if column chain_id starts with 'eip155' then do checksum address
                df_new['address'] = df_new.apply(
                    lambda row: to_checksum_address(row['address']) if row['chain_id'].startswith('eip155') else row['address'], axis=1
                )
                
                df_new['attester'] = df_new['attester'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))
                
                # exchange the category with the id & make it a list
                cat = api.table(AIRTABLE_BASE_ID, 'Sub Categories')
                df_cat = at.read_airtable(cat)
                df_new = df_new.replace({'usage_category': df_cat.set_index('category_id')['id']})
                df_new['usage_category'] = df_new['usage_category'].apply(lambda x: [x])
                
                # exchange the project with the id & make it a list
                proj = api.table(AIRTABLE_BASE_ID, 'OSS Projects')
                df_proj = at.read_airtable(proj)
                df_new = df_new.replace({'owner_project': df_proj.set_index('Name')['id']})
                df_new['owner_project'] = df_new['owner_project'].apply(lambda x: [x])
                
                # exchange the chain chain_id with the id & make it a list
                chains = api.table(AIRTABLE_BASE_ID, 'Chains')
                df_chains = at.read_airtable(chains)
                df_new = df_new.replace({'chain_id': df_chains.set_index('caip2')['id']})
                df_new['chain_id'] = df_new['chain_id'].apply(lambda x: [x])
                
                # rename chain_id to origin_key
                df_new = df_new.rename(columns={'chain_id': 'origin_key'})
                
                # catch errors and log them in the error column (for owner_project and usage_category)
                df_new['error'] = ''
                df_new['error'] = df_new.apply(
                    lambda row: row['error'] + f" failed owner_project: '{row['owner_project'][0]}'"
                    if isinstance(row['owner_project'][0], str) and not row['owner_project'][0].startswith('rec') else row['error'], axis=1
                )
                df_new['error'] = df_new.apply(
                    lambda row: row['error'] + f" failed usage_category: '{row['usage_category'][0]}'"
                    if isinstance(row['usage_category'][0], str) and not row['usage_category'][0].startswith('rec') else row['error'], axis=1
                )
                df_new['error'] = df_new['error'].apply(lambda x: x.strip() or None)
                df_new['usage_category'] = df_new['usage_category'].apply(lambda x: x if isinstance(x[0], str) and x[0].startswith('rec') else [])
                df_new['owner_project'] = df_new['owner_project'].apply(lambda x: x if isinstance(x[0], str) and x[0].startswith('rec') else [])
                
                # write to airtable df
                at.push_to_airtable(table, df_new)

                # send discord message
                send_discord_message(f"{df_new.shape[0]} new attestations submitted to label pool, please review in airtable.", os.getenv('DISCORD_CONTRACTS'))
            else:
                print('No new attestations to submit to label pool.')
        else:
            print(f'No attestations since {yesterday}.')

    @task()
    def airtable_write_protocol_likely_to_automated():
        """
        Surface contracts flagged as 'protocol-contract-likely' by the automated labeler back into
        'Label Pool Automated' so a human reviewer can assign owner_project / temp_owner_project /
        gtp_no_owner_project — all validation lives in a single Airtable table.

        Source: extract_automated_protocol_likely.sql.j2 (rows where the automated attester wrote
        owner_project='protocol-contract-likely' as a sentinel for human review).

        Rows arrive with `is_protocol_contract_likely=True`, `approve=False`, and a `_comment`
        prompting owner-project assignment. `owner_project` is left blank (linked record); the
        reviewer fills `owner_project` (linked) for known protocols OR `temp_owner_project` (text)
        for novel ones, OR checks `gtp_no_owner_project` to confirm there is no owner.
        """
        import pandas as pd
        from eth_utils import to_checksum_address
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from pyairtable import Api
        import os

        db_connector = DbConnector()
        df = db_connector.execute_jinja('/oli/extract_automated_protocol_likely.sql.j2', {}, load_into_df=True)
        if df.empty:
            print('No protocol-likely contracts to surface.')
            return

        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, 'Label Pool Automated')

        # Normalise address to checksum format
        df['address'] = df.apply(
            lambda row: to_checksum_address('0x' + bytes(row['address']).hex())
            if isinstance(row['address'], (bytes, bytearray, memoryview))
            else to_checksum_address(row['address'])
            if row['chain_id'].startswith('eip155') else row['address'],
            axis=1
        )

        # Dedup against existing 'Label Pool Automated' rows (address only — origin_key is a
        # linked record and would require resolving recIDs; address is unique enough here).
        df_existing = at.read_airtable(table)
        if not df_existing.empty and 'address' in df_existing.columns:
            existing_addrs = set(df_existing['address'].str.lower())
            df = df[~df['address'].str.lower().isin(existing_addrs)]
        if df.empty:
            print('All protocol-likely contracts already in Label Pool Automated.')
            return

        # Map usage_category slug → Airtable Sub Categories recID (linked record)
        df_cat = at.read_airtable(api.table(AIRTABLE_BASE_ID, 'Sub Categories'))
        df = df.replace({'usage_category': df_cat.set_index('category_id')['id']})
        df['usage_category'] = df['usage_category'].apply(
            lambda x: [x] if pd.notna(x) and str(x).startswith('rec') else [])

        # Map chain_id (caip2) → Airtable Chains recID, rename column to origin_key (linked)
        df_chains = at.read_airtable(api.table(AIRTABLE_BASE_ID, 'Chains'))
        df = df.replace({'chain_id': df_chains.set_index('caip2')['id']})
        df['chain_id'] = df['chain_id'].apply(
            lambda x: [x] if pd.notna(x) and str(x).startswith('rec') else [])
        df = df.rename(columns={'chain_id': 'origin_key'})

        # Reviewer-facing scaffold
        df['_comment'] = 'protocol-likely: assign owner_project, set temp_owner_project (novel), or check gtp_no_owner_project.'
        df['_source'] = 'gtp-automated_labeler_protocol_likely'
        df['is_protocol_contract_likely'] = True
        df['approve'] = False

        # Drop the sentinel slug — leaving owner_project blank prompts manual assignment
        df = df.drop(columns=[c for c in ['attester', 'owner_project'] if c in df.columns],
                     errors='ignore')

        at.push_to_airtable(table, df)
        print(f'Wrote {len(df)} protocol-likely contracts to Label Pool Automated.')

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
        from oli import OLI # v2.0.4
        import pandas as pd
        import os
        # airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, 'Label Pool Reattest')
        # read all approved labels in 'Label Pool Reattest'
        df = at.read_all_label_pool_reattest(api, AIRTABLE_BASE_ID, table, approved=True)
        if df is not None and not df.empty:
            # initialize db connection
            db_connector = DbConnector()
            # OLI instance
            oli = OLI(private_key=os.getenv("OLI_gtp_pk"), api_key=os.getenv("OLI_API_KEY"))
            # remove duplicates address, origin_key
            df = df.drop_duplicates(subset=['address', 'chain_id'])
            # keep track of ids
            ids = df['id'].tolist()
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
                # get all other label attested by us to fill in missing tags
                df_gold = db_connector.get_all_prior_attested_labels(group[0][0], group[0][1], "0xA725646C05E6BB813D98C5ABB4E72DF4BCF00B56")
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
                # submit offchain attestation
                response = oli.submit_label(address, chain_id, tags)
                print(f"Successfully attested label for {address} on {chain_id}: {response}")
            # at the end delete just uploaded rows from airtable
            at.delete_airtable_ids(table, ids)

    @task()
    def airtable_read_automated_labels():
        """
        Reads approved rows from 'Label Pool Automated', re-attests them with the high-trust
        OLI_gtp_pk key (supplemented with any existing gold-table tags), then deletes them from Airtable.
        """
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from pyairtable import Api
        from oli import OLI  # v2.0.4
        import pandas as pd
        import os

        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, 'Label Pool Automated')

        df = at.read_all_label_pool_automated(api, AIRTABLE_BASE_ID, table, approved=True)
        if df is None or df.empty:
            print('No approved automated labels to re-attest.')
            return

        db_connector = DbConnector()
        oli = OLI(private_key=os.getenv("OLI_gtp_pk"), api_key=os.getenv("OLI_API_KEY"))

        df = df.drop_duplicates(subset=['address', 'chain_id'])
        ids = df['id'].tolist()

        # Per-row review-diff context (NOT melted — keep the row data intact)
        review_ctx = df.set_index(['address', 'chain_id'])[
            ['ai_contract_name', 'ai_usage_category', 'temp_owner_project', 'gtp_no_owner_project']
        ].to_dict('index')

        # Melt to tag_id/value pairs. owner_project IS now possibly set (linked OSS slug after coalesce).
        df_tags = df[['address', 'chain_id', 'contract_name', 'owner_project', 'usage_category']]
        df_tags = df_tags.melt(id_vars=['address', 'chain_id'], var_name='tag_id', value_name='value')
        df_tags['address'] = df_tags['address'].str.lower()
        df_tags = df_tags[df_tags['value'].notnull()]

        # caip2 → origin_key inverse lookup (for contract_label_features primary key)
        from src.main_config import get_main_config
        chain_id_to_origin_key = {f"eip155:{c.chain_id}": c.origin_key for c in get_main_config()
                                  if getattr(c, 'chain_id', None)}

        for group in df_tags.groupby(['address', 'chain_id']):
            df_auto = group[1].copy()
            df_auto['source'] = 'automated'
            df_gold = db_connector.get_all_prior_attested_labels(
                group[0][0], group[0][1], "0xA725646C05E6BB813D98C5ABB4E72DF4BCF00B56")
            if not df_gold.empty:
                df_gold = df_gold[['address', 'caip2', 'tag_id', 'value']].rename(
                    columns={'caip2': 'chain_id'})
                df_gold['source'] = 'gold_table'
                df_merged = pd.concat([df_auto, df_gold], ignore_index=True)
            else:
                df_merged = df_auto
            df_merged = df_merged.drop_duplicates(
                subset=['address', 'chain_id', 'tag_id'], keep='first')
            df_merged = df_merged.drop(columns=['source'])

            tags = df_merged.set_index('tag_id')['value'].to_dict()
            address = group[0][0].replace('\\x', '0x')
            chain_id = group[0][1]
            origin_key = chain_id_to_origin_key.get(chain_id)

            # ── Review diff capture ────────────────────────────────────────────
            try:
                ctx = review_ctx.get(group[0], {})
                ai_row = {}
                if origin_key:
                    ai_row = db_connector.get_contract_label_features([(address, origin_key)]).get(
                        (address.lower(), origin_key), {})
                ai_name = ai_row.get('ai_contract_name') or ctx.get('ai_contract_name')
                ai_cat  = ai_row.get('ai_usage_category') or ctx.get('ai_usage_category')
                gtp_name = tags.get('contract_name')  if tags.get('contract_name')  not in (None, ai_name) else None
                gtp_cat  = tags.get('usage_category') if tags.get('usage_category') not in (None, ai_cat)  else None
                gtp_owner = tags.get('owner_project') or ctx.get('temp_owner_project') or None
                gtp_no_owner = bool(ctx.get('gtp_no_owner_project', False))
                if origin_key:
                    db_connector.insert_contract_label_review([{
                        'address': address,
                        'origin_key': origin_key,
                        'labeled_at': ai_row.get('labeled_at'),
                        'reviewed_at': datetime.utcnow(),
                        'review_source': 'airtable_automated',
                        'ai_contract_name': ai_name,
                        'ai_usage_category': ai_cat,
                        'ai_confidence': ai_row.get('ai_confidence'),
                        'ai_owner_project_likely': bool(ai_row.get('ai_owner_project_likely', False)),
                        'gtp_contract_name': gtp_name,
                        'gtp_usage_category': gtp_cat,
                        'gtp_owner_project': gtp_owner,
                        'gtp_no_owner_project': gtp_no_owner,
                    }])
            except Exception as e:
                # Non-fatal — attestation must still proceed
                print(f"[contract_label_review] insert failed for {address}/{chain_id}: {e}")

            response = oli.submit_label(address, chain_id, tags)
            print(f'Re-attested {address} on {chain_id}: {response}')

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
        from oli import OLI # v2.0.4

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
            oli = OLI(private_key=os.getenv("OLI_gtp_pk"), api_key=os.getenv("OLI_API_KEY"))
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
                    # post the label to OLI Label Pool
                    response = oli.submit_label(address, chain_id, tags)
                    print(f"Successfully attested label for {address} on {chain_id}: {response}")
                    # deleting the rows is triggered in airtable_write_depreciated_owner_project

    @task()
    def revoke_old_attestations():
        """
        This task revokes old attestations from the label pool.
        It reads the labels from the DB and revokes them in batches of 500.
        """
        from src.db_connector import DbConnector
        from oli import OLI # v2.0.4
        import os

        db_connector = DbConnector()

        df = db_connector.get_oli_to_be_revoked()
        uids_offchain = df[df['is_offchain'] == True]['id_hex'].tolist()
        uids_onchain = df[df['is_offchain'] == False]['id_hex'].tolist()

        if uids_offchain == [] and uids_onchain == []:
            print("No labels to be revoked")
        else:
            oli = OLI(private_key=os.getenv("OLI_gtp_pk"), api_key=os.getenv("OLI_API_KEY"))
            # revoke with max 500 uids at once
            for i in range(0, len(uids_offchain), 500):
                response = oli.revoke_bulk_by_uids(uids_offchain[i:i + 500], onchain=False, gas_limit=15000000)
                print(f"Revoked {len(uids_offchain[i:i + 500])} offchain labels with tx_hash {response}")
            for i in range(0, len(uids_onchain), 500):
                response = oli.revoke_bulk_by_uids(uids_onchain[i:i + 500], onchain=True, gas_limit=15000000)
                print(f"Revoked {len(uids_onchain[i:i + 500])} onchain labels with tx_hash {response}")

    @task()
    def sync_categories_to_airtable():
        """
        This task updates categories in airtable based on the ones in the db under oli_categories_main & oli_categories.
        """
        import os
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at

        #initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

        # get all categories from the db
        db_connector = DbConnector()
        df_sub = db_connector.get_table('oli_categories')
        df_main = db_connector.get_table('oli_categories_main')

        # read in the airtable 'Sub Categories' & 'Main Categories' tables
        table_sub = api.table(AIRTABLE_BASE_ID, 'Sub Categories')
        air_sub = at.read_airtable(table_sub)
        table_main = api.table(AIRTABLE_BASE_ID, 'Main Categories')
        air_main = at.read_airtable(table_main)

        ## Update main categories airtable
        # merge any changes to existing records
        df_main = df_main.merge(air_main[['id', 'main_category_id']], how='left', left_on='main_category_id', right_on='main_category_id')
        at.update_airtable(table_main, df_main)
        # add new records
        at.push_to_airtable(table_main, df_main[df_main['id'].isna()].drop(columns=['id']))

        ## Update sub categories airtable
        # exchange out the main_category_id for the correct ids & make it a list
        df_sub = df_sub.replace({'main_category_id': df_main.set_index('main_category_id')['id']})
        df_sub['main_category_id'] = df_sub['main_category_id'].apply(lambda x: [x])
        # merge any changes to existing records
        df_sub = df_sub.merge(air_sub[['id', 'category_id']], how='left', left_on='category_id', right_on='category_id')
        at.update_airtable(table_sub, df_sub)
        # add new records
        at.push_to_airtable(table_sub, df_sub[df_sub['id'].isna()].drop(columns=['id']))

    @task()
    def refresh_materialized_views():
        """
        This task refreshes the OLI materialized views in our web3 database.
        """
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        materialized_views = [
            'vw_oli_label_pool_gold_v2',
            'vw_oli_label_pool_gold_pivoted_v2',
            'mv_verified_contracts'
        ]
        for view in materialized_views:
            db_connector.refresh_materialized_view(view)

    ## Sync things from db to airtable
    sync_categories = sync_categories_to_airtable() ## sync categories from db to airtable
    sync_chains = sync_chains_to_airtable() ## sync chains from db to airtable

    ## Read in new labels from airtable and attest to label pool
    read_contracts = airtable_read_contracts()  ## read in contracts from airtable and attest
    read_pool = airtable_read_label_pool_reattest() ## read in approved labels from airtable and attest
    read_automated = airtable_read_automated_labels() ## read in approved automated labels and re-attest with gtp key
    read_remap = airtable_read_depreciated_owner_project() ## read in remap owner project from airtable and attest

    ## Refresh materialized views
    refresh_views = refresh_materialized_views()

    ## Write new unlabeled contracts and depreciated owner project to airtable from db
    write_contracts = airtable_write_contracts()  ## write contracts from DB to airtable
    write_pool = airtable_write_label_pool_reattest() ## write label pool reattest from DB to airtable
    write_remap = airtable_write_depreciated_owner_project() ## write remap owner project from DB to airtable
    write_protocol_likely = airtable_write_protocol_likely_to_automated() ## write protocol-likely contracts back to Label Pool Automated for owner-project review

    ## Revoke old attestations from label pool
    revoke_onchain = revoke_old_attestations() ## revoke old attestations from the label pool

    # Define execution order
    # read_automated runs in parallel with read_pool; write side is handled by oli_automated_labeler DAG
    # write_protocol_likely runs in parallel with write_pool and write_remap after view refresh
    sync_categories >> sync_chains >> read_contracts >> [read_pool, read_automated] >> read_remap >> refresh_views >> write_contracts >> write_pool >> write_remap >> revoke_onchain
    
etl()


## When writing to DB in general? when bronze and silver getting filled?
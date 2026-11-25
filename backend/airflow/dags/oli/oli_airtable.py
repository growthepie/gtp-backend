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
        # table can be empty after clearing (no rows with temp_owner_project) -> df has no columns
        df_unapprove = at.read_airtable(table)
        if not df_unapprove.empty:
            df_unapprove['approve'] = False
            df_unapprove = df_unapprove[['id', 'approve']]
            at.update_airtable(table, df_unapprove)

        # get top unlabelled contracts, short and long term and also inactive contracts
        df0 = db_connector.get_unlabelled_contracts('20', '720') # top 20 contracts per chain from last 720 days
        df1 = db_connector.get_unlabelled_contracts('20', '180') # top 20 contracts per chain from last 3 months
        df2 = db_connector.get_unlabelled_contracts('20', '30') # top 20 contracts per chain from last month
        df3 = db_connector.get_unlabelled_contracts('20', '7') # top 20 contracts per chain from last week
        df4 = db_connector.get_unlabelled_stables_holders(20) # top 20 addresses per chain by stablecoin balance (metrics_stables_balances), reason in _comment

        # merge the all dataframes & reset index (stablecoin holders first, so their _comment survives the dedup below)
        df = pd.concat([df4, df0, df1, df2, df3])
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
        # dedup again: get_unlabelled_stables_holders returns hex strings, the other readers bytes
        df = df.drop_duplicates(subset=['address', 'origin_key'])

        # stablecoin balance (USD, latest measured day) for every address we track in metrics_stables_balances; empty otherwise
        df_bal = db_connector.get_latest_stables_balances_usd()
        df['_address_lower'] = df['address'].str.lower()
        df = df.merge(df_bal.rename(columns={'address': '_address_lower'}), on=['_address_lower', 'origin_key'], how='left')
        df['stablecoin_balance'] = df['stablecoin_balance'].round(0)
        df = df.drop(columns=['_address_lower'])

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
        from src.oli.api.oli_private_attesters import private_attester_notification_suffix
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

            def normalize_attester(value):
                if isinstance(value, memoryview):
                    value = value.tobytes()
                if isinstance(value, (bytes, bytearray)):
                    return '0x' + bytes(value).hex()
                return str(value).replace('\\x', '0x').lower()

            df_attested['_attester_key'] = df_attested['attester'].apply(normalize_attester)

            # from airtable, get the labels that are in the Label Pool Reattest table
            AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
            AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
            api = Api(AIRTABLE_API_KEY)
            table = api.table(AIRTABLE_BASE_ID, 'Label Pool Reattest')

            # read all approved labels in 'Label Pool Reattest'
            df_in_airtable = at.read_all_label_pool_reattest(api, AIRTABLE_BASE_ID, table, approved=False)

            ## create df_new with source attestations that are not already pending in Airtable
            if df_in_airtable is None:
                df_new = df_attested
            else:
                df_in_airtable = df_in_airtable[['address', 'chain_id', 'attester']]
                df_in_airtable['address'] = df_in_airtable['address'].str.replace('\\x', '0x')
                df_in_airtable['_attester_key'] = df_in_airtable['attester'].apply(normalize_attester)
                df_in_airtable = df_in_airtable[['address', 'chain_id', '_attester_key']]
                df_new = df_attested.merge(df_in_airtable, on=['address', 'chain_id', '_attester_key'], how='left', indicator=True)
                df_new = df_new[df_new['_merge'] == 'left_only']
                df_new = df_new.drop(columns=['_merge', '_attester_key'])
            if '_attester_key' in df_new.columns:
                df_new = df_new.drop(columns=['_attester_key'])

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
                suffix = private_attester_notification_suffix(df_new['attester'])
                send_discord_message(f"{df_new.shape[0]} new attestations submitted to label pool, please review in airtable.{suffix}", os.getenv('DISCORD_CONTRACTS'))
            else:
                print('No new attestations to submit to label pool.')
        else:
            print(f'No attestations since {yesterday}.')

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
            send_discord_message(f"<@1139314584671490089> Inactive projects with assigned contracts: {df['tag_value'].unique().tolist()}", os.getenv('DISCORD_CONTRACTS'))
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
            # keep track of Airtable ids per (lowercased address, chain_id) so only successfully attested rows get deleted
            ids_by_key = {}
            for rec_id, addr, cid in zip(df['id'], df['address'], df['chain_id']):
                ids_by_key.setdefault((str(addr).lower(), cid), []).append(rec_id)
            # keep columns address, origin_key and unpivot the other columns
            df = df[['address', 'chain_id', 'contract_name', 'owner_project', 'usage_category']]
            df = df.melt(id_vars=['address', 'chain_id'], var_name='tag_id', value_name='value')
            # turn address into lower case (IMPORTANT for later concatenation)
            df['address'] = df['address'].str.lower()
            # filter out rows with empty values
            df = df[df['value'].notnull()]
            # go one by one and attest the new labels
            succeeded, failed, failed_keys = [], [], []
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
                # submit_label never raises — it returns success=False on validation/API errors
                if response.get('success'):
                    succeeded.append(group[0])
                    print(f"Successfully attested label for {address} on {chain_id}: {response}")
                else:
                    failed.append((address, chain_id, response.get('error')))
                    failed_keys.append(group[0])
                    print(f"FAILED to attest label for {address} on {chain_id}: {response.get('error')}")
            # delete only successfully attested rows; failed (and tag-less) rows stay approved in Airtable for the next run
            ids = [rec_id for key in succeeded for rec_id in ids_by_key.get(key, [])]
            if ids:
                at.delete_airtable_ids(table, ids)
            skipped = set(ids_by_key) - set(succeeded) - set(failed_keys)
            if skipped:
                print(f"Skipped {len(skipped)} approved rows with no tag values (left in Airtable): {sorted(skipped)}")
            if failed:
                # Don't raise: read_remap >> refresh_views >> write_contracts >> ... depend on this task.
                # Failed rows stay approved in Airtable and are retried on the next run.
                msg = f"oli_airtable: {len(failed)} of {len(failed) + len(succeeded)} Label Pool Reattest attestations failed (rows kept in Airtable for retry): {failed}"
                print(msg)
                try:
                    from src.misc.helper_functions import send_discord_message
                    send_discord_message(msg[:1900])
                except Exception as e:
                    print(f"Discord alert failed: {e}")

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
            'vw_oli_label_pool_gold_pivoted_cross_chain_v1',
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
    read_remap = airtable_read_depreciated_owner_project() ## read in remap owner project from airtable and attest

    ## Refresh materialized views
    refresh_views = refresh_materialized_views()

    ## Write new unlabeled contracts and depreciated owner project to airtable from db
    write_contracts = airtable_write_contracts()  ## write contracts from DB to airtable
    # Label Pool Reattest writes are handled by the hourly oli_reattest DAG.
    # Do not also write them here: this DAG's 24h lookback can recreate rows
    # that reviewers manually deleted from Airtable.
    write_remap = airtable_write_depreciated_owner_project() ## write remap owner project from DB to airtable

    ## Revoke old attestations from label pool
    revoke_onchain = revoke_old_attestations() ## revoke old attestations from the label pool

    # Define execution order
    sync_categories >> sync_chains >> read_contracts >> read_pool >> read_remap >> refresh_views >> write_contracts >> write_remap >> revoke_onchain
    
etl()


## When writing to DB in general? when bronze and silver getting filled?

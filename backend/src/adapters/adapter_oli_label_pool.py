import time
import pandas as pd
from datetime import datetime
import os
import requests

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract, send_discord_message

class AdapterLabelPool(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("OLI Label Pool", adapter_params, db_connector)
        self.base_url = 'https://base.easscan.org/graphql'
        self.base_sepolia_url = 'https://base-sepolia.easscan.org/graphql' # testnet
        self.schemaId = '0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68' # OLI v1.0.0 schema
        self.webhook = os.getenv('DISCORD_CONTRACTS')
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        optional: time_created (int), from when to load data
    """
    def extract(self, load_params:dict = None):
        if load_params is None:
            # "time_created" is the time when the label is indexed, "time" is the time the user defines when he creates the label; therefore we use "time_created" to get the latest labels
            load_params = {'time_created': int(self.db_connector.get_max_value('oli_label_pool_bronze', 'time_created'))}
        self.load_params = load_params
        # get latest new attestations
        df_new = self.get_latest_new_attestations(self.load_params)
        # get latest revoked attestations
        df_rev = self.get_latest_revoked_attestations(self.load_params)
        # merge new and revoked attestations
        df = pd.concat([df_new, df_rev])
        # remove duplicates (can only happen if attested and revoked within 30min)
        df = df.drop_duplicates(subset=['id'], keep='last')
        print_extract(self.name, self.load_params, df.shape)
        return df

    def load(self, df:pd.DataFrame):
        # upsert attestations into bronze table
        if df.empty == False:
            # convert id, attester, recipient and tx_id columns to bytea
            df['id'] = df['id'].apply(lambda x: '\\x' + x[2:])
            df['attester'] = df['attester'].apply(lambda x: '\\x' + x[2:])
            df['recipient'] = df['recipient'].apply(lambda x: '\\x' + x[2:])
            df['tx_id'] = df['tx_id'].apply(lambda x: '\\x' + x[2:] if isinstance(x, str) and x.startswith('0x') else '\\x')
            # set id as index
            df = df.set_index('id')
            # load df into db, update row if id already exists
            self.db_connector.upsert_table('oli_label_pool_bronze', df, if_exists='update')
        print_load(self.name + ' bronze', {}, df.shape)

        # upsert attestations from bronze to silver table (starting from the time_created timestamp passed in extract function)
            # TODO: add more checks than just null, NaN, nan filters
        if df.empty == False:
            if self.upsert_from_bronze_to_silver(): # executes label_pool_from_bronze_to_silver.sql.j2
                print("Successfully upserted attestations from bronze to silver.")

        # add untrusted owner_project attestations to airtable (eventually to be replaced with trust algorithms)
        if df.empty == False:
            from eth_utils import to_checksum_address
            import src.misc.airtable_functions as at
            from pyairtable import Api
            import os
            # get new untrusted owner_project attestations
            df_air = self.db_connector.execute_jinja('/oli/extract_labels_for_review.sql.j2', self.load_params, load_into_df=True)
            if df_air.empty == False:
                # push to airtable
                AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
                AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
                api = Api(AIRTABLE_API_KEY)
                table = api.table(AIRTABLE_BASE_ID, 'Label Pool Reattest')
                df_air['address'] = df_air['address'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))
                df_air['attester'] = df_air['attester'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))
                # exchange the category with the id & make it a list
                cat = api.table(AIRTABLE_BASE_ID, 'Usage Categories')
                df_cat = at.read_airtable(cat)
                df_air = df_air.replace({'usage_category': df_cat.set_index('Category')['id']})
                df_air['usage_category'] = df_air['usage_category'].apply(lambda x: [x])
                # exchange the project with the id & make it a list
                proj = api.table(AIRTABLE_BASE_ID, 'OSS Projects')
                df_proj = at.read_airtable(proj)
                df_air = df_air.replace({'owner_project': df_proj.set_index('Name')['id']})
                df_air['owner_project'] = df_air['owner_project'].apply(lambda x: [x])
                # exchange the chain chain_id with the id & make it a list
                chains = api.table(AIRTABLE_BASE_ID, 'Chain List')
                df_chains = at.read_airtable(chains)
                df_air = df_air.replace({'chain_id': df_chains.set_index('caip2')['id']})
                df_air['chain_id'] = df_air['chain_id'].apply(lambda x: [x])
                # rename chain_id to origin_key
                df_air = df_air.rename(columns={'chain_id': 'origin_key'})
                # catch errors and log them in the error column (for owner_project and usage_category)
                df_air['error'] = None
                df_air['error'] = df_air.apply(
                    lambda row: (row['error'] or '') + f" failed owner_project: '{row['owner_project'][0]}'" 
                    if row['owner_project'][0] is not None and not row['owner_project'][0].startswith('rec') else row['error'], axis=1
                )
                df_air['error'] = df_air.apply(
                    lambda row: (row['error'] or '') + f" failed usage_category: '{row['usage_category'][0]}'" 
                    if row['usage_category'][0] is not None and not row['usage_category'][0].startswith('rec') else row['error'], axis=1
                )
                df_air['usage_category'] = df_air['usage_category'].apply(lambda x: x if x[0] is not None and x[0].startswith('rec') else [])
                df_air['owner_project'] = df_air['owner_project'].apply(lambda x: x if x[0] is not None and x[0].startswith('rec') else [])
                # write to airtable df
                at.push_to_airtable(table, df_air)
                # send discord message
                send_discord_message(f"{df_air.shape[0]} new attestations submitted to label pool, please review in airtable.", self.webhook)
                #send_discord_message(f"```\n{df_air.to_markdown(index=False)}\n```", self.webhook)

    ## ----------------- Helper functions --------------------

    def upsert_from_bronze_to_silver(self):
        # upsert attestations from bronze to silver
        self.db_connector.execute_jinja('/oli/label_pool_from_bronze_to_silver.sql.j2', {'time_created': self.load_params['time_created']})
        return True # return True if successful (no exception)

    def get_latest_new_attestations(self, load_params):
        # get latest attestations
        j = self.graphql_query_attestations(self.schemaId, 999999, load_params['time_created'], 0)
        if len(j['data']['attestations']) == 0:
            return pd.DataFrame()
        # create df from json
        df = pd.DataFrame(j['data']['attestations'])
        # rename columns & sort
        df = df[['id', 'attester', 'recipient', 'isOffchain', 'revoked', 'ipfsHash', 'txid', 'decodedDataJson', 'time', 'timeCreated', 'revocationTime']]
        df = df.rename(columns={'isOffchain': 'is_offchain', 'ipfsHash': 'ipfs_hash', 'txid': 'tx_id', 'decodedDataJson': 'decoded_data_json', 'timeCreated': 'time_created', 'revocationTime': 'revocation_time'})
        # return df with new attestations
        return df

    def get_latest_revoked_attestations(self, load_params):
        # get latest revoked attestations
        j = self.graphql_query_attestations(self.schemaId, 999999, 0, load_params['time_created'])
        if len(j['data']['attestations']) == 0:
            return pd.DataFrame()
        # create df from json
        df = pd.DataFrame(j['data']['attestations'])
        # rename columns & sort
        df = df[['id', 'attester', 'recipient', 'isOffchain', 'revoked', 'ipfsHash', 'txid', 'decodedDataJson', 'time', 'timeCreated', 'revocationTime']]
        df = df.rename(columns={'isOffchain': 'is_offchain', 'ipfsHash': 'ipfs_hash', 'txid': 'tx_id', 'decodedDataJson': 'decoded_data_json', 'timeCreated': 'time_created', 'revocationTime': 'revocation_time'})
        # return df with revoked attestations
        return df
        
    def graphql_query_attestations(self, schemaId, count, timeCreated, revocationTime):
        query = """
            query Attestations($take: Int, $where: AttestationWhereInput, $orderBy: [AttestationOrderByWithRelationInput!]) {
            attestations(take: $take, where: $where, orderBy: $orderBy) {
                attester
                decodedDataJson
                expirationTime
                id
                ipfsHash
                isOffchain
                recipient
                refUID
                revocable
                revocationTime
                revoked
                time
                timeCreated
                txid
            }
            }
        """
            
        variables = {
            "take": count,
            "where": {
                "schemaId": {
                    "equals": schemaId
                },
                "timeCreated": {
                    "gt": timeCreated
                },
                "revocationTime": {
                    "gte": revocationTime
                }
            },
            "orderBy": [
                {
                "timeCreated": "desc"
                }
            ]
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        response = requests.post(self.base_url, json={"query": query, "variables": variables}, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"GraphQL query failed with status code {response.status_code}: {response.text}")


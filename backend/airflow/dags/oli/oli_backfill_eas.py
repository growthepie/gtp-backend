from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='oli_backfill_eas',
    description='This DAG backfills offchain attestations made to the OLI label pool via EAS (instead of using our new API).',
    tags=['oli'],
    start_date=datetime(2025,11,11),
    schedule='0 23 * * *',  # Runs every day at 11:00 PM
)

def etl(): 
    @task()
    def run_backfill():
        import requests
        import pandas as pd
        import json
        from web3 import Web3
        from src.db_connector import DbConnector

        db_connector = DbConnector(db_name='oli')
        w3 = Web3()

        count = 10000
        schemaId = '0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68' # OLI v1.0.0 schema
        
        timeCreated = db_connector.execute_query("""
                SELECT EXTRACT(EPOCH FROM MAX(time)) AS value
                FROM attestations
                WHERE source = 'eas_graphql' """, load_df=True)
        timeCreated = int(timeCreated.value[0]) + 1

        ## define chain_str and url (i.e. Base or OP Mainnet)
        chain_str = '8453'
        url = 'https://base.easscan.org/graphql'

        def decode_hex_to_json(hex_with_0x: str):
            """Decode ABI-encoded ['string', 'string'] -> return readable JSON string."""
            try:
                s1, s2 = w3.codec.decode(['string', 'string'], bytes.fromhex(hex_with_0x[2:]))
                obj = json.loads(s2)
                obj["chain_id"] = s1
                return json.dumps(obj, ensure_ascii=False)
            except Exception:
                return hex_with_0x  # fallback: keep original
            
        def prep_df(df: pd.DataFrame, chain_str) -> pd.DataFrame:
            """Prepare DataFrame for insertion into DB."""

            mask = (~df["isOffchain"]) & df["data"].astype("string").str.startswith("0x")
            df.loc[mask, "data"] = df.loc[mask, "data"].map(decode_hex_to_json)

            ## make timestamps out of unix time, timeCreated, expirationTime, revocationTime
            cols = ['time', 'expirationTime', 'revocationTime']
            for col in cols:
                df[col] = pd.to_datetime(df[col], unit='s')

            ## make sure hashes are bytea compatible
            cols = ['id', 'attester', 'recipient', 'txid']
            for col in cols:
                df[col] = df[col].apply(lambda x: bytes.fromhex(x[2:]) if x.startswith('0x') else x)

            # Assuming df["decodedDataJson"] is your column with JSON strings
            df["chain_id"] = df["decodedDataJson"].apply(
                lambda x: json.loads(x)[0]["value"]["value"]
            )

            df["tags_json"] = df["decodedDataJson"].apply(
                lambda x: json.loads(x)[1]["value"]["value"]
            )
            ## if tags_json is not valid json, set to null
            df["tags_json"] = df["tags_json"].apply(
                lambda x: x if isinstance(x, str) and x.startswith('{') and x.endswith('}') else None 
            )

            df['last_updated_time'] = pd.Timestamp.now()
            df['schema_info'] = chain_str + '__' + schemaId

            ## rename df columns
            df = df.rename(columns={'id': 'uid', 'expirationTime': 'expiration_time', 'revocationTime': 'revocation_time', 'isOffchain': 'is_offchain', 'ipfsHash': 'ipfs_hash', 'txid': 'tx_hash', 'data': 'raw'})

            df = df[['uid', 'time', 'chain_id', 'attester', 'recipient', 'revoked', 'is_offchain', 'tx_hash', 'ipfs_hash', 'revocation_time', 'tags_json', 'raw', 'last_updated_time', 'schema_info']]

            df['source'] = 'eas_graphql'    
            df.set_index('uid', inplace=True)
            return df

        def graphql_query_attestations(url, schemaId, count, timeCreated):
            query = """
                query Attestations($take: Int, $where: AttestationWhereInput, $orderBy: [AttestationOrderByWithRelationInput!]) {
                attestations(take: $take, where: $where, orderBy: $orderBy) {
                    id
                    data
                    decodedDataJson
                    recipient
                    attester
                    time
                    timeCreated
                    expirationTime
                    revocationTime
                    refUID
                    revocable
                    revoked
                    txid
                    schemaId
                    ipfsHash
                    isOffchain
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
                    "isOffchain": {
                        "equals": True
                    }
                },
                "orderBy": [
                    {
                    "timeCreated": "asc"
                    }
                ]
            }
            
            headers = {
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, json={"query": query, "variables": variables}, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Query failed with status code {response.status_code}: {response.text}")
            
        while True:
            print(f"Fetching attestations after timeCreated={timeCreated}...")
            result = graphql_query_attestations(url, schemaId, count, timeCreated)
            df = pd.DataFrame(result['data']['attestations'])
            print(f"Fetched {len(df)} attestations.")
            if len(df)>0:
                timeCreated = int(df['timeCreated'].max())
                df = prep_df(df, chain_str)
                db_connector.upsert_table('attestations', df, if_exists='ignore')
            else:
                print("No attestations fetched.")
                break
            if len(df) < count:
                print("No more attestations to fetch. Exiting.")
                break
            print(f"Inserted {len(df)} attestations into the database. New timeCreated={timeCreated}.")
    
    run_backfill()

etl()
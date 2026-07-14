from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Define the DAG and task using decorators
# for ipfs we use the platform "filebase"
@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 1,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='oli_indexer_ipfs',
    description='Uploads offchain OLI attestations to IPFS and updates the database with the IPFS hashes',
    tags=['oli'],
    start_date=datetime(2025, 10, 27),
    schedule='0 * * * *',  # Runs every hour
    catchup=False  # Prevents backfilling
)

def main():

    @task()
    def sync():

        import time
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_json_to_filebase_ipfs
        import concurrent.futures

        db_connector = DbConnector(db_name='oli')

        ## track time ##
        start_time = time.time()

        while True:

            ## fetch data to upload ##
            query_select = """
                SELECT 
                    uid, 
                    time, 
                    is_offchain, 
                    raw, 
                    schema_info,
                    table_name
                FROM (
                    SELECT uid, time, is_offchain, raw, schema_info, ipfs_hash, 'attestations' AS table_name
                    FROM public.attestations
                    UNION ALL
                    SELECT uid, time, is_offchain, raw, schema_info, ipfs_hash, 'trust_lists' AS table_name
                    FROM trust_lists
                ) AS combined_tables
                WHERE is_offchain = true
                    AND (ipfs_hash IS NULL OR ipfs_hash = '')
                ORDER BY time
                LIMIT 250
            """
            df = db_connector.execute_query(query_select, load_df=True)
            df['uid'] = df['uid'].apply(lambda x: '0x' + x.hex())

            ## pause if we have no more data to process ##
            if df.empty:
                time.sleep(30)
                continue

            ## upload to IPFS ##
            bucket = 'oli-offchain-attestations'

            # Function to upload a single row
            def upload_row(row):
                path_name = f'{row["table_name"]}/{row["schema_info"]}/{row["uid"]}.json'
                cid = upload_json_to_filebase_ipfs(bucket, path_name, row['raw'])
                print(f'Uploaded UID {row["uid"]} to IPFS with CID: {cid}')
                return {
                    'uid': row['uid'],
                    'cid': cid
                }

            # Upload all files concurrently with 10 workers
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_schema = {
                    executor.submit(upload_row, row): row.get('table_name') 
                    for _, row in df.iterrows()
                }
                for future in concurrent.futures.as_completed(future_to_schema):
                    try:
                        result = future.result()
                        table_name = future_to_schema[future]       
                        results.append(result | {'table_name': table_name})
                    except Exception as e:
                        print(f'Error uploading: {e}')

            # Group results by table_name
            if results:
                results_by_table = {}
                for result in results:
                    table_name = result['table_name']
                    if table_name not in results_by_table:
                        results_by_table[table_name] = []
                    results_by_table[table_name].append(result)
                
                # Batch update for each table
                for table_name, table_results in results_by_table.items():
                    values_list = []
                    for result in table_results:
                        uid_hex = result['uid'][2:] if result['uid'].startswith('0x') else result['uid']
                        values_list.append(f"(decode('{uid_hex}', 'hex'), '{result['cid']}')")
                    
                    values_str = ',\n        '.join(values_list)
                    
                    query_update = f"""
                        UPDATE public.{table_name} AS t
                        SET
                            ipfs_hash = v.cid,
                            last_updated_time = NOW()
                        FROM (VALUES
                            {values_str}
                        ) AS v(uid, cid)
                        WHERE t.uid = v.uid;
                    """
                    
                    db_connector.execute_query(query_update)
                    print(f'Updated {len(table_results)} rows in table {table_name}')

            ## break the loop if time reached ##
            if time.time() - start_time >= 59 * 60:  # 59 minutes
                print("⏰ Approaching task timeout, exiting loop to allow for graceful restart.")
                break

            time.sleep(1)

    sync()

main()

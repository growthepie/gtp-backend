from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Define the DAG and task using decorators
@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=15),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_website_analytics',
    description='Create json files for the OLI website analytics page',
    tags=['oli', 'daily'],
    start_date=datetime(2025, 9, 25),
    schedule='01 00 * * *', # Runs daily at 00:01
    catchup=False  # Prevents backfilling
)

def main():

    @task()
    def create_json_files():

        from src.db_connector import DbConnector
        from datetime import datetime
        from src.misc.jinja_helper import execute_jinja_query
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        import os
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")
        db_connector_oli = DbConnector(db_name='oli')


        ### Attester analytics
        # get all attesters
        query_parameters = {}
        df = execute_jinja_query(db_connector_oli, "oli/analytics_all_attesters.sql.j2", query_parameters, return_df=True)
        # get timestamp of 2 days ago
        threshold_time = datetime.now().astimezone() - timedelta(days=2)
        # Now compare datetime to datetime
        df = df[(df['last_time_created'] > threshold_time) | (df['last_time_created'].isna())]
        df = df[(df['last_time_revoked'] > threshold_time) | (df['last_time_revoked'].isna())]
        # iterate over each attester in df and create custom json file for each attester
        for i, row in df.iterrows():
            query_parameters = {"attester": row['attester']}
            df_att = execute_jinja_query(db_connector_oli, "oli/analytics_count_by_attester.sql.j2", query_parameters, return_df=True)
            query_parameters = {"attester": row['attester'], "take": 25}
            df_att_latest = execute_jinja_query(db_connector_oli, "oli/analytics_latest_by_attester.sql.j2", query_parameters, return_df=True)
            # turn time_created into unix timestamp
            df_att_latest['time_created'] = df_att_latest['time_created'].apply(lambda x: int(x.timestamp()) if x is not None else None)
            data_dict = {
                "data": {
                    "timestamp": int(datetime.now().timestamp()),
                    "attester": "0x" + row['attester'],
                    "totals": {
                        "types": ["chain_id", "tag_id", "row_count"],
                        "values": [[chain_id, tag_id, row_count] for chain_id, tag_id, row_count in zip(
                            df_att['chain_id'].tolist(),
                            df_att['tag_id'].tolist(),
                            df_att['row_count'].tolist()
                        )]
                    },
                    "latest_25_attestations": {
                        "types": ["id", "attester", "recipient", "is_offchain", "revoked", "ipfs_hash", "tx_id", "decoded_data_json", "time", "time_created", "revocation_time"],
                        "values": [[
                            row['id'],
                            row['attester'],
                            row['recipient'],
                            row['is_offchain'],
                            row['revoked'],
                            row['ipfs_hash'],
                            row['tx_id'],
                            row['chain_id'],
                            row['tags_json'],
                            row['time_created']
                        ] for index, row in df_att_latest.iterrows()]
                    }
                }
            }
            # fix NaN values in the data_dict
            data_dict = fix_dict_nan(data_dict, f"attester_{row['attester']}", send_notification=False)
            # Upload to S3 & invalidate
            upload_json_to_cf_s3(s3_bucket, f"v1/oli/analytics/attester/{row['attester']}", data_dict, cf_distribution_id, invalidate=False)


        ### Totals by chain & tag id
        query_parameters = {}
        df_totals = execute_jinja_query(db_connector_oli, "oli/analytics_count_chain_tag_id.sql.j2", query_parameters, return_df=True)
        data_dict = {
            "data": {
                "timestamp": int(datetime.now().timestamp()),
                "attester": "all",
                "totals": {
                    "types": ["chain_id", "tag_id", "row_count"],
                    "values": [[chain_id, tag_id, row_count] for chain_id, tag_id, row_count in zip(
                        df_totals['chain_id'].tolist(),
                        df_totals['tag_id'].tolist(),
                        df_totals['row_count'].tolist()
                    )]
                }
            }
        }
        # fix NaN values in the data_dict
        data_dict = fix_dict_nan(data_dict, "attester_all")
        # Upload to S3 & invalidate
        upload_json_to_cf_s3(s3_bucket, "v1/oli/analytics/totals_chain_tag", data_dict, cf_distribution_id, invalidate=False)


        ### Totals overall
        query_parameters = {}
        df_totals = execute_jinja_query(db_connector_oli, "oli/analytics_totals.j2", query_parameters, return_df=True)
        data_dict = {
            "data": {
                "timestamp": int(datetime.now().timestamp()),
                "count_tags": int(df_totals["count_tags"].iloc[0]),
                "count_attestations": int(df_totals["count_attestations"].iloc[0]),
                "revoked_count_attestations": int(df_totals["revoked_count_attestations"].iloc[0]),
                "offchain_count_tags": int(df_totals["offchain_count_tags"].iloc[0]),
                "offchain_count_attestations": int(df_totals["offchain_count_attestations"].iloc[0]),
                "onchain_count_tags": int(df_totals["onchain_count_tags"].iloc[0]),
                "onchain_count_attestations": int(df_totals["onchain_count_attestations"].iloc[0])
            }
        }
        # fix NaN values in the data_dict
        data_dict = fix_dict_nan(data_dict, "totals")
        # Upload to S3 & invalidate
        upload_json_to_cf_s3(s3_bucket, "v1/oli/analytics/totals", data_dict, cf_distribution_id, invalidate=False)


        ### invalidate cloudfront cache
        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/oli/analytics/*')


    create_json_files()

main()

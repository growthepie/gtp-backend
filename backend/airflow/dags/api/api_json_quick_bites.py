import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

api_version = "v1"

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=1),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_json_quick_bites',
    description='Create json files for the Quick Bites section.',
    tags=['api', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='20 05 * * *'
)

def json_creation():
    @task()
    def run_pectra_fork():        
        
        import datetime
        import pandas as pd
        from datetime import datetime, timezone
        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        data_dict = {
            "data": {
                "ethereum_blob_count" : {},
                "ethereum_blob_target" : {},
                "type4_tx_count" : {
                    "ethereum": {},
                    "optimism": {},
                    "base": {},
                    "unichain": {},
                    "arbitrum": {}
                }
            }    
        }

        ## Ethereum blob count and target
        query_parameters = {}
        df = execute_jinja_query(db_connector, "api/quick_bites/select_ethereum_blob_count_per_block.sql.j2", query_parameters, return_df=True)
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['date'])

        df_blob_count = df[['unix', 'blob_count']]
        data_dict["data"]["ethereum_blob_count"]= {
            "daily": {
                "types": df_blob_count.columns.tolist(),
                "values": df_blob_count.values.tolist()
            }
        }

        df_blob_target = df[['unix', 'blob_target']]
        data_dict["data"]["ethereum_blob_target"]= {
            "daily": {
                "types": df_blob_target.columns.tolist(),
                "values": df_blob_target.values.tolist()
            }
        }    

        ##type4 tx count
        for origin_key in ['ethereum', 'optimism', 'base', 'unichain', 'arbitrum']:
            query_parameters = {
                'origin_key': origin_key,
                'metric_key': 'txcount_type4',
                'days': 120
            }
            df = execute_jinja_query(db_connector, "api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
            df.sort_values(by=['date'], inplace=True, ascending=True)
            df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
            df = df.drop(columns=['date'])

            data_dict["data"]["type4_tx_count"][origin_key]= {
                "daily": {
                    "types": df.columns.tolist(),
                    "values": df.values.tolist()
                }
            }

        data_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        data_dict = fix_dict_nan(data_dict, 'pectra-fork')

        upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/pectra-fork', data_dict, cf_distribution_id)

    @task()
    def run_arbitrum_timeboost():
        import datetime
        import pandas as pd
        from datetime import datetime, timezone
        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        data_dict = {
            "data": {
                "fees_paid_base_eth" : {},
                "fees_paid_priority_eth" : {}
            }    
        }    

        for metric_key in ['fees_paid_base_eth', 'fees_paid_priority_eth', 'fees_paid_priority_usd']:
            query_parameters = {
                'origin_key': 'arbitrum',
                'metric_key': metric_key,
                'days': (datetime.now(timezone.utc) - datetime(2025, 4, 10, tzinfo=timezone.utc)).days ## days since '2025-04-10' to today
            }
            df = execute_jinja_query(db_connector, "api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
            df.sort_values(by=['date'], inplace=True, ascending=True)
            df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
            df = df.drop(columns=['date'])

            data_dict["data"][metric_key]= {
                "daily": {
                    "types": df.columns.tolist(),
                    "values": df.values.tolist()
                }
            }

            if metric_key in ['fees_paid_priority_eth', 'fees_paid_priority_usd']:
                data_dict["data"][metric_key]["total"] = df['value'].sum()

                data_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                data_dict = fix_dict_nan(data_dict, 'arbitrum-timeboost')

        upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/arbitrum-timeboost', data_dict, cf_distribution_id)
        
    @task()
    def run_shopify_usdc():
        import datetime
        import pandas as pd
        from datetime import datetime, timezone
        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        data_dict = {
            "data": {
                "gross_volume_usdc" : {},
                "total_unique_merchants": {},
                "total_unique_payers": {},
                "new_payers": {},
                "returning_payers": {},
                "new_merchants": {},
                "returning_merchants": {},
            }    
        }    

        for metric_key in ['gross_volume_usdc', 'total_unique_merchants', 'total_unique_payers', 'new_payers', 'returning_payers', 'new_merchants', 'returning_merchants']:
            query_parameters = {
                'origin_key': 'shopify_usdc',
                'metric_key': metric_key,
                'days': (datetime.now(timezone.utc) - datetime(2025, 6, 1, tzinfo=timezone.utc)).days ## days since '2025-06-01' to today
            }
            df = execute_jinja_query(db_connector, "api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
            df.sort_values(by=['date'], inplace=True, ascending=True)
            
            ## fill missing dates with 0
            df_all_dates = pd.DataFrame({'date': pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')})
            df = pd.merge(df_all_dates, df, on='date', how='left')
            df['value'] = df['value'].fillna(0)  # Fill NaN values with 0
            
            df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
            df = df.drop(columns=['date'])
                
            ## Get over time data for charts
            if metric_key in ['gross_volume_usdc', 'new_payers', 'returning_payers', 'new_merchants', 'returning_merchants']:
                data_dict["data"][metric_key]= {
                    "daily": {
                        "types": df.columns.tolist(),
                        "values": df.values.tolist()
                    }
                }

            ## Get total for KPI cards as sum
            if metric_key in ['gross_volume_usdc']:
                data_dict["data"][metric_key]["total"] = df['value'].sum()

            ## Get total for KPI cards as highest value
            if metric_key in ['total_unique_merchants', 'total_unique_payers']:
                data_dict["data"][metric_key]["total"] = df['value'].max()

        data_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        data_dict = fix_dict_nan(data_dict, 'shopify-usdc')

        upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/shopify-usdc', data_dict, cf_distribution_id)

    run_pectra_fork()    
    run_arbitrum_timeboost()
    run_shopify_usdc()

json_creation()
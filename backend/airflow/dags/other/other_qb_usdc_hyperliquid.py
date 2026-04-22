from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='other_qb_usdc_hyperliquid',
    description='Data for USDC on Arbitrum bridge to Hyperliquid.',
    tags=['other'],
    start_date=datetime(2025,9,9),
    schedule='24 3 * * *' # Every day at 03:24
)

def run_dag():
    @task()
    def get_USDC_in_bridge_supply():
        from web3 import Web3
        import pandas as pd
        from src.db_connector import DbConnector

        # Initialize DB Connector
        db_connector = DbConnector()

        # Get block numbers for source chain
        df_blocknumbers = db_connector.get_data_from_table(
                "fact_kpis", 
                filters={
                    "metric_key": "first_block_of_day",
                    "origin_key": "arbitrum"
                },
                days=3
            )

        rpc_url = db_connector.get_special_use_rpc("arbitrum")
        w3 = Web3(Web3.HTTPProvider(rpc_url))

        # usdc balanceOf function 
        contract = w3.eth.contract(address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831", abi=[
            {
                "constant":True,
                "inputs":[{"name":"_owner","type":"address"}],
                "name":"balanceOf",
                "outputs":[{"name":"balance","type":"uint256"}],
                "payable":False,
                "stateMutability":"view",
                "type":"function"
            }
        ])

        df = pd.DataFrame()
        df['date'] = df_blocknumbers['date']
        df['metric_key'] = 'qb_hyperliquid_bridge_usdc'
        df['origin_key'] = 'arbitrum'

        for i in range(len(df_blocknumbers)-1, -1, -1):  # Go backwards in time
            balance = contract.functions.balanceOf("0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7").call(block_identifier= int(df_blocknumbers.iloc[i]['value'])) / 10**6
            df.at[i, 'value'] = balance
            print(f"Balance: {balance} on date: {df_blocknumbers.iloc[i]['date']}")

        df = df.set_index(['metric_key', 'origin_key', 'date'])
        db_connector.upsert_table('fact_kpis', df)

    @task()
    def create_json():
        from src.adapters.adapter_defillama import AdapterDefillama
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        import pandas as pd
        import os

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        # get data from db
        df = execute_jinja_query(db_connector, "api/quick_bites/hyperliquid_usdc_arb.sql.j2", query_parameters={}, return_df=True)
        dt = pd.to_datetime(df['date'], errors="raise", utc=True)
        df['unix_timestamp'] = (
            (dt - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
        ).astype("int64")
        df = df.sort_values('date').reset_index(drop=True)
        df['date'] = pd.to_datetime(df['date'])

        # get data from defillama adapter
        ll = AdapterDefillama({}, db_connector)
        df1 = ll.df_ethereum.loc[ll.df_ethereum['protocol'] == 'Circle USDC']
        df2 = ll.stables_dfs[2]
        df_merged = df1.merge(df2, how='left', left_on='unix', right_on='unix')
        df_merged['rev_per_usdc'] = df_merged['value'] / df_merged['circ_total']
        df_merged = df_merged.drop(columns=['protocol', 'value', 'unix'])
        df_merged = df_merged[df_merged['date'] >= '2024-01-01'].reset_index(drop=True)

        # combine db data and defillama data into df_final
        df_final = df.merge(df_merged, how='left', left_on='date', right_on='date')
        df_final['circ_without_hl'] = df_final['circ_total'] - df_final['hyperliquid_usdc']

        # Create data_dict, for time series
        data_dict = {
            "data": {
                "hyperliquid_usdc": {
                    "daily": {
                        "types": ["unix", "stables_on_arb_with_hl", "stables_on_arb_without_hl", "hyperliquid_usdc", "circ_total", "circ_without_hl", "rev_per_usdc"],
                        "values": [[unix_timestamp, stables_on_arb_with_hl, stables_on_arb_without_hl, hyperliquid_usdc, circ_total, circ_without_hl, rev_per_usdc] for unix_timestamp, stables_on_arb_with_hl, stables_on_arb_without_hl, hyperliquid_usdc, circ_total, circ_without_hl, rev_per_usdc in zip(
                            df_final['unix_timestamp'].tolist(),
                            df_final['stables_on_arb_with_hl'].tolist(),
                            df_final['stables_on_arb_without_hl'].tolist(),
                            df_final['hyperliquid_usdc'].tolist(),
                            df_final['circ_total'].tolist(),
                            df_final['circ_without_hl'].tolist(),
                            df_final['rev_per_usdc'].tolist()
                        )]
                    }
                }
            }
        }

        # Create kpi_dict, for KPI cards
        total_revenue_for_circle = (df_final['hyperliquid_usdc'] * df_final['rev_per_usdc']).sum()
        hyperliquid_usdc_last = df_final['hyperliquid_usdc'].iloc[-1]
        percentage_hyperliquid_of_circle = round(hyperliquid_usdc_last / df_final['circ_total'].iloc[-1], 4) * 100
        estimates_yearly_revenue_hyperliquid_circle = round(df_final['rev_per_usdc'].iloc[-1] * hyperliquid_usdc_last * 365, 2)
        kpi_dict = {
            "data": {
                "total_revenue_for_circle": total_revenue_for_circle,
                "hyperliquid_usdc_last": hyperliquid_usdc_last,
                "percentage_hyperliquid_of_circle": percentage_hyperliquid_of_circle,
                "estimates_yearly_revenue_hyperliquid_circle": estimates_yearly_revenue_hyperliquid_circle
            }
        }

        # fix NaN values in the data_dict
        data_dict = fix_dict_nan(data_dict, 'hyperliquid_usdc')
        kpi_dict = fix_dict_nan(kpi_dict, 'kpis')

        # Upload to S3 & invalidate
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/hyperliquid/usdc', data_dict, cf_distribution_id, invalidate=False)
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/hyperliquid/kpis', kpi_dict, cf_distribution_id, invalidate=False)

        # empty_cloudfront_cache
        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/hyperliquid/*')

    get_USDC_in_bridge_supply_task = get_USDC_in_bridge_supply()
    create_json_task = create_json()

    get_USDC_in_bridge_supply_task >> create_json_task
    
run_dag()

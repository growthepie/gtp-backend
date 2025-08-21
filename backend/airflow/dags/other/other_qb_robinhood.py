import sys
import getpass

sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta,timezone
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='other_qb_robinhood',
    description='Data for Robinhood stock tracker.',
    tags=['other'],
    start_date=datetime(2025,7,22),
    schedule='12 1 * * *' # Every day at 01:12
)

def run_dag():

    @task.branch(task_id="decide_branch")
    def decide_branch():
        """Decide which branch to execute based on the day of the week"""
        # Use current UTC time
        current_utc = datetime.now(timezone.utc)
        day_of_week = current_utc.weekday()
        
        print(f"Current UTC time: {current_utc}")
        
        if day_of_week in [6, 0]:  # Sunday or Monday
            print("Choosing json_only_branch")
            return 'json_only_branch'
        else:  # Tuesday through Saturday
            print("Choosing full_pipeline_branch")
            return 'full_pipeline_branch'

    # Empty operators for branching
    json_only_branch = EmptyOperator(task_id='json_only_branch')
    full_pipeline_branch = EmptyOperator(task_id='full_pipeline_branch')

    @task()
    def pull_data_from_dune():      
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)

        # first load new daily values for all tokenized assets
        load_params = {
            'queries': [
                {
                    'name': 'Robinhood_stock_daily',
                    'query_id': 5435746,
                    'params': {'days': 3}
                }
            ],
            'prepare_df': 'prepare_robinhood_daily',
            'load_type': 'robinhood_daily'
        }
        df = ad.extract(load_params)
        ad.load(df)

        # load list of known tokenized assets from db
        current_list = db_connector.get_table('robinhood_stock_list')

        # find any unknown tokenized assets based on the contract_address unique identifier
        unknown = (
                df.reset_index()['contract_address']
                .drop_duplicates()
                .to_frame()
                .merge(current_list[['contract_address']], 
                        on='contract_address', 
                        how='left', 
                        indicator=True)
                .query("_merge == 'left_only'")
            )

        if not unknown.empty:
            print(f"Found {unknown.shape[0]} unknown tokenized assets. Fetching updated stock list from Dune...")

            # load list of new tokenized assets from Dune
            load_params2 = {
                    'queries': [
                        {
                            'name': 'Robinhood_stock_list',
                            'query_id': 5429585,
                            'params': {'days': 90}
                        }
                    ],
                    'prepare_df': 'prepare_robinhood_list',
                    'load_type': 'robinhood_stock_list'
                }
            df = ad.extract(load_params2)
            ad.load(df)

    @task()
    def pull_data_from_yfinance():
        from src.adapters.adapter_yfinance_stocks import AdapterYFinance
        from src.db_connector import DbConnector

        db = DbConnector()
        yfi = AdapterYFinance({}, db)

        load_params = {
                'tickers': ['*'],
                'endpoints': ['Close'],
                'days': 5,
                'table': 'robinhood_daily',
                'prepare_df': 'prepare_df_robinhood_daily'
            }

        df = yfi.extract(load_params)
        yfi.load(df)

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def create_json_file():
        import pandas as pd
        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        ### Load the first dataset for daily
        df = execute_jinja_query(db_connector, "api/quick_bites/robinhood_merged_daily.sql.j2", query_parameters={}, return_df=True)
        ticker_list = df['ticker'].unique().tolist()
        df['unix_timestamp'] = pd.to_datetime(df['date']).astype(int) // 10**6  # Convert to milliseconds

        def find_last_zero_value_index(series: pd.Series) -> int:
            """
            Find the index of the last zero value in series before non-zero values begin.
            Returns -1 if no zeros found or series is empty.
            """
            # Find all indices where value is 0
            zero_indices = series[series == 0].index.tolist()
            
            if len(zero_indices) == 0:
                return -1
            
            # Return the last zero index
            return series.index.get_loc(zero_indices[-1])

        for ticker in ticker_list:
            # Initialize data_dict for each ticker
            data_dict = {"data": {}}

            # filter the DataFrame for the current ticker
            ticker_data = df[df['ticker'] == ticker].copy()
            
            # Sort by date to ensure proper chronological order
            ticker_data = ticker_data.sort_values('date').reset_index(drop=True)
            
            # Find the last zero total_market_value index
            last_zero_idx = find_last_zero_value_index(ticker_data['total_market_value'])
            
            # Skip ticker if no zeros found
            if last_zero_idx < 0:
                continue
            
            # Filter data from last zero onwards
            filtered_data = ticker_data.iloc[last_zero_idx:]
            
            # Create combined values array: [unix_timestamp, close_price_used, total_supply]
            values = [
                [row['unix_timestamp'], row['close_price_used'], row['total_supply']] 
                for _, row in filtered_data.iterrows()
            ]
            
            # Create the data_dict for the current ticker
            data_dict["data"] = {
                "daily": {
                    "types": ["unix", "close_price", "stock_outstanding"],
                    "values": values
                },
                "ticker": ticker,
                "name": filtered_data['name'].iloc[0]
            }

            # Fix any NaN values in the data_dict
            data_dict = fix_dict_nan(data_dict, f'robinhood_daily_{ticker}')

            # Upload to S3
            upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/robinhood/stocks/{ticker}', data_dict, cf_distribution_id, invalidate=False)


        ### Load the second dataset for totals
        df2 = execute_jinja_query(db_connector, "api/quick_bites/robinhood_totals_daily.sql.j2", query_parameters={}, return_df=True)
        df2['unix_timestamp'] = pd.to_datetime(df2['date']).astype(int) // 10**6  # Convert to milliseconds

        # Sort by date to ensure proper chronological order
        df2 = df2.sort_values('date').reset_index(drop=True)

        # Find the last zero total_market_value_sum index
        last_zero_idx = find_last_zero_value_index(df2['total_market_value_sum'])

        # Filter data from last zero onwards (skip if no zeros found)
        if last_zero_idx >= 0:
            filtered_df2 = df2.iloc[last_zero_idx:]
        else:
            filtered_df2 = df2  # Keep all data if no zeros found

        # calculate percentage change
        total_market_value_sum_usd = filtered_df2['total_market_value_sum'].iloc[-1] if not filtered_df2.empty else None
        perc_change_market_value_usd_1d = filtered_df2['total_market_value_sum'].pct_change().iloc[-1] * 100 if not filtered_df2.empty else None
        perc_change_market_value_usd_7d = filtered_df2['total_market_value_sum'].pct_change(periods=7).iloc[-1] * 100 if not filtered_df2.empty else None
        perc_change_market_value_usd_30d = filtered_df2['total_market_value_sum'].pct_change(periods=30).iloc[-1] * 100 if not filtered_df2.empty else None
        #perc_change_market_value_usd_365d = filtered_df2['total_market_value_sum'].pct_change(periods=365).iloc[-1] * 100 if not filtered_df2.empty else None

        # Create data_dict2 for the totals data
        data_dict2 = {
            "data": {
                "total_market_value_sum": {
                    "daily": {
                        "types": ["value", "unix"],
                        "values": [[value, timestamp] for value, timestamp in zip(
                            filtered_df2['total_market_value_sum'].tolist(),
                            filtered_df2['unix_timestamp'].tolist()
                        )]
                    }
                }
            }
        }

        # fix NaN values in the data_dict2
        data_dict2 = fix_dict_nan(data_dict2, 'robinhood_totals')

        # Upload to S3
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/robinhood/totals', data_dict2, cf_distribution_id, invalidate=False)


        ### Load the stock table
        df3 = execute_jinja_query(db_connector, "api/quick_bites/robinhood_stock_table.sql.j2", query_parameters={}, return_df=True)

        # replace NaN with 0
        df3 = df3.fillna(0)

        # Define columns and types
        columns = [
            "contract_address",
            "ticker", 
            "name",
            "usd_outstanding",
            "stocks_tokenized",
            "stocks_tokenized_7d_change_pct",
            "usd_stock_price"
        ]

        types = [
            "string",
            "string", 
            "string",
            "number",
            "number",
            "number",
            "number"
        ]

        # Convert DataFrame rows to list of lists
        rows_data = []
        for _, row in df3.iterrows():
            row_list = []
            for col in columns:
                row_list.append(row[col])
            rows_data.append(row_list)

        # Create the new data structure
        data_dict3 = {
            "data": {
                "stocks": {
                    "columns": columns,
                    "types": types,
                    "rows": rows_data
                }
            }
        }

        stockCount = len(rows_data)

        # Fix NaN values in the data_dict3
        data_dict3 = fix_dict_nan(data_dict3, 'robinhood_stocks')

        # Upload to S3
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/robinhood/stock_table', data_dict3, cf_distribution_id, invalidate=False)

        ## Create json for dropdown
        ## create a list of dictionaries for each row in df3 with the ticker and the name
        df3['name_extended'] = df3['ticker'] + ' | ' + df3['name']
        ticker_name_list = df3[['ticker', 'name_extended']].to_dict(orient='records')
        
        dict_dropdown = {
            "dropdown_values": ticker_name_list,
        }

        # Fix NaN values in the dict_dropdown
        dict_dropdown = fix_dict_nan(dict_dropdown, 'robinhood_dropdown')
        # Upload to S3
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/robinhood/dropdown', dict_dropdown, cf_distribution_id, invalidate=False)

        ### KPI json

        data_dict4 = {
            "data": {
                "total_market_value_sum_usd": total_market_value_sum_usd,
                "perc_change_market_value_usd_1d": perc_change_market_value_usd_1d,
                "perc_change_market_value_usd_7d": perc_change_market_value_usd_7d,
                "perc_change_market_value_usd_30d": perc_change_market_value_usd_30d,
                #"perc_change_market_value_usd_365d": perc_change_market_value_usd_365d,
                "stockCount": stockCount
            }
        }

        # fix NaN and upload to S3
        data_dict4 = fix_dict_nan(data_dict4, 'robinhood_kpi')
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/robinhood/kpi', data_dict4, cf_distribution_id, invalidate=False)


        ### empty_cloudfront_cache
        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/robinhood/*')
    
    # temporary to be removed once Phase 2 launches
    @task()
    def notification_in_case_of_transfer():
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        # Implement notification logic here
        df = db_connector.execute_query(
        """
        WITH latest_date AS (
            SELECT MAX(date) as max_date
            FROM public.robinhood_daily
            WHERE metric_key = 'total_transferred'
        )
        SELECT 
            contract_address, 
            "date", 
            metric_key, 
            value
        FROM public.robinhood_daily rd
        JOIN latest_date ld ON rd.date = ld.max_date
        WHERE metric_key = 'total_transferred'
            AND value != 0
        ORDER BY value DESC;
        """, load_df=True)
        if df.empty == False:
            from src.misc.helper_functions import send_discord_message
            import os
            send_discord_message("Robinhood transfers detected (Phase 2 launched?) <@790276642660548619>", webhook_url=os.getenv("DISCORD_ALERTS"))

    # Create task instances
    branch_task = decide_branch()
    pull_dune = pull_data_from_dune()
    pull_yfinance = pull_data_from_yfinance() 
    create_jsons = create_json_file()
    alert_system = notification_in_case_of_transfer()

    # Define execution order with branching
    branch_task >> [json_only_branch, full_pipeline_branch]
    
    # Both branches lead to create_json_file
    json_only_branch >> create_jsons
    full_pipeline_branch >> pull_dune >> pull_yfinance >> alert_system >> create_jsons

run_dag()
from datetime import datetime,timedelta,timezone
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook, claude_fix_on_failure
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': [alert_via_webhook, claude_fix_on_failure]
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
        """Decide which branch to execute based on the day of the week. We pull in data from Dune only on certain days."""
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

        # update list of tokenized assets from Dune
        load_params2 = {
                'queries': [
                    {
                        'name': 'Robinhood_stock_list',
                        'query_id': 5429585,
                        'params': {'days': 3}
                    }
                ],
                'prepare_df': 'prepare_robinhood_list',
                'load_type': 'robinhood_stock_list'
            }
        df = ad.extract(load_params2)
        ad.load(df)

        # load new daily values for all tokenized assets (requires tokenized asset dune table to be up to date)
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

    @task()
    def pull_data_from_yfinance():
        from src.adapters.adapter_yfinance_stocks import AdapterYFinance
        from src.db_connector import DbConnector
        import time

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

        # sometimes yahoo finance api fails so we check data and refill missing tickers
        df_check = db.execute_query(
            """
            SELECT 
                rsl."name", 
                rsl.ticker, 
                rd.value as last_close_price,
                rd.date::TEXT as last_close_date
            FROM public.robinhood_stock_list rsl
            LEFT JOIN LATERAL (
                SELECT contract_address, "date", value
                FROM public.robinhood_daily
                WHERE 
                    metric_key = 'Close'
                    AND contract_address = rsl.contract_address
                ORDER BY "date" DESC
                LIMIT 1
            ) rd ON true
            WHERE rsl.active = true
            ORDER BY last_close_date ASC
            """,
            load_df=True
        )
        if df_check['last_close_date'].iloc[-1] != df_check['last_close_date'].iloc[0]:
            print("Refilling missing tickers from Yahoo Finance...")
            time.sleep(300)  # wait for 5 minutes before refilling to avoid rate limits!
            missing_tickers = df_check[df_check['last_close_date'] == df_check['last_close_date'].iloc[0]]['ticker'].tolist()
            print(f"Missing tickers: {missing_tickers}")

            load_params_refill = {
                'tickers': missing_tickers,
                'endpoints': ['Close'],
                'days': 5,
                'table': 'robinhood_daily',
                'prepare_df': 'prepare_df_robinhood_daily'
            }
            df_refill = yfi.extract(load_params_refill)
            yfi.load(df_refill)

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def create_json_file():
        import pandas as pd
        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan, send_discord_message
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        ### Load the first dataset for daily
        df = execute_jinja_query(db_connector, "api/quick_bites/robinhood_merged_daily.sql.j2", query_parameters={}, return_df=True)
        ticker_list = df['ticker'].unique().tolist()
        dt = pd.to_datetime(df['date'], errors="raise", utc=True)
        df['unix_timestamp'] = (
            (dt - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
        ).astype("int64")

        # send alert for tickers which have to be reviewed (no close price data)
        alert_df = df.groupby('ticker').last()
        alert_df = alert_df[alert_df['close_price_used'].isnull()]
        if alert_df.empty == False:
            alert_message = "<@790276642660548619> The following Robinhood tickers have no price data:\n"
            for ticker in alert_df.index:
                alert_message += f"- {ticker}, {alert_df.loc[ticker, 'name']}\n"
            alert_message += "Please check if stock ticker on Yahoo Finance is different from Robinhood (e.g. '.' rather than '-'), then edit the 'ticker' value in robinhood_stock_list table. In case the stock was delisted, set column 'active' to False."
            send_discord_message(alert_message)

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

        counter = 0
        for ticker in ticker_list:
            print(f"Processing ticker: {ticker}. Overall progress: {counter}/{len(ticker_list)}")
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
                "name": filtered_data['name'].iloc[0],
                "address": filtered_data['contract_address'].iloc[0]
            }

            # Fix any NaN values in the data_dict
            data_dict = fix_dict_nan(data_dict, f'robinhood_daily_{ticker}', send_notification=False)

            # Upload to S3
            upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/robinhood/stocks/{ticker}', data_dict, cf_distribution_id, invalidate=False)
            counter += 1


        ### Load the second dataset for totals
        df2 = execute_jinja_query(db_connector, "api/quick_bites/robinhood_totals_daily.sql.j2", query_parameters={}, return_df=True)
        dt2 = pd.to_datetime(df2['date'], errors="raise", utc=True)
        df2['unix_timestamp'] = (
            (dt2 - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
        ).astype("int64")
        ## filter df to date >= June 30th 2025
        df2 = df2[df2['date'] >= datetime.date(datetime(2025, 6, 30))]

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
        data_dict2 = fix_dict_nan(data_dict2, 'robinhood_totals', send_notification=True)

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
            "tokenization_date",
            "usd_outstanding",
            "stocks_tokenized",
            "stocks_tokenized_7d_change_pct",
            "usd_stock_price"
        ]

        types = [
            "string",
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
        data_dict3 = fix_dict_nan(data_dict3, 'robinhood_stocks', send_notification=True)

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
        dict_dropdown = fix_dict_nan(dict_dropdown, 'robinhood_dropdown', send_notification=True)
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
        data_dict4 = fix_dict_nan(data_dict4, 'robinhood_kpi', send_notification=True)
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/robinhood/kpi', data_dict4, cf_distribution_id, invalidate=False)


        ### empty_cloudfront_cache
        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/robinhood/*')
    
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def run_robinhood_alerts():
        """
        Discord alerts for the Robinhood Stocks Quick Bite:
          - total tokenized stock supply (sum of `total_supply` across active stocks)
            crossing each 1,000 milestone
          - total market value (USD) crossing each $10,000,000 milestone
          - 7-day % change in total market value crossing above +20% (rising edge only)
        Source: existing robinhood_totals_daily.sql.j2 plus an inline query for total
        supply. Self-contained: no state table; for the +20% alert, we compare today's
        7d % change to yesterday's so we only fire on the day it crosses upward.
        """
        import os
        import pandas as pd
        from src.misc.helper_functions import send_discord_message, generate_screenshot
        from src.misc.jinja_helper import execute_jinja_query
        from src.db_connector import DbConnector

        db_connector = DbConnector()
        qb_url = "https://www.growthepie.com/quick-bites/robinhood-stock"
        webhook_url = os.getenv("GTP_AI_WEBHOOK_URL")

        def _maybe_screenshot(label):
            try:
                fname = f"robinhood_{label}_{pd.Timestamp.utcnow().strftime('%Y%m%d')}.png"
                generate_screenshot(qb_url, fname, width=1400, height=1200, wait_for_timeout=4000)
                return f"generated_images/{fname}"
            except Exception as e:
                print(f"⚠️ Screenshot failed for {label}: {e}")
                return None

        def _fmt_usd(v): return f"${v:,.0f}"
        def _fmt_int(v): return f"{int(v):,}"

        ## ---- daily total tokenized stock supply (across all active stocks) ----
        supply_query = """
            WITH date_range AS (
                SELECT generate_series(
                    (SELECT MIN(date) FROM public.robinhood_daily),
                    CURRENT_DATE - INTERVAL '1 day',
                    '1 day'::interval
                )::date as date
            ),
            all_stocks AS (
                SELECT contract_address
                FROM public.robinhood_stock_list
                WHERE active = TRUE
            ),
            grid AS (
                SELECT s.contract_address, d.date
                FROM all_stocks s CROSS JOIN date_range d
            ),
            daily_metrics AS (
                SELECT
                    contract_address,
                    date,
                    SUM(CASE WHEN metric_key = 'total_minted' THEN value ELSE 0 END) -
                    SUM(CASE WHEN metric_key = 'total_burned' THEN value ELSE 0 END) AS net_change
                FROM public.robinhood_daily
                WHERE metric_key IN ('total_minted', 'total_burned')
                GROUP BY contract_address, date
            ),
            joined AS (
                SELECT g.contract_address, g.date, COALESCE(dm.net_change, 0) AS net_change
                FROM grid g
                LEFT JOIN daily_metrics dm
                    ON g.contract_address = dm.contract_address AND g.date = dm.date
            ),
            cum AS (
                SELECT
                    contract_address,
                    date,
                    SUM(net_change) OVER (
                        PARTITION BY contract_address
                        ORDER BY date
                        ROWS UNBOUNDED PRECEDING
                    ) AS total_supply
                FROM joined
            )
            SELECT date, SUM(total_supply) AS total_stocks_tokenized
            FROM cum
            GROUP BY date
            ORDER BY date;
        """
        df_supply = db_connector.execute_query(supply_query, load_df=True)

        if df_supply is None or df_supply.empty:
            print("No supply data; skipping supply milestone.")
        else:
            df_supply['date'] = pd.to_datetime(df_supply['date'])
            df_supply['total_stocks_tokenized'] = pd.to_numeric(
                df_supply['total_stocks_tokenized'], errors='coerce'
            ).fillna(0)
            df_supply = df_supply.sort_values('date').reset_index(drop=True)

            latest_day = df_supply['date'].iloc[-1]
            latest_supply = float(df_supply['total_stocks_tokenized'].iloc[-1])
            prior = df_supply[df_supply['date'] < latest_day]
            prior_supply = float(prior['total_stocks_tokenized'].iloc[-1]) if not prior.empty else 0.0
            threshold = 1_000
            n_now = int(latest_supply // threshold)
            n_before = int(prior_supply // threshold)
            if n_now > n_before:
                milestone = n_now * threshold
                crossed = n_now - n_before
                extra = "" if crossed == 1 else f" (crossed {crossed} milestones in one day)"
                message = (
                    f"🥧 **Robinhood Stocks — crossed `{_fmt_int(milestone)}` total tokenized stocks milestone**{extra}\n"
                    f"Total now `{_fmt_int(latest_supply)}` (was `{_fmt_int(prior_supply)}` before {latest_day.date()})\n"
                    f"[View on growthepie.com]({qb_url})"
                )
                send_discord_message(message, webhook_url, image_paths=_maybe_screenshot("supply_1k"))
            else:
                print(f"ℹ️ supply_1k: total {_fmt_int(latest_supply)} below next milestone.")

        ## ---- daily total market value milestone ($10M) ----
        df_tot = execute_jinja_query(
            db_connector,
            "api/quick_bites/robinhood_totals_daily.sql.j2",
            query_parameters={},
            return_df=True,
        )

        if df_tot is None or df_tot.empty:
            print("No totals data; skipping market value milestone & 7d alert.")
            return

        df_tot['date'] = pd.to_datetime(df_tot['date'])
        df_tot['total_market_value_sum'] = pd.to_numeric(
            df_tot['total_market_value_sum'], errors='coerce'
        ).fillna(0)
        df_tot = df_tot.sort_values('date').reset_index(drop=True)

        latest_day = df_tot['date'].iloc[-1]
        latest_value = float(df_tot['total_market_value_sum'].iloc[-1])
        prior = df_tot[df_tot['date'] < latest_day]
        prior_value = float(prior['total_market_value_sum'].iloc[-1]) if not prior.empty else 0.0

        threshold = 10_000_000
        n_now = int(latest_value // threshold)
        n_before = int(prior_value // threshold)
        if n_now > n_before:
            milestone = n_now * threshold
            crossed = n_now - n_before
            extra = "" if crossed == 1 else f" (crossed {crossed} milestones in one day)"
            message = (
                f"🥧 **Robinhood Stocks — crossed `{_fmt_usd(milestone)}` total market value milestone**{extra}\n"
                f"Total now `{_fmt_usd(latest_value)}` (was `{_fmt_usd(prior_value)}` before {latest_day.date()})\n"
                f"[View on growthepie.com]({qb_url})"
            )
            send_discord_message(message, webhook_url, image_paths=_maybe_screenshot("mv_10m"))
        else:
            print(f"ℹ️ mv_10m: total {_fmt_usd(latest_value)} below next milestone.")

        ## ---- 7-day % change crossing above +20% (rising edge) ----
        if len(df_tot) < 9:
            print("ℹ️ 7d alert: need at least 9 days of data for crossing comparison.")
        else:
            df_tot['pct_7d'] = df_tot['total_market_value_sum'].pct_change(periods=7) * 100
            latest_pct = df_tot['pct_7d'].iloc[-1]
            prior_pct = df_tot['pct_7d'].iloc[-2]
            if pd.isna(latest_pct) or pd.isna(prior_pct):
                print(f"ℹ️ 7d alert: insufficient data (latest={latest_pct}, prior={prior_pct}).")
            elif prior_pct <= 20.0 and latest_pct > 20.0:
                message = (
                    f"🥧 **Robinhood Stocks — 7-day total market value change crossed above +20%**\n"
                    f"Now `+{latest_pct:.2f}%` on {latest_day.date()} (yesterday `{prior_pct:+.2f}%`)\n"
                    f"Total market value now `{_fmt_usd(latest_value)}`\n"
                    f"[View on growthepie.com]({qb_url})"
                )
                send_discord_message(message, webhook_url, image_paths=_maybe_screenshot("pct7d_20"))
            else:
                print(f"ℹ️ 7d alert: latest {latest_pct:+.2f}%, prior {prior_pct:+.2f}% — no upward crossing of +20%.")

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
    qb_alerts = run_robinhood_alerts()

    # Define execution order with branching
    branch_task >> [json_only_branch, full_pipeline_branch]

    # Both branches lead to create_json_file
    json_only_branch >> create_jsons
    full_pipeline_branch >> pull_dune >> pull_yfinance >> alert_system >> create_jsons
    create_jsons >> qb_alerts

run_dag()

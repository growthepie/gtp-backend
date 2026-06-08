from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_highlights',
    description='Run highlights (ATHs, growth, others).',
    tags=['metrics', 'daily'],
    start_date=datetime(2025,10,12),
    schedule='10 05 * * *' # after metrics_sql (04:30), before api_json_creation (05:30)
)

def etl():
    @task()
    def run_aths():
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        from src.main_config import get_main_config
        from src.config import highlights_daily_thresholds

        db_connector = DbConnector()
        main_config = get_main_config()
        days = 30

        ## dict that has metric_key as key and the ath_multiples dict as value (if exists)
        ath_thresholds = {k: v['ath_multiples'] for k, v in highlights_daily_thresholds.items() if 'ath_multiples' in v}

        for chain in main_config:
            if chain.api_in_main:
                print(f'Processing chain for ATHs: {chain.origin_key}')
                
                query_params = {
                    "origin_key": chain.origin_key,
                    "lookback_days": days,
                    "thresholds_by_metric": ath_thresholds
                }

                execute_jinja_query(db_connector, 'chain_metrics/upsert_highlights_ath.sql.j2', query_params)
                
        query_params = {
                    "origin_key": "ethereum_ecosystem",
                    "lookback_days": days,
                    "thresholds_by_metric": ath_thresholds
                }
        execute_jinja_query(db_connector, 'chain_metrics/upsert_highlights_ath.sql.j2', query_params)

        ## MegaETH only: additionally track new highs measured since 2026-01-30 (not all-time).
        ## Emitted as separate highlight types (ath_multiple_since / ath_regular_since) so they
        ## coexist with megaeth's all-time ATH rows. Same metric set as the regular ATH run.
        ## Display text for the *_since types lives in helper_functions.highlights_prep().
        megaeth_high_since = '2026-01-30'
        query_params = {
                    "origin_key": "megaeth",
                    "lookback_days": days,
                    "thresholds_by_metric": ath_thresholds,
                    "since_date": megaeth_high_since,
                    "type_suffix": "_since"
                }
        execute_jinja_query(db_connector, 'chain_metrics/upsert_highlights_ath.sql.j2', query_params)


    
    @task()           
    def run_growth():
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        from src.main_config import get_main_config
        from src.config import highlights_daily_thresholds

        db_connector = DbConnector()
        main_config = get_main_config()

        ## dict that has metric_key as key and the relative_growth dict as value (if exists)
        growth_config = {k: v['relative_growth'] for k, v in highlights_daily_thresholds.items() if 'relative_growth' in v}

        for chain in main_config:
            if chain.api_in_main:
                print(f'Processing chain: {chain.origin_key}')

                query_params = {
                    "metric_configs": growth_config,
                    "origin_key": chain.origin_key,
                }

                execute_jinja_query(db_connector, 'chain_metrics/upsert_highlights_growth.sql.j2', query_params)
                
        query_params = {
                    "metric_configs": growth_config,
                    "origin_key": "ethereum_ecosystem",
                }
        execute_jinja_query(db_connector, 'chain_metrics/upsert_highlights_growth.sql.j2', query_params)
        
    @task()
    def run_lifetime():
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        from src.main_config import get_main_config
        from src.config import gtp_metrics_new
        from datetime import datetime
        import pandas as pd

        db_connector = DbConnector()
        main_config = get_main_config()

        days_back = 3

        for chain_conf in main_config:
            chain = chain_conf.origin_key

            # Create a list of all metric keys we need to query
            metrics_sum = ['txcount', 'fees', 'profit', 'rent_paid']
            metric_keys = {}
            for metric in metrics_sum:
                for mk in gtp_metrics_new['chains'][metric]['metric_keys']:
                    metric_keys[mk] = metric

            # Query and process standard metrics
            metric_keys_list = list(metric_keys.keys())
            ##replace txcount with txcount_plain
            metric_keys_list = [mk.replace('txcount', 'txcount_plain') for mk in metric_keys_list]

            for i in range(0, days_back):
                print(f"Processing {chain} for day offset {i}")
                query_parameters = {
                    'metric_keys': metric_keys_list,
                    'origin_key': chain,
                    'days': i
                }
                result_df = execute_jinja_query(
                    db_connector, 
                    "api/select_fact_kpis_achievements.sql.j2", 
                    query_parameters, 
                    return_df=True
                )

                result_df['metric_key'] = result_df['metric_key'].str.replace('txcount_plain', 'txcount')
                result_df.rename(columns={'total_value': 'value'}, inplace=True)
                
                
                if chain_conf.runs_aggregate_addresses:
                    # Query and process DAA metric separately
                    query_parameters = {
                        'origin_key': chain,
                        'days': i
                    }
                    result_df_daa = execute_jinja_query(
                        db_connector, 
                        "api/select_total_aa.sql.j2", 
                        query_parameters, 
                        return_df=True
                    )
                    result_df_daa['metric_key'] = 'daa'
                    
                    ## combine dfs
                    result_df = pd.concat([result_df, result_df_daa], ignore_index=True)

                # prep df for upload
                ## add date column and set to yesterday
                result_df['date'] = datetime.today() - pd.Timedelta(days=i+1)
                result_df['date'] = result_df['date'].dt.date
                result_df['metric_key'] = 'lifetime_' + result_df['metric_key']
                result_df['origin_key'] = chain

                result_df.set_index(['origin_key', 'metric_key', 'date'], inplace=True)
                db_connector.upsert_table('fact_kpis', result_df)
        
            #TODO: send messages for these lifetime highlights as well
            
            from src.db_connector import DbConnector
            from src.misc.jinja_helper import execute_jinja_query
            from src.main_config import get_main_config
            from src.config import levels_dict

            db_connector = DbConnector()
            main_config = get_main_config()
            days = 5

            ## for each key in levels_dict, create a list of values in the dict
            level_thresholds = {}
            for level_key in levels_dict:
                values = list(levels_dict[level_key].values())
                level_key = 'lifetime_' + level_key
                level_thresholds[level_key] = values

            for chain in main_config:
                if chain.api_in_main:
                    print(f'Processing chain for Lifetime level highlights: {chain.origin_key}')
                    
                    query_params = {
                        "origin_key": chain.origin_key,
                        "lookback_days": days,
                        "thresholds_by_metric": level_thresholds
                    }

                    execute_jinja_query(db_connector, 'chain_metrics/upsert_highlights_lifetime.sql.j2', query_params)
        
                
    run_aths()
    run_growth()
    run_lifetime()
etl()

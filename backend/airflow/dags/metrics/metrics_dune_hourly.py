from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

## Incremental loading: every query is a *_v2 Dune query with a 'start_time' param (SQL in src/queries/dune/hourly/).
## start_time = latest hour already in our target table + 1h (AdapterDune.get_hourly_start_time), and the queries only return
## hours Dune has fully ingested -> each hour is exported exactly once (Dune bills exported MB). If Dune lags, a run returns
## nothing and the next run catches up. Trigger with {"overlap_hours": N} to reload the last N loaded hours.

NATIVE_ADDRESS = "decode('000000000000000000000000000000000000eeee', 'hex')" ## placeholder address of the 'eth' app (native transfers)

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_dune_hourly',
    description='Load aggregates metrics hourly.',
    tags=['metrics', 'hourly'],
    start_date=datetime(2026,2,10),
    schedule='10 * * * *',
    params={'overlap_hours': 0},
)

def etl():
    def get_overlap_hours() -> int:
        from airflow.sdk import get_current_context
        return int(get_current_context()['params'].get('overlap_hours') or 0)

    @task()
    def run_fundamentals_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        overlap_hours = get_overlap_hours()

        for origin_key, name, query_id in [
            ('megaeth', 'megaeth_fundamentals_hourly', 9121753),
            ('polygon_pos', 'polygon_pos_fundamentals_hourly', 9121754),
            ('ronin', 'ronin_fundamentals_hourly', 9121755),
            ('robinhood', 'robinhood_fundamentals_hourly', 9121756),
        ]:
            start_time = ad.get_hourly_start_time('fact_kpis_granular', 'timestamp', origin_key, "granularity = 'hourly' AND metric_key = 'txcount'", overlap_hours=overlap_hours)
            load_params = {
                'queries': [
                    {
                        'name': name,
                        'query_id': query_id,
                        'params': {'start_time': start_time}
                    }
                ],
                'prepare_df': 'prepare_df_metric_hourly',
                'load_type': 'fact_kpis_granular'
            }
            df = ad.extract(load_params)
            if df.empty:
                print(f"No new complete hours for {origin_key} since {start_time}.")
                continue
            ad.load(df)


    @task()
    def run_starknet_contract_level_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        start_time = ad.get_hourly_start_time('blockspace_fact_contract_level_hourly', 'hour', 'starknet', overlap_hours=get_overlap_hours())
        load_params = {
            'queries': [
                {
                    'name': 'starknet_contract_level_hourly',
                    'query_id': 9121757,
                    'params': {'start_time': start_time}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_hourly',
            'load_type': 'blockspace_fact_contract_level_hourly'
        }

        # extract
        df = ad.extract(load_params)
        if df.empty:
            print(f"No new complete hours since {start_time}.")
            return

        print(f"Loaded {df.shape[0]} rows for contract level.")

        # additional prep steps
        df['origin_key'] = 'starknet'
        df.set_index(['address', 'hour', 'origin_key'], inplace=True)

        # load
        ad.load(df)

    @task()
    def run_starknet_contract_aa_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        start_time = ad.get_hourly_start_time('fact_active_addresses_contract_hourly', 'hour', 'starknet', overlap_hours=get_overlap_hours())
        load_params = {
            'queries': [
                {
                    'name': 'starknet_aa',
                    'query_id': 9121758,
                    'params': {'start_time': start_time}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_aa_hourly',
            'load_type': 'CUSTOM'
        }

        # extract
        df = ad.extract(load_params)
        if df.empty:
            print(f"No new complete hours since {start_time}.")
            return

        print(f"Loaded {df.shape[0]} rows for starknet active addresses on contract level.")

        # prepare for fact_active_addresses_contract
        df_contract_aa = df.copy()
        df_contract_aa = df_contract_aa[df_contract_aa.address != '<nil>']

        df_contract_aa['origin_key'] = 'starknet'
        df_contract_aa.set_index(['address', 'hour', 'origin_key', 'from_address'], inplace=True)
        db_connector.upsert_table('fact_active_addresses_contract_hourly', df_contract_aa)

    @task()
    def run_megaeth_contract_level_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        start_time = ad.get_hourly_start_time('blockspace_fact_contract_level_hourly', 'hour', 'megaeth', f"address <> {NATIVE_ADDRESS}", overlap_hours=get_overlap_hours())
        load_params = {
            'queries': [
                {
                    'name': 'megaeth_contract_level_hourly',
                    'query_id': 9121759,
                    'params': {'start_time': start_time}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_hourly',
            'load_type': 'blockspace_fact_contract_level_hourly'
        }

        # extract
        df = ad.extract(load_params)
        if df.empty:
            print(f"No new complete hours since {start_time}.")
            return

        print(f"Loaded {df.shape[0]} rows for contract level.")

        # additional prep steps
        df['origin_key'] = 'megaeth'
        df.set_index(['address', 'hour', 'origin_key'], inplace=True)

        # load
        ad.load(df)

    @task()
    def run_megaeth_contract_aa_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        start_time = ad.get_hourly_start_time('fact_active_addresses_contract_hourly', 'hour', 'megaeth', f"address <> {NATIVE_ADDRESS}", overlap_hours=get_overlap_hours())
        load_params = {
            'queries': [
                {
                    'name': 'megaeth_aa',
                    'query_id': 9121760,
                    'params': {'start_time': start_time}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_aa_hourly',
            'load_type': 'CUSTOM'
        }

        # extract
        df = ad.extract(load_params)
        if df.empty:
            print(f"No new complete hours since {start_time}.")
            return

        print(f"Loaded {df.shape[0]} rows for megaeth active addresses on contract level.")

        # prepare for fact_active_addresses_contract
        df_contract_aa = df.copy()
        df_contract_aa = df_contract_aa[df_contract_aa.address != '<nil>']

        df_contract_aa['origin_key'] = 'megaeth'
        df_contract_aa.set_index(['address', 'hour', 'origin_key', 'from_address'], inplace=True)
        db_connector.upsert_table('fact_active_addresses_contract_hourly', df_contract_aa)

    @task()
    def run_polygon_pos_contract_level_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        start_time = ad.get_hourly_start_time('blockspace_fact_contract_level_hourly', 'hour', 'polygon_pos', overlap_hours=get_overlap_hours())
        load_params = {
            'queries': [
                {
                    'name': 'polygon_pos_contract_level_hourly',
                    'query_id': 9121761,
                    'params': {'start_time': start_time}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_hourly',
            'load_type': 'blockspace_fact_contract_level_hourly'
        }

        # extract
        df = ad.extract(load_params)
        if df.empty:
            print(f"No new complete hours since {start_time}.")
            return

        print(f"Loaded {df.shape[0]} rows for contract level.")

        # additional prep steps
        df['origin_key'] = 'polygon_pos'
        df.set_index(['address', 'hour', 'origin_key'], inplace=True)

        # load
        ad.load(df)

    @task()
    def run_polygon_contract_aa_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        start_time = ad.get_hourly_start_time('fact_active_addresses_contract_hourly', 'hour', 'polygon_pos', overlap_hours=get_overlap_hours())
        load_params = {
            'queries': [
                {
                    'name': 'polygon_aa',
                    'query_id': 9121762,
                    'params': {'start_time': start_time}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_aa_hourly',
            'load_type': 'CUSTOM'
        }

        # extract
        df = ad.extract(load_params)
        if df.empty:
            print(f"No new complete hours since {start_time}.")
            return

        print(f"Loaded {df.shape[0]} rows for polygon active addresses on contract level.")

        # prepare for fact_active_addresses_contract
        df_contract_aa = df.copy()
        df_contract_aa = df_contract_aa[df_contract_aa.address != '<nil>']

        df_contract_aa['origin_key'] = 'polygon_pos'
        df_contract_aa.set_index(['address', 'hour', 'origin_key', 'from_address'], inplace=True)
        db_connector.upsert_table('fact_active_addresses_contract_hourly', df_contract_aa)

    @task()
    def run_robinhood_contract_level_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        start_time = ad.get_hourly_start_time('blockspace_fact_contract_level_hourly', 'hour', 'robinhood', f"address <> {NATIVE_ADDRESS}", overlap_hours=get_overlap_hours())
        load_params = {
            'queries': [
                {
                    'name': 'robinhood_contract_level_hourly',
                    'query_id': 9121763,
                    'params': {'start_time': start_time}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_hourly',
            'load_type': 'blockspace_fact_contract_level_hourly'
        }

        # extract
        df = ad.extract(load_params)
        if df.empty:
            print(f"No new complete hours since {start_time}.")
            return

        print(f"Loaded {df.shape[0]} rows for contract level.")

        # additional prep steps
        df['origin_key'] = 'robinhood'
        df.set_index(['address', 'hour', 'origin_key'], inplace=True)

        # load
        ad.load(df)

    @task()
    def run_robinhood_contract_aa_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        start_time = ad.get_hourly_start_time('fact_active_addresses_contract_hourly', 'hour', 'robinhood', f"address <> {NATIVE_ADDRESS}", overlap_hours=get_overlap_hours())
        load_params = {
            'queries': [
                {
                    'name': 'robinhood_aa',
                    'query_id': 9121764,
                    'params': {'start_time': start_time}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_aa_hourly',
            'load_type': 'CUSTOM'
        }

        # extract
        df = ad.extract(load_params)
        if df.empty:
            print(f"No new complete hours since {start_time}.")
            return

        print(f"Loaded {df.shape[0]} rows for robinhood active addresses on contract level.")

        # prepare for fact_active_addresses_contract
        df_contract_aa = df.copy()
        df_contract_aa = df_contract_aa[df_contract_aa.address != '<nil>']

        df_contract_aa['origin_key'] = 'robinhood'
        df_contract_aa.set_index(['address', 'hour', 'origin_key', 'from_address'], inplace=True)
        db_connector.upsert_table('fact_active_addresses_contract_hourly', df_contract_aa)

    @task()
    def run_native_transfer_hourly():
        ## hourly native transfers for the 'eth' app, mapped to placeholder address 0x000000000000000000000000000000000000EeEe
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        overlap_hours = get_overlap_hours()

        for origin_key, query_id in [('megaeth', 9121765), ('robinhood', 9121766)]:
            start_time = ad.get_hourly_start_time('blockspace_fact_contract_level_hourly', 'hour', origin_key, f"address = {NATIVE_ADDRESS}", overlap_hours=overlap_hours)
            load_params = {
                'queries': [
                    {
                        'name': f'{origin_key}_native_transfer_hourly',
                        'query_id': query_id,
                        'params': {'start_time': start_time}
                    }
                ],
                'prepare_df': 'prepare_df_contract_level_hourly',
                'load_type': 'blockspace_fact_contract_level_hourly'
            }
            df = ad.extract(load_params)
            if df.empty:
                print(f"No new complete hours for {origin_key} since {start_time}.")
                continue
            print(f"Loaded {df.shape[0]} rows for {origin_key} hourly native transfers.")

            df['origin_key'] = origin_key
            df.set_index(['address', 'hour', 'origin_key'], inplace=True)
            ad.load(df)

    @task()
    def run_native_transfer_aa_hourly():
        ## hourly native transfer senders for the 'eth' app; the contract hourly hll aggregation (metrics_sql_hourly) picks them up from fact_active_addresses_contract_hourly
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        overlap_hours = get_overlap_hours()

        for origin_key, query_id in [('megaeth', 9121767), ('robinhood', 9121768)]:
            start_time = ad.get_hourly_start_time('fact_active_addresses_contract_hourly', 'hour', origin_key, f"address = {NATIVE_ADDRESS}", overlap_hours=overlap_hours)
            load_params = {
                'queries': [
                    {
                        'name': f'{origin_key}_native_transfer_aa_hourly',
                        'query_id': query_id,
                        'params': {'start_time': start_time}
                    }
                ],
                'prepare_df': 'prepare_df_contract_level_aa_hourly',
                'load_type': 'CUSTOM'
            }
            df = ad.extract(load_params)
            if df.empty:
                print(f"No new complete hours for {origin_key} since {start_time}.")
                continue
            print(f"Loaded {df.shape[0]} rows for {origin_key} hourly native transfer senders.")

            df['origin_key'] = origin_key
            df.set_index(['address', 'hour', 'origin_key', 'from_address'], inplace=True)
            db_connector.upsert_table('fact_active_addresses_contract_hourly', df)

    run_fundamentals_hourly()

    run_starknet_contract_level_hourly()
    run_starknet_contract_aa_hourly()

    run_megaeth_contract_level_hourly()
    run_megaeth_contract_aa_hourly()

    run_polygon_pos_contract_level_hourly()
    run_polygon_contract_aa_hourly()

    run_robinhood_contract_level_hourly()
    run_robinhood_contract_aa_hourly()

    ## ETH app (hourly native transfers on MegaETH & Robinhood)
    run_native_transfer_hourly()
    run_native_transfer_aa_hourly()

etl()

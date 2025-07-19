import os
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import upsert_to_kpis, get_missing_days_blockspace, send_discord_message
from src.misc.helper_functions import print_init, print_load, print_extract, check_projects_to_load

##ToDos: 
# Add logs (query execution, execution fails, etc)

class AdapterSQL(AbstractAdapter):
    """
    adapter_params require the following fields
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("SQL Aggregation", adapter_params, db_connector)
        self.discord_webhook = os.getenv('DISCORD_ALERTS')
        self.main_config = get_main_config()
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        load_type:str - can be 'usd_to_eth' or 'eth_to_usd' or 'metrics' or 'blockspace' or 'profit'
        days:str - days of historical data that should be loaded, starting from today.
        origin_keys:list - list of origin_keys
        metric_keys:list - the metrics that should be loaded. If None, all available metrics will be loaded
    """
    def extract(self, load_params:dict):
        ## Set variables
        load_type = load_params['load_type']
        days = load_params.get('days', None)

        currency_dependent = load_params.get('currency_dependent', None)
        metric_keys = load_params.get('metric_keys', None)
        origin_keys = load_params.get('origin_keys', None)

        ## check if load_params['days_start'] exists and if so, overwrite days
        if 'days_start' in load_params:
            days_start = load_params['days_start']
        else:
            days_start = 1

        ## aggregation types
        if load_type == 'usd_to_eth': ## also make sure to add new metrics in db_connector
            raw_metrics = ['tvl', 'stables_mcap', 'fdv_usd', 'app_fees_usd']
            ## only keep metrics that are in raw_metrics and metric_keys
            if metric_keys is not None:
                metric_keys = [x for x in metric_keys if x in raw_metrics]
            else:
                metric_keys = raw_metrics
            df = self.db_connector.get_values_in_eth(metric_keys, days, origin_keys)

        elif load_type == 'eth_to_usd': ## also make sure to add new metrics in db_connector
            raw_metrics = ['fees_paid_eth', 'fees_paid_base_eth', 'fees_paid_priority_eth', 'txcosts_median_eth', 'profit_eth', 'rent_paid_eth', 'cost_l1_raw_eth', 
                           'ethereum_blobs_eth', 'celestia_blobs_eth', 'costs_blobs_eth', 'costs_l1_eth', 'costs_total_eth', 'da_fees_eth',
                           'da_fees_per_mbyte_eth']
            ## only keep metrics that are in raw_metrics and metric_keys
            if metric_keys is not None:
                metric_keys = [x for x in metric_keys if x in raw_metrics]
            else:
                metric_keys = raw_metrics
            df = self.db_connector.get_values_in_usd(metric_keys, days, origin_keys)

        elif load_type == 'economics':
            ##exclude_chains = ['imx', 'mantle'] ## chains that should be excluded from profit calc
            exclude_chains = [chain.origin_key for chain in self.main_config if 'profit' in chain.api_exclude_metrics]
            print(f'...excluding chains from profit calculation: {exclude_chains}')

            ## FULL ECONOMICS - economics cals incl profit          
            df = self.db_connector.get_economics_in_eth(days, exclude_chains, origin_keys)
            ## unpivot df
            df = df.melt(id_vars=['date', 'origin_key'], var_name='metric_key', value_name='value')
            print(f"...loaded full economics data for origin_keys: {origin_keys} excluding {exclude_chains}. Shape: {df.shape}")

            ## ECONOMICS WITHOUT PROFIT - economics calc without profit
            if origin_keys is not None:
                ok_without_profit = [ok for ok in origin_keys if ok in exclude_chains]
            else:
                ok_without_profit = exclude_chains

            df2 = self.db_connector.get_economics_in_eth(days, None, ok_without_profit, incl_profit=False)
            ## unpivot df
            df2 = df2.melt(id_vars=['date', 'origin_key'], var_name='metric_key', value_name='value')
            print(f"...loaded economics data without profit for {ok_without_profit}. Shape: {df2.shape}")

            df = pd.concat([df, df2], ignore_index=True)

            ## remove all rows where value is na
            df = df.dropna(subset=['value'])

        elif load_type == 'fdv':
            df = self.db_connector.get_fdv_in_usd(days, origin_keys)

        elif load_type == 'da_metrics':
            df = self.db_connector.get_da_metrics_in_eth(days, origin_keys)

        elif load_type == 'metrics':        
            from src.queries.sql_queries import sql_queries
            upsert = load_params.get('upsert', False)
            ## Prepare queries to load
            check_projects_to_load(sql_queries, origin_keys)

            if origin_keys is not None:
                self.queries_to_load = [x for x in sql_queries if x.origin_key in origin_keys]
            else:
                self.queries_to_load = sql_queries

            if currency_dependent is not None:
                if currency_dependent == True:
                    self.queries_to_load = [x for x in self.queries_to_load if x.currency_dependent == True]
                else:
                    self.queries_to_load = [x for x in self.queries_to_load if x.currency_dependent == False]

            if metric_keys is not None:
                self.queries_to_load = [x for x in self.queries_to_load if x.metric_key in metric_keys]
            else:
                ## remove queries that are have metric_key = 'profit_usd' since this should be triggered afterwards
                self.queries_to_load = [x for x in self.queries_to_load if x.metric_key != 'profit_usd']

            ## Load data
            df = self.extract_data_from_db(self.queries_to_load, days, days_start, upsert=upsert)

        elif load_type == 'blockspace':
            self.run_blockspace_queries(origin_keys, days)
            return None
        
        elif load_type == 'active_addresses_agg':
            days_end = load_params.get('days_end', None)
            self.run_active_addresses_agg(origin_keys, days, days_end)
            return None
        
        elif load_type == 'fees':
            granularities = load_params.get('granularities', None)
            self.run_fees_queries(origin_keys, days, granularities, metric_keys)
            return None
        
        elif load_type == 'jinja':
            query_parameters = load_params.get('query_parameters', None)
            destination_table = load_params.get('destination_table', None)

            if query_parameters is None:
                if days:
                    query_parameters = {'days': days}
                else:
                    query_parameters = {}

            jinja_queries = load_params.get('queries', None)
            
            for jinja_q in jinja_queries:
                print(f"...executing jinja query: {jinja_q} with params: {query_parameters}")
                if destination_table is not None:
                    df = self.db_connector.execute_jinja(query_name=jinja_q, query_params=query_parameters, load_into_df=True)
                else:
                    self.db_connector.execute_jinja(query_name=jinja_q, query_params=query_parameters)
                    df = pd.DataFrame() ## dummy df, TODO: make it more clear what queries are fact_kpis and what are not

        else:
            raise ValueError('load_type not supported')

        if df.empty:
            print(f"...empty df for {load_type}. Upsert flag set to: {load_params.get('upsert', False)}.")
        else:
            df.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
            df.value.fillna(0, inplace=True)

        print_extract(self.name, load_params,df.shape)
        return df

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)

    def extract_data_from_db(self, queries_to_load, days, days_start=1, upsert=False):
        query_list = []
        for query in queries_to_load:
            query_list.append(query.origin_key + ' - ' + query.metric_key)

        print(f"...loading data from db for {len(queries_to_load)} queries: {query_list}...")
        
        dfMain = pd.DataFrame()
        for query in queries_to_load:
            try:
                if days == 'auto':
                    pass ## just use the default days defined in the query
                else:
                    day_val = days
                    query.query_parameters['days'] = day_val

                print(f"...executing query: {query.metric_key} - {query.origin_key} with {query.query_parameters}")
                query.sql = query.template.render(query.query_parameters)
                df = pd.read_sql(query.sql, self.db_connector.engine.connect())
                
                df['date'] = df['day'].apply(pd.to_datetime)
                df['date'] = df['date'].dt.date
                df.drop(['day'], axis=1, inplace=True)
                df.rename(columns= {'val':'value'}, inplace=True)
                if 'metric_key' not in df.columns:
                    df['metric_key'] = query.metric_key
                if 'origin_key' not in df.columns:
                    df['origin_key'] = query.origin_key
                df.value.fillna(0, inplace=True)

                print(f"Query loaded: {query.metric_key} {query.origin_key} with {days} days. DF shape: {df.shape}")

                if upsert == True:
                    df.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
                    upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
                    print_load(self.name, upserted, tbl_name)
                else:
                    dfMain = pd.concat([dfMain, df], ignore_index=True)
            except Exception as e:
                print(f"Error loading query: {query.metric_key} - {query.origin_key}. Error: {e}")
                send_discord_message( f"Error loading query: {query.metric_key} - {query.origin_key}. Error: {e}", self.discord_webhook)
                continue
            
        return dfMain
    
    def run_blockspace_queries(self, origin_keys, days):
        if origin_keys is None:
            origin_keys = [chain.origin_key for chain in self.main_config if chain.runs_aggregate_blockspace == True]
            print(f"...no specific origin_key found, aggregating blockspace for all chains: {origin_keys}...")

        for chain in origin_keys:
            if days == 'auto':
                days = get_missing_days_blockspace(self.db_connector, chain)
            else:
                days = days

            if chain == 'imx':
                print(f"...aggregating imx data for last {days} days...")
                df = self.db_connector.get_blockspace_imx(days)
                df.set_index(['date', 'category_id' ,'origin_key'], inplace=True)

                print(f"...upserting imx data . Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_category_level', df)

                ## determin total usage
                print(f"...aggregating total usage for imx and last {days} days...")
                df = self.db_connector.get_blockspace_total_imx(days)
                df.set_index(['date', 'category_id' ,'origin_key'], inplace=True)
                print(f"...upserting total usage usage for imx. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_category_level', df)
            
            else:
                ## aggregate contract data
                print(f"...aggregating + upserting contract data for {chain} and last {days} days...")
                self.db_connector.get_blockspace_contracts(chain, days)

                ## determine total usage
                print(f"...aggregating total usage for {chain} and last {days} days...")
                df = self.db_connector.get_blockspace_total(chain, days)
                df.set_index(['date', 'category_id' ,'origin_key'], inplace=True)
                print(f"...upserting total usage usage for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_category_level', df)

                ## aggregate native transfers
                print(f"...aggregating native_transfers for {chain} and last {days} days...")
                df = self.db_connector.get_blockspace_native_transfers(chain, days)
                df.set_index(['date', 'category_id' ,'origin_key'], inplace=True)
                print(f"...upserting native_transfers for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_category_level', df)

                ## aggregate contract deployments
                print(f"...aggregating smart_contract_deployments for {chain} and last {days} days...")
                df = self.db_connector.get_blockspace_contract_deplyments(chain, days)
                df.set_index(['date', 'category_id' ,'origin_key'], inplace=True)
                print(f"...upserting smart_contract_deployments for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_category_level', df)

                ## aggregate inscriptions
                print(f"...aggregating inscriptions for {chain} and last {days} days...")
                df = self.db_connector.get_blockspace_inscriptions(chain, days)
                df.set_index(['date', 'category_id' ,'origin_key'], inplace=True)
                print(f"...upserting inscriptions for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_category_level', df)

                # ALL below needs to be retriggerd when mapping changes (e.g. new addresses got labeled or new categories added etc.)
                ## aggregate by sub categories

                days_mapping = 5000
                print(f"...aggregating sub categories for {chain} and last {days_mapping} days...")
                df = self.db_connector.get_blockspace_sub_categories(chain, days_mapping)
                df.set_index(['date', 'category_id' ,'origin_key'], inplace=True)

                print(f"...upserting sub categories for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_category_level', df)

                ## determine unlabeled usage
                print(f"...aggregating unlabeled usage for {chain} and last {days_mapping} days...")
                df = self.db_connector.get_blockspace_unlabeled(chain, days_mapping)
                df.set_index(['date', 'category_id' ,'origin_key'], inplace=True)

                print(f"...upserting unlabeled usage for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_category_level', df)

            print(F"Finished loading blockspace queries for {chain}")
        print(F"Finished loading blockspace for all chains")

    def run_active_addresses_agg(self, origin_keys, days, days_end=None):
        if origin_keys is None:
            origin_keys = [chain.origin_key for chain in self.main_config if chain.runs_aggregate_addresses == True]
            print(f"...no specific origin_key found, aggregating active addresses for all chains: {origin_keys}...")

        for origin_key in origin_keys:
            if days == 'auto':
                days = 3
            else:
                days = int(days)
            
            ## Starknet data isn't stored in fact_active_addresses as we only keep EVM addresses in here
            if origin_key != 'starknet':
                print(f"...aggregating + inserting active addresses data for {origin_key} and last {days} days and days_end set to {days_end}...")
                self.db_connector.aggregate_unique_addresses(origin_key, days, days_end)

            print(f"...HLL: aggregating + inserting active addresses data for {origin_key} and last {days} days and days_end set to {days_end}...")
            self.db_connector.aggregate_unique_addresses_hll(origin_key, days, days_end)

            if origin_key in [chain.origin_key for chain in self.main_config if chain.runs_aggregate_apps == True]:
                print(f"...HLL: aggregating + inserting app addresses data for {origin_key} and last {days} days and days_end set to {days_end}...")
                self.db_connector.aggregate_unique_addresses_contracts_hll(origin_key, days, days_end)
            else:
                print(f"...HLL: no app addresses for {origin_key} to be aggregated...")

        ## run aggregation for weekly active addresses (landing page chart) - this needs to be re-run completely whenever a new chain is added . TODO: re-run this for 1000d once a week (same for all cca_weekly_exclusives)
        print(f"...run cca_weekly_multiple l2s for last {days} days (can take a while for longer timeframes)...")
        self.db_connector.execute_jinja('chain_metrics/upsert_cca_weekly_multiple_l2s.sql.j2', {'days': days})

        ## STOPPED loading this on October 15th, 2024 (currently not used)
        # print(f'...aggregate_addresses_first_seen_global for last {days} days...')
        # self.db_connector.aggregate_addresses_first_seen_global(days)

    def run_fees_queries(self, origin_keys, days, granularities, metric_keys=None):
        if origin_keys is None:
            origin_keys = [chain.origin_key for chain in self.main_config if chain.api_in_fees == True]
            print(f"...no specific origin_key found, aggregating fees for all chains: {origin_keys}...")
        
        ## currently excluding the 10th and 90th percentile for regular runs
        if metric_keys is None:
            metric_keys = ['txcosts_avg_eth', 'txcosts_median_eth', 'txcosts_native_median_eth', 'txcosts_swap_eth', 'txcount', 'throughput']
        
        ## different granularities have different timestamp queries, seconds conversion and end timestamps 
        granularities_dict = {
                'daily': [
                    """date_trunc('day', "block_timestamp")""", 
                    (24*60*60), 
                    """date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"""
                    ],
                '4_hours': [
                    """date_trunc('day', block_timestamp) +  INTERVAL '1 hour' * (EXTRACT(hour FROM block_timestamp)::int / 4 * 4)""", 
                    (4*60*60),
                    """---"""
                    ],
                'hourly': [
                    """date_trunc('hour', "block_timestamp")""", 
                    (60*60),
                    """date_trunc('hour', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"""
                    ],
                '10_min': [
                    """date_trunc('hour', block_timestamp) + INTERVAL '10 min' * FLOOR(EXTRACT(minute FROM block_timestamp) / 10)""", 
                    (10*60),
                    """---"""
                    ],
        }

        if granularities is None:
            granularities = granularities_dict
        else: 
            granularities = {k: granularities_dict[k] for k in granularities}

        for origin_key in origin_keys: 
                if '4_hours' in granularities:
                    query = f"select date_trunc('day', max(block_timestamp)) +  INTERVAL '1 hour' * (EXTRACT(hour FROM max(block_timestamp))::int / 4 * 4) from {origin_key}_tx limit 1"
                    with self.db_connector.engine.connect() as connection:
                        granularities['4_hours'][2] = f""" '{str(connection.execute(query).scalar())}' """

                if '10_min' in granularities:
                    query = f"select date_trunc('hour', max(block_timestamp)) + INTERVAL '10 min' * FLOOR(EXTRACT(minute FROM max(block_timestamp)) / 10) from {origin_key}_tx limit 1"
                    with self.db_connector.engine.connect() as connection:
                        granularities['10_min'][2] = f""" '{str(connection.execute(query).scalar())}' """                 
                                   
                for granularity in granularities:
                        timestamp_query = granularities[granularity][0]
                        seconds_conversion = granularities[granularity][1]
                        timestamp_end = granularities[granularity][2]

                        ## for ethereuem only hourly granularity is supported
                        if origin_key == 'ethereum' and granularity != 'hourly':
                                continue
                        
                        if origin_key in ['mantle', 'metis', 'celo']:
                            ## a little more complex cte because our coingecko data can be 1 day behind
                            additional_cte = f"""
                                date_series AS (
                                    SELECT CURRENT_DATE - i AS date
                                    FROM generate_series(0, {days + 1}) i -- this generates the last 3 days including today
                                ),
                                latest_value AS (
                                    SELECT value
                                    FROM public.fact_kpis
                                    WHERE origin_key = '{origin_key}' AND metric_key = 'price_eth'
                                    ORDER BY date DESC
                                    LIMIT 1
                                ),
                                token_price AS (
                                    SELECT ds.date, COALESCE(fk.value, lv.value) AS value
                                    FROM date_series ds
                                    LEFT JOIN public.fact_kpis fk ON ds.date = fk.date AND fk.origin_key = '{origin_key}' AND fk.metric_key = 'price_eth'
                                    CROSS JOIN latest_value lv
                                )
                            """
                            tx_fee_eth_string = 'tx_fee * mp.value'
                            additional_join = """LEFT JOIN token_price mp on date_trunc('day', block_timestamp) = mp."date" """
                        else:
                            additional_cte = ''
                            tx_fee_eth_string = 'tx_fee'
                            additional_join = ''

                        ## txcosts_average
                        if 'txcosts_avg_eth' in metric_keys:
                            print(f"... processing txcosts_average for {origin_key} and {granularity} granularity")
                            if additional_cte != '':
                                additional_cte_full = 'WITH ' + additional_cte 
                            else:
                                additional_cte_full = ''
                            exec_string = f"""
                                {additional_cte_full}
                                SELECT
                                        {timestamp_query} AS timestamp,
                                        '{origin_key}' as origin_key,
                                        'txcosts_avg_eth' as metric_key,
                                        '{granularity}' as granularity,
                                        AVG({tx_fee_eth_string}) as value
                                FROM public.{origin_key}_tx
                                {additional_join}
                                WHERE tx_fee <> 0 
                                    AND block_date >= current_date - interval '{days} days' 
                                    and  block_timestamp < {timestamp_end}                
                                GROUP BY 1,2,3,4
                            """
                            df = pd.read_sql(exec_string, self.db_connector.engine.connect())
                            df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                            self.db_connector.upsert_table('fact_kpis_granular', df)

                        ## txcosts_median
                        if 'txcosts_median_eth' in metric_keys:
                            print(f"... processing txcosts_median for {origin_key} and {granularity} granularity")
                            if additional_cte != '':
                                additional_cte_full = additional_cte + ', '
                            else:
                                additional_cte_full = ''
                            exec_string = f"""
                                WITH 
                                {additional_cte_full}

                                median_tx AS (
                                        SELECT
                                                {timestamp_query} AS block_timestamp,
                                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS tx_fee
                                        FROM public.{origin_key}_tx
                                        WHERE tx_fee <> 0 
                                            AND block_date >= current_date - interval '{days} days' 
                                            and  block_timestamp < {timestamp_end}   
                                        GROUP BY 1
                                )

                                SELECT
                                        '{origin_key}' as origin_key,
                                        'txcosts_median_eth' as metric_key,
                                        z.block_timestamp as timestamp,
                                        '{granularity}' as granularity,
                                        {tx_fee_eth_string} as value
                                FROM median_tx z
                                {additional_join}
                            """
                            df = pd.read_sql(exec_string, self.db_connector.engine.connect())
                            df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                            self.db_connector.upsert_table('fact_kpis_granular', df)

                        ## txcosts_90th_percentile
                        if 'txcosts_90th_eth' in metric_keys:
                            print(f"... processing txcosts_90th for {origin_key} and {granularity} granularity")
                            if additional_cte != '':
                                additional_cte_full = additional_cte + ', '
                            else:
                                additional_cte_full = ''
                            exec_string = f"""
                                WITH 
                                {additional_cte_full}

                                median_tx AS (
                                        SELECT
                                                {timestamp_query} AS block_timestamp,
                                                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY tx_fee) AS tx_fee
                                        FROM public.{origin_key}_tx
                                        WHERE tx_fee <> 0 
                                            AND block_date >= current_date - interval '{days} days' 
                                            and  block_timestamp < {timestamp_end}   
                                        GROUP BY 1
                                )

                                SELECT
                                        '{origin_key}' as origin_key,
                                        'txcosts_90th_eth' as metric_key,
                                        z.block_timestamp as timestamp,
                                        '{granularity}' as granularity,
                                        {tx_fee_eth_string} as value
                                FROM median_tx z
                                {additional_join}
                            """
                            df = pd.read_sql(exec_string, self.db_connector.engine.connect())
                            df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                            self.db_connector.upsert_table('fact_kpis_granular', df)

                        ## txcosts_10th_percentile
                        if 'txcosts_10th_eth' in metric_keys:
                            print(f"... processing txcosts_10th for {origin_key} and {granularity} granularity")
                            if additional_cte != '':
                                additional_cte_full = additional_cte + ', '
                            else:
                                additional_cte_full = ''
                            exec_string = f"""
                                WITH 
                                {additional_cte_full}

                                median_tx AS (
                                        SELECT
                                                {timestamp_query} AS block_timestamp,
                                                PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY tx_fee) AS tx_fee
                                        FROM public.{origin_key}_tx
                                        WHERE tx_fee <> 0 
                                            AND block_date >= current_date - interval '{days} days' 
                                            and  block_timestamp < {timestamp_end} 
                                        GROUP BY 1
                                )

                                SELECT
                                        '{origin_key}' as origin_key,
                                        'txcosts_10th_eth' as metric_key,
                                        z.block_timestamp as timestamp,
                                        '{granularity}' as granularity,
                                        {tx_fee_eth_string} as value
                                FROM median_tx z
                                {additional_join}
                            """
                            df = pd.read_sql(exec_string, self.db_connector.engine.connect())
                            df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                            self.db_connector.upsert_table('fact_kpis_granular', df)

                        ## txcosts_native_median
                        if 'txcosts_native_median_eth' in metric_keys:
                            if origin_key != 'starknet':
                                filter_string = 'AND empty_input = TRUE'
                            else:
                                filter_string = 'AND gas_used > 20 and gas_used < 28'

                            print(f"... processing txcosts_median_native for {origin_key} and {granularity} granularity")  
                            if additional_cte != '':
                                additional_cte_full = additional_cte + ', '    
                            else:
                                additional_cte_full = ''                                  
                            exec_string = f"""
                                WITH 
                                {additional_cte_full}

                                median_tx AS (
                                        SELECT
                                                {timestamp_query} AS block_timestamp,
                                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS tx_fee
                                        FROM public.{origin_key}_tx
                                        WHERE tx_fee <> 0 
                                            AND block_date >= current_date - interval '{days} days' 
                                            and  block_timestamp < {timestamp_end}  
                                            {filter_string}
                                        GROUP BY 1
                                )

                                SELECT
                                        '{origin_key}' as origin_key,
                                        'txcosts_native_median_eth' as metric_key,
                                        z.block_timestamp as timestamp,
                                        '{granularity}' as granularity,
                                        {tx_fee_eth_string} as value
                                FROM median_tx z
                                {additional_join}
                            """

                            df = pd.read_sql(exec_string, self.db_connector.engine.connect())
                            df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                            self.db_connector.upsert_table('fact_kpis_granular', df)

                        ## txcosts_swap_eth
                        if 'txcosts_swap_eth' in metric_keys:
                            if origin_key == 'starknet':
                                gas_used_lower = 140
                                gas_used_upper = 200
                            else:
                                gas_used_lower = 150000
                                gas_used_upper = 350000

                            print(f"... processing txcosts_swap_eth for {origin_key} and {granularity} granularity")         
                            if additional_cte != '':
                                additional_cte_full = additional_cte + ', '    
                            else:
                                additional_cte_full = '' 
                                
                            exec_string = f"""
                                WITH 
                                {additional_cte_full}

                                median_tx AS (
                                        SELECT
                                                {timestamp_query} AS block_timestamp,
                                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS tx_fee
                                        FROM public.{origin_key}_tx
                                        WHERE tx_fee <> 0 
                                            AND block_date >= current_date - interval '{days} days' 
                                            and  block_timestamp < {timestamp_end}    
                                        AND gas_used between {gas_used_lower} AND {gas_used_upper}
                                        GROUP BY 1
                                )

                                SELECT
                                        '{origin_key}' as origin_key,
                                        'txcosts_swap_eth' as metric_key,
                                        z.block_timestamp as timestamp,
                                        '{granularity}' as granularity,
                                        {tx_fee_eth_string} as value
                                FROM median_tx z
                                {additional_join}
                            """
                            df = pd.read_sql(exec_string, self.db_connector.engine.connect())
                            df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                            self.db_connector.upsert_table('fact_kpis_granular', df)
                    
                        if 'txcount' in metric_keys:
                            print(f"... processing txcount for {origin_key} and {granularity} granularity")
                            exec_string = f"""
                                    SELECT
                                            {timestamp_query} AS timestamp,
                                            '{origin_key}' as origin_key,
                                            'txcount' as metric_key,
                                            '{granularity}' as granularity,
                                            Count(*) as value
                                    FROM public.{origin_key}_tx
                                    WHERE tx_fee <> 0 
                                        AND block_date >= current_date - interval '{days} days' 
                                        and block_timestamp < {timestamp_end}                  
                                    GROUP BY 1,2,3,4
                            """
                            df = pd.read_sql(exec_string, self.db_connector.engine.connect())
                            df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                            self.db_connector.upsert_table('fact_kpis_granular', df)

                        if 'throughput' in metric_keys:
                            ## skip throughput calc for certain chains
                            if origin_key in ['starknet', 'mantle', 'zksync_era']:
                                    continue
                            print(f"... processing throughput for {origin_key} and {granularity} granularity")

                            if origin_key in ['arbitrum']:
                                gas_query = "(sum(gas_used) - sum(l1_gas_used))"
                            else:
                                gas_query = "sum(gas_used)"
                            exec_string = f"""
                                    SELECT
                                            {timestamp_query} AS timestamp,
                                            '{origin_key}' as origin_key,
                                            'gas_per_second' as metric_key,
                                            '{granularity}' as granularity,
                                            {gas_query} / {seconds_conversion} AS value
                                    FROM public.{origin_key}_tx
                                    WHERE tx_fee <> 0 
                                        AND block_date >= current_date - interval '{days} days' 
                                        and  block_timestamp < {timestamp_end} 
                                    GROUP BY 1,2,3,4
                            """
                            df = pd.read_sql(exec_string, self.db_connector.engine.connect())
                            df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                            self.db_connector.upsert_table('fact_kpis_granular', df)


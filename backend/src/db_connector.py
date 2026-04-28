from datetime import datetime, timedelta
from pangres import upsert
from sqlalchemy import text
import sqlalchemy
import pandas as pd
import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from dotenv import load_dotenv
load_dotenv() 
import os

db_user = os.getenv("DB_USERNAME")
db_passwd = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_DATABASE")

from jinja2 import Environment, FileSystemLoader, StrictUndefined
if sys_user == 'ubuntu':
        jinja_env = Environment(loader=FileSystemLoader(f'/home/{sys_user}/gtp/backend/src/queries/postgres'), undefined=StrictUndefined)
else:
        jinja_env = Environment(loader=FileSystemLoader('src/queries/postgres'), undefined=StrictUndefined)

class DbConnector:
        @staticmethod
        def _coerce_text(statement):
                return text(statement) if isinstance(statement, str) else statement

        def __init__(self, db_user=db_user, db_passwd=db_passwd, db_host=db_host, db_name=db_name):
            self.url = f"postgresql+psycopg2://{db_user}:{db_passwd}@{db_host}/{db_name}"
            self.uri = f"postgresql://{db_user}:{db_passwd}@{db_host}/{db_name}"
            self.engine = sqlalchemy.create_engine(
                self.url,
                connect_args={
                        "keepalives": 1,
                        "keepalives_idle": 30,
                        "keepalives_interval": 10,
                        "keepalives_count": 5,
                },
                pool_size=20, max_overflow=20
                )
            ## test connection
            try:
                with self.engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                    print("Database connection successful.")
            except Exception as e:
                print(f"Database connection failed: {e}")
                raise e

        def upsert_table(self, table_name:str, df:pd.DataFrame, if_exists='update'):
                batch_size = 100000
                if df.shape[0] > 0:
                        if df.shape[0] > batch_size:
                                print(f"Batch upload necessary. Total size: {df.shape[0]}")
                                total_length = df.shape[0]
                                batch_start = 0
                                while batch_start < total_length:
                                        batch_end = batch_start + batch_size
                                        upsert(con=self.engine, df=df.iloc[batch_start:batch_end], table_name=table_name, if_row_exists=if_exists, create_table=False)
                                        print("Batch " + str(batch_end))
                                        batch_start = batch_end
                        else:
                                upsert(con=self.engine, df=df, table_name=table_name, if_row_exists=if_exists, create_table=False)
                        return df.shape[0]
                
        def execute_jinja(self, query_name, query_params={}, load_into_df=False):
                query = jinja_env.get_template(query_name).render(query_params)
                #print(f"Executing query {query_name} with params {query_params}. Query: {query}")
                if 'trusted_entities' in query_params.keys():
                        query_params['trusted_entities'] = '... see gtp-dna/oli/trusted_entities.yml'
                if load_into_df:
                        #print(f"Executing query {query_name} with params {query_params} and loading into DataFrame.")
                        return pd.read_sql(text(query), self.engine)
                else:
                        #print(f"Executing query {query_name} with params {query_params}")
                        with self.engine.begin() as connection:
                                connection.execute(text(query))
                return None                
        
        def get_data_from_table(self, table_name: str, filters: dict = None, days: int = None):
                """
                Get data from a specific table with optional filtering by columns and date range.
                
                Args:
                        table_name (str): Name of the table to query
                        filters (dict, optional): Dictionary of column name and value pairs to filter by
                        days (int, optional): Number of days to look back from today
                        
                Returns:
                        pd.DataFrame: DataFrame containing the requested data
                """
                try:
                        # Start building the query
                        query = f"SELECT * FROM {table_name}"
                        
                        # Add WHERE clauses for filters
                        where_clauses = []
                        
                        # Add date range filter if days is specified
                        if days is not None:
                                current_date = datetime.now().date()
                                start_date = current_date - timedelta(days=days)
                                where_clauses.append(f"date >= '{start_date}'")
                        
                        # Add column filters
                        if filters is not None:
                                for column, value in filters.items():
                                        if isinstance(value, list):
                                                # If value is a list, use IN clause
                                                values_str = ', '.join([f"'{v}'" for v in value])
                                                where_clauses.append(f"{column} IN ({values_str})")
                                        else:
                                                # Otherwise use equals
                                                where_clauses.append(f"{column} = '{value}'")
                        
                        # Add WHERE clause to query if we have conditions
                        if where_clauses:
                                query += " WHERE " + " AND ".join(where_clauses)

                        df = pd.read_sql(query, self.engine)
                        
                        # Convert date column to datetime if it exists
                        if 'date' in df.columns:
                                df['date'] = pd.to_datetime(df['date'])
                        
                        return df
                        
                except Exception as e:
                        print(f"Error getting data from {table_name}: {e}")
                        # Return empty DataFrame with proper structure
                        return pd.DataFrame()
                
# ------------------------- additional db functions --------------------------------
        def execute_query(self, query:str, load_df=False):     
                if load_df:
                        #print(f"Executing query and loading into DataFrame: {query}")
                        df = pd.read_sql(self._coerce_text(query), self.engine)
                        return df
                else:   
                        with self.engine.begin() as connection:
                                connection.execute(self._coerce_text(query))
                        return None
                
        def refresh_materialized_view(self, view_name:str):
                exec_string = f"REFRESH MATERIALIZED VIEW {view_name};"
                with self.engine.begin() as connection:
                        connection.execute(text(exec_string))
                print(f"Materialized view {view_name} refreshed.")

        def get_table(self, table_name:str, limit:int=10000):
                exec_string = f"SELECT * FROM {table_name} LIMIT {limit};"
                df = pd.read_sql(text(exec_string), self.engine)
                return df
        
        def delete_all_rows(self, table_name:str): # dangerous function, use with caution!
                exec_string = f"DELETE FROM {table_name};"
                with self.engine.begin() as connection:
                        connection.execute(text(exec_string))
                print(f"All rows deleted from {table_name}.")
        
        def get_last_price_eth(self, origin_key:str, granularity:str='daily'):
                if granularity == 'daily':
                        table_name = 'fact_kpis'
                        granularity_filter = ''
                        order_by_col = 'date'
                elif granularity == 'hourly':
                        table_name = 'fact_kpis_granular'
                        granularity_filter = "AND granularity = 'hourly'"
                        order_by_col = 'timestamp'

                try:
                        query = f"SELECT value FROM {table_name} WHERE origin_key = '{origin_key}' AND metric_key = 'price_eth' {granularity_filter} ORDER BY {order_by_col} DESC LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(text(query))
                                latest_price = result.scalar()
                                return latest_price
                except Exception as e:
                        print(f"Error retrieving the latest price in ETH for {origin_key}.")
                        print(e)
                        return None
                
        def get_last_price_usd(self, origin_key:str, granularity:str='daily'):
                if granularity == 'daily':
                        table_name = 'fact_kpis'
                        granularity_filter = ''
                        order_by_col = 'date'
                elif granularity == 'hourly':
                        table_name = 'fact_kpis_granular'
                        granularity_filter = "AND granularity = 'hourly'"
                        order_by_col = 'timestamp'

                try:
                        query = f"SELECT value FROM {table_name} WHERE origin_key = '{origin_key}' AND metric_key = 'price_usd' {granularity_filter} ORDER BY {order_by_col} DESC LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(text(query))
                                latest_price = result.scalar()
                                return latest_price
                except Exception as e:
                        print(f"Error retrieving the latest price in USD for {origin_key}.")
                        print(e)
                        return None
                
        def get_stage(self, origin_key:str):
                try:
                        query = f"SELECT l2beat_stage FROM sys_main_conf WHERE origin_key = '{origin_key}' LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(text(query))
                                stage = result.scalar()
                                return stage
                except Exception as e:
                        print(f"Error retrieving the stage for {origin_key}.")
                        print(e)
                        return None
                
        def get_stages_dict(self):
                exec_string = "SELECT origin_key, l2beat_stage FROM sys_main_conf WHERE l2beat_stage IS NOT NULL"
                df = pd.read_sql(exec_string, self.engine.connect())
                stages_dict = df.set_index("origin_key").to_dict()["l2beat_stage"]
                return stages_dict
        
        def get_maturity_dict(self):
                exec_string = "SELECT origin_key, maturity FROM sys_main_conf WHERE maturity IS NOT NULL"
                df = pd.read_sql(exec_string, self.engine.connect())
                maturity_dict = df.set_index("origin_key").to_dict()["maturity"]
                return maturity_dict
                
        def get_chain_info(self, origin_key:str, column:str):
                try:
                        query = f"SELECT {column} FROM sys_main_conf WHERE origin_key = '{origin_key}' LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(text(query))
                                value = result.scalar()
                                return value
                except Exception as e:
                        print(f"Error retrieving {column} for {origin_key}.")
                        print(e)
                        return None
                
        def get_max_date(self, metric_key:str, origin_key:str):
                exec_string = f"SELECT MAX(date) as val FROM fact_kpis WHERE metric_key = '{metric_key}' AND origin_key = '{origin_key}';"

                with self.engine.connect() as connection:
                        val = connection.execute(text(exec_string)).scalar()
                return val
        
        def get_blockspace_max_date(self, origin_key:str):
                exec_string = text(f"SELECT MAX(date) as val FROM blockspace_fact_contract_level WHERE origin_key = '{origin_key}'")

                with self.engine.connect() as connection:
                        row = connection.execute(exec_string).mappings().first()
                val = row["val"] if row else None
                
                return val

        def get_max_block(self, table_name: str, date: str | None = None) -> int:
                if date is None:
                        exec_string = text(f"SELECT MAX(block_number) AS val FROM {table_name}")
                else:
                        exec_string = text(
                        f"""
                        SELECT MAX(block_number) AS val
                        FROM {table_name}
                        WHERE date_trunc('day', block_timestamp) = '{date}'
                        """
                        )

                with self.engine.connect() as connection:
                        row = connection.execute(exec_string).mappings().first()
                val = row["val"] if row else None

                return 0 if val is None else int(val)
                
        def get_min_block(self, table_name:str, date:str=None):
                if date is None:
                        exec_string = f"SELECT MIN(block_number) as val FROM {table_name};"
                else:
                        exec_string = f"SELECT MIN(block_number) as val FROM {table_name} WHERE date_trunc('day', block_timestamp) = '{date}';"

                with self.engine.connect() as connection:
                        val = connection.execute(text(exec_string)).scalar()
                
                if val is None:
                        return 0
                else:
                        return val
                
        def get_eim_fact(self, metric_key, origin_keys=None, days=7):
                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND origin_key in ('" + "', '".join(origin_keys) + "')"

                exec_string = f'''
                        SELECT 
                                date
                                ,origin_key
                                ,value
                        FROM eim_fact
                        WHERE metric_key = '{metric_key}'
                                {ok_string}
                                AND date >= date_trunc('day',now()) - interval '{days} days'
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        def get_eth_exported(self, days):
                exec_string = f"""
                        SELECT origin_key, "date", value, split_part(metric_key, '_', 3) AS asset
                        FROM public.eim_fact
                        where metric_key like 'eth_exported_%%'
                        AND date >= date_trunc('day', current_date - interval '{days}' day)
                        """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_holders_raw_balances(self, days):
                exec_string = f"""
                        SELECT holder_key, "date", value, split_part(metric_key, '_', 2) AS asset
                        FROM public.eim_holders_balance
                        where metric_key like 'balance_%%'
                        AND date >= date_trunc('day', current_date - interval '{days}' day)
                        """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_holders_with_balances(self):
                exec_string = f"""
                        WITH latest_data AS (
                                SELECT 
                                        holder_key, 
                                        metric_key, 
                                        value,
                                        ROW_NUMBER() OVER (PARTITION BY holder_key, metric_key ORDER BY date DESC) AS rn
                                FROM eim_holders_balance
                                WHERE metric_key IN ('eth_equivalent_balance_eth', 'eth_equivalent_balance_usd')
                        )
                        SELECT 
                                holder_key, 
                                name, 
                                type, 
                                holding_type,
                                SUM(CASE WHEN metric_key = 'eth_equivalent_balance_eth' THEN value END) AS eth_equivalent_balance_eth,
                                SUM(CASE WHEN metric_key = 'eth_equivalent_balance_usd' THEN value END) AS eth_equivalent_balance_usd
                        FROM latest_data
                        LEFT JOIN eim_holders USING (holder_key)
                        WHERE rn = 1
                        GROUP BY 1,2,3,4
                """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df       

        """
        The get_economics_in_eth function is used to get the economics data on chain level in ETH. The following metrics are calculated:
        - blob_size_bytes: total blob size in bytes (currently sum of celestia and ethereum blob size)
        - costs_blobs_eth: costs for storing blobs in ETH (sum of celestia and ethereum blobs)
        - costs_l1_eth: costs for ethereuem excl blobs (sum of l1 data availability and l1 settlement)
        - rent_paid_eth: total amount of fees that is paid to Ethereum (blobs and l1 data availability and l1 settlement)
        - costs_total_eth: total costs in ETH (sum of all costs of a chain)
        - profit_eth: profit in ETH (fees paid - costs_total_eth)
        """
        def get_economics_in_eth(self, days, exclude_chains, origin_keys = None, incl_profit = True):               
                if exclude_chains is None or len(exclude_chains) == 0:
                        exclude_string = ''
                else:
                        exclude_string = "AND tkd.origin_key not in ('" + "', '".join(exclude_chains) + "')"
                
                if origin_keys is None:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"
                
                if incl_profit:
                        profit_string = ",SUM(CASE WHEN metric_key = 'fees_paid_eth' THEN value END) - SUM(CASE WHEN metric_key in ('ethereum_blobs_eth', 'celestia_blobs_eth', 'eigenda_blobs_eth', 'cost_l1_raw_eth', 'l1_settlement_custom_eth') THEN value END) AS profit_eth"
                else:
                        profit_string = ''

                exec_string = f'''
                        SELECT 
                                date
                                ,origin_key
                                ,SUM(CASE WHEN metric_key in ('ethereum_blob_size_bytes', 'celestia_blob_size_bytes', 'eigenda_blob_size_bytes') THEN value END) AS blob_size_bytes

                                ,SUM(CASE WHEN metric_key in ('ethereum_blobs_eth', 'celestia_blobs_eth', 'eigenda_blobs_eth') THEN value END) AS costs_blobs_eth
                                ,SUM(CASE WHEN metric_key in ('cost_l1_raw_eth', 'l1_settlement_custom_eth') THEN value END) AS costs_l1_eth
                                ,SUM(CASE WHEN metric_key in ('cost_l1_raw_eth', 'ethereum_blobs_eth') THEN value END) AS rent_paid_eth

                                ,SUM(CASE WHEN metric_key in ('ethereum_blobs_eth', 'celestia_blobs_eth', 'eigenda_blobs_eth', 'cost_l1_raw_eth', 'l1_settlement_custom_eth') THEN value END) AS costs_total_eth
                                {profit_string}
                        FROM fact_kpis tkd
                        WHERE metric_key in ('ethereum_blob_size_bytes', 'eigenda_blob_size_bytes', 'celestia_blob_size_bytes', 'eigenda_blobs_eth', 'celestia_blobs_eth', 'ethereum_blobs_eth', 'cost_l1_raw_eth', 'l1_settlement_custom_eth', 'fees_paid_eth')
                                {exclude_string}
                                {ok_string}
                                AND date >= date_trunc('day',now()) - interval '{days} days'
                                AND date < date_trunc('day', now())
                        GROUP BY 1,2
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_da_metrics_in_eth(self, days, origin_keys = None):
                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                exec_string = f'''
                        SELECT 
                                date
                                ,origin_key
                                ,'da_fees_per_mbyte_eth' as metric_key
                                ,SUM(CASE WHEN metric_key = 'da_fees_eth' THEN value END) / (SUM(CASE WHEN metric_key = 'da_data_posted_bytes' THEN value END)/1024/1024) AS value
                        FROM fact_kpis tkd
                        WHERE metric_key in ('da_data_posted_bytes', 'da_fees_eth')
                                {ok_string}
                                AND date >= date_trunc('day',now()) - interval '{days} days'
                                AND date < date_trunc('day', now())
                        GROUP BY 1,2

                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_fdv_in_usd(self, days, origin_keys = None):               
                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                exec_string = f'''
                        with tmp as (
                                SELECT 
                                        date,
                                        origin_key,
                                        SUM(CASE WHEN metric_key = 'price_usd' THEN value END) AS price_usd,
                                        SUM(CASE WHEN metric_key = 'total_supply' THEN value END) AS total_supply
                                FROM fact_kpis tkd
                                WHERE metric_key = 'price_usd' or metric_key = 'total_supply'
                                        {ok_string}
                                        AND date >= date_trunc('day',now()) - interval '{days} days'
                                GROUP BY 1,2
                        )

                        SELECT
                                date, 
                                origin_key,
                                'fdv_usd' as metric_key,
                                price_usd * total_supply as value 
                        FROM tmp
                        WHERE price_usd > 0 and total_supply > 0
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_values_in_eth(self, metric_keys, days, origin_keys = None): ## also make sure to add new metrics in adapter_sql
                mk_string = "'" + "', '".join(metric_keys) + "'"

                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                print(f"load eth values for : {mk_string} and {origin_keys}")
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        SELECT 
                                Case tkd.metric_key 
                                        WHEN 'rent_paid_usd' THEN 'rent_paid_eth'
                                        WHEN 'fees_paid_usd' THEN 'fees_paid_eth'
                                        WHEN 'profit_usd' THEN 'profit_eth'
                                        WHEN 'app_fees_usd' THEN 'app_fees_eth'
                                        WHEN 'tvl' THEN 'tvl_eth'
                                        WHEN 'stables_mcap' THEN 'stables_mcap_eth' 
                                        WHEN 'txcosts_median_usd' THEN 'txcosts_median_eth'
                                        WHEN 'fdv_usd' THEN 'fdv_eth'
                                        ELSE 'error'
                                END AS metric_key, 
                                tkd.origin_key,
                                tkd."date", 
                                tkd.value / p.value as value
                        FROM fact_kpis tkd
                        LEFT JOIN eth_price p on tkd."date" = p."date"
                        WHERE tkd.metric_key in ({mk_string})
                                {ok_string}
                                AND tkd.date >= date_trunc('day',now()) - interval '{days} days'
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_values_in_usd(self, metric_keys, days, origin_keys = None): ## also make sure to add new metrics in adapter_sql
                mk_string = "'" + "', '".join(metric_keys) + "'"

                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                print(f"load usd values for : {mk_string} and {origin_keys}")
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        SELECT 
                                Case tkd.metric_key 
                                        WHEN 'rent_paid_eth' THEN 'rent_paid_usd'
                                        WHEN 'ethereum_blobs_eth' THEN 'ethereum_blobs_usd'
                                        WHEN 'celestia_blobs_eth' THEN 'celestia_blobs_usd'
                                        WHEN 'eigenda_blobs_eth' THEN 'eigenda_blobs_usd'
                                        WHEN 'costs_blobs_eth' THEN 'costs_blobs_usd'
                                        WHEN 'cost_l1_raw_eth' THEN 'cost_l1_raw_usd'
                                        WHEN 'costs_l1_eth' THEN 'costs_l1_usd'
                                        WHEN 'costs_total_eth' THEN 'costs_total_usd'
                                        WHEN 'da_fees_eth' THEN 'da_fees_usd'
                                        WHEN 'fees_paid_eth' THEN 'fees_paid_usd'
                                        WHEN 'fees_paid_base_eth' THEN 'fees_paid_base_usd'
                                        WHEN 'fees_paid_priority_eth' THEN 'fees_paid_priority_usd'
                                        WHEN 'profit_eth' THEN 'profit_usd'
                                        WHEN 'txcosts_median_eth' THEN 'txcosts_median_usd'
                                        WHEN 'da_fees_per_mbyte_eth' THEN 'da_fees_per_mbyte_usd'
                                        ELSE 'error'
                                END AS metric_key, 
                                tkd.origin_key,
                                tkd."date", 
                                tkd.value * p.value as value
                        FROM fact_kpis tkd
                        LEFT JOIN eth_price p on tkd."date" = p."date"
                        WHERE tkd.metric_key in ({mk_string})
                                {ok_string}
                                AND tkd.date < date_trunc('day', NOW()) 
                                AND tkd.date >= date_trunc('day',now()) - interval '{days} days'
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_values_in_usd_hourly(self, metric_keys, days, origin_keys = None): ## also make sure to add new metrics in adapter_sql
                mk_string = "'" + "', '".join(metric_keys) + "'"

                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                print(f"load usd values for : {mk_string} and {origin_keys}")
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        SELECT 
                                Case tkd.metric_key 
                                        WHEN 'rent_paid_eth' THEN 'rent_paid_usd'
                                        WHEN 'ethereum_blobs_eth' THEN 'ethereum_blobs_usd'
                                        WHEN 'celestia_blobs_eth' THEN 'celestia_blobs_usd'
                                        WHEN 'eigenda_blobs_eth' THEN 'eigenda_blobs_usd'
                                        WHEN 'costs_blobs_eth' THEN 'costs_blobs_usd'
                                        WHEN 'cost_l1_raw_eth' THEN 'cost_l1_raw_usd'
                                        WHEN 'costs_l1_eth' THEN 'costs_l1_usd'
                                        WHEN 'costs_total_eth' THEN 'costs_total_usd'
                                        WHEN 'da_fees_eth' THEN 'da_fees_usd'
                                        WHEN 'fees_paid_eth' THEN 'fees_paid_usd'
                                        WHEN 'fees_paid_base_eth' THEN 'fees_paid_base_usd'
                                        WHEN 'fees_paid_priority_eth' THEN 'fees_paid_priority_usd'
                                        WHEN 'profit_eth' THEN 'profit_usd'
                                        WHEN 'txcosts_median_eth' THEN 'txcosts_median_usd'
                                        WHEN 'da_fees_per_mbyte_eth' THEN 'da_fees_per_mbyte_usd'
                                        ELSE 'error'
                                END AS metric_key, 
                                tkd.origin_key,
                                tkd.timestamp as timestamp, 
                                tkd.value * p.value as value,
                                'hourly' as granularity
                        FROM fact_kpis_granular tkd
                        LEFT JOIN eth_price p on date_trunc('day', tkd.timestamp) = p."date"
                        WHERE tkd.metric_key in ({mk_string})
                                {ok_string}
                                AND tkd.timestamp < date_trunc('hour', NOW()) 
                                AND tkd.timestamp >= date_trunc('hour',now()) - interval '{days} days'
                                AND tkd.granularity = 'hourly'
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_values_in_usd_eim(self, metric_keys, days, origin_keys = None):
                mk_string = "'" + "', '".join(metric_keys) + "'"

                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                print(f"load usd values for : {mk_string} and {origin_keys}")
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        SELECT 
                                Case tkd.metric_key 
                                        WHEN 'eth_equivalent_exported_eth' THEN 'eth_equivalent_exported_usd'
                                        WHEN 'eth_supply_eth' THEN 'eth_supply_usd'
                                        ELSE 'error'
                                END AS metric_key, 
                                tkd.origin_key,
                                tkd."date", 
                                tkd.value * p.value as value
                        FROM eim_fact tkd
                        LEFT JOIN eth_price p on tkd."date" = p."date"
                        WHERE tkd.metric_key in ({mk_string})
                                {ok_string}
                                AND tkd.date >= date_trunc('day',now()) - interval '{days} days'
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_values_in_usd_eim_holders(self, metric_keys, days, holder_keys = None):
                mk_string = "'" + "', '".join(metric_keys) + "'"

                if holder_keys is None or len(holder_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(holder_keys) + "')"

                print(f"load usd values for : {mk_string} and {holder_keys}")
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        SELECT 
                                Case tkd.metric_key 
                                        WHEN 'eth_equivalent_balance_eth' THEN 'eth_equivalent_balance_usd'
                                        ELSE 'error'
                                END AS metric_key, 
                                tkd.holder_key,
                                tkd."date", 
                                tkd.value * p.value as value
                        FROM eim_holders_balance tkd
                        LEFT JOIN eth_price p on tkd."date" = p."date"
                        WHERE tkd.metric_key in ({mk_string})
                                {ok_string}
                                AND tkd.date >= date_trunc('day',now()) - interval '{days} days'
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        """
        Get the inflation rate for ETH. The inflation rate is calculated as the daily change in ETH supply over the past 7 days.       
        """
        def get_eth_issuance_rate(self, days):
                exec_string = f'''
                        WITH rolling_avg AS (
                                SELECT
                                        date,
                                        value,
                                        AVG(value) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_avg
                                FROM
                                        eim_fact
                                WHERE metric_key = 'eth_supply_eth' and origin_key = 'ethereum'
                                        and date >= date_trunc('day',now()) - interval '{days+14} days'
                                ),
                        dod_change AS (
                                SELECT
                                        date,
                                        rolling_7d_avg,
                                        LAG(rolling_7d_avg) OVER (ORDER BY date) AS previous_day_avg
                                FROM
                                        rolling_avg
                        )
                        SELECT
                                date,
                                CASE
                                        WHEN previous_day_avg IS NULL THEN NULL
                                        ELSE (rolling_7d_avg - previous_day_avg) / previous_day_avg
                                END * 365 AS value
                        FROM dod_change
                        WHERE date >= date_trunc('day',now()) - interval '{days} days'

                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
                
        def get_metric_sources(self, metric_id:str, origin_keys:list):
                if len(origin_keys) == 0:
                        exec_string = f'''
                                SELECT DISTINCT source
                                FROM metric_sources
                                WHERE metric_id = '{metric_id}'
                        '''
                else:
                        ok_string = "'" + "', '".join(origin_keys) + "'"
                        exec_string = f'''
                                SELECT DISTINCT source
                                FROM metric_sources
                                WHERE metric_id = '{metric_id}'
                                        AND origin_key in ({ok_string})
                        '''
                        # print (exec_string)
                

                df = pd.read_sql(exec_string, self.engine.connect())
                return df['source'].to_list()
        
        ## Unique sender and addresses
        def aggregate_unique_addresses(self, chain:str, days:int, days_end:int=None):

                if days_end is None:
                        days_end_string = "DATE_TRUNC('day', NOW())"
                else:
                        if days_end > days:
                                raise ValueError("days_end must be smaller than days")
                        days_end_string = f"DATE_TRUNC('day', NOW() - INTERVAL '{days_end} days')"

                exec_string = f'''
                        INSERT INTO fact_active_addresses (address, date, origin_key, txcount)
                                SELECT 
                                        from_address as address,
                                        date_trunc('day', block_timestamp) as date,                                                
                                        '{chain}' as origin_key,
                                        count(*) as txcount
                                FROM {chain}_tx
                                WHERE block_timestamp < {days_end_string}
                                        AND block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and from_address is not null
                                GROUP BY 1,2,3
                        ON CONFLICT (origin_key, date, address)
                        DO UPDATE SET txcount = EXCLUDED.txcount;
                '''

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"Unique addresses for {chain} and {days} days aggregated and loaded into fact_active_addresses.")

                
        
        ## This method aggregates on top of fact_active_addresses and stores memory efficient hll hashes in fact_active_addresses_hll
        def aggregate_unique_addresses_hll(self, chain:str, days:int, days_end:int=None):   
                if days_end is None:
                        days_end_string = "DATE_TRUNC('day', NOW())"
                else:
                        if days_end > days:
                                raise ValueError("days_end must be smaller than days")
                        days_end_string = f"DATE_TRUNC('day', NOW() - INTERVAL '{days_end} days')"

                ## in starknets case go straight to the source (raw tx data) because we don't add it to fact_active_addresses  
                if chain in ['starknet']:    
                        exec_string = f'''
                                INSERT INTO fact_active_addresses_hll (origin_key, date, hll_addresses)
                                        SELECT 
                                                '{chain}' as origin_key,
                                                date_trunc('day', block_timestamp) as date,
                                                hll_add_agg(hll_hash_text(from_address), 17,5,-1,1)        
                                        FROM {chain}_tx
                                        WHERE block_timestamp < {days_end_string}
                                                AND block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        GROUP BY 1,2
                                ON CONFLICT (origin_key, date)
                                DO UPDATE SET hll_addresses = EXCLUDED.hll_addresses;
                        '''

                else:
                        exec_string = f'''
                                INSERT INTO fact_active_addresses_hll (origin_key, date, hll_addresses) 
                                        select 
                                                origin_key, 
                                                date,
                                                hll_add_agg(hll_hash_bytea(address), 17,5,-1,1)
                                        from fact_active_addresses 
                                        where origin_key = '{chain}' 
                                                and date < {days_end_string}
                                                AND date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        group by 1,2
                                ON CONFLICT (origin_key, date)
                                DO UPDATE SET hll_addresses = EXCLUDED.hll_addresses;
                        '''

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"HLL hashes for {chain} and {days} days loaded into fact_active_addresses_hll.")

        def aggregate_unique_addresses_contracts_hll(self, chain:str, days:int, days_end:int=None):   
                if days_end is None:
                        days_end_string = "DATE_TRUNC('day', NOW())"
                else:
                        if days_end > days:
                                raise ValueError("days_end must be smaller than days")
                        days_end_string = f"DATE_TRUNC('day', NOW() - INTERVAL '{days_end} days')"
                
                
                if chain in ['megaeth', 'polygon_pos', 'starknet', 'ronin']:
                        exec_string= f'''
                        INSERT INTO fact_active_addresses_contract_hll (address, origin_key, date, hll_addresses)
                                SELECT 
                                        address,
                                        origin_key,
                                        date,
                                        hll_add_agg(hll_hash_bytea(from_address), 17,5,-1,1)        
                                FROM fact_active_addresses_contract
                                WHERE 
                                        date < {days_end_string}
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and origin_key = '{chain}'
                                GROUP BY 1,2,3
                        ON CONFLICT (address, origin_key, date)
                        DO UPDATE SET hll_addresses = EXCLUDED.hll_addresses;
                        '''     
                ## Having clause: not worth it to add addresses that where only called by one from_address (which is the case for 90% of to_addresses)
                else:
                        exec_string = f'''
                                INSERT INTO fact_active_addresses_contract_hll (address, origin_key, date, hll_addresses)
                                        SELECT 
                                                to_address as address,
                                                '{chain}' as origin_key,
                                                date_trunc('day', block_timestamp) as date,
                                                hll_add_agg(hll_hash_bytea(from_address), 17,5,-1,1)        
                                        FROM {chain}_tx
                                        WHERE 
                                                to_address is not null
                                                AND block_timestamp < {days_end_string}
                                                AND block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        GROUP BY 1,2,3
                                        HAVING COUNT(DISTINCT from_address) > 1
                                ON CONFLICT (address, origin_key, date)
                                DO UPDATE SET hll_addresses = EXCLUDED.hll_addresses;
                        '''

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"HLL hashes on contract level for {chain} and {days} days loaded into fact_active_addresses_contract_hll.")
               
        def aggregate_unique_addresses_contracts_hll_hourly(self, chain:str, hours:int, hours_end:int=None):   
                if hours_end is None:
                        hours_end_string = "DATE_TRUNC('hour', NOW())"
                else:
                        if hours_end > hours:
                                raise ValueError("hours_end must be smaller than days * 24")
                        hours_end_string = f"DATE_TRUNC('hour', NOW() - INTERVAL '{hours_end} hours')"
                
                
                if chain in ['megaeth', 'polygon_pos', 'starknet', 'ronin']:
                        exec_string= f'''
                        INSERT INTO fact_active_addresses_contract_hourly_hll (address, origin_key, hour, hll_addresses)
                                SELECT 
                                        address,
                                        origin_key,
                                        hour,
                                        hll_add_agg(hll_hash_bytea(from_address), 17,5,-1,1)        
                                FROM fact_active_addresses_contract_hourly
                                WHERE 
                                        hour < {hours_end_string}
                                        and hour >= DATE_TRUNC('hour', NOW() - INTERVAL '{hours} hours')
                                        and origin_key = '{chain}'
                                GROUP BY 1,2,3
                        ON CONFLICT (address, origin_key, hour)
                        DO UPDATE SET hll_addresses = EXCLUDED.hll_addresses;
                        '''     
                ## Having clause: not worth it to add addresses that where only called by one from_address (which is the case for 90% of to_addresses)
                else:
                        exec_string = f'''
                                INSERT INTO fact_active_addresses_contract_hourly_hll (address, origin_key, hour, hll_addresses)
                                        SELECT 
                                                to_address as address,
                                                '{chain}' as origin_key,
                                                date_trunc('hour', block_timestamp) as hour,
                                                hll_add_agg(hll_hash_bytea(from_address), 17,5,-1,1)        
                                        FROM {chain}_tx
                                        WHERE 
                                                to_address is not null
                                                AND block_timestamp < {hours_end_string}
                                                AND block_timestamp >= DATE_TRUNC('hour', NOW() - INTERVAL '{hours} hours')
                                        GROUP BY 1,2,3
                                        HAVING COUNT(DISTINCT from_address) > 1
                                ON CONFLICT (address, origin_key, hour)
                                DO UPDATE SET hll_addresses = EXCLUDED.hll_addresses;
                        '''

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"HLL hashes on contract level for {chain} and {hours} hours loaded into fact_active_addresses_contract_hourly_hll.")
               
        
        def get_total_supply_blocks(self, origin_key, days):
                ## changed to Min instead of Max on July 29th, 2025 (to run it earlier in the day)
                exec_string = f'''
                        SELECT 
                                block_date AS date,
                                MIN(block_number) AS block_number
                        FROM public.{origin_key}_tx
                        WHERE block_date > (CURRENT_DATE - INTERVAL '{days+1} days')
                        GROUP BY 1;
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        ## Blockspace queries
        # This function is used to get aggregate the blockspace data on contract level for a specific chain. The data will be loaded into fact_contract_level table
        # it only aggregates transactions that are NOT native transfers, system transactionsm contract creations, or inscriptions. These are aggregated separately and output is stored directly in the fact_sub_category_level table
        def get_blockspace_contracts(self, chain, days):
                ## Mantle and Metis store fees in own tokens: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis', 'celo']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                       
                        additional_join = ''

                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        {additional_cte}
                        
                        INSERT INTO blockspace_fact_contract_level (address, date, gas_fees_eth, gas_fees_usd, txcount, daa, origin_key, success_rate, median_tx_fee)
                                select
                                        to_address as address,
                                        date_trunc('day', block_timestamp) as date,
                                        sum({tx_fee_eth_string}) as gas_fees_eth,
                                        sum({tx_fee_usd_string}) as gas_fees_usd,
                                        count(*) as txcount,
                                        count(distinct from_address) as daa,
                                        '{chain}' as origin_key,
                                        sum(status)*1.0/count(*) as success_rate,
                                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                                from {chain}_tx tx 
                                LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                                {additional_join}
                                where block_timestamp < DATE_TRUNC('day', NOW())
                                        and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and empty_input = false -- we don't have to store addresses that received native transfers
                                        and tx_fee > 0 -- no point in counting txs with 0 fees (most likely system tx)
                                        and to_address <> '' 
                                        and to_address is not null -- filter out contract creations arbitrum, optimism
                                        and to_address <> '\\x0000000000000000000000000000000000008006' -- filter out contract creations zksync
                                        and to_address <> 'None' -- filter out zora and pgn contract creation
                                        and not (from_address = to_address and empty_input = false) -- filter out inscriptions (step 1: from_address = to_address + empty_input = false)
                                        and not (to_address in (select distinct address from inscription_addresses) and empty_input = false) -- filter out inscriptions (step 2: to_address in inscription_addresses + empty_input = false)
                                group by 1,2
                                having count(*) > 1
                        ON CONFLICT (origin_key, date, address)
                        DO UPDATE SET
                                txcount = EXCLUDED.txcount,
                                daa = EXCLUDED.daa,
                                gas_fees_eth = EXCLUDED.gas_fees_eth,
                                gas_fees_usd = EXCLUDED.gas_fees_usd,
                                success_rate = EXCLUDED.success_rate,
                                median_tx_fee = EXCLUDED.median_tx_fee;
                '''
                # df = pd.read_sql(exec_string, self.engine.connect())
                # return df

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"...data inserted successfully for {chain} and {days}.")
                
        def get_blockspace_contracts_hourly(self, chain, hours):
                ## Mantle and Metis store fees in own tokens: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis', 'celo']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                       
                        additional_join = ''

                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        {additional_cte}
                        
                        INSERT INTO blockspace_fact_contract_level_hourly (address, hour, gas_fees_eth, gas_fees_usd, txcount, daa, origin_key, success_rate, median_tx_fee, gas_used)
                                select
                                        to_address as address,
                                        date_trunc('hour', block_timestamp) as hour,
                                        sum({tx_fee_eth_string}) as gas_fees_eth,
                                        sum({tx_fee_usd_string}) as gas_fees_usd,
                                        count(*) as txcount,
                                        count(distinct from_address) as daa,
                                        '{chain}' as origin_key,
                                        sum(status)*1.0/count(*) as success_rate,
                                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee,
                                        sum(gas_used) as gas_used
                                from {chain}_tx tx 
                                LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                                {additional_join}
                                where block_timestamp < DATE_TRUNC('hour', NOW())
                                        and block_timestamp >= DATE_TRUNC('hour', NOW() - INTERVAL '{hours} hours')
                                        and empty_input = false -- we don't have to store addresses that received native transfers
                                        and tx_fee > 0 -- no point in counting txs with 0 fees (most likely system tx)
                                        and to_address <> '' 
                                        and to_address is not null -- filter out contract creations arbitrum, optimism
                                        and to_address <> '\\x0000000000000000000000000000000000008006' -- filter out contract creations zksync
                                        and to_address <> 'None' -- filter out zora and pgn contract creation
                                group by 1,2
                                having count(*) > 1
                        ON CONFLICT (origin_key, hour, address)
                        DO UPDATE SET
                                txcount = EXCLUDED.txcount,
                                daa = EXCLUDED.daa,
                                gas_fees_eth = EXCLUDED.gas_fees_eth,
                                gas_fees_usd = EXCLUDED.gas_fees_usd,
                                success_rate = EXCLUDED.success_rate,
                                median_tx_fee = EXCLUDED.median_tx_fee,
                                gas_used = EXCLUDED.gas_used;
                '''
                # df = pd.read_sql(exec_string, self.engine.connect())
                # return df

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"...data inserted successfully for {chain} and {hours} hours.")
        
        # This function is used to get the native_transfer daily aggregate per chain. The data will be loaded into fact_sub_category_level table        
        def get_blockspace_native_transfers(self, chain, days):
                ## Mantle and Metis store fees in own tokens: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis', 'celo']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                        
                        additional_join = ''


                ## native transfers: all transactions that have no input data
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )
                        {additional_cte}

                        SELECT 
                                date_trunc('day', block_timestamp) as date,
                                'native_transfer' as category_id,
                                '{chain}' as origin_key,
                                sum({tx_fee_eth_string}) as gas_fees_eth,
                                sum({tx_fee_usd_string}) as gas_fees_usd,
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        FROM {chain}_tx tx 
                        LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        {additional_join}
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and empty_input = true
                        group by 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
         # This function is used to get the inscriptions per chain. The data will be loaded into fact_sub_category_level table        
        def get_blockspace_inscriptions(self, chain, days):
                ## Mantle and Metis stores fees in own token: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis', 'celo']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                        
                        additional_join = ''


                ## native transfers: all transactions that have no input data
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )
                        {additional_cte}

                        SELECT 
                                date_trunc('day', block_timestamp) as date,
                                'inscriptions' as category_id,
                                '{chain}' as origin_key,
                                sum({tx_fee_eth_string}) as gas_fees_eth,
                                sum({tx_fee_usd_string}) as gas_fees_usd,
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        FROM {chain}_tx tx 
                        LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        {additional_join}
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and tx_fee > 0
                                and (
                                        (from_address = to_address and empty_input = false)
                                        or 
                                        (to_address in (select distinct address from inscription_addresses) and empty_input = false)
                                )
                        group by 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # This function is used to get the contract_deployment daily aggregate per chain. The data will be loaded into fact_sub_category_level table
        def get_blockspace_contract_deplyments(self, chain, days):
                ## Mantle and Metis store fees in own token: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis', 'celo']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                        
                        additional_join = ''

                if chain == 'zksync_era':
                        filter_string = "and to_address = '\\x0000000000000000000000000000000000008006'"
                elif chain == 'polygon_zkevm':
                        filter_string = "and receipt_contract_address is not null"
                else:
                        filter_string = "and (to_address = '' or to_address is null)"

                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )
                        {additional_cte}

                        SELECT 
                                date_trunc('day', block_timestamp) as date,
                                'contract_deployment' as category_id,
                                '{chain}' as origin_key,
                                sum({tx_fee_eth_string}) as gas_fees_eth,
                                sum({tx_fee_usd_string}) as gas_fees_usd,
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        FROM {chain}_tx tx 
                        LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        {additional_join}
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                {filter_string}
                        group by 1
                '''
                #print(exec_string)
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # This function is used to get the total blockspace fees per day for a specific chain. The data will be loaded into fact_sub_category_level table.
        def get_blockspace_total(self, chain, days):
                ## Mantle and Metis store fees in own tokens: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis', 'celo']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                        
                        additional_join = ''

                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )
                        {additional_cte}

                        select 
                                date_trunc('day', block_timestamp) as date,
                                'total_usage' as category_id,
                                '{chain}' as origin_key,
                                sum({tx_fee_eth_string}) as gas_fees_eth,
                                sum({tx_fee_usd_string}) as gas_fees_usd, 
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        from {chain}_tx tx
                        left join eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        {additional_join}
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and tx_fee > 0 -- no point in counting txs with 0 fees (most likely system tx)
                        group by 1
                        '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_total_starknet(self, days):
                exec_string = f'''
                        SELECT 
                                "date", 
                                'total_usage' as category_id,
                                'starknet' as origin_key,
                                sum(gas_fees_eth) as gas_fees_eth,
                                sum(gas_fees_usd) as gas_fees_usd, 
                                sum(txcount) as txcount,
                                sum(daa) as daa
                        FROM public.blockspace_fact_contract_level
                        where origin_key = 'starknet'
                                and "date" < DATE_TRUNC('day', NOW())
                                and "date" >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                        group by 1
                        '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # This function is used to aggregate the blockspace data from contract_level, map it to categories, and then it will be loaded into the sub_category level table. The data will be loaded into fact_sub_category_level table
        def get_blockspace_sub_categories(self, chain, days):
                exec_string = f'''
                        SELECT 
                                lower(bl.usage_category) as category_id,
                                '{chain}' as origin_key,
                                date,
                                sum(gas_fees_eth) as gas_fees_eth,
                                sum(gas_fees_usd) as gas_fees_usd,
                                sum(txcount) as txcount,
                                sum(daa) as daa
                        FROM public.blockspace_fact_contract_level cl
                        inner join vw_oli_label_pool_gold_pivoted_v2 bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
                        inner join vw_oli_category_mapping oli on oli.category_id = bl.usage_category
                        where date < DATE_TRUNC('day', NOW())
                                and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and cl.origin_key = '{chain}'
                                and bl.usage_category is not null and bl.usage_category <> 'contract_deployment'
                        group by 1,2,3
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # This function is used to calculate the unlabeled blockspace data for a specific chain. The data will be loaded into fact_sub_category_level table
        def get_blockspace_unlabeled(self, chain, days):
                exec_string = f'''
                        with labeled_usage as (
                                SELECT 
                                        date,
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount
                                FROM public.blockspace_fact_category_level
                                where date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and origin_key = '{chain}'
                                        and category_id <> 'unlabeled' and category_id <> 'total_usage'
                                group by 1
                        ),
                        total_usage as (
                                SELECT 
                                        date,
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount
                                FROM public.blockspace_fact_category_level
                                where date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and origin_key = '{chain}'
                                        and category_id = 'total_usage'
                                group by 1
                        )

                        select 
                                t.date,
                                'unlabeled' as category_id,
                                '{chain}' as origin_key,
                                t.gas_fees_eth - l.gas_fees_eth as gas_fees_eth,
                                t.gas_fees_usd - l.gas_fees_usd as gas_fees_usd,
                                t.txcount - l.txcount as txcount
                        from total_usage t
                        left join labeled_usage l on t.date = l.date
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        
        """
        function for the blockspace overview json and the single chain blockspace jsons
        it returns the top 100 contracts by gas fees for the given main category
        and the top 20 contracts by gas fees for each chain in the main category
        
        """
        def get_contracts_overview(self, main_category, days, origin_keys, contract_limit=100):
                
                date_string = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ''

                if main_category.lower() != 'unlabeled':
                        main_category_string = f"and bcm.main_category_id = lower('{main_category}')" 
                        sub_main_string = """
                                bl.usage_category as sub_category_key,
                                bcm.category_name as sub_category_name,
                                bcm.main_category_id as main_category_key,
                                bcm.main_category_name,
                        """
                else:
                        main_category_string = 'and bcm.main_category_id is null'
                        sub_main_string = """
                                'unlabeled' as sub_category_key,
                                'Unlabeled' as sub_category_name,
                                'unlabeled' as main_category_key,
                                'Unlabeled' as main_category_name,
                        """       

                exec_string = f'''
                        with top_contracts as (
                                SELECT 
                                        cl.address,
                                        cl.origin_key,
                                        UPPER(LEFT(bl.contract_name , 1)) || SUBSTRING(bl.contract_name FROM 2) as contract_name,
                                        oss.display_name as project_name,
                                        {sub_main_string}
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                left join vw_oli_label_pool_gold_pivoted_v2 bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
                                left join vw_oli_category_mapping bcm on lower(bl.usage_category) = lower(bcm.category_id) 
                                left join oli_oss_directory oss on bl.owner_project = oss.name
                                where 
                                        date < DATE_TRUNC('day', NOW())
                                        {date_string}
                                        {main_category_string}
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
                                group by 1,2,3,4,5,6,7,8
                                order by gas_fees_eth  desc
                                ),
                                
                        top_contracts_main_category_and_origin_key_by_gas as (
                                SELECT
                                        address,origin_key,contract_name,project_name,sub_category_key,sub_category_name,main_category_key,main_category_name,gas_fees_eth,gas_fees_usd,txcount,daa
                                FROM (
                                        SELECT
                                                ROW_NUMBER() OVER (PARTITION BY main_category_key, origin_key ORDER BY gas_fees_eth desc) AS r,
                                                t.*
                                        FROM top_contracts t) x
                                WHERE x.r <= 20
                                ),

                        top_contracts_main_category_and_origin_key_by_txcount as (
                                SELECT
                                        address,origin_key,contract_name,project_name,sub_category_key,sub_category_name,main_category_key,main_category_name,gas_fees_eth,gas_fees_usd,txcount,daa
                                FROM (
                                        SELECT
                                                ROW_NUMBER() OVER (PARTITION BY main_category_key, origin_key ORDER BY txcount desc) AS r,
                                                t.*
                                        FROM top_contracts t) x
                                WHERE x.r <= 20
                                )
                                
                        select * from (select * from top_contracts order by gas_fees_eth desc limit {contract_limit}) a
                        union 
                        select * from top_contracts_main_category_and_origin_key_by_gas
                        union 
                        select * from top_contracts_main_category_and_origin_key_by_txcount
                '''
                # print(main_category)
                # print(exec_string)
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        

        """
        This function is used to get the top contracts by category for the landing page's top 6 contracts section and the single chain hottest contract.
        It returns the top 6 contracts by gas fees for all categories and also returns the change in the top_by metric for the given contract and time period.
        top_by: gas or txcount
        days: 1, 7, 30, 90, 180, 365
        """
        def get_top_contracts_for_all_chains_with_change(self, top_by, days, origin_keys, limit=6):
                if top_by == 'gas':
                        top_by_string = 'gas_fees_eth'
                elif top_by == 'txcount':
                        top_by_string = 'txcount'
                elif top_by == 'daa':
                        top_by_string = 'daa'


                exec_string = f'''
                        -- get top 6 contracts by gas fees for all chains for the given time period
                        with top_contracts as (
                                SELECT
                                        cl.address,
                                        cl.origin_key,
                                        UPPER(LEFT(bl.contract_name , 1)) || SUBSTRING(bl.contract_name FROM 2) as contract_name,
                                        oss.display_name as project_name,
                                        bl.usage_category as sub_category_key,
                                        bcm.category_name as sub_category_name,
                                        bcm.main_category_id as main_category_key,
                                        bcm.main_category_name,
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                left join vw_oli_label_pool_gold_pivoted_v2 bl on cl.address = bl.address and cl.origin_key = bl.origin_key
                                left join vw_oli_category_mapping bcm on lower(bl.usage_category) = lower(bcm.category_id)
                                left join oli_oss_directory oss on bl.owner_project = oss.name
                                where
                                        date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
                                group by 1,2,3,4,5,6,7,8
                                order by {top_by_string} desc
                                limit {limit}
                        ),
                        -- get the change in the gas_fees_eth, gas_fees_usd, txcount, and daa for the given contracts for the time period before the selected time period
                        prev as (
                                SELECT
                                        cl.address,
                                        cl.origin_key,
                                        sum(cl.gas_fees_eth) as gas_fees_eth,
                                        sum(cl.gas_fees_usd) as gas_fees_usd,
                                        sum(cl.txcount) as txcount,
                                        round(avg(cl.daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                inner join top_contracts tc on tc.address = cl.address and tc.origin_key = cl.origin_key 
                                where
                                        date < DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days*2} days')
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
                                group by 1,2
                        )
                        -- join the two tables together to get the change in the top_by metric for the given contracts
                        select
                                tc.address,
                                tc.origin_key,
                                tc.contract_name,
                                tc.project_name,
                                tc.sub_category_key,
                                tc.sub_category_name,
                                tc.main_category_key,
                                tc.main_category_name,
                                tc.gas_fees_eth,
                                tc.gas_fees_usd,
                                tc.txcount,
                                tc.daa,
                                tc.gas_fees_eth - p.gas_fees_eth as gas_fees_eth_change,
                                tc.gas_fees_usd - p.gas_fees_usd as gas_fees_usd_change,
                                tc.txcount - p.txcount as txcount_change,
                                tc.daa - p.daa as daa_change,
                                p.gas_fees_eth as prev_gas_fees_eth,
                                p.gas_fees_usd as prev_gas_fees_usd,
                                p.txcount as prev_txcount,
                                p.daa as prev_daa,
                                ROUND(((tc.gas_fees_eth - p.gas_fees_eth) / p.gas_fees_eth)::numeric, 4) as gas_fees_eth_change_percent,
                                ROUND(((tc.gas_fees_usd - p.gas_fees_usd) / p.gas_fees_usd)::numeric, 4) as gas_fees_usd_change_percent,
                                ROUND(((tc.txcount - p.txcount) / p.txcount)::numeric, 4) as txcount_change_percent,
                                ROUND(((tc.daa - p.daa) / p.daa)::numeric, 4) as daa_change_percent
                        from top_contracts tc
                        left join prev p on tc.address = p.address and tc.origin_key = p.origin_key
                '''

                df = pd.read_sql(exec_string, self.engine.connect())

                return df
                
        
        """
        special function for the blockspace category comparison dashboard
        it returns the top 50 contracts by gas fees for the given main category
        and the top 10 contracts by gas fees for each sub category in the main category
        
        """
        def get_contracts_category_comparison(self, main_category, days, origin_keys:list):
                date_string = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ''
                if main_category.lower() != 'unlabeled':
                        main_category_string = f"and bcm.main_category_id = lower('{main_category}')"
                        sub_main_string = """
                                bl.usage_category as sub_category_key,
                                bcm.category_name as sub_category_name,
                                bcm.main_category_id as main_category_key,
                                bcm.main_category_name,
                        """
                else:
                        main_category_string = 'and bcm.main_category_id is null'
                        sub_main_string = """
                                'unlabeled' as sub_category_key,
                                'Unlabeled' as sub_category_name,
                                'unlabeled' as main_category_key,
                                'Unlabeled' as main_category_name,
                        """


                exec_string = f'''
                        with top_contracts as (
                                SELECT
                                        cl.address,
                                        cl.origin_key,
                                        UPPER(LEFT(bl.contract_name , 1)) || SUBSTRING(bl.contract_name FROM 2) as contract_name,
                                        oss.display_name as project_name,
                                        {sub_main_string}
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                left join vw_oli_label_pool_gold_pivoted_v2 bl on cl.address = bl.address and cl.origin_key = bl.origin_key
                                left join vw_oli_category_mapping bcm on lower(bl.usage_category) = lower(bcm.category_id)
                                left join oli_oss_directory oss on bl.owner_project = oss.name
                                where
                                        date < DATE_TRUNC('day', NOW())
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
                                        {date_string}
                                        {main_category_string}
                                group by 1,2,3,4,5,6,7,8
                                order by gas_fees_eth desc
                                ),

                        top_contracts_category_and_origin_key_by_gas as (
                                SELECT
                                        address,origin_key,contract_name,project_name,sub_category_key,sub_category_name,main_category_key,main_category_name,gas_fees_eth,gas_fees_usd,txcount,daa
                                FROM (
                                        SELECT
                                                ROW_NUMBER() OVER (PARTITION BY sub_category_key, origin_key ORDER BY gas_fees_eth desc) AS r,
                                                t.*
                                        FROM
                                                top_contracts t) x
                                WHERE
                                x.r <= 20
                                ),

                        top_contracts_category_and_origin_key_by_txcount as (
                                SELECT
                                        address,origin_key,contract_name,project_name,sub_category_key,sub_category_name,main_category_key,main_category_name,gas_fees_eth,gas_fees_usd,txcount,daa
                                FROM (
                                        SELECT
                                                ROW_NUMBER() OVER (PARTITION BY sub_category_key, origin_key ORDER BY txcount desc) AS r,
                                                t.*
                                        FROM
                                                top_contracts t) x
                                WHERE
                                x.r <= 20
                                )

                        select * from (select * from top_contracts order by gas_fees_eth desc limit 50) a
                        union select * from top_contracts_category_and_origin_key_by_gas
                        union select * from top_contracts_category_and_origin_key_by_txcount
                '''
                # print(main_category)
                # print(exec_string)
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # function to return the most used contracts by chain (used for blockscout adapter)
        def get_most_used_contracts(self, number_of_contracts, origin_key, days):
                number_of_contracts = int(number_of_contracts)
                exec_string = f'''
                        WITH ranked_contracts AS (
                                SELECT
                                        address, 
                                        origin_key, 
                                        SUM(gas_fees_eth) AS gas_eth, 
                                        SUM(txcount) AS txcount,
                                        ROUND(AVG(daa)) AS avg_daa,
                                        ROW_NUMBER() OVER (PARTITION BY origin_key ORDER BY SUM(gas_fees_eth) DESC) AS row_num_gas,
                                        ROW_NUMBER() OVER (PARTITION BY origin_key ORDER BY SUM(txcount) DESC) AS row_num_txcount,
                                        ROW_NUMBER() OVER (PARTITION BY origin_key ORDER BY SUM(daa) DESC) AS row_num_daa
                                FROM public.blockspace_fact_contract_level
                                WHERE origin_key = '{origin_key}' AND date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                GROUP BY 1,2
                        )
                        SELECT 
                                rc.address, 
                                rc.origin_key
                        FROM ranked_contracts rc
                        WHERE row_num_gas <= {str(number_of_contracts)} or row_num_txcount <= {str(number_of_contracts)} or row_num_daa <= {str(number_of_contracts)}
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        ### Sys Chains functions
        # This function takes a dataframe with origin_key and an additional columns as input and updates row-by-row the table sys_main_conf without overwriting other columns
        def update_sys_main_conf(self, df, column_type='str'):
                columns = df.columns.str.lower()
                
                if 'origin_key' not in columns:
                        raise Exception("origin_key column is missing")
                
                value_columns = [col for col in columns if col != 'origin_key']
                
                if len(value_columns) == 0:
                        raise Exception("At least one additional column is required in the dataframe")
                
                ## for each row in the dataframe, create an update statement
                for index, row in df.iterrows():
                        set_clauses = []
                        
                        for col in value_columns:
                                if column_type == 'str':
                                        set_clauses.append(f"{col} = '{row[col]}'")
                                else:
                                        raise NotImplementedError("Only string type is supported so far")
                        
                        set_clause = ", ".join(set_clauses)
                        
                        exec_string = f"""
                        UPDATE sys_main_conf
                        SET {set_clause}
                        WHERE origin_key = '{row['origin_key']}';
                        """

                        with self.engine.begin() as connection:
                                connection.execute(text(exec_string))
                
                print(f"{len(df)} records updated in sys_main_conf")
                

        ### OLI functions, Open Labels Initative ###

        ## This function is used to get all active projects from the OLI directory for json creation
        def get_active_projects(self, add_category=False, filtered_by_chains=[]):
                if len(filtered_by_chains) >0:
                        chains_str = ', '.join([f"'{chain}'" for chain in filtered_by_chains])
                        exec_string = f"""
                                WITH app_chain_stats AS (
                                SELECT 
                                        fact.owner_project AS name, 
                                        ood.display_name, 
                                        ood.description, 
                                        REPLACE(ood.github->0->>'url', 'https://github.com/', '') AS main_github,
                                        REPLACE(REPLACE(ood.social->'twitter'->0->>'url','https://twitter.com/',''),'https://x.com/','') AS twitter,
                                        ood.websites->0->>'url' AS website,
                                        ood.logo_path,
                                        upper(cg.symbol) AS token_symbol,
                                        fact.origin_key,
                                        COALESCE(SUM(fact.txcount) FILTER (WHERE fact.date > current_date - 30),0) AS txcount
                                FROM vw_apps_contract_level_materialized fact
                                JOIN oli_oss_directory ood 
                                        ON fact.owner_project = ood.name
                                LEFT JOIN coingecko_coins_list cg
                                        ON ood.token_coingecko_api_id = cg.id
                                WHERE fact.origin_key IN ({chains_str})
                                AND ood.active
                                GROUP BY 1,2,3,4,5,6,7,8,9
                                ),
                                app_stats AS (
                                        SELECT
                                                name,
                                                display_name,
                                                description,
                                                main_github,
                                                twitter,
                                                website,
                                                logo_path,
                                                token_symbol,
                                                SUM(txcount) AS txcount,
                                                jsonb_object_agg(origin_key, txcount ORDER BY origin_key) FILTER (WHERE txcount > 0) AS active_on
                                        FROM app_chain_stats
                                        GROUP by 1,2,3,4,5,6,7,8
                                )

                                SELECT
                                *,
                                RANK() OVER (ORDER BY txcount DESC) AS ecosystem_rank
                                FROM app_stats
                                ORDER BY txcount DESC;
                                """       
                else:
                        exec_string = """
                                SELECT 
                                        ood."name", 
                                        ood.display_name, 
                                        ood.description, 
                                        replace((ood.github->0->>'url'), 'https://github.com/', '') AS main_github,
                                        replace(replace((ood.social->'twitter'->0->>'url'), 'https://twitter.com/', ''),'https://x.com/', '') AS twitter,
                                        (ood.websites->0->>'url') AS website,
                                        ood.logo_path,
                                        upper(cg.symbol) AS token_symbol
                                FROM public.oli_oss_directory ood
                                LEFT JOIN coingecko_coins_list cg
                                        ON ood.token_coingecko_api_id = cg.id
                                WHERE active = true
                                """
                df = pd.read_sql(exec_string, self.engine.connect())

                if add_category:
                        exec_string = """
                       with raw_counts as (
                                select 
                                        owner_project,
                                        sub_category_key as category_id,
                                        case 
                                                when sub_category_key in ('native_transfer', 'fungible_tokens') then sum(txcount) * 0.05
                                                else sum(txcount)
                                        end as txcount
                                from vw_apps_contract_level_materialized vw
                                where "date" >= current_date - interval '365 days'
                                and vw.sub_category_key is not null
                                group by 1,2
                        ),
                        
                        raw_counts_2 as (
                                select 
                                        owner_project,
                                        category_id,
                                                        txcount,
                                        ROW_NUMBER() OVER (PARTITION BY owner_project ORDER BY sum(txcount) DESC) AS row_num
                                from raw_counts
                                group by 1,2,3
                        ),

                        sub_cats as (
                                select 
                                        owner_project,
                                        array_agg(distinct sub_category_key) as category_ids
                                from vw_apps_contract_level_materialized vw
                                where "date" >= current_date - interval '365 days'
                                and vw.sub_category_key is not null
                                group by 1
                        )

                        SELECT 
                                owner_project as "name", 
                                oc.name as "sub_category",
                                ocm."name" as main_category,
                                sc.category_ids as "sub_categories"
                        FROM raw_counts_2 r
                        left join oli_categories oc using (category_id)
                        left join oli_categories_main ocm using (main_category_id)
                        left join sub_cats sc using (owner_project)
                        WHERE row_num = 1
                        """
                        df_categories = pd.read_sql(exec_string, self.engine.connect())
                        df = df.merge(df_categories, on='name', how='left')

                return df

        def get_projects_for_airtable(self):
                exec_string = """
                        SELECT 
                                "name" AS "Name", 
                                display_name AS "Display Name", 
                                description AS "Description", 
                                replace((github->0->>'url'), 'https://github.com/', '') AS "Github" 
                        FROM public.oli_oss_directory
                        WHERE active = True;
                        """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        def deactivate_projects(self, names:list):
                exec_string = f"""
                        UPDATE oli_oss_directory
                        SET active = false
                        WHERE name IN ({', '.join([f"'{name}'" for name in names])})
                """
                with self.engine.begin() as connection:
                        connection.execute(text(exec_string))
                print(f"{len(names)} projects deactivated in oli_oss_directory: {names}")

        def get_tags_inactive_projects(self):
                exec_string = """
                        WITH active_projects AS (
                                SELECT * 
                                FROM oli_oss_directory ood 
                                WHERE active = True 
                        )

                        SELECT gt.*
                        FROM vw_oli_label_pool_gold_v2 gt 
                        LEFT JOIN active_projects ip ON ip.name = gt.tag_value
                        WHERE 
                                tag_id = 'owner_project'
                                and ip.name IS NULL
                        ORDER BY tag_value ASC
                """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        ## This function is used for our Airtable setup - it returns all rows from the gold table that match with an owner_project
        def get_oli_labels_gold_by_owner_project(self, owner_project):
                exec_string = f"""
                        SELECT address, origin_key, caip2, tag_id, tag_value, attester, time_created
                        FROM public.vw_oli_label_pool_gold_v2
                        WHERE (address, caip2) IN (
                                SELECT DISTINCT address, caip2
                                FROM public.vw_oli_label_pool_gold_v2
                                WHERE tag_id = 'owner_project'
                                AND tag_value = '{owner_project}'
                        );
                """
                df = pd.read_sql(exec_string, self.engine.connect())
                df['address'] = '\\x' + df['address'].apply(lambda x: x.hex())
                df['attester'] = '\\x' + df['attester'].apply(lambda x: x.hex())
                return df
        
        ## This function is used for our Airtable setup - it returns all prior attestations made by a specific attester for a specific address on a specific chain
        def get_all_prior_attested_labels(self, address, chain_id, attester):
                address = address.lower()
                if address.startswith('\\x'):
                        address = address.replace('\\x', '0x')
                attester = attester[2:] if attester.startswith('0x') or attester.startswith('\\') else attester
                exec_string = f'''
                        SELECT 
                                l.address,
                                s.origin_key,
                                l.chain_id AS caip2,
                                l.tag_id,
                                l.tag_value AS value,
                                l.attester,
                                l."time"
                        FROM public.labels l
                        LEFT JOIN public.sys_main_conf s ON l.chain_id = s.caip2
                        WHERE 
                                l.address = '{address}'
                                AND l.chain_id = '{chain_id}'
                                AND l.attester = decode('{attester}', 'hex');
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                df['address'] = df['address'].str.replace('0x', '\\x')
                df['attester'] = '\\x' + df['attester'].apply(lambda x: x.hex())
                return df
        
        # This function is used for our Airtable setup - it returns list of all attestations that can be revoked, table has two columns UID (hex) & is_offchain (boolean). Based on later time_created value.
        def get_oli_to_be_revoked(self, attester = '0xA725646C05E6BB813D98C5ABB4E72DF4BCF00B56'):
                if attester.startswith('0x') or attester.startswith('\\'):
                        attester = attester[2:]
                exec_string = f'''
                        WITH filtered_labels AS (
                                SELECT *
                                FROM public.labels
                                WHERE attester = decode('{attester}', 'hex')
                        ),
                        with_max_time AS (
                                SELECT 
                                        *,
                                        MAX(time) OVER (PARTITION BY chain_id, address) AS max_time
                                FROM filtered_labels
                        )
                        SELECT DISTINCT ON (uid)
                                -- address,
                                '0x' || encode(uid, 'hex') AS id_hex,
                                is_offchain
                        FROM with_max_time
                        WHERE time < max_time;
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df


        ## This function is used for our Airtable setup - it returns the top unlabelled contracts (usage_category IS NULL) by gas fees
        def get_unlabelled_contracts(self, number_of_contracts, days):
                number_of_contracts = int(number_of_contracts)
                exec_string = f'''
                        WITH pivoted_gold AS(
                                SELECT
                                        address,
                                        caip2,
                                        MAX(origin_key) AS origin_key,
                                        MAX(CASE WHEN tag_id = 'contract_name' THEN tag_value END) AS contract_name,
                                        MAX(CASE WHEN tag_id = 'is_proxy' THEN tag_value END) AS is_proxy,
                                        MAX(CASE WHEN tag_id = 'owner_project' THEN tag_value END) AS owner_project,
                                        MAX(CASE WHEN tag_id = 'usage_category' THEN tag_value END) AS usage_category,
                                        MAX(CASE WHEN tag_id = '_comment' THEN tag_value END) AS _comment
                                FROM public.vw_oli_label_pool_gold_v2
                                GROUP BY
                                        address, caip2
                        ),
                        ranked_contracts AS (
                                SELECT
                                        cl.address, 
                                        cl.origin_key, 
                                        max(pg._comment) AS _comment,
                                        max(pg.contract_name) AS name,
                                        bool_and(
                                                CASE 
                                                        WHEN pg.is_proxy = 'true' THEN true
                                                        WHEN pg.is_proxy = 'false' THEN false
                                                        ELSE NULL
                                                END) AS is_proxy,
                                        max(pg.owner_project) AS owner_project,
                                        max(pg.usage_category) AS usage_category,
                                        SUM(cl.gas_fees_eth) AS gas_eth, 
                                        SUM(cl.txcount) AS txcount, 
                                        ROUND(AVG(cl.daa)) AS avg_daa,   
                                        AVG(cl.success_rate) AS avg_success,
                                        SUM(cl.gas_fees_eth)/SUM(cl.txcount) AS avg_contract_txcost_eth,
                                        ROW_NUMBER() OVER (PARTITION BY cl.origin_key ORDER BY SUM(gas_fees_eth) DESC) AS row_num_gas,
                                        ROW_NUMBER() OVER (PARTITION BY cl.origin_key ORDER BY SUM(daa) DESC) AS row_num_daa
                                FROM public.blockspace_fact_contract_level cl 
                                LEFT JOIN pivoted_gold pg ON cl.address = pg.address AND cl.origin_key = pg.origin_key 
                                WHERE
                                        pg.usage_category IS NULL
                                        AND cl.date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        -- Exclude contracts already attested by the automated labeler.
                                        -- Automated attestations never reach vw_oli_label_pool_gold_pivoted_v2
                                        -- (no owner_project), so pg.usage_category IS NULL stays true for them.
                                        -- Without this filter the pipeline would re-process its own output every run.
                                        AND NOT EXISTS (
                                                SELECT 1 FROM public.labels lbl
                                                JOIN sys_main_conf smc2 ON lbl.chain_id = smc2.caip2
                                                WHERE lbl.address = '0x' || encode(cl.address, 'hex')
                                                  AND smc2.origin_key = cl.origin_key
                                                  AND lbl.attester IN (
                                                        decode('aDbf2b56995b57525Aa9a45df19091a2C5a2A970', 'hex'),
                                                        decode('ab81887E560e7C7b5993cE45D1c584d7FC22e898', 'hex')
                                                  )
                                        )
                                GROUP BY 1,2
                        ),
                        chain_txcost AS(
                                SELECT
                                        origin_key,
                                        AVG(value) AS avg_chain_txcost_median_eth
                                FROM public.fact_kpis
                                WHERE
                                        metric_key = 'txcosts_median_eth'
                                        AND "date" >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                GROUP BY origin_key
                        )
                        SELECT 
                                rc.address, 
                                rc.origin_key,
                                smc.caip2,
                                rc._comment,
                                rc.name AS contract_name,
                                rc.is_proxy,
                                rc.owner_project,
                                rc.usage_category,
                                rc.gas_eth, 
                                rc.txcount, 
                                rc.avg_daa,
                                rc.avg_success,
                                rc.avg_contract_txcost_eth / mc.avg_chain_txcost_median_eth - 1 AS rel_cost
                        FROM ranked_contracts rc
                        LEFT JOIN chain_txcost mc ON rc.origin_key = mc.origin_key
                        LEFT JOIN sys_main_conf smc ON rc.origin_key = smc.origin_key
                        WHERE row_num_gas <= {str(int(number_of_contracts/2))} OR row_num_daa <= {str(int(number_of_contracts/2))}
                        ORDER BY origin_key, row_num_gas, row_num_daa
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                df['day_range'] = int(days)
                return df

        def get_unlabelled_contracts_v2(self, days, origin_keys=None):
                """
                Returns top unlabeled contracts per chain using a relative txcount threshold (0.1% of
                total chain txcount over the window) instead of an absolute floor.

                At most 10 contracts per chain are returned, ranked by txcount descending, from those
                that pass the 0.1% threshold. Contracts already attested by the automated labeler
                (0xaDbf...) are excluded directly from public.labels to avoid view-refresh lag.

                Args:
                    days: lookback window in days
                    origin_keys: list of origin_key strings to restrict to; None = all chains
                """
                origin_key_filter = ""
                origin_key_filter_noalias = ""
                if origin_keys:
                        keys_str = "','".join(origin_keys)
                        origin_key_filter = f"AND cl.origin_key IN ('{keys_str}')"
                        origin_key_filter_noalias = f"AND origin_key IN ('{keys_str}')"

                exec_string = f'''
                        WITH chain_totals AS (
                                SELECT
                                        origin_key,
                                        SUM(txcount) AS total_txcount
                                FROM public.blockspace_fact_contract_level
                                WHERE date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                  AND date < DATE_TRUNC('day', NOW())
                                  {origin_key_filter_noalias}
                                GROUP BY origin_key
                        ),
                        pivoted_gold AS (
                                SELECT
                                        address,
                                        caip2,
                                        MAX(origin_key) AS origin_key,
                                        MAX(CASE WHEN tag_id = 'contract_name' THEN tag_value END) AS contract_name,
                                        MAX(CASE WHEN tag_id = 'is_proxy' THEN tag_value END) AS is_proxy,
                                        MAX(CASE WHEN tag_id = 'owner_project' THEN tag_value END) AS owner_project,
                                        MAX(CASE WHEN tag_id = 'usage_category' THEN tag_value END) AS usage_category,
                                        MAX(CASE WHEN tag_id = '_comment' THEN tag_value END) AS _comment
                                FROM public.vw_oli_label_pool_gold_v2
                                GROUP BY address, caip2
                        ),
                        ranked_contracts AS (
                                SELECT
                                        cl.address,
                                        cl.origin_key,
                                        MAX(pg._comment) AS _comment,
                                        MAX(pg.contract_name) AS name,
                                        bool_and(
                                                CASE
                                                        WHEN pg.is_proxy = 'true' THEN true
                                                        WHEN pg.is_proxy = 'false' THEN false
                                                        ELSE NULL
                                                END) AS is_proxy,
                                        MAX(pg.owner_project) AS owner_project,
                                        MAX(pg.usage_category) AS usage_category,
                                        SUM(cl.gas_fees_eth) AS gas_eth,
                                        SUM(cl.txcount) AS txcount,
                                        ROUND(AVG(cl.daa)) AS avg_daa,
                                        AVG(cl.success_rate) AS avg_success,
                                        SUM(cl.gas_fees_eth) / NULLIF(SUM(cl.txcount), 0) AS avg_contract_txcost_eth,
                                        SUM(cl.txcount)::numeric / NULLIF(ct.total_txcount, 0) AS chain_share,
                                        ROW_NUMBER() OVER (
                                                PARTITION BY cl.origin_key
                                                ORDER BY SUM(cl.txcount) DESC
                                        ) AS rn
                                FROM public.blockspace_fact_contract_level cl
                                LEFT JOIN pivoted_gold pg ON cl.address = pg.address AND cl.origin_key = pg.origin_key
                                JOIN chain_totals ct ON cl.origin_key = ct.origin_key
                                WHERE
                                        pg.usage_category IS NULL
                                        AND cl.date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        AND cl.date < DATE_TRUNC('day', NOW())
                                        {origin_key_filter}
                                        AND NOT EXISTS (
                                                SELECT 1 FROM public.labels lbl
                                                JOIN sys_main_conf smc2 ON lbl.chain_id = smc2.caip2
                                                WHERE lbl.address = '0x' || encode(cl.address, 'hex')
                                                  AND smc2.origin_key = cl.origin_key
                                                  AND lbl.attester IN (
                                                        decode('aDbf2b56995b57525Aa9a45df19091a2C5a2A970', 'hex'),
                                                        decode('ab81887E560e7C7b5993cE45D1c584d7FC22e898', 'hex')
                                                  )
                                        )
                                GROUP BY cl.address, cl.origin_key, ct.total_txcount
                                HAVING SUM(cl.txcount)::numeric / NULLIF(ct.total_txcount, 0) >= 0.001
                        ),
                        chain_txcost AS (
                                SELECT
                                        origin_key,
                                        AVG(value) AS avg_chain_txcost_median_eth
                                FROM public.fact_kpis
                                WHERE
                                        metric_key = 'txcosts_median_eth'
                                        AND "date" >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                GROUP BY origin_key
                        )
                        SELECT
                                rc.address,
                                rc.origin_key,
                                smc.caip2,
                                rc._comment,
                                rc.name AS contract_name,
                                rc.is_proxy,
                                rc.owner_project,
                                rc.usage_category,
                                rc.gas_eth,
                                rc.txcount,
                                rc.avg_daa,
                                rc.avg_success,
                                rc.chain_share,
                                rc.avg_contract_txcost_eth / NULLIF(mc.avg_chain_txcost_median_eth, 0) - 1 AS rel_cost
                        FROM ranked_contracts rc
                        LEFT JOIN chain_txcost mc ON rc.origin_key = mc.origin_key
                        LEFT JOIN sys_main_conf smc ON rc.origin_key = smc.origin_key
                        WHERE rc.rn <= 10
                        ORDER BY rc.origin_key, rc.rn
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                df['day_range'] = int(days)
                return df


        ## This function is used to generate the API endpoints for the OLI labels
        def get_oli_labels(self, chain_id='origin_key'):
                if chain_id == 'origin_key':
                        chain_str = 'g.origin_key'
                elif chain_id == 'caip2':
                        chain_str = 's.caip2 as chain_id'
                else:
                        raise ValueError("chain_id must be either 'origin_key' or 'caip2'")
                
                exec_string = f"""
                        SELECT 
                                g.address,
                                {chain_str},
                                g.contract_name as name,
                                g.owner_project,
                                g.usage_category,
                                g.is_factory_contract,
                                g.deployment_tx,
                                g.deployer_address,
                                g.deployment_date
                        FROM public.vw_oli_label_pool_gold_pivoted_v2 g
                        LEFT JOIN sys_main_conf s USING (origin_key)
                        WHERE owner_project IS NOT NULL
                        """

                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        # ── contract_label_features / contract_label_review ──────────────────
        # Schema: docs/contract_label_features_schema.md
        # DDL:    backend/db_migrations/001_contract_label_features.sql

        def upsert_contract_label_features(self, rows: list[dict]) -> int:
                """Bulk-upsert AI feature rows. Idempotent on (address, origin_key)."""
                if not rows:
                        return 0
                df = pd.DataFrame(rows)
                # Address → bytea via the same \\x-prefix convention used elsewhere.
                df['address'] = df['address'].astype(str).str.lower().str.replace('0x', '\\x', regex=False)
                # jsonb cols: pangres serialises dict/list automatically; coerce None safely.
                for jcol in ('bs_token_transfers', 'log_top_events'):
                        if jcol in df.columns:
                                df[jcol] = df[jcol].apply(lambda v: v if v is not None else None)
                # Set composite PK so pangres knows what to ON CONFLICT against.
                df = df.set_index(['address', 'origin_key'])
                return self.upsert_table('public.contract_label_features', df, if_exists='update') or 0

        def insert_contract_label_review(self, rows: list[dict]) -> int:
                """Append-only insert. Generated diff-flag columns auto-populate."""
                if not rows:
                        return 0
                insert_sql = text("""
                    INSERT INTO public.contract_label_review (
                        address, origin_key, labeled_at, reviewed_at, review_source,
                        ai_contract_name, ai_usage_category, ai_confidence, ai_owner_project_likely,
                        gtp_contract_name, gtp_usage_category, gtp_owner_project, gtp_no_owner_project
                    ) VALUES (
                        :address, :origin_key, :labeled_at, :reviewed_at, :review_source,
                        :ai_contract_name, :ai_usage_category, :ai_confidence, :ai_owner_project_likely,
                        :gtp_contract_name, :gtp_usage_category, :gtp_owner_project, :gtp_no_owner_project
                    )
                """)
                normalised = []
                for r in rows:
                        n = dict(r)
                        if isinstance(n.get('address'), str) and n['address'].startswith('0x'):
                                n['address'] = n['address'].lower().replace('0x', '\\x')
                        normalised.append(n)
                with self.engine.begin() as conn:
                        conn.execute(insert_sql, normalised)
                return len(normalised)

        def get_contract_label_features(self, address_origin_pairs: list[tuple]) -> dict:
                """Lookup AI baseline for diff capture. Returns dict[(address_0x, origin_key)] -> row."""
                if not address_origin_pairs:
                        return {}
                # Normalise lookup keys (always 0x-lower hex).
                pairs = [(a.lower() if a.startswith('0x') else '0x' + a.lower().lstrip('\\x'), ok)
                         for a, ok in address_origin_pairs]
                hex_addrs = [a[2:] for a, _ in pairs]
                origin_keys = [ok for _, ok in pairs]
                sql = text("""
                    SELECT '0x' || encode(address, 'hex') AS address, origin_key,
                           labeled_at, ai_contract_name, ai_usage_category, ai_confidence,
                           ai_owner_project_likely
                    FROM public.contract_label_features
                    WHERE (encode(address, 'hex'), origin_key) = ANY(
                        SELECT unnest(CAST(:addrs AS text[])), unnest(CAST(:oks AS text[]))
                    )
                """)
                with self.engine.begin() as conn:
                        rows = conn.execute(sql, {'addrs': hex_addrs, 'oks': origin_keys}).mappings().all()
                return {(r['address'], r['origin_key']): dict(r) for r in rows}

        ## TODO: filter by contracts only?
        def get_labels_lite_db(self, limit=50000, order_by='txcount', origin_keys=None):
                exec_string = f"""
                        with prev_period as (
                                SELECT 
                                        cl.address, 
                                        cl.origin_key, 
                                        sum(txcount) as txcount, 
                                        sum(gas_fees_usd) as gas_fees_usd, 	
                                        sum(daa) as daa	
                                FROM public.blockspace_fact_contract_level cl
                                where "date"  >= current_date - interval '14 days'
                                        and "date" < current_date - interval '7 days'
                                group by 1,2
                        )

                        , current_period as (
                                SELECT 
                                        cl.address, 
                                        cl.origin_key, 
                                        sum(txcount) as txcount, 
                                        sum(gas_fees_usd) as gas_fees_usd, 	
                                        sum(daa) as daa	
                                FROM public.blockspace_fact_contract_level cl
                                where "date"  >= current_date - interval '7 days'
                                        and "date" < current_date
                                group by 1,2
                        )

                        SELECT 
                                cl.address, 
                                cl.origin_key, 
                                syc.caip2 as chain_id,
                                UPPER(LEFT(lab.contract_name , 1)) || SUBSTRING(lab.contract_name FROM 2) as name,
                                lab.owner_project,
                                oss.display_name as owner_project_clear,
                                lab.usage_category,
                                lab.deployment_tx,
                                lab.deployer_address,
                                lab.deployment_date,
                                cl.txcount as txcount,
                                (cl.txcount - prev.txcount) / prev.txcount as txcount_change,
                                cl.gas_fees_usd as gas_fees_usd, 	
                                (cl.gas_fees_usd - prev.gas_fees_usd) / prev.gas_fees_usd as gas_fees_usd_change,
                                cl.daa as daa,
                                (cl.daa - prev.daa) / prev.daa as daa_change
                        FROM current_period cl
                        left join prev_period prev using (address, origin_key)
                        left join vw_oli_label_pool_gold_pivoted_v2 lab using (address, origin_key)
                        left join oli_oss_directory oss on oss.name = lab.owner_project
                        left join sys_main_conf syc on cl.origin_key = syc.origin_key
                        where cl.origin_key IN ('{"','".join(origin_keys)}')
                                and (lab.owner_project is null OR oss.active = true)
                        order by {order_by} desc
                        limit {limit}
                """

                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_labels_export_df(self, limit=50000, origin_keys=None, incl_aggregation=True):
                if incl_aggregation:
                        aggregation = """
                                ,sum(txcount) as txcount_180d 
                                ,sum(gas_fees_usd) as gas_fees_usd_180d
                        """
                else:
                        aggregation = ""

                exec_string = f"""
                        SELECT 
                                cl.address, 
                                cl.origin_key, 
                                syc.caip2 as chain_id,
                                UPPER(LEFT(lab.contract_name , 1)) || SUBSTRING(lab.contract_name FROM 2) as name,
                                lab.owner_project,
                                lab.usage_category,
                                lab.deployment_tx,
                                lab.deployer_address,
                                lab.deployment_date
                                {aggregation}
                        FROM public.blockspace_fact_contract_level cl
                        left join vw_oli_label_pool_gold_pivoted_v2 lab using (address, origin_key)
                        left join sys_main_conf syc on cl.origin_key = syc.origin_key
                        where cl."date"  >= current_date - interval '180 days'
                                and cl."date" < current_date
                                and (lab.contract_name is not null OR lab.owner_project is not null OR lab.deployment_tx is not null OR lab.deployer_address is not null OR lab.deployment_date is not null)
                                and cl.origin_key IN ('{"','".join(origin_keys)}')
                        group by 1,2,3,4,5,6,7,8,9
                        order by sum(txcount) desc
                        limit {limit}
                """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_labels_export_gold_table(self):
                exec_string = f"""
                        SELECT
                                '0x' || encode(address, 'hex') AS address,
                                caip2 AS chain_id,
                                MAX(CASE WHEN tag_id = 'is_eoa' THEN tag_value END) AS is_eoa,
                                MAX(CASE WHEN tag_id = 'is_contract' THEN tag_value END) AS is_contract,
                                MAX(CASE WHEN tag_id = 'is_factory_contract' THEN tag_value END) AS is_factory_contract,
                                MAX(CASE WHEN tag_id = 'is_proxy' THEN tag_value END) AS is_proxy,
                                MAX(CASE WHEN tag_id = 'is_safe_contract' THEN tag_value END) AS is_safe_contract,
                                MAX(CASE WHEN tag_id = 'contract_name' THEN tag_value END) AS contract_name,
                                MAX(CASE WHEN tag_id = 'deployment_tx' THEN tag_value END) AS deployment_tx,
                                MAX(CASE WHEN tag_id = 'deployer_address' THEN tag_value END) AS deployer_address,
                                MAX(CASE WHEN tag_id = 'owner_project' THEN tag_value END) AS owner_project,
                                MAX(CASE WHEN tag_id = 'deployment_date' THEN tag_value END) AS deployment_date,
                                MAX(CASE WHEN tag_id = 'erc_type' THEN tag_value END) AS erc_type,
                                MAX(CASE WHEN tag_id = 'erc20.symbol' THEN tag_value END) AS "erc20.symbol",
                                MAX(CASE WHEN tag_id = 'erc20.decimals' THEN tag_value END) AS "erc20.decimals",
                                MAX(CASE WHEN tag_id = 'erc721.name' THEN tag_value END) AS "erc721.name",
                                MAX(CASE WHEN tag_id = 'erc721.symbol' THEN tag_value END) AS "erc721.symbol",
                                MAX(CASE WHEN tag_id = 'erc1155.name' THEN tag_value END) AS "erc1155.name",
                                MAX(CASE WHEN tag_id = 'erc1155.symbol' THEN tag_value END) AS "erc1155.symbol",
                                MAX(CASE WHEN tag_id = 'usage_category' THEN tag_value END) AS usage_category,
                                MAX(CASE WHEN tag_id = 'version' THEN tag_value END) AS version,
                                MAX(CASE WHEN tag_id = 'audit' THEN tag_value END) AS audit,
                                MAX(CASE WHEN tag_id = 'contract_monitored' THEN tag_value END) AS contract_monitored,
                                MAX(CASE WHEN tag_id = 'source_code_verified' THEN tag_value END) AS source_code_verified,
                                MAX(CASE WHEN tag_id = '_comment' THEN tag_value END) AS _comment
                        FROM public.vw_oli_label_pool_gold_v2
                        GROUP BY 1, 2
                """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        ## OLI Label Pool functions
        def get_max_value(self, table_name:str, column_name:str):
                exec_string = f"""
                SELECT MAX({column_name}) AS max_value
                FROM public.{table_name};
                """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df['max_value'][0]

        def get_labels_page_sparkline(self, limit=100, origin_keys=None):
                exec_string = f"""
                        with top as (
                                SELECT 
                                address, 
                                origin_key, 
                                sum(txcount) as txcount_limit
                        FROM public.blockspace_fact_contract_level
                        where "date"  >= date_trunc('day',now()) - interval '7 days'
                                and "date" < date_trunc('day', now())
                                and origin_key IN ('{"','".join(origin_keys)}')
                        group by 1,2
                        order by 3 desc
                        limit {limit}
                        )

                        SELECT 
                                address, 
                                origin_key,
                                "date",
                                sum(txcount) as txcount, 
                                sum(gas_fees_usd) as gas_fees_usd, 	
                                sum(daa) as daa
                        FROM blockspace_fact_contract_level
                        inner join top using (address, origin_key)
                        where "date"  >= date_trunc('day',now()) - interval '30 days'
                                and "date" < date_trunc('day', now())
                        group by 1,2,3
                """

                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_special_use_rpc(self, origin_key:str, check_realtime:bool = False):
                if check_realtime:
                        query = f"""
                                SELECT url
                                FROM sys_rpc_config
                                WHERE realtime_use = true
                                and origin_key = '{origin_key}'

                                UNION ALL

                                SELECT url
                                FROM sys_rpc_config
                                WHERE special_use = true
                                and origin_key = '{origin_key}'
                                AND NOT EXISTS (
                                        SELECT 1
                                        FROM sys_rpc_config
                                        WHERE realtime_use = true
                                        and origin_key = '{origin_key}'
                                )
                                LIMIT 1;
                        """
                else:
                         query = f"SELECT url FROM sys_rpc_config WHERE origin_key = '{origin_key}' and special_use = true LIMIT 1"
                try:
                       
                        with self.engine.connect() as connection:
                                result = connection.execute(text(query))
                                rpc = result.scalar()
                                return rpc
                except Exception as e:
                        print(f"Error retrieving a synced rpc for {origin_key}.")
                        print(e)
                        return None
        
        def get_all_rpcs_for_chain(self, origin_key:str):
                query = f"SELECT url FROM sys_rpc_config WHERE origin_key = '{origin_key}'"
                try:
                        with self.engine.connect() as connection:
                                result = connection.execute(text(query))
                                rpcs = [row[0] for row in result.fetchall()]
                                return rpcs
                except Exception as e:
                        print(f"Error retrieving rpcs for {origin_key}.")
                        print(e)
                        return []
                
        def get_block_by_date(self, table_name:str, date:datetime):
                # Check if the table has a 'block_timestamp' column
                insp = sqlalchemy.inspect(self.engine)
                columns = [col['name'] for col in insp.get_columns(table_name)]
                
                if 'block_timestamp' not in columns:
                        raise ValueError(f"Table {table_name} does not have a 'block_timestamp' column.")
                
                # Format the date to ensure it's in the correct format for SQL
                date_str = date.strftime('%Y-%m-%d')
                
                # Execute the query to get the block number for the given date
                exec_string = f"""
                SELECT MIN(block_number) as block_number 
                FROM {table_name} 
                WHERE date_trunc('day', block_timestamp) = '{date_str}';
                """
                
                with self.engine.connect() as connection:
                        row = connection.execute(text(exec_string)).mappings().first()
                        block_number = row['block_number'] if row else None
                                
                if block_number is None:
                        raise ValueError(f"No block found for the date {date_str} in table {table_name}.")
                
                return block_number
        
        def get_first_block_of_the_day(self, chain:str, date:datetime='today'):
                if date == 'today':
                        date = datetime.today().date()
                query = f"""
                        SELECT value
                        FROM public.fact_kpis
                        where 
                                metric_key = 'first_block_of_day'
                                and origin_key = '{chain}'
                                and "date" = '{date.strftime('%Y-%m-%d')}';
                        """
                with self.engine.connect() as connection:
                        block_number = connection.execute(text(query)).scalar()
                return int(block_number)
        
        def get_first_block_of_the_day_range(self, chain: str, from_date: str, to_date: str='latest'):
                # make sure from_date & to_date are in the correct format
                from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
                if to_date != 'latest':
                        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()
                else:
                        to_date = datetime.today().date()
                query = f"""
                        SELECT date, value AS block
                        FROM public.fact_kpis
                        WHERE 
                                metric_key = 'first_block_of_day'
                                AND origin_key = '{chain}'
                                AND date >= '{from_date}'
                                AND date <= '{to_date}'
                        ORDER BY date DESC; -- desc order here is important, please do not change!
                """
                with self.engine.connect() as connection:
                        result = connection.execute(text(query))
                        blocks = {row[0].strftime('%Y-%m-%d'): int(row[1]) for row in result}
                return blocks
        
        def get_all_L1_L2_origin_keys_prod(self, api_deployment_flags = ['PROD']):
                query = f"""
                        SELECT DISTINCT origin_key
                        FROM public.sys_main_conf
                        WHERE 
                                api_deployment_flag = ANY (ARRAY{api_deployment_flags})
                                AND chain_type IN ('L1', 'L2');
                """
                with self.engine.connect() as connection:
                        result = connection.execute(text(query))
                        origin_keys = [row[0] for row in result]
                return origin_keys

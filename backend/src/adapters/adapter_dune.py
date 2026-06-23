import pandas as pd
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract, send_discord_message

from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

class AdapterDune(AbstractAdapter):
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Dune", adapter_params, db_connector)
        self.api_key = adapter_params['api_key']
        self.query_speed = adapter_params.get('query_speed', 'large')  # e.g. 'medium', 'large'
        self.client = DuneClient(self.api_key)
        print_init(self.name, self.adapter_params)

    def extract(self, load_params:dict):
        """
        Extract utilizes the `load_params` dictionary to execute specified queries, prepare the resulting data and load it into a desired table.

        Parameters:
        
        load_params : dict
            - 'queries' (list): A list of dictionaries, each specifying a query to be executed. Each query dictionary contains:
                - 'name' (str): A descriptive name for the query.
                - 'query_id' (int): The unique identifier for the query.
                - 'params' (dict): Parameters for the query, can be any as set in dune:
                    - 'days' (int): The time range for the query in days.
                    - 'chain' (str): The origin_key of the chain.
            - 'prepare_df' (str): The name of the function or method to be used for preparing the resulting df into the desired format.
            - 'load_type' (str): Specifies the table to load the df into.
        """
        self.load_params = load_params

        # create list of QueryBase objects
        self.queries = []
        for query in self.load_params.get('queries'):
            self.queries.append(QueryBase(name = query['name'], query_id = query['query_id']))
            if 'params' in query:
                self.queries[-1].params = [QueryParameter.text_type(name = k, value = v) for k, v in query['params'].items()]

        # load all queries and merge them into one dataframe
        df_main = pd.DataFrame()
        for query in self.queries:
            try:
                print(f"...start loading {query.name} with query_id: {query.query_id} and params: {query.params}")
                df = self.client.refresh_into_dataframe(query, performance=self.query_speed)
                print(f"...finished loading {query.name}. Loaded {df.shape[0]} rows")
            except Exception as e:
                print(f"Error loading {query.name}: {e}")
                send_discord_message(f"Dune Error loading {query.name} with query_id {query.query_id}: {e}")
                continue
            
            if df.shape[0] == 0:
                print(f"No data returned for {query.name}, skipping...")
                continue
            
            # Prepare df if set in load_params
            prep_df = self.load_params.get('prepare_df')
            if prep_df is not None:
                if not isinstance(prep_df, str) or not prep_df.startswith('prepare_'):
                    raise ValueError(f"Invalid prepare_df method name: {prep_df}")

                prepare_df = getattr(self, prep_df, None)
                if not callable(prepare_df):
                    raise AttributeError(f"prepare_df method not found: {prep_df}")

                query_date = None
                if prep_df == 'prepare_df_contract_level_aa_daily':
                    if hasattr(query, 'parameters'):
                        query_params = query.parameters()
                    else:
                        query_params = getattr(query, 'params', []) or []

                    if isinstance(query_params, dict):
                        query_date = query_params.get('date')
                    else:
                        query_date = next(
                            (param.value for param in query_params if getattr(param, 'key', None) == 'date'),
                            None,
                        )

                    print(f"Extracted query_date {query_date} for {query.name} to prepare contract level active addresses daily dataframe.")

                if query_date is not None:
                    df = prepare_df(df, query_date)
                else:
                    df = prepare_df(df)
            
            # Concatenate dataframes into one
            df_main = pd.concat([df_main, df])

        print_extract(self.name, self.load_params, df_main.shape)
        return df_main

    def load(self, df:pd.DataFrame):
        table = self.load_params.get('load_type')
        if table != None:
            try:
                upserted = self.db_connector.upsert_table(table, df)
                print_load(self.name, upserted, table)
            except Exception as e:
                print(f"Error loading {table}: {e}")
        else:
            print("No load_type specified in load_params. Data not loaded!")
        
        
    ## ----------------- Helper functions --------------------
    
    def prepare_df_metric_daily(self, df):
        # unpivot df only if not already in the correct format
        if 'metric_key' not in df.columns and 'value' not in df.columns:
            df = df.melt(id_vars=['day', 'origin_key'], var_name='metric_key', value_name='value')
        # change day column to date
        df['date'] = df['day'].apply(pd.to_datetime).dt.date
        df.drop(['day'], axis=1, inplace=True)
        # replace nil or None values with 0
        df['value'] = df['value'].replace('<nil>', 0)
        df['value'] = df.value.fillna(0)
        # turn value column into float
        df['value'] = df['value'].astype(float)
        # set primary keys as index
        df = df.set_index(['metric_key', 'origin_key', 'date'])
        return df
    
    def prepare_df_metric_hourly(self, df):
        # unpivot df only if not already in the correct format
        if 'metric_key' not in df.columns and 'value' not in df.columns:
            df = df.melt(id_vars=['timestamp', 'origin_key'], var_name='metric_key', value_name='value')

        # replace nil or None values with 0
        df['value'] = df['value'].replace('<nil>', 0)
        df['value'] = df.value.fillna(0)
        # turn value column into float
        df['value'] = df['value'].astype(float)
        
        df['granularity'] = 'hourly'
        # set timestamp column to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # set primary keys as index
        df = df.set_index(['metric_key', 'origin_key', 'timestamp', 'granularity'])
        return df
    
    def prepare_df_contract_level_aa_daily(self, df, query_date:str=None):
        print(f"Preparing df with {df.shape[0]} (compact) rows for contract level active addresses daily...")
        
        # 1) parse "[0x.. 0x..]" -> ["0x..", "0x.."]
        df["from_addresses"] = (
            df["from_addresses"]
            .astype(str)
            .str.strip("[]")
            .str.split()          # splits on whitespace
        )

        # 2) explode
        df = (
            df.explode("from_addresses")
            .rename(columns={"from_addresses": "from_address"})
            .dropna(subset=["from_address"])
            .reset_index(drop=True)
        )
        
        ## drop column chunk_id
        if 'chunk_id' in df.columns:
            df = df.drop(columns=['chunk_id'])
        
        print(f"Exploded to {df.shape[0]} rows for contract level active addresses daily...")

        # 3) check if day column is present, if yes rename to date. If not, create date column with query_date
        if 'day' in df.columns:
            df = df.rename(columns={"day": "date"})
        else:
            df['date'] = query_date

        # 4) rename column, format address
        df['address'] = df['address'].str.replace('0x', '\\x', regex=False)
        df['from_address'] = df['from_address'].str.replace('0x', '\\x', regex=False)
        return df
    
    def prepare_df_contract_level_aa_hourly(self, df):
        print(f"Preparing df with {df.shape[0]} (compact) rows for contract level active addresses hourly...")
        
        # 1) parse "[0x.. 0x..]" -> ["0x..", "0x.."]
        df["from_addresses"] = (
            df["from_addresses"]
            .astype(str)
            .str.strip("[]")
            .str.split()          # splits on whitespace
        )

        # 2) explode
        df = (
            df.explode("from_addresses")
            .rename(columns={"from_addresses": "from_address"})
            .dropna(subset=["from_address"])
            .reset_index(drop=True)
        )
        
        ## drop column chunk_id
        if 'chunk_id' in df.columns:
            df = df.drop(columns=['chunk_id'])
        
        print(f"Exploded to {df.shape[0]} rows for contract level active addresses hourly...")

        # 3) rename column, format address
        df['address'] = df['address'].str.replace('0x', '\\x', regex=False)
        df['from_address'] = df['from_address'].str.replace('0x', '\\x', regex=False)
        return df
    
    def prepare_df_contract_level_daily(self, df:pd.DataFrame):
        df["metrics"] = (
            df["metrics"]
            .astype(str)
            .str.strip("[]")
            .str.split()          # splits on whitespace
        )

        cols = [
            "gas_fees_eth",
            "gas_fees_usd",
            "txcount",
            "daa",
            "success_rate",
            "median_tx_fee",
            "gas_used",
        ]

        df[cols] = pd.DataFrame(df["metrics"].tolist(), index=df.index)

        df["gas_fees_eth"]   = df["gas_fees_eth"].astype(float)
        df["gas_fees_usd"]   = df["gas_fees_usd"].astype(float)
        df["txcount"]        = df["txcount"].astype(float)
        df["daa"]            = df["daa"].astype(float)
        df["success_rate"]   = df["success_rate"].astype(float)
        df["median_tx_fee"]  = df["median_tx_fee"].astype(float)
        df["gas_used"]       = df["gas_used"].astype(float)

        df = df.drop(columns=["metrics"])

        df = df.rename(columns={"day": "date"})
        df['address'] = df['address'].str.replace('0x', '\\x', regex=False)
        return df
    
    def prepare_df_contract_level_hourly(self, df:pd.DataFrame):
        df["metrics"] = (
            df["metrics"]
            .astype(str)
            .str.strip("[]")
            .str.split()          # splits on whitespace
        )

        cols = [
            "gas_fees_eth",
            "gas_fees_usd",
            "txcount",
            "daa",
            "success_rate",
            "median_tx_fee",
            "gas_used",
        ]

        df[cols] = pd.DataFrame(df["metrics"].tolist(), index=df.index)

        df["gas_fees_eth"]   = df["gas_fees_eth"].astype(float)
        df["gas_fees_usd"]   = df["gas_fees_usd"].astype(float)
        df["txcount"]        = df["txcount"].astype(float)
        df["daa"]            = df["daa"].astype(float)
        df["success_rate"]   = df["success_rate"].astype(float)
        df["median_tx_fee"]  = df["median_tx_fee"].astype(float)
        df["gas_used"]       = df["gas_used"].astype(float)

        df = df.drop(columns=["metrics"])

        df['address'] = df['address'].str.replace('0x', '\\x', regex=False)
        return df
    
    def prepare_df_category_level_daily(self, df):
        df["metrics"] = (
            df["metrics"]
            .astype(str)
            .str.strip("[]")
            .str.split()          # splits on whitespace
        )

        cols = [
            "gas_fees_eth",
            "gas_fees_usd",
            "txcount",
            "daa",
            "gas_used",
        ]

        df[cols] = pd.DataFrame(df["metrics"].tolist(), index=df.index)

        df["gas_fees_eth"]   = df["gas_fees_eth"].astype(float)
        df["gas_fees_usd"]   = df["gas_fees_usd"].astype(float)
        df["txcount"]        = df["txcount"].astype(float)
        df["daa"]            = df["daa"].astype(float)
        df["gas_used"]       = df["gas_used"].astype(float)

        df = df.drop(columns=["metrics"])

        df = df.rename(columns={"day": "date"})
        return df
    
    def prepare_df_incriptions(self, df):
        # address column to bytea
        df['address'] = df['address'].apply(lambda x: bytes.fromhex(x[2:]))
        # set primary keys as index
        df = df.set_index(['address', 'origin_key'])
        return df
    
    def prepare_df_glo_holders(self, df):
        # address column to bytea
        df['address'] = df['address'].apply(lambda x: bytes.fromhex(x[2:]))
        # date column with current date
        df['date'] = datetime.now().date()
        # parse origin_keys column in df so that it can be loaded into a postgres array - split by comma and add curly braces
        df['origin_keys'] = df['origin_keys'].apply(lambda x: '{"' + x.replace(',', '","') + '"}')
        # set primary keys as index
        df = df.set_index(['address', 'date'])
        return df
    
    def prepare_robinhood_list(self, df):
        # add active column and set to True
        df['active'] = True
        # Dune uses Robinhood token identifiers; yfinance expects Yahoo symbols.
        df['ticker'] = df['ticker'].str.replace(r'_(STOCK|ETF)$', '', regex=True)
        # Private assets without a Yahoo Finance price source cannot be priced here.
        df.loc[df['ticker'].isin(['SPACEX']), 'active'] = False
        # set index to contract_address
        df = df.set_index(['contract_address'])
        return df
    
    def prepare_robinhood_daily(self, df):
        # unpivot df to match the format of robinhood_daily table
        df = df.melt(id_vars=['date', 'contract_address'], var_name='metric_key', value_name='value')
        df = df.set_index(['contract_address', 'date', 'metric_key'])
        return df

    ## ----------------- Dune table upload functions --------------------

    def create_table(self, table_name: str, schema: list, description: str = "", is_private: bool = False, namespace: str = "growthepie"):
        """
        Create a table on Dune Analytics. Safe to call if the table already exists.

        Parameters:
            table_name  : Dune table name (e.g. 'l2beat_tokens')
            schema      : List of {"name": ..., "type": ...} dicts (Dune column types: varchar, bigint, double, boolean, timestamp)
            description : Optional human-readable description shown in Dune UI
            is_private  : Whether the table should be private (default False)
            namespace   : Dune namespace/team (default 'growthepie')
        """
        import requests
        url = "https://api.dune.com/api/v1/table/create"
        headers = {
            "X-DUNE-API-KEY": self.api_key,
            "Content-Type": "application/json"
        }
        payload = {
            "namespace": namespace,
            "table_name": table_name,
            "description": description,
            "is_private": is_private,
            "schema": schema,
        }
        response = requests.post(url, json=payload, headers=headers)
        result = response.json()
        # Treat "already exists" as success
        if not response.ok and 'already exists' not in str(result).lower():
            raise RuntimeError(f"Dune API table creation failed: {result}")
        print(f"Table {namespace}.{table_name} ready: {result}")

    def clear_table(self, table_name: str, namespace: str = "growthepie"):
        """
        Clear all rows from a Dune table without dropping it.

        Parameters:
            table_name : Dune table name (e.g. 'l2beat_tokens')
            namespace  : Dune namespace/team (default 'growthepie')
        """
        import requests
        url = f"https://api.dune.com/api/v1/table/{namespace}/{table_name}/clear"
        headers = {"X-DUNE-API-KEY": self.api_key}
        response = requests.post(url, headers=headers)
        if not response.ok:
            raise RuntimeError(f"Dune API table clear failed: {response.json()}")
        print(f"Table {namespace}.{table_name} cleared.")

    def upload_to_table(self, table_name: str, df: pd.DataFrame, namespace: str = "growthepie"):
        """
        Clear a Dune table and upload a fresh DataFrame as CSV.

        Parameters:
            table_name : Dune table name (e.g. 'l2beat_tokens')
            df         : DataFrame whose columns match the table schema
            namespace  : Dune namespace/team (default 'growthepie')
        """
        import requests
        import io

        headers = {"X-DUNE-API-KEY": self.api_key}

        # Clear existing rows
        self.clear_table(table_name, namespace)

        # Serialize DataFrame to CSV and upload
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        url_insert = f"https://api.dune.com/api/v1/table/{namespace}/{table_name}/insert"
        upload_headers = {**headers, "Content-Type": "text/csv"}
        response = requests.post(url_insert, data=csv_bytes, headers=upload_headers)
        result = response.json()
        if not response.ok or "error" in result:
            raise RuntimeError(f"Dune API upload failed: {result}")
        print(f"Uploaded {len(df)} rows to {namespace}.{table_name} on Dune.")

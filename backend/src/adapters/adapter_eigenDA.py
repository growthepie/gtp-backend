import pandas as pd
import requests
import json
import yaml

from src.adapters.abstract_adapters import AbstractAdapter
from src.gtp_dna import get_gtp_dna_file
from src.misc.helper_functions import print_init, print_load, print_extract, convert_economics_mapping_into_df

class AdapterEigenDA(AbstractAdapter):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("EigenDA", adapter_params, db_connector)
        self.adapter_params = adapter_params
        print_init(self.name, self.adapter_params)
        # initialize economics mapping
        self.map = self.get_economics_mapping()

    def extract(self, load_params: dict):
        """
        Extract data from EigenDA API and prepare it for loading into the database.

        Parameters:
        load_params : dict
            - 'days' (int): The number of days to look back for data extraction (min date is hard coded to 2025-01-01).
            - 'endpoint' (str): The API endpoint to fetch data from.
            - 'table' (str): The name of the table to load the data into.
        """
        # save load params
        self.load_params = load_params

        # Fetch data from EigenDA API
        df = self.call_api_endpoint()
        
        # prepare df
        df = self.prepare_df(df)

        # print extract info
        print_extract(self.name, load_params, df.shape[0])

        return df
        
    def load(self, df:pd.DataFrame):
        table = self.load_params.get('table', None)
        if table != None:
            try:
                upserted = self.db_connector.upsert_table(table, df)
                print_load(self.name, upserted, table)
            except Exception as e:
                print(f"Error loading {table}: {e}")
        else:
            print("No load_type specified in load_params. Data not loaded!")

    ### api call function

    def get_economics_mapping(self):
        # map namespace to origin_key for df, based on economics_mapping.yml
        data = yaml.load(get_gtp_dna_file("economics_da/economics_mapping.yml"), Loader=yaml.FullLoader)
        map = convert_economics_mapping_into_df(data)
        map = map[map['da_layer'] == 'eigenda'][['origin_key', 'namespace']]
        return map

    def call_api_endpoint(self):
        today = pd.Timestamp.now().strftime('%Y-%m-%d')
        self.endpoint = f"{self.load_params.get('endpoint')}/{today}.json"
        
        response = requests.get(self.endpoint)
        if response.status_code == 200:
            data = [json.loads(line) for line in response.text.strip().split('\n')]
            df = pd.DataFrame(data)
            return df
        else:
            raise Exception(f"API call failed with status code {response.status_code}")
        
    def prepare_df(self, df: pd.DataFrame):
        # convert from hourly to daily values
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['datetime'] = df['datetime'].dt.date
        df = df.groupby(['datetime', 'customer_id']).agg({
            'blob_count': 'sum',
            'total_size_mb': 'sum',
            'account_name': 'first'
        }).reset_index()

        # hard drop everything pre 2025-01-01
        df = df[df['datetime'] >= pd.Timestamp('2025-01-01').date()]

        # drop todays value as the day is not fully over yet
        df = df[df['datetime'] < pd.Timestamp.now().date()]

        # drop account_name (namespace is more accurate)
        df = df.drop(columns=['account_name'])

        # convert mb to bytes
        df["eigenda_blob_size_bytes"] = (df["total_size_mb"] * 1024 * 1024) # convert MiB to kiB to bytes, this is MiB (according to EigenDA, but number was rounded)
        df = df.drop(columns=['total_size_mb'])

        # rename blob_count to eigenda_blob_count, datetime to date
        df = df.rename(columns={
            'blob_count': 'eigenda_blob_count',
            'datetime': 'date'
        })

        # only get the last x days as defined in load_params, else pull in all data
        day = pd.Timestamp.now() - pd.Timedelta(days=self.load_params.get('days', 99999))
        df = df[df['date'] >= day.date()]

        # calculate daily total values
        df_grouped = df.groupby('date').agg({
            'eigenda_blob_count': 'sum',
            'eigenda_blob_size_bytes': 'sum',
            'customer_id': 'nunique'
        }).reset_index()
        df_grouped = df_grouped.sort_values(by='date', ascending=False)
        # rename columns to correct metric_keys
        df_grouped = df_grouped.rename(columns={
            'eigenda_blob_count': 'da_blob_count', # daily blob count EigenDA
            'eigenda_blob_size_bytes': 'da_data_posted_bytes', # daily data posted to EigenDA in bytes
            'customer_id': 'da_unique_blob_producers' # daily number of unique blob producers to EigenDA
        })
        # add origin_key
        df_grouped['origin_key'] = 'da_eigenda'

        # left join map on df, thenn remove unmapped (unknown blob producers) & drop customer_id + namespace
        df = df.merge(self.map, left_on='customer_id', right_on='namespace', how='left')
        df = df[df['origin_key'].notnull()]
        df = df.drop(columns=['namespace', 'customer_id'])

        # calculate fees paid by converting bytes to GiB and multiply by price
        pricing = 0.015 # ETH per GiB (source: https://www.blog.eigenlayer.xyz/eigenda-updated-pricing/)
        df['eigenda_blobs_eth'] = (df['eigenda_blob_size_bytes'] / (1024 * 1024 * 1024)) * pricing
        df_grouped['da_fees_eth'] = (df_grouped['da_data_posted_bytes'] / (1024 * 1024 * 1024)) * pricing

        # melt columns
        df_melted = df_grouped.melt(id_vars=['date', 'origin_key'], var_name='metric_key', value_name='value')
        df_melted2 = df.melt(id_vars=['date', 'origin_key'], var_name='metric_key', value_name='value')
        # merge melted dataframes
        df_melt = pd.concat([df_melted, df_melted2], ignore_index=True)

        # group one last time to merge v1 & v2 data (e.g. SOON who switched account_name)
        df_melt = df_melt.groupby(['date', 'origin_key', 'metric_key']).sum().reset_index()

        # set index (moved set index into the DAG)
        #df_melt = df_melt.set_index(['date', 'origin_key', 'metric_key'])

        return df_melt

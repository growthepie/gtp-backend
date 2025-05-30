import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

class AdapterEigenDA(AbstractAdapter):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("EigenDA", adapter_params, db_connector)
        self.adapter_params = adapter_params
        print_init(self.name, self.adapter_params)

    def extract(self, load_params: dict):
        """
        Extract data from EigenDA API and prepare it for loading into the database.

        Parameters:
        load_params : dict
            - 'days' (int): The number of days to look back for data extraction.
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
        print_load(self.name, upserted, table)

    ### api call function

    def call_api_endpoint(self):
        import requests
        import json

        yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        self.endpoint = f"{self.load_params.get('endpoint')}/{yesterday}.json"
        
        response = requests.get(self.endpoint)
        if response.status_code == 200:
            data = [json.loads(line) for line in response.text.strip().split('\n')]
            df = pd.DataFrame(data)
            return df
        else:
            raise Exception(f"API call failed with status code {response.status_code}")
        
    def prepare_df(self, df: pd.DataFrame):
        # convert mb to bytes
        df["eigenda_blob_size_bytes"] = (df["total_size_mb"] * 1000).astype(int)
        df = df.drop(columns=['total_size_mb'])

        # rename blob_count to eigenda_blob_count
        df = df.rename(columns={'blob_count': 'eigenda_blob_count'})

        # only get the last x days as defined in load_params, else max
        day = pd.Timestamp.now() - pd.Timedelta(days=self.load_params.get('days', 999))
        df = df[df['date'] >= day.strftime('%Y-%m-%d')]

        # TODO: left join gtp-dna
        # TODO: calculate daily totals
        # TODO: melt df

        return df
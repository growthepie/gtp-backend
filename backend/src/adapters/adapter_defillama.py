import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import api_get_call, return_projects_to_load, check_projects_to_load, upsert_to_kpis, print_init, print_load, print_extract, send_discord_message

class AdapterDefillama(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("DefiLlama", adapter_params, db_connector)
        self.base_url = 'https://api.llama.fi/'
        self.stables = {
            'Tether': 1,
            'Circle USDC': 2,
        }
        self.stables_dfs = self.get_stables_dfs()
        self.df_ethereum = self.get_total_stables_fees_df()

        main_conf = get_main_config()
        self.projects = [chain for chain in main_conf if chain.aliases_defillama is not None and chain.api_deployment_flag != "ARCHIVE"]
        
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
    """
    def extract(self, load_params:dict):
        origin_keys = load_params['origin_keys']

        check_projects_to_load(self.projects, origin_keys)
        projects_to_load = return_projects_to_load(self.projects, origin_keys)

        ## Load data
        df = self.extract_app_fees(projects_to_load=projects_to_load)
        
        print_extract(self.name, load_params,df.shape)
        return df

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)  


    ## ----------------- Helper functions --------------------

    def extract_app_fees(self, projects_to_load):
        df_main = pd.DataFrame()
        for chain in projects_to_load:
            alias = chain.aliases_defillama
            origin_key = chain.origin_key
            
            print(f'Processing {origin_key} with alias {alias}')

            url = f'{self.base_url}overview/fees/{alias}?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=false&dataType=dailyFees'
            print(f'..fetching: {url}')
            response_json = api_get_call(url)
            
            if not response_json:
                print(f'...API call failed for {origin_key} with alias {alias}. Skipping...')
                send_discord_message(f"DefiLlama App Fees: API call failed for DefiLlama app fees for {origin_key} with alias {alias}.")
                continue

            # Build a list of dicts, then create the DataFrame at once (avoiding deprecated .append)
            if len(response_json['totalDataChartBreakdown']) == 0:
                print(f'...no data found for {origin_key}. Skipping...')
                continue
                
            rows = []
            for item in response_json['totalDataChartBreakdown']:
                for key, value in item[1:][0].items():
                    rows.append({
                        'unix': item[0],
                        'protocol': key,
                        'value': value
                    })
            df = pd.DataFrame(rows)
            df['date'] = pd.to_datetime(df['unix'], unit='s')
            df = df.drop(columns=['unix'])
            
            ## remove all stables from the main df
            df = df[~df['protocol'].isin(self.stables.keys())]
            
            # load the stables data and append it to the main df
            df_stables = self.load_stables_df(alias)
            df = pd.concat([df, df_stables], ignore_index=True)
            df['origin_key'] = origin_key
            
            # For Arbitrum, we need to remove Timeboost (since it already included in the chain REV/revenue)
            if origin_key == 'arbitrum':
                df = df[~df['protocol'].isin(['Timeboost'])]
            
            df_main = pd.concat([df_main, df], ignore_index=True)
            time.sleep(1)  # Respect API rate limits
        
        ## aggregate by origin_key, date (this will drop the protocol column -> might be interesting for later though on app level)
        df_main = df_main.groupby(['origin_key', 'date']).agg({'value': 'sum'}).reset_index()
        df_main['metric_key'] = 'app_fees_usd'
        df_main.sort_values(by=['date'], inplace=True, ascending=False)

        df_main.set_index(['date', 'origin_key', 'metric_key'], inplace=True)
        return df_main
    
    def get_stables_dfs(self):
        """
        Returns a list of dictionaries for each stablecoin with its name and ID.
        This is used to fetch stablecoin data from the DefiLlama API.
        """
        print('..initializing stablecoins dataframes')
        stables_dfs = {}
        for name, id in self.stables.items():
            print(f'..adding stablecoin {name} ({id}) to stables_dicts')
            url = f"https://stablecoins.llama.fi/stablecoin/{id}"
            print(f'..fetching: {url}')
            response_json = api_get_call(url)

            # Build a list of dicts, then create the DataFrame at once (avoiding deprecated .append)
            rows = []
            for item in response_json['tokens']:
                rows.append({
                    'unix': int(item['date']),
                    'circ_total': item['circulating']['peggedUSD']
                })
            df = pd.DataFrame(rows)

            time.sleep(1)  # To avoid hitting the API too hard
            
            stables_dfs[id] = df
        return stables_dfs
    
    def get_total_stables_fees_df(self):
        ## retrieve ethereum protocol fee data
        url = f'{self.base_url}overview/fees/offchain?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=false&dataType=dailyFees'
        response_json = api_get_call(url)
        
        rows = []
        for item in response_json['totalDataChartBreakdown']:
            for key, value in item[1:][0].items():
                rows.append({
                    'unix': item[0],
                    'protocol': key,
                    'value': value
                })
        df_ethereum = pd.DataFrame(rows)
        ## filter down to Tether and Circle (stables keys)
        df_ethereum = df_ethereum[df_ethereum['protocol'].isin(self.stables.keys())]
        df_ethereum['date'] = pd.to_datetime(df_ethereum['unix'], unit='s')
        return df_ethereum
    
    def load_stables_df(self, chain):
        df_stables = pd.DataFrame()
        for name, id in self.stables.items():
            url = f"https://stablecoins.llama.fi/stablecoincharts/{chain}?stablecoin={id}"
            print(f'..fetching: {url}')
            response_json = api_get_call(url)
            
            if response_json == {}:
                print(f'...no data found for {name} ({id})')
                continue
            
            rows = []
            for item in response_json:
                rows.append({
                    'unix': int(item['date']),
                    'chain_circulating': item['totalCirculating']['peggedUSD']
                })
            df2 = pd.DataFrame(rows)

            df = pd.merge(self.stables_dfs[id], df2, on='unix', how='outer')
            df['protocol'] = name
            df_stables = pd.concat([df_stables, df], ignore_index=True)

            time.sleep(1)  # To avoid hitting the API too hard
            
        if df_stables.empty:
            print(f'...no stablecoin data found for {chain}. Returning empty DataFrame.')
            return pd.DataFrame(columns=['date', 'protocol', 'value'])

        df_stables['date'] = pd.to_datetime(df_stables['unix'], unit='s')
        df_stables['dominance'] = df_stables['chain_circulating'] / df_stables['circ_total']
        
        df_stables = pd.merge(self.df_ethereum, df_stables[['date', 'protocol', 'dominance']], on=['date', 'protocol'], how='left')
        df_stables['value'] *= df_stables['dominance']
        
        return df_stables[['date', 'protocol', 'value']]
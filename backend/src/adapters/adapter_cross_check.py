import time
import pandas as pd
import os
import io
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import return_projects_to_load, upsert_to_kpis, check_projects_to_load, api_get_call
from src.misc.helper_functions import print_init, print_load, send_discord_message

##ToDos: 
# Add logs (query execution, execution fails, etc)

class AdapterCrossCheck(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Cross-Check", adapter_params, db_connector)
        main_conf = get_main_config()
        self.projects = [chain for chain in main_conf if chain.cross_check_type is not None and chain.api_deployment_flag !=  "ARCHIVE"]
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        self.proxy =  {
            'https': os.getenv('PROXY'),
        }
        self.webhook_url = os.getenv('DISCORD_ALERTS')
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
    """
    def extract(self, load_params:dict):
        origin_keys = load_params['origin_keys']

        check_projects_to_load(self.projects, origin_keys)
        projects_to_load = return_projects_to_load(self.projects, origin_keys)

        dfMain = pd.DataFrame()
        metric_key = 'txcount_explorer'

        for project in projects_to_load:
            print(f"... loading {project.origin_key} txcount data from explorer ({project.cross_check_type})...")
            
            try:
                if project.cross_check_type == 'etherscan':
                    response = api_get_call(project.cross_check_url, header = self.headers, as_json=False, proxy=self.proxy)
                    
                    data = io.StringIO(response)
                    df = pd.read_csv(data)

                    df['date'] = pd.to_datetime(df['Date(UTC)'])
                    df['metric_key'] = metric_key
                    df['origin_key'] = project.origin_key
                    df.rename(columns={'Value': 'value'}, inplace=True)
                    df = df[['date', 'metric_key', 'origin_key', 'value']]

                    dfMain = pd.concat([dfMain, df], ignore_index=True)

                elif project.cross_check_type == 'blockscout':
                    response = api_get_call(project.cross_check_url, header = self.headers, proxy=self.proxy)
                    df = pd.DataFrame(response['chart_data'])

                    df['date'] = pd.to_datetime(df['date'])
                    df['metric_key'] = metric_key
                    df['origin_key'] = project.origin_key
                    df.rename(columns={'tx_count': 'value'}, inplace=True)
                    df = df[['date', 'metric_key', 'origin_key', 'value']]

                    dfMain = pd.concat([dfMain, df], ignore_index=True)        

                elif project.cross_check_type == 'l2beat':
                    if project.origin_key == 'ethereum':
                        response_json = api_get_call(f"https://l2beat.com/api/scaling/activity/{project.aliases_l2beat_slug}?range=1y")
                    else:
                        response_json = api_get_call(f"https://l2beat.com/api/scaling/activity/{project.aliases_l2beat_slug}?range=max")
                    
                    if response_json:
                        df = df = pd.json_normalize(response_json['data']['chart'], record_path=['data'])
                        ## only keep the columns 0 (date) and 1 (transactions)
                        df = df.iloc[:,[0,1]]                     
                        df.rename(columns={1:'value'}, inplace=True)

                        df['date'] = pd.to_datetime(df[0],unit='s').dt.date
                        df.drop([0], axis=1, inplace=True)
                        df['metric_key'] = metric_key
                        df['origin_key'] = project.origin_key

                        dfMain = pd.concat([dfMain, df], ignore_index=True)  
                    else: 
                        print(f"Error in extracting cross-chain txcount data for {project.origin_key} - moving on...")
                        send_discord_message(f"Error in extracting cross-chain txcount data for {project.origin_key}", self.webhook_url)

                elif project.cross_check_type == 'NA':
                    print(f"no block explorer defined for {project.origin_key} - moving on...")
                
                else:
                    print(f'not implemented {project.cross_check_type}')
                    raise ValueError('Block Explorer Type not supported')
            except Exception as e:
                print(f"TxCount Cross Check: Error loading comparison txcount data for {project.origin_key}: {e}")
                send_discord_message(f"Error loading TxCount Cross Check data for {project.origin_key}: {e}", self.webhook_url)
                continue
        
        today = datetime.today().strftime('%Y-%m-%d')
        dfMain.drop(dfMain[dfMain.date == today].index, inplace=True, errors='ignore')
        dfMain['value'] = dfMain['value'].fillna(0)

        dfMain.set_index(['date', 'origin_key', 'metric_key'], inplace=True)
        return dfMain

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)     

    def cross_check(self):
        origin_keys = [x.origin_key for x in self.projects]

        exec_string = f"""
            with temp as (
            SELECT 
                origin_key,
                SUM(case when metric_key = 'txcount_comparison' then value end) as raw,
                SUM(case when metric_key = 'txcount_explorer' then value end) as explorer
            FROM fact_kpis 
            WHERE metric_key in ('txcount_comparison', 'txcount_explorer')
            and date < date_trunc('day', NOW()) 
            and date >= date_trunc('day',now()) - interval '3 days' 
            and origin_key in ('"""+ "', '".join(origin_keys) + """')
            group by 1
        )

        SELECT
            *,
            explorer - raw AS diff,
            COALESCE((explorer - raw) / NULLIF(explorer, 0), 999) AS diff_percent
        FROM temp
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        for index, row in df.iterrows():
            if row['origin_key'] == 'rhino':
                threshold = 0.6
            elif row['origin_key'] in ['arbitrum_nova']:
                threshold = 0.12 # 12% discrepancy allowed because comp dataset is weird
            elif row['origin_key'] in ['megaeth']:
                threshold = 0.05 # 5% discrepancy allowed because comp dataset is weird
            else:
                threshold = 0.03 ## max 3% discrepancy

            if row['diff_percent'] > threshold:
                try:
                    print(f"txcount discrepancy for {row['origin_key']}: {row['diff_percent'] * 100:.2f}% ({row['diff']})")
                    send_discord_message(f"We are missing tx: txcount discrepancy in last 3 days for {row['origin_key']}: {row['diff_percent'] * 100:.2f}% ({int(row['diff'])} tx)", self.webhook_url)
                except:
                    send_discord_message(f"We are missing tx: txcount discrepancy in last 3 days for {row['origin_key']}: Not sure how much? No data available.", self.webhook_url)
                
            elif row['diff_percent'] < -threshold:
                try:
                    print(f"txcount discrepancy for {row['origin_key']}: {row['diff_percent'] * 100:.2f}% ({row['diff']})")
                    send_discord_message(f"We have too many tx: txcount discrepancy in last 3 days for {row['origin_key']}: {row['diff_percent'] * 100:.2f}% ({int(row['diff'])} tx)", self.webhook_url)
                except:
                    send_discord_message(f"We have too many tx: txcount discrepancy in last 3 days for {row['origin_key']}: Not sure how much? No data available.", self.webhook_url)
                
    def cross_check_celestia(self):
        user_id = '326358477335298050'
        days = 7

        ## get Celenium data
        now = datetime.now()
        now = now.replace(hour=0, minute=0, second=0, microsecond=0)
        now = time.mktime(now.timetuple())
        unix = int(now - days*24*60*60)

        response_json = api_get_call(f'https://api.celenium.io/v1/stats/series/tx_count/day?from={unix}')
        df_celenium = pd.DataFrame(response_json)
        df_celenium['time'] = pd.to_datetime(df_celenium['time'])
        df_celenium = df_celenium.rename(columns={'time': 'day', 'value': 'txcount_celenium'})
        df_celenium['day'] = df_celenium['day'].dt.date
        df_celenium = df_celenium[df_celenium['day'] < datetime.now().date()]
        df_celenium['txcount_celenium'] = df_celenium['txcount_celenium'].astype(int)

        ## get our data
        exec_string = f"""
            select
                date_trunc('day', block_timestamp) as day,
                count(*) as txcount_db
            from celestia_tx
            where block_timestamp >= current_date - interval '{days} days' and block_timestamp < current_date
            group by 1
        """
        df_our_db = pd.read_sql(exec_string, self.db_connector.engine.connect())
        df_our_db['day'] = df_our_db['day'].dt.date

        ## merge
        df = df_celenium.merge(df_our_db, on='day', how='outer')
        df['diff'] = df['txcount_celenium'] - df['txcount_db']

        ## send message if discrepancy
        if df['diff'].sum() / df['txcount_celenium'].sum() > 0.02:
            send_discord_message(f"<@{user_id}> -- txcount discrepancy in last 7 days for Celestia: {df['diff'].sum()} tx which equals {(df['diff'].sum() / df['txcount_celenium'].sum()) * 100:.2f}%", self.webhook_url)
            send_discord_message(f"```\n{df.to_markdown(index=False)}\n```", self.webhook_url)
            print(f"txcount discrepancy for Celestia: {df['diff'].sum()} tx")


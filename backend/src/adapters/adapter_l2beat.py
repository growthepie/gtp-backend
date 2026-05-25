import time
import pandas as pd
from datetime import datetime, date
import os

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import api_get_call, return_projects_to_load, check_projects_to_load, get_df_kpis, upsert_to_kpis
from src.misc.helper_functions import print_init, print_load, print_extract, send_discord_message
from src.config import l2_maturity_levels


class AdapterL2Beat(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("L2Beat", adapter_params, db_connector)
        self.base_url = 'https://l2beat.com/api/'
        self.webhook = os.getenv('DISCORD_ALERTS')
        self.webhook_analyst = os.getenv('GTP_AI_WEBHOOK_URL')
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
    """
    def extract(self, load_params:dict):
        main_conf = get_main_config()

        ## Set variables
        origin_keys = load_params['origin_keys']
        self.load_type = load_params['load_type']
        self.l2beat_chains = load_params.get('l2beat_chains', None)

        projects = [chain for chain in main_conf if chain.aliases_l2beat is not None and chain.api_deployment_flag != "ARCHIVE"]
        
        ## Prepare projects to load (can be a subset of all projects)
        check_projects_to_load(projects, origin_keys)
        projects_to_load = return_projects_to_load(projects, origin_keys)

        ## Load data
        if self.load_type == 'tvs':
            df = self.extract_tvs(
                projects_to_load=projects_to_load)
        elif self.load_type == 'stages':
            df = self.extract_stages(
                projects_to_load=projects_to_load)
        elif self.load_type == 'sys_l2beat':
            df = self.extract_sys_l2beat()
        elif self.load_type == 'txcount':
            df = self.extract_txcount(self.l2beat_chains)
        else:
            raise NotImplementedError(f"load_type {self.load_type} not recognized")

        print_extract(self.name, load_params,df.shape)
        return df 

    def load(self, df:pd.DataFrame):
        if self.load_type == 'tvs' or self.load_type == 'txcount':
            upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
            print_load(self.name, upserted, tbl_name)
        elif self.load_type == 'stages':
            self.db_connector.update_sys_main_conf(df, 'str')
            print_load(self.name, df.shape, 'sys_main_conf')
        elif self.load_type == 'sys_l2beat':
            self.db_connector.upsert_table('sys_l2beat', df)
            print_load(self.name, df.shape[0], 'sys_l2beat')
        else:
            raise NotImplementedError(f"load_type {self.load_type} not recognized")

    ## ----------------- Helper functions --------------------

    def extract_txcount(self, l2beat_chains):
        dfMain = get_df_kpis()
        for chain in l2beat_chains:
            origin_key = chain ## L2BEAT naming
            naming = chain
            
            if origin_key == 'ethereum':
                continue

            url = f"https://l2beat.com/api/scaling/activity/{naming}?range=max"       
            print(url)
            response_json = api_get_call(url, sleeper=10, retries=10)
            if response_json['success']:
                df = pd.json_normalize(response_json['data']['chart'], record_path=['data'], sep='_')

                ## only keep the columns 0 (date), 1 (txcount)
                df = df.iloc[:,[0,1]]
                df['date'] = pd.to_datetime(df[0],unit='s')
                df['date'] = df['date'].dt.date
                df.drop([0], axis=1, inplace=True)
                
                ## rename col 1 into value
                df.rename(columns={1: 'value'}, inplace=True)
                df = df[['date','value']]
                df['metric_key'] = 'txcount'
                df['origin_key'] = origin_key

                today = datetime.today().strftime('%Y-%m-%d')
                df.drop(df[df.date == today].index, inplace=True, errors='ignore')
                df['value'] = df['value'].fillna(0)
                dfMain = pd.concat([dfMain,df])

                print(f"...{self.name} - loaded TXCOUNT for {origin_key}. Shape: {df.shape}")
                time.sleep(10)
            else:
                print(f'Error loading TVS data for {origin_key}')
                send_discord_message(f'L2Beat: Error loading TVS data for {origin_key}. Other chains are not impacted.', self.webhook)            

        dfMain.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain
    
    def extract_tvs(self, projects_to_load):
        dfMain = get_df_kpis()
        for chain in projects_to_load:
            origin_key = chain.origin_key
            
            if origin_key == 'ethereum':
                continue

            naming = chain.aliases_l2beat_slug
            url = f"https://l2beat.com/api/scaling/tvs/{naming}?range=max"       
            print(url)
            response_json = api_get_call(url, sleeper=10, retries=10)
            if response_json['success']:
                df = pd.json_normalize(response_json['data']['chart'], record_path=['data'], sep='_')

                ## only keep the columns 0 (date), 1 (canonical tvl), 2 (external tvl), 3 (native tvl)
                df = df.iloc[:,[0,1,2,3]]
                df['date'] = pd.to_datetime(df[0],unit='s')
                df['date'] = df['date'].dt.date

                df.drop([0], axis=1, inplace=True)
                ## sum column 1,2,3
                df['value'] = df.iloc[:,0:3].sum(axis=1)
                ## drop the 3 tvl columns
                df = df[['date','value']]
                df['metric_key'] = 'tvl'
                df['origin_key'] = origin_key
                # max_date = df['date'].max()
                # df.drop(df[df.date == max_date].index, inplace=True)
                today = datetime.today().strftime('%Y-%m-%d')
                df.drop(df[df.date == today].index, inplace=True, errors='ignore')
                df['value'] = df['value'].fillna(0)
                dfMain = pd.concat([dfMain,df])

                print(f"...{self.name} - loaded TVS for {origin_key}. Shape: {df.shape}")
                time.sleep(10)
            else:
                print(f'Error loading TVS data for {origin_key}')
                send_discord_message(f'L2Beat: Error loading TVS data for {origin_key}. Other chains are not impacted.', self.webhook)            

        dfMain.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain
    
    def extract_stages(self, projects_to_load):
        stages = []
        url = f"https://l2beat.com/api/scaling/summary"
        response = api_get_call(url)

        # Get current main_config to compare against new L2Beat values
        current_config = get_main_config()

        for chain in projects_to_load:
            origin_key = chain.origin_key
            if origin_key == 'ethereum':
                maturity_level = "10_foundational"
            else:
                l2beat_id = str(chain.aliases_l2beat)
                print(f'...loading stage and maturity info for {origin_key} with l2beat_id: {l2beat_id}') 

                ## processing stage
                if l2beat_id not in response['projects']:
                    print(f'Error loading stage data for {origin_key}. L2Beat ID {l2beat_id} not found in response.')
                    send_discord_message(f'L2Beat: Error loading stage data for {origin_key}. L2Beat ID {l2beat_id} not found in response. Other chains are not impacted.', self.webhook)            
                    stage = 'NA'
                else:
                    stage = response['projects'][l2beat_id]['stage']
                    if stage in ['NotApplicable', 'Not applicable']:
                        stage = 'NA'
                        
                # Compare with the existing stage in the main_config
                current_stage = next((config.l2beat_stage for config in current_config if config.origin_key == origin_key), 'NA')
                # If the stage has changed, send a notification
                if stage != current_stage:
                    send_discord_message(f'L2Beat: {origin_key} has changed stage from {current_stage} to {stage}', self.webhook)


                ## processing maturity
                maturity_level = 'NA'

                tvs = response['projects'][l2beat_id]['tvs']['breakdown']['total']
                positive_risk_count = 0
                for risks in response['projects'][l2beat_id]['risks']:
                    if risks['sentiment'] == 'good':
                        positive_risk_count += 1
                launch_date = chain.metadata_launch_date
                age = (datetime.now() - datetime.strptime(launch_date, '%Y-%m-%d')).days
                
                ## iterate over all maturity levels and check if all conditions are met, if yes than break and assign maturity level # TODO
                for maturity in l2_maturity_levels:
                    if maturity == '10_foundational':
                        continue
                    
                    maturity_dict = l2_maturity_levels[maturity]

                    if 'conditions' in maturity_dict:
                        all_and_conditions_met = True
                        all_or_conditions_met = False

                        ## check if all conditions in 'and' are met
                        for condition in maturity_dict['conditions']['and']:
                            if condition == 'tvs':
                                if tvs < maturity_dict['conditions']['and']['tvs']:
                                    all_and_conditions_met = False
                                    break
                            elif condition == 'risks':
                                if positive_risk_count < maturity_dict['conditions']['and']['risks']:
                                    all_and_conditions_met = False
                                    break
                            elif condition == 'age':
                                if age < maturity_dict['conditions']['and']['age']:
                                    all_and_conditions_met = False
                                    break
                            elif condition == 'stage':
                                if stage != maturity_dict['conditions']['and']['stage']:
                                    all_and_conditions_met = False
                                    break

                        ## check if any conditions in 'or' are met
                        if 'or' in maturity_dict['conditions']:
                            for condition in maturity_dict['conditions']['or']:
                                if condition == 'tvs':
                                    if tvs >= maturity_dict['conditions']['or']['tvs']:
                                        all_or_conditions_met = True
                                        break
                                elif condition == 'risks':
                                    if positive_risk_count >= maturity_dict['conditions']['or']['risks']:
                                        all_or_conditions_met = True
                                        break
                                elif condition == 'age':
                                    if age >= maturity_dict['conditions']['or']['age']:
                                        all_or_conditions_met = True
                                        break
                                elif condition == 'stage':
                                    if stage == maturity_dict['conditions']['or']['stage']:
                                        all_or_conditions_met = True
                                        break
                        else:
                            all_or_conditions_met = True

                        if all_and_conditions_met and all_or_conditions_met:
                            maturity_level = maturity
                            break
                    else:
                        maturity_level = maturity
                        break


            stages.append({'origin_key': origin_key, 'l2beat_stage': stage, 'maturity': maturity_level})
            print(f"...{self.name} - loaded Stage & Maturity: {stage} - {maturity_level} for {origin_key}")
            time.sleep(0.5)
            
        df = pd.DataFrame(stages)
        return df
    
    def extract_sys_l2beat(self):
        data = api_get_call("https://l2beat.com/api/scaling/summary")
        data['projects']
        df = pd.DataFrame(data['projects'])
        # transpose the dataframe
        df = df.T

        ## extract the tvs column
        for badge in ['VM', 'DA', 'Stack', 'Infra']:
            col_name = badge.lower()
            df[col_name] = None
            ## iterate over list in column 'tvs'
            for i in range(len(df)):
                badges = df['badges'].iloc[i] or []
                for badge_obj in badges:
                    if badge_obj['type'] == badge:
                        df.at[df.index[i], col_name] = badge_obj['id']
                        break

        df.reset_index(inplace=True)

        ## filter to type = 'layer2'
        df = df[df['type'] == 'layer2']
        
        ## create a new column 'provider' that extracts the first element of the 'providers' list
        df['provider'] = df['providers'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
        
        ## get tvs total and stable value
        df['tvs_total'] = df['tvs'].apply(lambda x: x['breakdown']['total'] if isinstance(x, dict) and 'breakdown' in x and 'total' in x['breakdown'] else None)
        df['tvs_stables'] = df['tvs'].apply(lambda x: x['breakdown']['stablecoin'] if isinstance(x, dict) and 'breakdown' in x and 'stablecoin' in x['breakdown'] else None)

        column_defaults = {
            'hostChain': None,
            'category': None,
            'isArchived': False,
            'isUpcoming': False,
            'isUnderReview': False,
            'stage': None,
        }
        for column, default in column_defaults.items():
            if column not in df.columns:
                df[column] = default

        ## keep only columns index, name, slug, type, hostChain, category, provider, isArchived, isUpcoming, isUnderReview, stage, VM, DA, Stack, Infra
        df = df[['index', 'name', 'slug', 'type', 'hostChain', 'category', 'provider', 'isArchived', 'isUpcoming', 'isUnderReview', 'stage', 'vm', 'da', 'stack', 'infra', 'tvs_total', 'tvs_stables']] 

        ## add underscore infront of each capital letter in column names
        df.columns = df.columns.str.replace(r'([A-Z])', r'_\1', regex=True).str.lower() 
        df.columns = df.columns.str.lower()

        df_meta = df.copy()
        df_main = pd.DataFrame()
        
        ####
        ## TODO: better: use liveness or activity data
        # for projects that aren't returned by the API anymore, set is_archived = True in sys_l2beat table
        df_sys = self.db_connector.get_table('sys_l2beat')

        ## filter df_sys to is_archived = False
        df_sys = df_sys[df_sys['is_archived'] == False]

        ## compare df_sys with df based on the 'index' column.
        ## only projects that are missing from the latest API response ("right_only")
        ## should be archived; "left_only" means newly added projects and must not be archived.
        df_merged = df.merge(df_sys, on='index', suffixes=('_new', '_sys'), how='outer', indicator=True)
        df_archived = df_merged[df_merged['_merge'] == 'right_only']
        df_new = df_merged[df_merged['_merge'] == 'left_only']

        if len(df_new) > 0:
            print(f"...detected {len(df_new)} new project(s) in L2Beat summary (not archived): {df_new['index'].dropna().tolist()}")
        
        if len(df_archived) > 0:
            df_archived = df_archived.reset_index(drop=True)
            df_archived = df_archived[['index']]

            df_archived['archived_on'] = date.today()
            df_archived['is_archived'] = True
            
            df_archived.set_index(['index'], inplace=True)
            self.db_connector.upsert_table('sys_l2beat', df_archived)
            send_discord_message(f"L2Beat: The following Layer 2 projects have been archived:\n{df_archived.index.tolist()}", self.webhook)
        else:
            print("...no projects to archive in sys_l2beat")
        ####

        ## iterate over each row of the df and call 
        for i, row in df_meta.iterrows():
            naming = row['slug']
            url = f"https://l2beat.com/api/scaling/tvs/{naming}?range=max"       
            response_json = api_get_call(url, sleeper=10)

            if 'data' not in response_json:
                print(f"Error with {naming}: {response_json}")
                time.sleep(10)
                continue

            df = pd.json_normalize(response_json['data']['chart'], record_path=['data'], sep='_')

            ## only keep the columns 0 (date), 1 (canonical tvl), 2 (external tvl), 3 (native tvl)
            df = df.iloc[:,[0,1,2,3]]
            df['date'] = pd.to_datetime(df[0],unit='s')
            df['date'] = df['date'].dt.date

            df.drop([0], axis=1, inplace=True)
            ## sum column 1,2,3
            df['value'] = df.iloc[:,0:3].sum(axis=1)
            ## drop the 3 tvl columns
            df = df[['date','value']]
            df['metric_key'] = 'tvl'
            df['l2beat_slug'] = naming
            df_main = pd.concat([df_main, df])
            
            print(f"..finished {naming}")
            time.sleep(10)

        ## order by date and l2beat_slug ascending
        df_main.sort_values(by=['date','l2beat_slug'], inplace=True)

        df_meta['date_10k'] = None
        df_meta['date_100k'] = None
        df_meta['date_1m'] = None
        df_meta['date_10m'] = None
        df_meta['date_100m'] = None
        df_meta['date_1b'] = None
        df_meta['date_10b'] = None


        ## iterate over df_meta
        for i, row in df_meta.iterrows():
            slug = row['slug']
            ## filter df_main by l2beat_slug
            df_filtered = df_main[df_main['l2beat_slug'] == slug]
            ## iterate over df_filtered and find first dates where TVL is above certain thresholds
            df_meta.at[i, 'date_10k'] = df_filtered[df_filtered['value'] > 10000]['date'].min()
            df_meta.at[i, 'date_100k'] = df_filtered[df_filtered['value'] > 100000]['date'].min()
            df_meta.at[i, 'date_1m'] = df_filtered[df_filtered['value'] > 1000000]['date'].min()
            df_meta.at[i, 'date_10m'] = df_filtered[df_filtered['value'] > 10000000]['date'].min()
            df_meta.at[i, 'date_100m'] = df_filtered[df_filtered['value'] > 100000000]['date'].min()
            df_meta.at[i, 'date_1b'] = df_filtered[df_filtered['value'] > 1000000000]['date'].min()
            df_meta.at[i, 'date_10b'] = df_filtered[df_filtered['value'] > 10000000000]['date'].min()

        df_meta.set_index(['index'], inplace=True)

        return df_meta

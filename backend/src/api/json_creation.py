import os
import simplejson as json
import datetime
import pandas as pd
import numpy as np
from datetime import timedelta, datetime, timezone
import zipfile
import io
import requests
import getpass

sys_user = getpass.getuser()

from src.main_config import get_main_config, get_multi_config
from src.da_config import get_da_config
from src.misc.helper_functions import upload_json_to_cf_s3, upload_parquet_to_cf_s3, db_addresses_to_checksummed_addresses, string_addresses_to_checksummed_addresses, fix_dict_nan, empty_cloudfront_cache, remove_file_from_s3, get_files_df_from_s3
from src.misc.glo_prep import Glo
from src.db_connector import DbConnector
from eim.funcs import get_eim_yamls
from src.misc.jinja_helper import execute_jinja_query
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.config import gtp_units, gtp_metrics, gtp_da_metrics, gtp_app_metrics, gtp_fees_types, gtp_fees_timespans, l2_maturity_levels, eim_metrics, main_chart_config

import warnings

# Suppress specific FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def merge_dicts(default, custom):
    merged = default.copy()
    for key, value in custom.items():
        if isinstance(value, dict) and key in merged:
            merged[key] = merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged

class JSONCreation():

    def __init__(self, s3_bucket, cf_distribution_id, db_connector:DbConnector, api_version='v1'):
        ## Constants
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        self.main_config = get_main_config(api_version=self.api_version)
        self.multi_config = get_multi_config()
        self.da_config = get_da_config(api_version=self.api_version)
        self.latest_eth_price = self.db_connector.get_last_price_usd('ethereum')

        eim_yamls = get_eim_yamls(['eth_exported_entities', 'ethereum_events'])
        self.eth_exported_entities = eim_yamls[0]
        self.ethereum_events = eim_yamls[1]

        ## asign configs
        self.units = gtp_units
        self.metrics = gtp_metrics
        self.da_metrics = gtp_da_metrics
        self.app_metrics = gtp_app_metrics
        self.fees_types = gtp_fees_types
        self.fees_timespans = gtp_fees_timespans
        self.maturity_levels = l2_maturity_levels
        self.eim_metrics = eim_metrics
        self.main_chart_config = main_chart_config

        for metric_key, metric_value in self.metrics.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        for metric_key, metric_value in self.fees_types.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        for metric_key, metric_value in self.da_metrics.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        for metric_key, metric_value in self.eim_metrics.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        for metric_key, metric_value in self.app_metrics.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        #append all values of metric_keys in metrics dict to a list
        self.metrics_list = [item for sublist in [self.metrics[metric]['metric_keys'] for metric in self.metrics] for item in sublist]
        self.da_metrics_list = [item for sublist in [self.da_metrics[metric]['metric_keys'] for metric in self.da_metrics] for item in sublist]
        self.app_metrics_list = [item for sublist in [self.app_metrics[metric]['metric_keys'] for metric in self.app_metrics] for item in sublist]
        #concat all values of metrics_list to a string and add apostrophes around each value
        self.metrics_string = "'" + "','".join(self.metrics_list) + "'"

        #append all keys of chains dict to a list
        self.chains_list = [x.origin_key for x in self.main_config]
        #concat all values of chains_list to a string and add apostrophes around each value
        self.chains_string = "'" + "','".join(self.chains_list) + "'"
        #only chains that are in the api output
        self.chains_list_in_api = [chain.origin_key for chain in self.main_config if chain.api_in_main == True]
        #only chains that are in the api output and deployment is "PROD"
        self.chains_list_in_api_prod = [chain.origin_key for chain in self.main_config if chain.api_in_main == True and chain.api_deployment_flag=='PROD']
        #only chains that are in the api output and deployment is "PROD" and in_labels_api is True
        self.chains_list_in_api_labels = [chain.origin_key for chain in self.main_config if chain.api_in_main == True and chain.api_in_labels == True]
        #only chains that are in the api output and deployment is "PROD" and in_labels_api is True
        self.chains_list_in_api_economics = [chain.origin_key for chain in self.main_config if chain.api_in_economics == True]
        #only chains that are in the api output and deployment is "PROD" and api_in_apps is True
        self.chains_list_in_api_apps = [chain.origin_key for chain in self.main_config if chain.api_in_apps == True]

        self.da_layers_list = [x.da_layer for x in self.da_config]
        self.da_layer_overview = [x.da_layer for x in self.da_config if x.incl_in_da_overview == True]

        ## all feest metrics keys
        self.fees_list = [item for sublist in [self.fees_types[metric]['metric_keys'] for metric in self.fees_types] for item in sublist]

    
    ###### gtp-dna methods ######
    def get_custom_logos(self):
        # Get the repository
        repo_url = "https://github.com/growthepie/gtp-dna/tree/main/"
        _, _, _, owner, repo_name, _, branch, *path = repo_url.split('/')

        # Download directory as ZIP file
        zip_url = f"https://github.com/{owner}/{repo_name}/archive/{branch}.zip"
        response = requests.get(zip_url)
        zip_content = io.BytesIO(response.content)

        with zipfile.ZipFile(zip_content) as zip_ref:
            path = 'gtp-dna-main/logos/custom_logos.json'
            with zip_ref.open(path) as file:
                content = file.read().decode('utf-8')
                content = json.loads(content)
        
        return content

    ###### CHAIN DETAILS AND METRIC DETAILS METHODS ########
    def get_metric_dict(self, metric_type):
        if metric_type == 'default':
            return self.metrics
        elif metric_type == 'da':
            return self.da_metrics
        elif metric_type == 'eim':
            return self.eim_metrics
        elif metric_type == 'app':
            return self.app_metrics
        else:
            raise ValueError(f"ERROR: metric type {metric_type} is not implemented")

    def df_rename(self, df, metric_id, tmp_metrics_dict, col_name_removal=False):
        if col_name_removal:
            df.columns.name = None

        if 'usd' in tmp_metrics_dict[metric_id]['units'] or 'eth' in tmp_metrics_dict[metric_id]['units']:
            for col in df.columns.to_list():
                if col == 'unix':
                    continue
                elif col.endswith('_eth'):
                    df.rename(columns={col: 'eth'}, inplace=True)
                else:
                    df.rename(columns={col: 'usd'}, inplace=True)

            if 'unix' in df.columns.to_list():
                df = df[['unix', 'usd', 'eth']]
            else:
                df = df[['usd', 'eth']]
        else:
            for col in df.columns.to_list():
                if col == 'unix':
                    continue
                else:
                    df.rename(columns={col: 'value'}, inplace=True)
        return df

    # Function to trim leading zeros for each group
    def trim_leading_zeros(self, group):
        first_non_zero_index = group['value'].ne(0).idxmax()  # Find first non-zero index in each group
        return group.loc[first_non_zero_index:]  # Return the DataFrame slice starting from this index
    
    def get_ranking(self, df, metric_id, origin_key, incl_value = False, filter_ethereum = True):
        mks = self.metrics[metric_id]['metric_keys']
        ## remove elements in mks list that end with _eth
        mks = [x for x in mks if not x.endswith('_eth')]

        ## First filter down to metric
        df_tmp = df.loc[(df.metric_key.isin(mks)), ["origin_key", "value", "metric_key", "date"]].copy()

        ## then max date of this metric per origin_key
        df_tmp = df_tmp.loc[df_tmp.groupby("origin_key")["date"].idxmax()]

        ## filter out chains that have this metric excluded
        chains_list_metric = [chain.origin_key for chain in self.main_config if metric_id not in chain.api_exclude_metrics]
        df_tmp = df_tmp.loc[(df_tmp.origin_key.isin(chains_list_metric))]

        ## if not dev api endpoint, filter out chains that are not on prod
        if self.api_version != 'dev':
            df_tmp = df_tmp.loc[(df_tmp.origin_key.isin(self.chains_list_in_api_prod))]

        ## filter out ethereum if filter_ethereum is True
        if filter_ethereum:
            df_tmp = df_tmp.loc[(df_tmp.origin_key != 'ethereum')]

        ## assign rank based on order (descending order if metric_key is not 'txcosts')
        if metric_id != 'txcosts':
            df_tmp['rank'] = df_tmp['value'].rank(ascending=False, method='first')
        else:
            df_tmp['rank'] = df_tmp['value'].rank(ascending=True, method='first')

        ## get rank for origin_key
        if df_tmp.loc[df_tmp.origin_key == origin_key].shape[0] > 0:
            rank = df_tmp.loc[df_tmp.origin_key == origin_key, 'rank'].values[0]
            rank_max = df_tmp['rank'].max()

            if incl_value:
                 ## check if metric units include eth and usd
                if 'usd' in self.metrics[metric_id]['units'].keys():
                    is_currency = True
                else:
                    is_currency = False
                
                if is_currency:
                    value_usd = df_tmp.loc[df_tmp.origin_key == origin_key, 'value'].values[0]
                    value_eth = value_usd / self.latest_eth_price
                    return {'rank': int(rank), 'out_of': int(rank_max), 'color_scale': round(rank/rank_max, 2), 'value_usd': value_usd, 'value_eth': value_eth}
                else:
                    value = df_tmp.loc[df_tmp.origin_key == origin_key, 'value'].values[0]
                    return {'rank': int(rank), 'out_of': int(rank_max), 'color_scale': round(rank/rank_max, 2), 'value': value}
            else:
                #print(f"...rank for {origin_key} and {metric_id} is {int(rank)} out of {int(rank_max)}")
                return {'rank': int(rank), 'out_of': int(rank_max), 'color_scale': round(rank/rank_max, 2)}    
        else:
            #print(f"...no rank for {origin_key} and {metric_id}")
            return {'rank': None, 'out_of': None, 'color_scale': None}         


    # this method returns a list of lists with the unix timestamp and all associated values for a certain metric_id and chain_id
    def generate_daily_list(self, df, metric_id, origin_key, start_date = None, metric_type='default', rolling_avg = False):
        tmp_metrics_dict = self.get_metric_dict(metric_type)            

        mks = tmp_metrics_dict[metric_id]['metric_keys']
        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["unix", "value", "metric_key", "date"]]

        ## if start_date is not None, filter df_tmp date to only values after start date
        if start_date is not None:
            df_tmp = df_tmp.loc[(df_tmp.date >= start_date), ["unix", "value", "metric_key", "date"]]
        
        max_date = df_tmp['date'].max()
        max_date = pd.to_datetime(max_date).replace(tzinfo=None)
        yesterday = datetime.now() - timedelta(days=1)
        yesterday = yesterday.date()

        ## if max_date_fill is True, fill missing rows until yesterday with 0
        if tmp_metrics_dict[metric_id]['max_date_fill']:
            #check if max_date is yesterday
            if max_date.date() != yesterday:
                print(f"max_date in df for {mks} is {max_date}. Will fill missing rows until {yesterday} with None.")

                date_range = pd.date_range(start=max_date + timedelta(days=1), end=yesterday, freq='D')

                for mkey in mks:
                    new_data = {'date': date_range, 'value': [0] * len(date_range), 'metric_key': mkey}
                    new_df = pd.DataFrame(new_data)
                    new_df['unix'] = new_df['date'].apply(lambda x: x.timestamp() * 1000)

                    df_tmp = pd.concat([df_tmp, new_df], ignore_index=True)

        ## trim leading zeros
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)
        df_tmp = df_tmp.groupby('metric_key').apply(self.trim_leading_zeros).reset_index(drop=True)

        df_tmp.drop(columns=['date'], inplace=True)
        df_tmp = df_tmp.pivot(index='unix', columns='metric_key', values='value').reset_index()
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)
        
        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict, col_name_removal=True)

        mk_list = df_tmp.values.tolist() ## creates a list of lists

        if len(tmp_metrics_dict[metric_id]['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        elif len(tmp_metrics_dict[metric_id]['units']) == 2:
            mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        else:
            raise NotImplementedError("Only 1 or 2 units are supported")
        
        if rolling_avg == True:
            mk_list_int = self.create_7d_rolling_avg(mk_list_int)
        
        return mk_list_int, df_tmp.columns.to_list()
    
    # this method returns a list of lists with the unix timestamp (first day of month) and all associated values for a certain metric_id and chain_id
    def generate_monthly_list(self, df, metric_id, origin_key, start_date = None, metric_type='default'):
        tmp_metrics_dict = self.get_metric_dict(metric_type)   

        mks = tmp_metrics_dict[metric_id]['metric_keys'].copy()
        if 'daa' in mks:
            mks[mks.index('daa')] = 'maa'

        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["date", "unix", "value", "metric_key"]]

        ## if start_date is not None, filter df_tmp date to only values after start date
        if start_date is not None:
            df_tmp = df_tmp.loc[(df_tmp.date >= start_date), ["unix", "value", "metric_key", "date"]]

        ## create monthly averages on value and min on unix column
        df_tmp['date'] = df_tmp['date'].dt.tz_convert(None) ## get rid of timezone in order to avoid warnings

        ## replace earliest date with first day of month (in unix) in unix column
        df_tmp['unix'] = (df_tmp['date'].dt.to_period("M").dt.start_time).astype(np.int64) // 10**6        

        if tmp_metrics_dict[metric_id]['monthly_agg'] == 'sum':
            df_tmp = df_tmp.groupby([df_tmp.date.dt.to_period("M"), df_tmp.metric_key]).agg({'value': 'sum', 'unix': 'min'}).reset_index()
        elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'avg':
            df_tmp = df_tmp.groupby([df_tmp.date.dt.to_period("M"), df_tmp.metric_key]).agg({'value': 'mean', 'unix': 'min'}).reset_index()
        elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'maa':
            pass ## no aggregation necessary
        else:
            raise NotImplementedError(f"monthly_agg {tmp_metrics_dict[metric_id]['monthly_agg']} not implemented")

        ## drop column date
        df_tmp = df_tmp.drop(columns=['date'])

        ## trim leading zeros
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)
        df_tmp = df_tmp.groupby('metric_key').apply(self.trim_leading_zeros).reset_index(drop=True)

        ## metric_key to column
        df_tmp = df_tmp.pivot(index='unix', columns='metric_key', values='value').reset_index()

        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict, col_name_removal=True)

        mk_list = df_tmp.values.tolist() ## creates a list of lists

        if len(tmp_metrics_dict[metric_id]['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        elif len(tmp_metrics_dict[metric_id]['units']) == 2:
            mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        else:
            raise NotImplementedError("Only 1 or 2 units are supported")

        return mk_list_int, df_tmp.columns.to_list()

    def generate_fees_list(self, df, metric, origin_key, granularity, timespan, eth_price, max_ts_all = None, normalize = False):
        metric_key = self.fees_types[metric]['metric_keys'][0]
        ## filter df to granularity = 'hourly' and metric_key = metric
        df = df[(df.granularity == granularity) & (df.metric_key == metric_key) & (df.origin_key == origin_key)]
        
        ## Create new rows for missing timestamps
        if max_ts_all is not None:
            ## certain metric_key/origin_key combinations are empty (i.e. Starknet native transfer fees)
            if df.empty:
                print(f"df is empty for {metric_key} and {origin_key}. Will skip.")
            else:
                max_ts = df['unix'].max()
                #check if filtered max_ts is the same as max_ts_all
                if max_ts != max_ts_all:
                    print(f"max_ts in df (shape {df.shape}) for {metric_key} and {origin_key} is {max_ts}. Will fill missing rows until {max_ts_all} with None.")

                    start_date = pd.to_datetime(max_ts, unit='ms', utc=True)
                    end_date = pd.to_datetime(max_ts_all, unit='ms', utc=True)

                    print(f"start_date: {start_date}, end_date: {end_date} for {metric_key} and {origin_key}")

                    if granularity == 'hourly':
                        date_range = pd.date_range(start=start_date, end=end_date, freq='H')
                    elif granularity == '10_min':
                        date_range = pd.date_range(start=start_date, end=end_date, freq='10T')
                    else:
                        raise NotImplementedError(f"Granularity {granularity} not implemented")

                    new_data = {'timestamp': date_range, 'value': [None] * len(date_range)}
                    new_df = pd.DataFrame(new_data)
                    new_df['unix'] = new_df['timestamp'].apply(lambda x: x.timestamp() * 1000)
                    ## drop row with smallest timestamp
                    new_df = new_df[new_df['unix'] != max_ts]

                    df = pd.concat([df, new_df], ignore_index=True)

        ## order df_tmp by timestamp desc
        df = df.sort_values(by='timestamp', ascending=False)
        ## only keep columns unix, value_usd
        df = df[['unix', 'value']]

        if self.fees_types[metric]['currency']:
            main_col = 'value_eth'
            ## calculate value_usd by multiplying value with eth_price
            df['value_usd'] = df['value'] * eth_price
            df.rename(columns={'value': main_col}, inplace=True)
            ## order columns in df unix, value_eth, value_usd
            df = df[['unix', main_col, 'value_usd']]
        elif metric == 'tps':
            df['value'] = df['value'] / self.fees_timespans[timespan]['tps_divisor']
            main_col = 'value'
        elif metric == 'throughput':
            df['value'] = df['value'] / (1000 * 1000)
            main_col = 'value'
        else:
            main_col = 'value'

        if normalize:
            ## get max value and min value
            max_value = df[main_col].max()
            min_value = df[main_col].min()
            ## create new column 'normalized' with normalized values between 0 and 1
            df['normalized'] = (df[main_col] - min_value) / (max_value - min_value)

            if self.fees_types[metric]['invert_normalization']:
                df['normalized'] = 1 - df['normalized']

            mk_list = df.values.tolist()
            if self.fees_types[metric]['currency']:
                mk_list_int = [
                    [
                        int(i[0]), ## unix timestamp
                        i[1], ## eth value
                        round(i[2], 4), ## usd value
                        round(i[3], 2) ## normalized value
                    ] for i in mk_list]
            else:
                mk_list_int = [
                    [
                        int(i[0]), ## unix timestamp
                        i[1], ## value
                        round(i[2], 2) ## normalized value
                    ] for i in mk_list]
        else:
            mk_list = df.values.tolist()
            if self.fees_types[metric]['currency']:
                mk_list_int = [
                    [
                        int(i[0]), ## unix timestamp
                        i[1], ## eth value
                        round(i[2], 4) ## usd value
                    ] for i in mk_list]
            else:
                mk_list_int = [
                    [
                        int(i[0]), ## unix timestamp
                        i[1] ## value
                    ] for i in mk_list]
                

        return mk_list_int, df.columns.to_list()

    ## create 7d rolling average over a list of lists where the first element is the date and the second element is the value (necessary for daily_avg field)
    def create_7d_rolling_avg(self, list_of_lists):
        avg_list = []
        if len(list_of_lists[0]) == 2: ## all non USD metrics e.g. txcount
            for i in range(len(list_of_lists)):
                if i < 7:
                    avg_list.append([list_of_lists[i][0], list_of_lists[i][1]])
                else:
                    avg = (list_of_lists[i][1] + list_of_lists[i-1][1] + list_of_lists[i-2][1] + list_of_lists[i-3][1] + list_of_lists[i-4][1] + list_of_lists[i-5][1] + list_of_lists[i-6][1]) / 7
                    ## round to 2 decimals
                    avg = round(avg, 2)
                    avg_list.append([list_of_lists[i][0], avg])
        else: ## all USD metrics e.g. fees that have USD and ETH values
            for i in range(len(list_of_lists)):
                if i < 7:
                    avg_list.append([list_of_lists[i][0], list_of_lists[i][1], list_of_lists[i][2]] )
                else:
                    avg_1 = (list_of_lists[i][1] + list_of_lists[i-1][1] + list_of_lists[i-2][1] + list_of_lists[i-3][1] + list_of_lists[i-4][1] + list_of_lists[i-5][1] + list_of_lists[i-6][1]) / 7
                    avg_2 = (list_of_lists[i][2] + list_of_lists[i-1][2] + list_of_lists[i-2][2] + list_of_lists[i-3][2] + list_of_lists[i-4][2] + list_of_lists[i-5][2] + list_of_lists[i-6][2]) / 7
    
                    avg_list.append([list_of_lists[i][0], avg_1, avg_2])
        
        return avg_list


    def download_data(self, chains_string, metrics_string):
        exec_string = f"""
            SELECT 
                kpi.metric_key, 
                kpi.origin_key as origin_key, 
                kpi."date", 
                kpi.value
            FROM public.fact_kpis kpi
            where kpi.origin_key in ({chains_string})
                and kpi.metric_key in ({metrics_string})
                and kpi."date" >= '2020-01-01'
                AND (
                    (kpi.metric_key not in ('market_cap_usd', 'market_cap_eth') AND kpi."date" < date_trunc('day', now()))
                    OR
                    (kpi.metric_key in ('market_cap_usd', 'market_cap_eth'))
                )
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        ## datetime to unix timestamp using timestamp() function
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        #df.drop(columns=['date'], inplace=True)
        # fill NaN values with 0
        df.value.fillna(0, inplace=True)
        return df
    
    def download_data_fees(self, metric_keys):
        exec_string = f"""
            SELECT *
            FROM public.fact_kpis_granular kpi
            where kpi."timestamp" >= CURRENT_DATE - INTERVAL '181 days'
                and kpi.metric_key in ({"'" + "','".join(metric_keys) + "'"})
                and granularity = 'daily'

            UNION ALL

            SELECT *
            FROM public.fact_kpis_granular kpi
            where kpi."timestamp" >= CURRENT_DATE - INTERVAL '31 days'
                and kpi.metric_key in ({"'" + "','".join(metric_keys) + "'"})
                and granularity = '4_hours'

            UNION ALL

            SELECT *
            FROM public.fact_kpis_granular kpi
            where kpi."timestamp" >= CURRENT_DATE - INTERVAL '8 days'
                and kpi.metric_key in ({"'" + "','".join(metric_keys) + "'"})
                and granularity = 'hourly'

            UNION ALL

            SELECT *
            FROM public.fact_kpis_granular kpi
            where kpi."timestamp" >= CURRENT_DATE - INTERVAL '2 days'
                and kpi.metric_key in ({"'" + "','".join(metric_keys) + "'"})
                and granularity = '10_min'
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## date to datetime column in UTC
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')
        ## datetime to unix timestamp using timestamp() function
        df['unix'] = df['timestamp'].apply(lambda x: x.timestamp() * 1000)
        # fill NaN values with 0
        df.value.fillna(0, inplace=True)
        return df
    
    def download_data_eim(self):
        exec_string = f"""
            SELECT 
                kpi.metric_key, 
                kpi.origin_key as origin_key, 
                kpi."date", 
                kpi.value
            FROM public.eim_fact kpi
            where kpi."date" >= '2015-01-01'
                and metric_key in ('eth_equivalent_exported_usd', 'eth_equivalent_exported_eth', 'eth_supply_eth', 'eth_issuance_rate')
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        ## datetime to unix timestamp using timestamp() function
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        # fill NaN values with 0
        df.value.fillna(0, inplace=True)
        return df

    def create_changes_dict(self, df, metric_id, origin_key, metric_type='default'):
        tmp_metrics_dict = self.get_metric_dict(metric_type)   

        #print(f'called create_changes_dict for {metric_id} and {origin_key}')
        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(tmp_metrics_dict[metric_id]['metric_keys'])), ["date", "value", "metric_key"]].pivot(index='date', columns='metric_key', values='value')
        df_tmp.sort_values(by=['date'], inplace=True, ascending=False)

        changes_dict = {
                        'types': [],
                        '1d': [],
                        '7d': [],
                        '30d': [],
                        '90d': [],
                        '180d': [],
                        '365d': []
                    }

        for mk in tmp_metrics_dict[metric_id]['metric_keys']:
            #print(mk)
            cur_val = df_tmp[mk].iloc[0]
            changes = [1,7,30,90,180,365]
            for change in changes:
                if df_tmp[mk].shape[0] <= change:
                    change_val = None
                elif df_tmp[mk].iloc[change] == 0:
                    change_val = None
                else:
                    prev_val = df_tmp[mk].iloc[change]
                    if (prev_val < 0) or (cur_val < 0) or (prev_val == 0):
                        change_val = None
                    else:
                        change_val = (cur_val - prev_val) / prev_val
                        change_val = round(change_val, 4)
                        if change_val > 100:
                            change_val = 99.99
                changes_dict[f'{change}d'].append(change_val)

        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict)
        changes_dict['types'] = df_tmp.columns.to_list()

        return changes_dict
    
    def create_changes_dict_monthly(self, df, metric_id, origin_key, metric_type='default'):
        tmp_metrics_dict = self.get_metric_dict(metric_type)   

        mks = tmp_metrics_dict[metric_id]['metric_keys'].copy()
        if 'daa' in mks:
            mks[mks.index('daa')] = 'aa_last30d'

        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["date", "value", "metric_key"]].pivot(index='date', columns='metric_key', values='value')
        df_tmp.sort_values(by=['date'], inplace=True, ascending=False)

        changes_dict = {
                        'types': [],
                        '30d': [],
                        '90d': [],
                        '180d': [],
                        '365d': [],
                    }

        for mk in mks:
            #print(mk)
            if tmp_metrics_dict[metric_id]['monthly_agg'] == 'sum':
                cur_val = df_tmp[mk].iloc[0:29].sum()
            elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'avg':
                cur_val = df_tmp[mk].iloc[0:29].mean()
            elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'maa':
                cur_val = df_tmp[mk].iloc[0]
            else:
                raise NotImplementedError(f"monthly_agg {tmp_metrics_dict[metric_id]['monthly_agg']} not implemented")
            
            changes = [30,90,180,365]
            for change in changes:
                if df_tmp[mk].shape[0] <= change:
                    change_val = None
                else:
                    if tmp_metrics_dict[metric_id]['monthly_agg'] == 'sum':
                        prev_val = df_tmp[mk].iloc[change:change+29].sum()
                    elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'avg':
                        prev_val = df_tmp[mk].iloc[change:change+29].mean()
                    elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'maa':
                        prev_val = df_tmp[mk].iloc[change]
                    else:
                        raise NotImplementedError(f"monthly_agg {tmp_metrics_dict[metric_id]['monthly_agg']} not implemented")
                    
                    if (prev_val < 0) or (cur_val < 0) or (prev_val == 0):
                        change_val = None
                    else:
                        change_val = (cur_val - prev_val) / prev_val
                        change_val = round(change_val, 4)
                        if change_val >= 100:
                            change_val = 99.99
                changes_dict[f'{change}d'].append(change_val)

        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict)
        changes_dict['types'] = df_tmp.columns.to_list()

        return changes_dict
    
    ## this function takes a dataframe and a metric_id and origin_key as input and returns a value that aggregates the last 30 days
    def value_last_30d(self, df, metric_id, origin_key, metric_type='default'):
        tmp_metrics_dict = self.get_metric_dict(metric_type)

        mks = tmp_metrics_dict[metric_id]['metric_keys'].copy()
        if 'daa' in mks:
            mks[mks.index('daa')] = 'aa_last30d'

        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["date", "value", "metric_key"]].pivot(index='date', columns='metric_key', values='value')
        df_tmp.sort_values(by=['date'], inplace=True, ascending=False)

        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict, col_name_removal=True)

        if tmp_metrics_dict[metric_id]['monthly_agg'] == 'sum':
            val = df_tmp.iloc[0:29].sum()
        elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'avg':
            val = df_tmp.iloc[0:29].mean()
        elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'maa':
            val = df_tmp.iloc[0]

        val_dict = {
            'types': val.keys().to_list(),
            'data': val.to_list()
        }

        return val_dict

    def get_all_data(self):
        ## Load all data from database
        chain_user_list = self.chains_list_in_api + self.da_layers_list + ['multiple', 'celestia']
        metric_user_list = self.metrics_list + self.da_metrics_list + ['user_base_daily', 'user_base_weekly', 'user_base_monthly', 'waa', 'maa', 'aa_last30d', 'aa_last7d', 
                                                'cca_last7d_exclusive', 'blob_size_bytes'] ## add metrics that are not in the metrics_list

        chain_user_string = "'" + "','".join(chain_user_list) + "'"
        metrics_user_string = "'" + "','".join(metric_user_list) + "'"

        df = self.download_data(chain_user_string, metrics_user_string)

        ## divide value by 1000000 where metric_key is gas_per_second --> mgas/s
        df.loc[df['metric_key'] == 'gas_per_second', 'value'] = df['value'] / 1000000
        df.loc[df['metric_key'] == 'da_data_posted_bytes', 'value'] = df['value'] / 1024 / 1024 / 1024
        return df
    
    def get_data_fees(self):
        df = self.download_data_fees(self.fees_list)
        return df
    
    def get_data_eim(self):
        df = self.download_data_eim()

        ## create new df that sums up all values for each metric_key/date combination and assign "total" to origin_key
        df_total = df.groupby(['date', 'unix', 'metric_key']).sum().reset_index()
        df_total['origin_key'] = 'total'

        ## append df_total to df
        df = pd.concat([df, df_total], ignore_index=True)

        return df
    
    ##### LANDING PAGE METHODS #####
    ## This method calculates the total l2 users or the users for a specific chain
    def end_of_month(self, dt):
        todays_month = dt.month
        tomorrows_month = (dt + datetime.timedelta(days=1)).month
        return tomorrows_month != todays_month

    def chain_users(self, df, aggregation, origin_key, comparison=False):
        ### filter the dataframes accordingly
        ## and get the correct max date (latest date with user info for the chain) so that we can make sure that we don't have any half weeks/months
        if origin_key == 'all_l2s':
            df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key=='user_base_'+aggregation)]
        elif origin_key == 'ethereum' and aggregation == 'daily':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='daa')]
        elif origin_key == 'ethereum' and aggregation == 'weekly':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='waa')]
        elif origin_key == 'ethereum' and aggregation == 'monthly':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='maa')]
        else:
            df_tmp = df.loc[(df.origin_key == origin_key) & (df.metric_key=='user_base_'+aggregation)]

        df_tmp = df_tmp[['date', 'value']]
        df_tmp = df_tmp.groupby(pd.Grouper(key='date')).sum().reset_index()

        if comparison == False:
            ## only keep latest date
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        else:
            ## drop latest date and keep the one before
            df_tmp = df_tmp.loc[df_tmp.date != df_tmp.date.max()]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]

        users = df_tmp.value.sum()    
        return users

    def create_chain_users_comparison_value(self, df, aggregation, origin_key):
        val = (self.chain_users(df, aggregation, origin_key) - self.chain_users(df, aggregation, origin_key, comparison=True)) / self.chain_users(df, aggregation, origin_key, comparison=True)
        return round(val, 4)

    ## This method divdes the total l2 users by the total users of the Ethereum chain 
    def l2_user_share(self, df, aggregation, comparison=False):
        l2_user_share = self.chain_users(df, aggregation, 'all_l2s', comparison) / self.chain_users(df, aggregation, 'ethereum', comparison)
        return round(l2_user_share,4)
    
    def create_l2_user_share_comparison_value(self, df, aggregation):
        current = self.l2_user_share(df, aggregation)
        previous = self.l2_user_share(df, aggregation, comparison=True)
        val = (current - previous) / previous 
        return round(val,4)
    
    def cross_chain_users(self, df, comparison=False):
        ## filter df down to origin_key = 'multiple' and metric_key = 'user_base_weekly
        df_tmp = df.loc[(df.origin_key == 'multiple') & (df.metric_key == 'user_base_weekly')]
        df_tmp = df_tmp[['date', 'value']]

        if comparison == False:
            ## filter df to latest date
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        else:
            ## drop latest date and keep the one before
            df_tmp = df_tmp.loc[df_tmp.date != df_tmp.date.max()]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        return int(df_tmp['value'].values[0])
    
    def create_cross_chain_users_comparison_value(self, df):
        val = (self.cross_chain_users(df) - self.cross_chain_users(df, comparison=True)) / self.cross_chain_users(df, comparison=True)
        return round(val, 4)
    
    def get_aa_last7d(self, df, origin_key):
        if origin_key == 'all':
            df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key=='aa_last7d')]
            df_tmp = df_tmp[['date', 'value']]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
            df_tmp = df_tmp.groupby(pd.Grouper(key='date')).sum().reset_index()
        else:
            df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key=='aa_last7d')]
            df_tmp = df_tmp[['date', 'value']]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        return int(df_tmp['value'].values[0])
    
    def get_cross_chain_activity(self, df, chain):
        print(f'...calculating cross_chain_activity for {chain.origin_key}')
        if chain.runs_aggregate_addresses == False or chain.origin_key == 'starknet':
            print(f'...cross_chain_activity for {chain.origin_key} is not calculated because aggregate_addresses is set to False.')
            return 0
        else:
            origin_key = chain.origin_key
            ## calculate the exclusive users for the chain
            df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key=='cca_last7d_exclusive')]
            df_tmp = df_tmp[['date', 'value']]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
            exclusive_ussers = int(df_tmp['value'].values[0])

            ## calculate the cross_chain activity percentage
            total_users = self.get_aa_last7d(df, origin_key)
            cross_chain_activity = 1 - (exclusive_ussers / total_users)

            if cross_chain_activity < 0:
                cross_chain_activity = 0
            if cross_chain_activity > 1:
                cross_chain_activity = 1
            #print(f'cross_chain_activity for {origin_key} is {cross_chain_activity} based on {exclusive_ussers} exclusive users and {total_users} total users.')

            return round(cross_chain_activity, 4)

    def get_landing_table_dict(self, df):
        chains_dict = {}
        all_users = self.get_aa_last7d(df, 'all')
        for chain in self.main_config:
            ##if chain.api_in_main == True and chain.origin_key != 'ethereum':
            if chain.api_in_main == True:
                ranking_dict = {}
                ranking_dict_w_eth = {}
                for metric in self.metrics:
                    if self.metrics[metric]['ranking_landing']:
                        ranking_dict[metric] = self.get_ranking(df, metric, chain.origin_key, incl_value=True, filter_ethereum=True)
                        ranking_dict_w_eth[metric] = self.get_ranking(df, metric, chain.origin_key, incl_value=True, filter_ethereum=False)

                ## reorder dict by keys - ['stables_mcap','tvl','daa', 'txcount', 'throughput', 'fees','txcosts', 'profit', 'rent_paid',  'market_cap', 'fdv'] 
                ranking_dict = {k: ranking_dict[k] for k in ['stables_mcap','tvl','daa', 'txcount', 'throughput', 'fees','txcosts', 'profit', 'rent_paid',  'market_cap', 'fdv'] if k in ranking_dict}
                ranking_dict_w_eth = {k: ranking_dict_w_eth[k] for k in ['stables_mcap','tvl','daa', 'txcount', 'throughput', 'fees','txcosts', 'profit', 'rent_paid',  'market_cap', 'fdv'] if k in ranking_dict_w_eth}

                chains_dict[chain.origin_key] = {
                    "chain_name": chain.name,
                    "technology": chain.metadata_technology,
                    "purpose": chain.metadata_purpose,
                    "company": chain.company if chain.company else "",
                    "users": self.get_aa_last7d(df, chain.origin_key),
                    "user_share": round(self.get_aa_last7d(df, chain.origin_key)/all_users,4),
                    "cross_chain_activity": self.get_cross_chain_activity(df, chain),
                    "ranking": ranking_dict,
                    "ranking_w_eth": ranking_dict_w_eth
                }
        
        return chains_dict

    def get_filtered_df(self, df, aggregation, origin_key):
        chains_list_no_eth = self.chains_list.copy()
        chains_list_no_eth.remove('ethereum')
        chains_list_no_eth.append('multiple')
        
        if origin_key == 'all_l2s':
            df_tmp = df.loc[(df.origin_key.isin(chains_list_no_eth)) & (df.metric_key=='user_base_'+aggregation)]
            df_tmp = df_tmp[['unix', 'value']]
            df_tmp = df_tmp.groupby(pd.Grouper(key='unix')).sum().reset_index()
        elif origin_key == 'ethereum' and aggregation == 'daily':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='daa')]
            df_tmp = df_tmp[['unix', 'value']]
        elif origin_key == 'ethereum' and aggregation == 'weekly':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='waa')]
            df_tmp = df_tmp[['unix', 'value']]
        elif origin_key == 'ethereum' and aggregation == 'monthly':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='maa')]    
            df_tmp = df_tmp[['unix', 'value']]
        else:
            df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key=='user_base_'+aggregation)]	
            df_tmp = df_tmp[['unix', 'value']]
        
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)	
        return df_tmp

    def create_userbase_list_of_lists(self, df, aggregation, origin_key):
        df_tmp = self.get_filtered_df(df, aggregation, origin_key)
        mk_list = df_tmp.values.tolist() ## creates a list of lists
        mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        return mk_list_int

    def generate_userbase_dict(self, df, chain, aggregation):
        dict = {
                    "data": {
                        "types": [
                            "unix",
                            "value"
                        ],
                        "data": self.create_userbase_list_of_lists(df, aggregation, chain.origin_key)
                        }	
                }
        return dict

    def generate_chains_userbase_dict(self, df, aggregation):             
        chains_dict = {} 
        # filter df to date >= 2021-09-01
        df = df.loc[df.date >= '2021-09-01']

        for chain in self.multi_config:
            chains_dict[chain.origin_key] = self.generate_userbase_dict(df, chain, aggregation)
        return chains_dict
    
    def generate_engagement_by_composition_dict(self):
        ## download and prep data
        df_engagement = self.db_connector.execute_jinja('api/select_weekly_engagement_by_composition.sql.j2', load_into_df=True)
        df_engagement['unix'] = df_engagement['week'].apply(lambda x: x.timestamp() * 1000).astype(int)
        df_engagement['value'] = df_engagement['value'].astype(int)
        df_engagement.sort_values(by='unix', ascending=True, inplace=True)

        ## create dict for each composition with list of lists as values
        composition_dict = {}
        for composition in df_engagement['metric_key'].unique():
            df_composition = df_engagement[df_engagement['metric_key'] == composition]
            composition_dict[composition] = df_composition[['unix', 'value']].values.tolist()

        engagement_dict = {
            "types": [
                "unix",
                "value"
            ],
            "compositions": composition_dict

        }

        return engagement_dict
    
    ## This method generates a dict containing aggregate daily values for all_l2s (all chains except Ethereum) for a specific metric_id
    def generate_all_l2s_metric_dict(self, df, metric_id, rolling_avg=False, economics_api=False, days=730, incl_monthly=False):
        metric = self.metrics[metric_id]
        mks = metric['metric_keys']

        # filter df for all_l2s (all chains except Ethereum and chains that aren't included in the API)
        if economics_api == True:
            df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key.isin(mks)) & (df.origin_key.isin(self.chains_list_in_api_economics))]
        else:
            if self.api_version == 'dev':
                df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key.isin(mks)) & (df.origin_key.isin(self.chains_list_in_api))]
            else:
                df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key.isin(mks)) & (df.origin_key.isin(self.chains_list_in_api_prod))]

        ## TODO: standardize to move this date to chain_config
        ## filter out data where origin_key is 'celo' and date is before 2025-03-25
        df_tmp = df_tmp.loc[~((df_tmp.origin_key=='celo') & (df_tmp.date < '2025-03-25'))]

        # filter df _tmp by date so that date is greather than the days set
        df_tmp = df_tmp.loc[df_tmp.date >= (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')]  
        
        # group by unix and metric_key and sum up the values or calculate the mean (depending on the metric)
        if metric['all_l2s_aggregate'] == 'sum':
            df_tmp = df_tmp.groupby(['date', 'unix', 'metric_key']).sum().reset_index()
        elif metric['all_l2s_aggregate'] == 'weighted_mean':
            ## calculate weighted mean based on txcount of each chain
            # get txcount of each chain
            df_txcount = df.loc[(df.origin_key!='ethereum') & (df.metric_key=='txcount')]

            # merge txcount df with df_tmp
            df_tmp = df_tmp.merge(df_txcount[['date', 'origin_key', 'value']], on=['date', 'origin_key'], how='left', suffixes=('', '_txcount'))

            # calculate weighted mean by multiplying the value of each chain with the txcount of that chain and then summing up all values and dividing by the sum of all txcounts
            df_tmp['value'] = df_tmp['value'] * df_tmp['value_txcount']
            df_tmp = df_tmp.groupby(['date', 'unix', 'metric_key']).sum().reset_index()
            df_tmp['value'] = df_tmp['value'] / df_tmp['value_txcount']

            # drop value_txcount column
            df_tmp.drop(columns=['value_txcount'], inplace=True)

        if incl_monthly:
            df_tmp_copy = df_tmp.copy()

        df_tmp = df_tmp.loc[df_tmp.metric_key.isin(mks), ["unix", "value", "metric_key"]].pivot(index='unix', columns='metric_key', values='value').reset_index()
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)
        df_tmp.columns.name = None

        df_tmp = self.df_rename(df_tmp, metric_id, self.metrics, col_name_removal=True)

        mk_list = df_tmp.values.tolist()

        if len(self.metrics[metric_id]['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list]
        elif len(self.metrics[metric_id]['units']) == 2:
            mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list]
        else:
            raise NotImplementedError("Only 1 or 2 units are supported")
        
        if rolling_avg == True:
            mk_list_int = self.create_7d_rolling_avg(mk_list_int)
        
        chains_list_no_eth = self.chains_list.copy()
        chains_list_no_eth.remove('ethereum')

        dict = {
            "metric_name": self.metrics[metric_id]['name'],
            "source": self.db_connector.get_metric_sources(metric_id, chains_list_no_eth),
            "avg": "true",
            "daily": {
                "types": df_tmp.columns.to_list(),
                "data": mk_list_int
            }
        }

        if incl_monthly:
            ## drop unix from df_tmp_copy
            df_tmp_copy.drop(columns=['unix'], inplace=True)
            # based on df_tmp_copy, create df_tmp_monthly that groups by month and metric_key and sums up the value column
            df_tmp_monthly = df_tmp_copy.groupby([pd.Grouper(key='date', freq='MS'), 'metric_key']).sum().reset_index()
            df_tmp_monthly['unix'] = df_tmp_monthly['date'].apply(lambda x: x.timestamp() * 1000)

            df_tmp_monthly = df_tmp_monthly.loc[df_tmp_monthly.metric_key.isin(mks), ["unix", "value", "metric_key"]].pivot(index='unix', columns='metric_key', values='value').reset_index()
            df_tmp_monthly.sort_values(by=['unix'], inplace=True, ascending=True)
            df_tmp_monthly.columns.name = None

            df_tmp_monthly = self.df_rename(df_tmp_monthly, metric_id, self.metrics, col_name_removal=True)

            mk_list = df_tmp_monthly.values.tolist()

            if len(self.metrics[metric_id]['units']) == 1:
                mk_list_int = [[int(i[0]),i[1]] for i in mk_list]
            elif len(self.metrics[metric_id]['units']) == 2:
                mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list]
            else:
                raise NotImplementedError("Only 1 or 2 units are supported")
            
            dict['monthly'] = {
                "types": df_tmp_monthly.columns.to_list(),
                "data": mk_list_int
            }

        return dict
    
    def get_default_selection(self, df, top_n=5):
        df_tmp = df[df['metric_key'] == 'aa_last7d']

        ## filter out Ethereuem and only keep chains that are in the API and have a deployment of "PROD"
        df_tmp = df_tmp.loc[df_tmp.origin_key != 'ethereum']
        df_tmp = df_tmp.loc[df_tmp.origin_key.isin(self.chains_list_in_api_prod)]

        df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        df_tmp = df_tmp.sort_values(by='value', ascending=False)
        top_chains = df_tmp['origin_key'].head(top_n).tolist()
        print(f'Default selection by aa_last7d: {top_chains}')
        return top_chains
    
    def gen_l2beat_link(self, chain):
        if chain.aliases_l2beat:
            return f'https://l2beat.com/scaling/projects/{chain.aliases_l2beat_slug}'
        else:
            return 'https://l2beat.com'

    def gen_l2beat_stage(self, chain):
        stage = self.db_connector.get_stage(chain.origin_key)
        if stage == 'Stage 0':
            hex = "#FF8B36"
        elif stage == 'Stage 1':
            hex = "#FFEC44"
        elif stage == 'Stage 2':
            hex = "#34762F"
        else:
            hex = "#DFDFDF"      
        
        return {'stage': stage, 'hex': hex}
    
    ##### FILE HANDLERS #####
    def save_to_json(self, data, path):
        #create directory if not exists
        os.makedirs(os.path.dirname(f'output/{self.api_version}/{path}.json'), exist_ok=True)
        ## save to file
        with open(f'output/{self.api_version}/{path}.json', 'w') as fp:
            json.dump(data, fp, ignore_nan=True)

    ############################################################
    ##### Main Platfrom #####
    ############################################################
    def create_master_json(self, df_data):
        exec_string = "SELECT category_id, category_name, main_category_id, main_category_name FROM vw_oli_category_mapping"
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## create dict with main_category_key as key and main_category_name as value, same for sub_categories
        ##main_category_dict = {}
        sub_category_dict = {}
        for index, row in df.iterrows():
            ##main_category_dict[row['main_category_id']] = row['main_category_name']
            sub_category_dict[row['category_id']] = row['category_name']

        main_category_dict = {
            'defi': 'DeFi',
            'cefi': 'CeFi',            
            'nft': 'NFT',
            'social': 'Social',
            'utility': 'Utility',
            'token_transfers': 'Token Transfers',
            'cross_chain': 'Cross-Chain',
            'unlabeled': 'Unlabeled'
        }

        ## create main_category <> sub_category mapping dict
        df_mapping = df.groupby(['main_category_id']).agg({'category_id': lambda x: list(x)}).reset_index()
        mapping_dict = {}
        for index, row in df_mapping.iterrows():
            mapping_dict[row['main_category_id']] = row['category_id']

        ## create dict with all chain info
        chain_dict = {}
        for chain in self.multi_config:
            origin_key = chain.origin_key
            if chain.api_in_main == False:
                print(f'..skipped: Master json export for {origin_key}. API is set to False')
                continue

            url_key = chain.origin_key.replace('_', '-')
            if chain.origin_key == 'imx':
                url_key = 'immutable-x'
            if chain.origin_key == 'rhino':
                url_key = 'rhino-fi'

            chain_dict[origin_key] = {
                'name': chain.name,
                'url_key': url_key,
                'chain_type': chain.chain_type,
                'caip2': self.db_connector.get_chain_info(origin_key, 'caip2'),
                'evm_chain_id': chain.evm_chain_id,
                'deployment': chain.api_deployment_flag,
                'name_short': chain.name_short,
                'company': chain.company,
                'description': chain.metadata_description,
                'da_layer': chain.metadata_da_layer,
                'symbol': chain.metadata_symbol,
                'bucket': chain.bucket,
                'ecosystem': chain.ecosystem,
                'colors': chain.colors,
                'logo': chain.logo,
                'technology': chain.metadata_technology,
                'purpose': chain.metadata_purpose,
                'launch_date': chain.metadata_launch_date,
                'enable_contracts': chain.api_in_labels,
                'maturity': chain.maturity,
                'l2beat_stage': self.gen_l2beat_stage(chain),
                'l2beat_link': self.gen_l2beat_link(chain),
                'l2beat_id': chain.aliases_l2beat,
                'raas': chain.metadata_raas,
                'stack': chain.metadata_stack,
                'website': chain.socials_website,
                'twitter': chain.socials_twitter,
                'block_explorer': next(iter(chain.block_explorers.values())) if chain.block_explorers else None,
                'block_explorers': chain.block_explorers,
                'rhino_listed': bool(getattr(chain, 'aliases_rhino', None)),
                'rhino_naming': getattr(chain, 'aliases_rhino', None)
            }

        ## sort chain_dict by key (origin_key) alphabetically asc
        chain_dict = dict(sorted(chain_dict.items()))
        ## move ethereum to the top
        chain_dict = dict(sorted(chain_dict.items(), key=lambda x: x[0] != 'ethereum'))

        da_dict = {}
        for da in self.da_config:
            da_key = da.da_layer
            url_key = da.da_layer.replace('_', '-')

            da_dict[da_key] = {
                    'name': da.name,
                    'name_short': da.name_short,
                    'url_key': url_key,
                    'block_explorers': da.block_explorers,
                    'website': str(da.socials_website),
                    'twitter': str(da.socials_twitter),
                    'logo': da.logo,
                    'colors': da.colors,
                    'incl_in_da_overview': da.incl_in_da_overview
            }
        
        ## create dict for fees without metric_keys field
        fees_types_api = {key: {sub_key: value for sub_key, value in sub_dict.items() if sub_key != 'metric_keys'} 
                                  for key, sub_dict in self.fees_types.items()}

        master_dict = {
            'current_version' : self.api_version,
            'default_chain_selection' : self.get_default_selection(df_data, top_n=5),
            'default_chain_sorting' : ['ethereum'] + self.get_default_selection(df_data, top_n=999),
            'chains' : chain_dict,
            'custom_logos' : self.get_custom_logos(),
            'da_layers' : da_dict,
            'metrics' : self.metrics,
            'da_metrics' : self.da_metrics,
            'app_metrics' : self.app_metrics,
            'fee_metrics' : fees_types_api,
            'blockspace_categories' : {
                'main_categories' : main_category_dict,
                'sub_categories' : sub_category_dict,
                'mapping' : mapping_dict,
            },
            'maturity_levels': self.maturity_levels,
            'main_chart_config': self.main_chart_config,
            'ethereum_events': self.ethereum_events['upgrades'],
        }

        master_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        master_dict = fix_dict_nan(master_dict, 'master')

        if self.s3_bucket == None:
            self.save_to_json(master_dict, 'master')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/master', master_dict, self.cf_distribution_id)

        print(f'DONE -- master.json export')

    def create_landingpage_json(self, df):
        # filter df for all_l2s (all chains except chains that aren't included in the API)
        chain_keys = self.chains_list_in_api + ['multiple']
        df = df.loc[(df.origin_key.isin(chain_keys))]

        composition_ts_dict = self.generate_engagement_by_composition_dict()
        cur_l2 = composition_ts_dict['compositions']['multiple_l2s'][-1][1] + composition_ts_dict['compositions']['single_l2'][-1][1] + composition_ts_dict['compositions']['cross_layer'][-1][1]
        prev_l2 = composition_ts_dict['compositions']['multiple_l2s'][-2][1] + composition_ts_dict['compositions']['single_l2'][-2][1] + composition_ts_dict['compositions']['cross_layer'][-2][1]
        cur_eth = composition_ts_dict['compositions']['only_l1'][-1][1]
        prev_eth = composition_ts_dict['compositions']['only_l1'][-2][1]
        cur_multi_chain = composition_ts_dict['compositions']['multiple_l2s'][-1][1] + composition_ts_dict['compositions']['cross_layer'][-1][1]
        prev_multi_chain = composition_ts_dict['compositions']['multiple_l2s'][-2][1] + composition_ts_dict['compositions']['cross_layer'][-2][1]

        landing_dict = {
            "data": {
                "metrics" : {
                    "engagement" : {
                        "metric_name": "Ethereum ecosystem engagement",
                        "source": ['RPC'],
                        "weekly": {
                            "latest_total_l2": cur_l2,
                            "latest_total": cur_l2 + cur_eth,
                            "latest_total_comparison_l2": (cur_l2 - prev_l2) / prev_l2,
                            "latest_total_comparison": ((cur_l2 + cur_eth) - (prev_l2 + prev_eth)) / (prev_l2 + prev_eth),
                            "l2_dominance": self.l2_user_share(df, 'weekly'),
                            "l2_dominance_comparison": self.create_l2_user_share_comparison_value(df, 'weekly'),
                            "cross_chain_users": cur_multi_chain,
                            "cross_chain_users_comparison": (cur_multi_chain - prev_multi_chain) / prev_multi_chain,
                            "timechart": composition_ts_dict
                        }
                    },
                    "table_visual" : self.get_landing_table_dict(df)
                    },
                "all_l2s": {
                    "chain_id": "all_l2s",
                    "chain_name": "All L2s",
                    "symbol": "-",
                    "metrics": {}
                },
                "ethereum": {
                    "chain_id": "ethereum",
                    "chain_name": "Ethereum (L1)",
                    "symbol": "ETH",
                    "metrics": {}
                },
                "top_contracts": {},
                "top_applications": {},
            }
        }

        #start_date = datetime.now() - timedelta(days=731)
        #start_date = start_date.replace(tzinfo=timezone.utc) 
        ## start date should be 2021-06-01
        start_date = datetime(2021, 6, 1, tzinfo=timezone.utc)
        ## calculate the days between start_date and today
        days = (datetime.now(timezone.utc) - start_date).days

        for metric_id in ['throughput', 'txcount', 'stables_mcap', 'rent_paid', 'market_cap']:
            landing_dict['data']['all_l2s']['metrics'][metric_id] = self.generate_all_l2s_metric_dict(df, metric_id, rolling_avg=True, days=days)

            if metric_id != 'rent_paid':
                eth_values, eth_types = self.generate_daily_list(df, metric_id, 'ethereum', start_date=start_date, rolling_avg=True)
                landing_dict['data']['ethereum']['metrics'][metric_id] = {
                    "metric_name": self.metrics[metric_id]['name'],
                    "source": [],
                    "avg": "true",
                    "daily": {
                        "types": eth_types,
                        "data": eth_values
                    }
                }

         ## put all origin_keys from main_config in a list where in_api is True
        chain_keys = [chain.origin_key for chain in self.main_config if chain.api_in_main == True and 'blockspace' not in chain.api_exclude_metrics]

        ## fill top applications
        df_gainers, df_losers = self.get_apps_landing(self.chains_list_in_api_apps)
        landing_dict['data']['top_applications'] = {
            'gainers': {
                'types': df_gainers.columns.to_list(),
                'data': df_gainers.values.tolist()
            },
            'losers': {
                'types': df_losers.columns.to_list(),
                'data': df_losers.values.tolist()
            }
        }

        landing_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        landing_dict = fix_dict_nan(landing_dict, 'landing_page')

        if self.s3_bucket == None:
            self.save_to_json(landing_dict, 'landing_page')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/landing_page', landing_dict, self.cf_distribution_id)
        print(f'DONE -- landingpage export')

    def create_chain_details_jsons(self, df, origin_keys:list=None):
        if origin_keys != None:
            ## create main_conf_filtered that only contains chains that are in the origin_keys list
            main_config_filtered = [chain for chain in self.main_config if chain.origin_key in origin_keys]
        else:
            main_config_filtered = self.main_config

        ## filter df to date < today (because mcap has also today's date)
        df_tmp = df.loc[df.date < datetime.now(timezone.utc).strftime('%Y-%m-%d')].copy()

        ## loop over all chains and generate a chain details json for all chains and with all possible metrics
        for chain in main_config_filtered:
            origin_key = chain.origin_key
            if chain.api_in_main == False:
                print(f'..skipped: Chain details export for {origin_key}. API is set to False')
                continue

            metrics_dict = {}
            ranking_dict = {}
            for metric in self.metrics:
                if self.metrics[metric]['fundamental'] == False:
                    continue

                if metric in chain.api_exclude_metrics:
                    print(f'..skipped: Chain details export for {origin_key} - {metric}. Metric is excluded')
                    continue

                print(f'..processing: Chain details for {origin_key} - {metric}')
                
                mk_list = self.generate_daily_list(df_tmp, metric, origin_key)
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                metrics_dict[metric] = {
                    'metric_name': self.metrics[metric]['name'],
                    'source': self.db_connector.get_metric_sources(metric, [origin_key]),
                    'avg': self.metrics[metric]['avg'],
                    'daily': {
                        'types' : mk_list_columns,
                        'data' : mk_list_int
                    }
                }

                if self.metrics[metric]['ranking_bubble']:
                    ranking_dict[metric] = self.get_ranking(df_tmp, metric, origin_key, incl_value=False, filter_ethereum=False)
            
            ## Hottest Contract
            if chain.runs_aggregate_blockspace and 'blockspace' not in chain.api_exclude_metrics:
                hottest_contract = self.db_connector.get_top_contracts_for_all_chains_with_change(top_by='gas', days=1, origin_keys=[origin_key], limit=1)
                hottest_contract = hottest_contract.replace({np.nan: None})
                hottest_contract = db_addresses_to_checksummed_addresses(hottest_contract, ['address'])
                
                hottest_contract = hottest_contract[
                    ['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key",
                     "gas_fees_eth", "gas_fees_usd", "gas_fees_eth_change", "gas_fees_usd_change", "prev_gas_fees_eth",
                     "prev_gas_fees_usd", "gas_fees_eth_change_percent", "gas_fees_usd_change_percent"]
                ]

                ## change column names contract_name to name and origin_key to chain
                hottest_contract = hottest_contract.rename(columns={'contract_name': 'name', 'origin_key': 'chain'})

                hottest_contract_dict = {
                    'types': hottest_contract.columns.to_list(),
                    'data': hottest_contract.values.tolist()
                }
            else:
                print(f'..skipped: Hottest Contract for {origin_key}. Flag aggregate_blockspace is set to False or blockspace is excluded.')
                hottest_contract_dict = None

            details_dict = {
                'data': {
                    'chain_id': origin_key,
                    'chain_name': chain.name,
                    'ranking': ranking_dict,
                    'hottest_contract': hottest_contract_dict,
                    'metrics': metrics_dict
                }
            }

            details_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            details_dict = fix_dict_nan(details_dict, f'chains/{origin_key}')

            if self.s3_bucket == None:
                self.save_to_json(details_dict, f'chains/{origin_key}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/chains/{origin_key}', details_dict, self.cf_distribution_id, invalidate=False)
            
            print(f'DONE -- Chain details export for {origin_key}')

        ## after all chain details jsons are created, invalidate the cache
        empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/chains/*')

    def create_metric_details_jsons(self, df, metric_keys:list=None):
        if metric_keys != None:
            ## create metrics_filtered that only contains metrics that are in the metric_keys list
            metrics_filtered = {key: value for key, value in self.metrics.items() if key in metric_keys}
        else:
            metrics_filtered = self.metrics

        ## loop over all metrics and generate a metric details json for all metrics and with all possible chainn
        for metric in metrics_filtered:
            if self.metrics[metric]['fundamental'] == False:
                continue
            
            chains_dict = {}    
            for chain in self.main_config:
                origin_key = chain.origin_key
                if chain.api_in_main == False:
                    print(f'..skipped: Metric details export for {origin_key}. API is set to False')
                    continue

                if metric in chain.api_exclude_metrics:
                    print(f'..skipped: Metric details export for {origin_key} - {metric}. Metric is excluded')
                    continue

                print(f'..processing: Metric details for {origin_key} - {metric}')

                mk_list = self.generate_daily_list(df, metric, origin_key)
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                mk_list_monthly = self.generate_monthly_list(df, metric, origin_key)
                mk_list_int_monthly = mk_list_monthly[0]
                mk_list_columns_monthly = mk_list_monthly[1]

                chains_dict[origin_key] = {
                    'chain_name': chain.name,
                    'changes': self.create_changes_dict(df, metric, origin_key),
                    'changes_monthly': self.create_changes_dict_monthly(df, metric, origin_key),
                    'daily': {
                        'types' : mk_list_columns,
                        'data' : mk_list_int
                    },
                    'last_30d': self.value_last_30d(df, metric, origin_key),
                    'monthly': {
                        'types' : mk_list_columns_monthly,
                        'data' : mk_list_int_monthly
                    }
                }

                ## check if metric should be averagd and add 7d rolling avg field
                if self.metrics[metric]['avg'] == True:
                    mk_list_int_7d = self.create_7d_rolling_avg(mk_list_int)
                    chains_dict[origin_key]['daily_7d_rolling'] = {
                        'types' : mk_list_columns,
                        'data' : mk_list_int_7d
                    }

            details_dict = {
                'data': {
                    'metric_id': metric,
                    'metric_name': self.metrics[metric]['name'],
                    'source': self.db_connector.get_metric_sources(metric, []),
                    'avg': self.metrics[metric]['avg'],
                    'monthly_agg': 'distinct' if self.metrics[metric]['monthly_agg'] in ['maa'] else self.metrics[metric]['monthly_agg'],
                    'chains': chains_dict
                }
            }

            details_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            details_dict = fix_dict_nan(details_dict, f'metrics/{metric}')

            if self.s3_bucket == None:
                self.save_to_json(details_dict, f'metrics/{metric}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/metrics/{metric}', details_dict, self.cf_distribution_id, invalidate=False)
            print(f'DONE -- Metric details export for {metric}')

        ## after all metric jsons are created, invalidate the cache
        empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/metrics/*')

    def create_da_metric_details_jsons(self, df, metric_keys:list=None):
        if metric_keys != None:
            ## create metrics_filtered that only contains metrics that are in the metric_keys list
            metrics_filtered = {key: value for key, value in self.da_metrics.items() if key in metric_keys}
        else:
            metrics_filtered = self.da_metrics

        ## loop over all metrics and generate a metric details json for all metrics and with all possible chainn
        for metric in metrics_filtered:
            if self.da_metrics[metric]['fundamental'] == False:
                continue
            
            da_dict = {}    
            for da in self.da_config:
                origin_key = da.da_layer
                # if chain.api_in_main == False:
                #     print(f'..skipped: Metric details export for {origin_key}. API is set to False')
                #     continue

                # if metric in chain.api_exclude_metrics:
                #     print(f'..skipped: Metric details export for {origin_key} - {metric}. Metric is excluded')
                #     continue

                if metric in ['blob_count', 'blob_producers', 'fees_per_mbyte', 'fees_paid'] and origin_key == 'da_ethereum_calldata':
                    print(f'..skipped: Metric details export for {origin_key} - {metric}. Metric is excluded')
                    continue

                mk_list = self.generate_daily_list(df, metric, origin_key, metric_type='da')
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                mk_list_monthly = self.generate_monthly_list(df, metric, origin_key, metric_type='da')
                mk_list_int_monthly = mk_list_monthly[0]
                mk_list_columns_monthly = mk_list_monthly[1]

                da_dict[origin_key] = {
                    'changes': self.create_changes_dict(df, metric, origin_key, metric_type='da'),
                    'changes_monthly': self.create_changes_dict_monthly(df, metric, origin_key, metric_type='da'),
                    'daily': {
                        'types' : mk_list_columns,
                        'data' : mk_list_int
                    },
                    'last_30d': self.value_last_30d(df, metric, origin_key, metric_type='da'),
                    'monthly': {
                        'types' : mk_list_columns_monthly,
                        'data' : mk_list_int_monthly
                    }
                }

                ## check if metric should be averagd and add 7d rolling avg field
                if self.da_metrics[metric]['avg'] == True:
                    mk_list_int_7d = self.create_7d_rolling_avg(mk_list_int)
                    da_dict[origin_key]['daily_7d_rolling'] = {
                        'types' : mk_list_columns,
                        'data' : mk_list_int_7d
                    }

            details_dict = {
                'data': {
                    'metric_id': metric,
                    'metric_name': self.da_metrics[metric]['name'],
                    'source': self.db_connector.get_metric_sources(metric, []),
                    'avg': self.da_metrics[metric]['avg'],
                    'monthly_agg': 'distinct' if self.da_metrics[metric]['monthly_agg'] in ['maa'] else self.da_metrics[metric]['monthly_agg'],
                    'chains': da_dict
                }
            }

            details_dict = fix_dict_nan(details_dict, f'da_metrics/{metric}')

            if self.s3_bucket == None:
                self.save_to_json(details_dict, f'da_metrics/{metric}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/da_metrics/{metric}', details_dict, self.cf_distribution_id)
            print(f'DONE -- DA Metric details export for {metric}')

    #######################################################################
    ### FEES PAGE
    #######################################################################

    def create_fees_table_json(self, df):
        fees_dict = {
            "chain_data" : {}
        } 

        ## filter df timestamp to be within the last 48 hours (not more needed for table visual)
        df = df[(df.granularity == 'hourly')]
        df = df[df['timestamp'].dt.tz_localize(None) > datetime.now() - timedelta(hours=30)]
        max_ts_all = df['unix'].max()

        ## loop over all chains and generate a fees json for all chains
        for chain in self.main_config:
            origin_key = chain.origin_key
            if chain.api_in_fees == False:
                print(f'..skipped: Fees export for {origin_key}. API is set to False')
                continue
            
            eth_price = self.db_connector.get_last_price_usd('ethereum')

            hourly_dict = {}
            for metric in self.fees_types:             
                generated = self.generate_fees_list(df, metric, origin_key, 'hourly', '7d', eth_price, max_ts_all, True)
                hourly_dict[metric] = {
                    "types": generated[1],
                    "data": generated[0]
                }
            
            fees_dict["chain_data"][origin_key] = {
                'hourly': hourly_dict,
                # 'ten_min': min_10_dict
            }

        fees_dict = fix_dict_nan(fees_dict, f'fees/table', False)

        if self.s3_bucket == None:
            self.save_to_json(fees_dict, f'fees/table')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fees/table', fees_dict, self.cf_distribution_id)
        print(f'DONE -- fees/table.json export')

    def create_fees_linechart_json(self, df):
        fees_dict = {
            "chain_data" : {}
        } 

        ## loop over all chains and generate a fees json for all chains
        for chain in self.main_config:
            origin_key = chain.origin_key
            if chain.api_in_fees == False:
                print(f'..skipped: Fees export for {origin_key}. API is set to False')
                continue
            # if origin_key == 'ethereum':
            #     print(f'..skipped: Fees export for {origin_key}. Ethereum is not included in the line chart')
            #     continue  
            
            eth_price = self.db_connector.get_last_price_usd('ethereum')                
            fees_dict["chain_data"][origin_key] = {}

            for metric in self.fees_types:
                timespan_dict = {}
                for timespan in self.fees_timespans:
                    granularity = self.fees_timespans[timespan]["granularity"]
                    filter_days = self.fees_timespans[timespan]["filter_days"]
                    df_tmp = df[df['timestamp'].dt.tz_localize(None) > datetime.now() - timedelta(hours=24*filter_days)].copy()
                    generated = self.generate_fees_list(df_tmp, metric, origin_key, granularity, timespan, eth_price)
                    timespan_dict[timespan] = {
                        "types": generated[1],
                        "granularity": granularity,
                        "data": generated[0]
                    }
                fees_dict["chain_data"][origin_key][metric] = timespan_dict

        fees_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        fees_dict = fix_dict_nan(fees_dict, f'fees/linechart')

        if self.s3_bucket == None:
            self.save_to_json(fees_dict, f'fees/linechart')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fees/linechart_test', fees_dict, self.cf_distribution_id, invalidate=False)
            fees_dict['chain_data'].pop('ethereum', None)
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fees/linechart', fees_dict, self.cf_distribution_id, invalidate=False)
            empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/fees/linechart*')
        
        print(f'DONE -- fees/linechart.json export')

    #######################################################################
    ### ECONOMICS SECTION
    #######################################################################

    def aggregate_metric(self, df, origin_key, metric_key, days):
        start_date = datetime.now() - timedelta(days=days+1)
        start_date = start_date.replace(tzinfo=timezone.utc) 

        df = df[(df['origin_key'] == origin_key) & (df['metric_key'] == metric_key)]
        df = df[(df['date'] >= start_date)]
        val = df['value'].sum()
        return val

    def gen_da_metric_dict(self, df, metric, origin_key):
        ##start_date is today 180 days ago
        start_date = datetime.now() - timedelta(days=130)
        start_date = start_date.replace(tzinfo=timezone.utc)  

        mk_list = self.generate_daily_list(df, metric, origin_key, start_date)
        mk_list_int = mk_list[0]
        mk_list_columns = mk_list[1]

        metric_dict = {}
        metric_dict['metric_name'] = self.metrics[metric]['name']
        metric_dict['daily'] = {
            'types' : mk_list_columns,
            'data' : mk_list_int
        }

        return metric_dict

    def create_economics_json(self, df):
        economics_dict = {
            "data": {
                "all_l2s" : {
                    "chain_id": "all_l2s",
                    "chain_name": "All L2s",
                    "metrics": {}
                },
                "chain_breakdown": {}
            }
        }

        economics_dict['data']['all_l2s']['metrics']['fees'] = self.generate_all_l2s_metric_dict(df, 'fees', rolling_avg=False, economics_api=True, days=720, incl_monthly=True)
        economics_dict['data']['all_l2s']['metrics']['costs'] = {
            'costs_l1': self.generate_all_l2s_metric_dict(df, 'costs_l1', rolling_avg=False, economics_api=True, days=720, incl_monthly=True),
            'costs_blobs': self.generate_all_l2s_metric_dict(df, 'costs_blobs', rolling_avg=False, economics_api=True, days=720, incl_monthly=True),
        }
        economics_dict['data']['all_l2s']['metrics']['profit'] = self.generate_all_l2s_metric_dict(df, 'profit', rolling_avg=False, economics_api=True, days=720, incl_monthly=True)

        # filter df for all_l2s (all chains except chains that aren't included in the API)
        # chain_keys = [chain.origin_key for chain in self.main_config if chain.api_in_economics == True and chain.api_deployment_flag == 'PROD']
        chain_keys = [chain.origin_key for chain in self.main_config if chain.api_in_economics == True]
        df = df.loc[(df.origin_key.isin(chain_keys))]
        timeframes = [1,7,30,90,180,365,'max']

        # iterate over each chain and generate table and chart data
        for origin_key in chain_keys:
            economics_dict['data']['chain_breakdown'][origin_key] = {}
            # get data for each timeframe (for table aggregates)
            for timeframe in timeframes:
                timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'    
                days = timeframe if timeframe != 'max' else 2000

                revenue_eth = self.aggregate_metric(df, origin_key, 'fees_paid_eth', days)
                profit_eth = self.aggregate_metric(df, origin_key, 'profit_eth', days)
                profit_margin = profit_eth / revenue_eth if revenue_eth != 0 else 0.0

                if profit_margin > 1:
                    profit_margin = 1

                economics_dict['data']['chain_breakdown'][origin_key][timeframe_key] = {
                    "revenue": {
                        "types": ["usd", "eth"],
                        "total": [self.aggregate_metric(df, origin_key, 'fees_paid_usd', days), self.aggregate_metric(df, origin_key, 'fees_paid_eth', days)]
                    },		
                    "costs": {
                        "types": ["usd", "eth"],
                        "total": [self.aggregate_metric(df, origin_key, 'costs_total_usd', days), self.aggregate_metric(df, origin_key, 'costs_total_eth', days)],
                        "costs_l1": [self.aggregate_metric(df, origin_key, 'costs_l1_usd', days), self.aggregate_metric(df, origin_key, 'costs_l1_eth', days)], 
                        "costs_blobs": [self.aggregate_metric(df, origin_key, 'costs_blobs_usd', days), self.aggregate_metric(df, origin_key, 'costs_blobs_eth', days)],
                    },
                    "profit": {
                        "types": ["usd", "eth"],
                        "total": [self.aggregate_metric(df, origin_key, 'profit_usd', days), self.aggregate_metric(df, origin_key, 'profit_eth', days)]
                    },	
                    "profit_margin": {
                        "types": ["percentage"],
                        "total": [profit_margin]
                    },
                    "size": {
                        "types": ["bytes"],
                        "total": [self.aggregate_metric(df, origin_key, 'blob_size_bytes', days)]
                    }
                }

            econ_metrics = ['fees', 'costs', 'profit']
            ## determine the min date for all metric_keys of econ_metrics in df
            min_date_fees = df.loc[(df.origin_key == origin_key) & (df.metric_key.isin(self.metrics['fees']['metric_keys']))]['date'].min()
            min_date_costs = df.loc[(df.origin_key == origin_key) & (df.metric_key.isin(self.metrics['costs']['metric_keys']))]['date'].min()
            min_date_profit = df.loc[(df.origin_key == origin_key) & (df.metric_key.isin(self.metrics['profit']['metric_keys']))]['date'].min()
            start_date = max(min_date_fees, min_date_costs, min_date_profit)

            metric_to_key = {
                    'fees': 'revenue',
                    'costs': 'costs',
                    'profit': 'profit'
                }
            
            ## add timeseries data for each chain
            economics_dict['data']['chain_breakdown'][origin_key]['daily'] = {}
            economics_dict['data']['chain_breakdown'][origin_key]['monthly'] = {}

            for metric in econ_metrics:
                key = metric_to_key.get(metric)

                ## Fill daily values
                mk_list = self.generate_daily_list(df, metric, origin_key, start_date)
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]                

                economics_dict['data']['chain_breakdown'][origin_key]['daily'][key] = {
                    'types' : mk_list_columns,
                    'data' : mk_list_int
                }

                ## Fill monthly values
                mk_list_monthly = self.generate_monthly_list(df, metric, origin_key, start_date)
                mk_list_int_monthly = mk_list_monthly[0]
                mk_list_columns_monthly = mk_list_monthly[1]

                economics_dict['data']['chain_breakdown'][origin_key]['monthly'][key] = {
                    'types' : mk_list_columns_monthly,
                    'data' : mk_list_int_monthly
                }
        
        ## Calculate and add the total revenue, costs, profit, profit margin, and blob size for all chains combined for each timeframe
        economics_dict['data']['chain_breakdown']['totals'] = {}
        for timeframe in timeframes:
            timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max' 

            # Initialize variables to accumulate total costs, revenue, and blob size for all chains over the specified timeframe
            total_costs_usd = 0
            total_costs_eth = 0
            total_revenue_usd = 0
            total_revenue_eth = 0
            total_blob_size = 0
            for key in economics_dict['data']['chain_breakdown']:
                if key != 'totals':
                    total_costs_usd += economics_dict['data']['chain_breakdown'][key][timeframe_key]['costs']['total'][0]
                    total_costs_eth += economics_dict['data']['chain_breakdown'][key][timeframe_key]['costs']['total'][1]
                    total_revenue_usd += economics_dict['data']['chain_breakdown'][key][timeframe_key]['revenue']['total'][0]
                    total_revenue_eth += economics_dict['data']['chain_breakdown'][key][timeframe_key]['revenue']['total'][1]
                    total_blob_size += economics_dict['data']['chain_breakdown'][key][timeframe_key]['size']['total'][0]

            total_profit_usd = total_revenue_usd - total_costs_usd
            total_profit_eth = total_revenue_eth - total_costs_eth
            total_profit_margin = total_profit_eth / total_revenue_eth if total_revenue_eth != 0 else 0.0
            if total_profit_margin > 1:
                total_profit_margin = 1                

            economics_dict['data']['chain_breakdown']['totals'][timeframe_key] = {
                    "revenue": {
                        "types": ["usd", "eth"],
                        "total": [total_revenue_usd, total_revenue_eth]
                    },		
                    "costs": {
                        "types": ["usd", "eth"],
                        "total": [total_costs_usd, total_costs_eth],
                    },
                    "profit": {
                        "types": ["usd", "eth"],
                        "total": [total_profit_usd, total_profit_eth]
                    },	
                    "profit_margin": {
                        "types": ["percentage"],
                        "total": [total_profit_margin]
                    },
                    "size": {
                        "types": ["bytes"],
                        "total": [total_blob_size]
                    }
                }

        economics_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        economics_dict = fix_dict_nan(economics_dict, 'economics')

        if self.s3_bucket == None:
            self.save_to_json(economics_dict, 'economics')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/economics', economics_dict, self.cf_distribution_id)
        print(f'DONE -- economics export')

    #######################################################################
    ### DA OVERVIEW
    #######################################################################

    ## This function is used to generate the top da consumers for blobs (top chart DA overview page)
    def get_top_da_consumers(self, days, da_layer, limit=5):
        if days == 'max':
            days = 2000

        query_parameters = {
            "days": days,
            "da_layer": da_layer,
            "limit": limit
        }
        df = execute_jinja_query(self.db_connector, "api/select_top_da_consumers.sql.j2", query_parameters, return_df=True)
        return df.values.tolist()

    ## This function is used to generate the da consumers for blobs (DA specific pie chart)
    def get_da_consumers_incl_others(self, days, da_layer, limit=8):
        if days == 'max':
            days = 2000

        query_parameters = {
            "days": days,
            "da_layer": da_layer,
            "limit": limit
        }
        df = execute_jinja_query(self.db_connector, "api/select_top_da_consumers_incl_others.sql.j2", query_parameters, return_df=True)
        return df.values.tolist()


    def create_da_overview_json(self, df):
        ## initialize da_dict with all_da and chain_breakdown
        da_dict = {
            "data": {
                "all_da" : {
                    "da_id": "all_da",
                    "da_name": "All DAs",
                    "metrics": {
                        "fees_paid": {},
                        "data_posted": {},
                    }
                },
                "da_breakdown": {}
            }
        }

        ## TOP AREA CHARTS
        # fill all_da with daily data 
        metrics_list = ['fees_paid', 'data_posted']
        ## data_posted, fees_paid
        for da in self.da_config:
            if da.incl_in_da_overview:
                da_layer = da.da_layer

                for metric in metrics_list:
                    mk_list = self.generate_daily_list(df, metric, da_layer, metric_type='da')
                    mk_list_int = mk_list[0]
                    mk_list_columns = mk_list[1]

                    monthly_mk_list = self.generate_monthly_list(df, metric, da_layer, metric_type='da')
                    monthly_mk_list_int = monthly_mk_list[0]
                    monthly_mk_list_columns = monthly_mk_list[1]

                    da_dict['data']['all_da']['metrics'][metric][da_layer] = {
                        'metric_name': da.name,
                        'source': [],
                        'avg': self.da_metrics[metric]['avg'],
                        'daily': {
                            'types' : mk_list_columns,
                            'data' : mk_list_int
                        },
                        'monthly': {
                            'types' : monthly_mk_list_columns,
                            'data' : monthly_mk_list_int
                        }
                    }
        
        ## TOP DA CONSUMERS BAR CHART
        timeframes = [1,7,30,90,180,365,'max']
        da_dict['data']['all_da']['top_da_consumers'] = {}
        for timeframe in timeframes:
            timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'  
            da_dict['data']['all_da']['top_da_consumers'][timeframe_key] = {
                'types': ['da_consumer_key', 'name', 'da_layer', 'origin_key', 'data_posted'],
                'data': self.get_top_da_consumers(timeframe, da_layer='all')
            }

        df = df.loc[(df.origin_key.isin(self.da_layer_overview))]

        ## TABLE and PIE CHARTS for each DA Layer
        # iterate over each da_layer and generate table and chart data
        for da in self.da_config:
            if da.incl_in_da_overview:
                origin_key = da.da_layer
                ## calc number of DA consumers and generate list
                query_parameters = {'da_layer': origin_key}

                ## TABLE: DA CONSUMERS CELL
                df_da_consumers = execute_jinja_query(self.db_connector, "api/select_top_da_consumers_list.sql.j2", query_parameters, return_df=True)
                da_consumers_count = df_da_consumers.shape[0]
                da_consumers_dict = {
                                        "types": df_da_consumers[['da_consumer_key', 'name', 'gtp_origin_key']].columns.tolist(),
                                        "values": df_da_consumers[['da_consumer_key', 'name', 'gtp_origin_key']].values.tolist()
                                    }

                ## TABLE: OTHER METRICS CELLS + PIE CHART (see end)
                da_dict['data']['da_breakdown'][origin_key] = {}
                for timeframe in timeframes:
                    timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'    
                    days = timeframe if timeframe != 'max' else 2000

                    fees_eth = self.aggregate_metric(df, origin_key, 'da_fees_eth', days)
                    fees_usd = self.aggregate_metric(df, origin_key, 'da_fees_usd', days)
                    data_posted = self.aggregate_metric(df, origin_key, 'da_data_posted_bytes', days) * 1024 * 1024 * 1024 # convert from GB to bytes
                    fees_per_mb_eth = fees_eth / data_posted * 1024 * 1024 if data_posted != 0 else 0.0
                    fees_per_mb_usd = fees_usd / data_posted * 1024 * 1024 if data_posted != 0 else 0.0

                    da_dict['data']['da_breakdown'][origin_key][timeframe_key] = {
                        "fees": {
                            "types": ["usd", "eth"],
                            "total": [fees_usd, fees_eth]
                        },		
                        "size": {
                            "types": ["bytes"],
                            "total": [data_posted]
                        },
                        "fees_per_mb": {
                            "types": ["usd", "eth"],
                            "total": [fees_per_mb_usd, fees_per_mb_eth]
                        },	
                        "da_consumers": {
                            "count": da_consumers_count,
                            "chains": da_consumers_dict
                        },
                        "fixed_params": da.parameters,

                        ## PIE CHART
                        "da_consumer_chart": {
                            'types': ['da_consumer_key', 'name', 'da_layer', 'origin_key', 'data_posted'],
                            'data': self.get_da_consumers_incl_others(timeframe, da_layer=origin_key, limit=7)
                        }
                    }

        ## TOTALS ROW
        ## Calculate and add the total data posted, fees paid, fees/mb for all da layers combined for each timeframe
        da_dict['data']['da_breakdown']['totals'] = {}
        for timeframe in timeframes:
            timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max' 

            # Initialize variables to accumulate total costs, revenue, and blob size for all chains over the specified timeframe
            total_fees_eth = 0
            total_fees_usd = 0
            total_data_posted = 0

            for key in da_dict['data']['da_breakdown']:
                if key != 'totals':
                    total_fees_usd += da_dict['data']['da_breakdown'][key][timeframe_key]['fees']['total'][0]
                    total_fees_eth += da_dict['data']['da_breakdown'][key][timeframe_key]['fees']['total'][1]
                    total_data_posted += da_dict['data']['da_breakdown'][key][timeframe_key]['size']['total'][0]

            total_data_per_mb_usd = total_fees_usd / total_data_posted * 1024 * 1024 if total_data_posted != 0 else 0.0
            total_data_per_mb_eth = total_fees_eth / total_data_posted * 1024 * 1024 if total_data_posted != 0 else 0.0           

            da_dict['data']['da_breakdown']['totals'][timeframe_key] = {
                "fees": {
                    "types": ["usd", "eth"],
                    "total": [total_fees_usd, total_fees_eth]
                },		
                "size": {
                    "types": ["bytes"],
                    "total": [total_data_posted]
                },
                "fees_per_mb": {
                    "types": ["usd", "eth"],
                    "total": [total_data_per_mb_usd, total_data_per_mb_eth],
                }
            }

        da_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        da_dict = fix_dict_nan(da_dict, 'da_overview')

        if self.s3_bucket == None:
            self.save_to_json(da_dict, 'da_overview')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/da_overview', da_dict, self.cf_distribution_id)
        print(f'DONE -- DA overview export')

    def create_da_timeseries_json(self):
        da_dict = {
            "data": {
                "da_layers" : {}
            }
        }

        timeframes = [1,7,30,90,180,365,'max']

        for da in self.da_layer_overview:
            da_dict["data"]["da_layers"][da] = {}

            for timeframe in timeframes:
                timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'    
                days = timeframe if timeframe != 'max' else 2000
                
                da_dict["data"]["da_layers"][da][timeframe_key] = {
                        "da_consumers": {},
                    }


                query_parameters = {
                    "days": days,
                    "da_layer": da
                }
                
                df = execute_jinja_query(self.db_connector, "api/select_da_consumers_incl_others_over_time.sql.j2", query_parameters, return_df=True)
                df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
                df.sort_values(by=['date'], inplace=True, ascending=True)

                #fill df gtp_origin_key with 'NA' if it is null
                df['gtp_origin_key'] = df['gtp_origin_key'].fillna('NA')

                df_monthly = df.groupby([pd.Grouper(key='date', freq='MS', ), 'da_consumer_key', 'name', 'gtp_origin_key']).sum().reset_index()

                df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
                df_monthly['unix'] = df_monthly['date'].apply(lambda x: x.timestamp() * 1000)

                df = df.drop(columns=['date'])
                df['gtp_origin_key'] = df['gtp_origin_key'].replace('NA', None)
                df_monthly = df_monthly.drop(columns=['date'])
                df_monthly['gtp_origin_key'] = df_monthly['gtp_origin_key'].replace('NA', None)


                ## for unique da_consumer_keys, create a list of all da_consumer_keys
                da_consumer_keys = df['da_consumer_key'].unique().tolist()
                for da_consumer_key in da_consumer_keys:
                    df_tmp = df[df['da_consumer_key'] == da_consumer_key]
                    df_tmp_monthly = df_monthly[df_monthly['da_consumer_key'] == da_consumer_key]

                    da_dict["data"]["da_layers"][da][timeframe_key]["da_consumers"][da_consumer_key] = {
                        "daily": {
                            "types": df_tmp.columns.tolist(),
                            "values": df_tmp.values.tolist()
                        },
                        "monthly": {
                            "types": df_tmp_monthly.columns.tolist(),
                            "values": df_tmp_monthly.values.tolist()
                        }
                    }         

        da_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        da_dict = fix_dict_nan(da_dict, 'da_timeseries')

        if self.s3_bucket == None:
            self.save_to_json(da_dict, 'da_timeseries')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/da_timeseries', da_dict, self.cf_distribution_id)
        print(f'DONE -- DA timeseries export')


    #######################################################################
    ### APPS
    #######################################################################
    def get_app_overview_data(self, chains:list, timeframe:int):
        chains_str = ', '.join([f"'{chain}'" for chain in chains])
        exec_string = f"""
            with apps_mat as (
            SELECT 
                fact.owner_project, 
                fact.origin_key,
                COUNT(DISTINCT fact.address) AS num_contracts,
                COALESCE(SUM(fees_paid_eth) FILTER (WHERE fact."date" > current_date - interval '{timeframe+1}  days'), 0) AS gas_fees_eth, 
                COALESCE(SUM(fees_paid_eth) FILTER (WHERE fact."date" < current_date - interval '{timeframe} days'), 0) AS prev_gas_fees_eth, 
                COALESCE(SUM(fees_paid_usd) FILTER (WHERE fact."date" > current_date - interval '{timeframe+1}  days'), 0) AS gas_fees_usd, 
                COALESCE(SUM(txcount) FILTER (WHERE fact."date" > current_date - interval '{timeframe+1}  days'), 0) AS txcount, 
                COALESCE(SUM(txcount) FILTER (WHERE fact."date" < current_date - interval '{timeframe} days'), 0) AS prev_txcount
            FROM vw_apps_contract_level_materialized fact
            WHERE fact."date" >= current_date - interval '{timeframe*2} days'
                AND fact.origin_key IN ({chains_str})
            GROUP BY 1,2
            HAVING SUM(txcount) > 30
            )
            select 
                fact.*,
                greatest(aa.daa, 1) AS daa,
                greatest(aa.prev_daa, 0) AS prev_daa
            from apps_mat fact
            LEFT JOIN (
                SELECT 
                    owner_project,
                    origin_key, 
                    coalesce(hll_cardinality(hll_union_agg(case when "date" > current_date - interval '{timeframe+1} days' then hll_addresses end))::int, 0) as daa, 
                    coalesce(hll_cardinality(hll_union_agg(case when "date" < current_date - interval '{timeframe} days' then hll_addresses end))::int, 0) as prev_daa
                FROM public.fact_active_addresses_contract_hll fact
                JOIN vw_oli_label_pool_gold_pivoted oli USING (address, origin_key)
                WHERE "date" >= current_date - interval '{timeframe*2} days'
                    AND fact.origin_key IN ({chains_str})
                    AND oli.owner_project IS NOT NULL
                GROUP BY 1, 2
            ) aa USING (owner_project, origin_key)
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())
        return df

    def create_app_overview_json(self):
        print(f'Creating App overview for chains: {self.chains_list_in_api_apps}')
        timeframes = [1,7,30,90,365,9999]
        for timeframe in timeframes:
            if timeframe == 9999:
                timeframe_key = 'max'
            else:
                timeframe_key = f'{timeframe}d'

            print(f'Creating App overview for for {timeframe_key}')
            df = self.get_app_overview_data(self.chains_list_in_api_apps, timeframe)
            
            projects_dict = {
                'data': {
                    'types': df.columns.to_list(),
                    'data': df.values.tolist()
                }
            }

            projects_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            projects_dict = fix_dict_nan(projects_dict, f'apps/app_overview_{timeframe_key}')

            if self.s3_bucket == None:
                self.save_to_json(projects_dict, f'apps/app_overview_{timeframe_key}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/apps/app_overview_{timeframe_key}', projects_dict, self.cf_distribution_id)

            print(f'DONE -- App overview export for {timeframe_key}')

    
    def get_apps_landing(self, origin_keys:list):
        df = self.get_app_overview_data(origin_keys,7)

        ## group df by owner_project and aggregate values 
        df = df.groupby(['owner_project']).agg({
            'origin_key':lambda x: ', '.join(x),
            'num_contracts':'sum', 
            'gas_fees_eth':'sum', 
            'prev_gas_fees_eth':'sum', 
            'gas_fees_usd':'sum', 
            'txcount':'sum', 
            'prev_txcount':'sum'        
        }).reset_index()

        ## add % change columns
        df['gas_fees_change_%'] = (df['gas_fees_eth'] - df['prev_gas_fees_eth']) / df['prev_gas_fees_eth'] * 100
        df['txcount_change_%'] = (df['txcount'] - df['prev_txcount']) / df['prev_txcount'] * 100

        ## order df by txcount descending
        df = df.sort_values(by='txcount', ascending=False)

        ## calc median txcount
        median_txcount= df['txcount'].median()

        ## filter df to only include projects with txcount > median_txcount
        df = df[df['txcount'] > median_txcount]
        ## filter out projects with pre_txcount = 0
        df = df[df['prev_txcount'] > 0]

        ## add rank column
        df['rank'] = df['txcount'].rank(ascending=False)

        ## keep top and bottom 3 as gainers and losers
        df_gainers = df.sort_values(by='txcount_change_%', ascending=False).head(3).copy()
        df_losers = df.sort_values(by='txcount_change_%', ascending=True).head(3).copy()

        return df_gainers, df_losers


    def fill_missing_dates(self, df):
        # Make sure date is in datetime format
        df['date'] = pd.to_datetime(df['date'])
        
        # Get yesterday's date
        yesterday = datetime.now().date() - timedelta(days=1)
        
        # Create a DataFrame with all combinations of origin_key and date
        # Get unique origin_keys
        origin_keys = df['origin_key'].unique()
        
        # Find min date per origin_key
        min_dates = df.groupby('origin_key')['date'].min()
        
        # Create an empty list to store dataframes for each origin_key
        all_dfs = []
        
        # For each origin_key, create a complete date range
        for origin_key in origin_keys:
            min_date = min_dates[origin_key]
            
            # Create a complete date range for this origin_key
            date_range = pd.date_range(start=min_date, end=yesterday)
            
            # Create a dataframe for this origin_key with all dates
            date_df = pd.DataFrame({
                'origin_key': origin_key,
                'date': date_range
            })
            
            all_dfs.append(date_df)
        
        # Combine all the dataframes
        complete_df = pd.concat(all_dfs)
        
        # Merge with original dataframe to fill in existing values
        # Use left join to keep all dates, even those not in original df
        result = pd.merge(complete_df, df, on=['origin_key', 'date'], how='left')
        
        # Fill missing values with 0
        fill_columns = ['txcount', 'fees_paid_eth', 'fees_paid_usd', 'daa']
        result[fill_columns] = result[fill_columns].fillna(0)
        
        # Sort the final dataframe
        result = result.sort_values(['origin_key', 'date']).reset_index(drop=True)
        
        return result

    def load_app_data(self, owner_project:str, chains:list):
        chains_str = ', '.join([f"'{chain}'" for chain in chains])

        exec_string = f"""
            SELECT 
                owner_project, 
                origin_key,
                date, 
                SUM(txcount) as txcount,
                SUM(fees_paid_eth) AS fees_paid_eth,
                SUM(fees_paid_usd) AS fees_paid_usd,
                SUM(daa) AS daa
            FROM vw_apps_contract_level_materialized AS fact
            WHERE 
                owner_project = '{owner_project}'
                AND fact.origin_key IN ({chains_str})
            GROUP BY 1,2,3
        """
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())
        df = df.drop(columns='owner_project')
        ## date to datetime column in
        df['date'] = pd.to_datetime(df['date'])

        ## fill missing dates with 0s
        df = self.fill_missing_dates(df)

        ## date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')

        ## unpivot table to have one row per date, origin_key and metric_key
        df = df.melt(id_vars=['origin_key', 'date'], var_name='metric_key', value_name='value')

        ## datetime to unix timestamp using timestamp() function
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        # fill NaN values with 0
        df.value.fillna(0, inplace=True)

        return df
    
    def get_app_contracts(self, owner_project:str, chains:list, days:int):
        chains_str = ', '.join([f"'{chain}'" for chain in chains])

        exec_string = f"""
            with apps_mat as (
                SELECT 
                    address,
                    UPPER(LEFT(contract_name , 1)) || SUBSTRING(contract_name FROM 2) as name,
                    main_category_key,
                    sub_category_key,
                    origin_key, 
                    SUM(txcount) as txcount,
                    SUM(fees_paid_eth) AS fees_paid_eth,
                    SUM(fees_paid_usd) AS fees_paid_usd
                FROM vw_apps_contract_level_materialized AS fact
                WHERE 
                    owner_project = '{owner_project}'
                    AND fact.origin_key IN ({chains_str})
                    and fact.date < current_date
                    and fact.date >= current_date - interval '{days} days'
                GROUP BY 1,2,3,4,5
            )
            select 
                fact.*,
                greatest(aa.daa, 1) AS daa
            from apps_mat fact
            LEFT JOIN (
                SELECT 
                    address,
                    origin_key, 
                    hll_cardinality(hll_union_agg(hll_addresses))::int AS daa
                FROM public.fact_active_addresses_contract_hll fact
                JOIN vw_oli_label_pool_gold_pivoted oli USING (address, origin_key)
                WHERE 
                    oli.owner_project  = '{owner_project}'
                    AND fact.origin_key IN ({chains_str})
                    and fact.date < current_date
                    and fact.date >= current_date - interval '{days} days'
                GROUP BY 1, 2
            ) aa USING (address, origin_key)
            ORDER BY fees_paid_eth desc
        """
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())
        df = db_addresses_to_checksummed_addresses(df, ['address'])

        return df
    
    def get_active_addresses_val(self, owner_project:str, origin_key:str, timeframe:int):
        exec_string = f"""
            SELECT 
                coalesce(hll_cardinality(hll_union_agg(case when "date" > current_date - interval '{timeframe+1} days' then hll_addresses end))::int, 0) as val
            FROM public.fact_active_addresses_contract_hll fact
            JOIN vw_oli_label_pool_gold_pivoted oli USING (address, origin_key)
            WHERE 
                owner_project = '{owner_project}'
                AND fact.origin_key = '{origin_key}'
                AND "date" >= current_date - interval '{timeframe} days'

        """
        with self.db_connector.engine.connect() as connection:
            result = connection.execute(exec_string)
            val = result.scalar()
            return val


    def create_app_details_json(self, project:str, timeframes, is_all=False):
        df = self.load_app_data(project, self.chains_list_in_api_apps)
        if len(df) > 0:
            df_first_seen = df.groupby('origin_key').agg({'date': 'min'}).copy()
            df_first_seen.reset_index(inplace=True)
            df_first_seen.date = df_first_seen.date.dt.strftime('%Y-%m-%d')
            df_first_seen_dict = df_first_seen.set_index('origin_key').to_dict()['date']

            app_dict = {
                'metrics': {},
                'first_seen': df_first_seen_dict
            }

            for metric in self.app_metrics:
                app_dict['metrics'][metric] = {
                    'metric_name': self.app_metrics[metric]['name'],
                    'avg': self.app_metrics[metric]['avg'],
                    'over_time': {},
                    'aggregated': {
                        'types': list(self.app_metrics[metric]['units'].keys()),
                        'data': {}
                    }
                }

                for origin_key in self.chains_list_in_api_apps:
                    ## check if origin_key is in df
                    if origin_key in df.origin_key.unique():
                        mk_list = self.generate_daily_list(df, metric, origin_key, metric_type='app')
                        mk_list_int = mk_list[0]
                        mk_list_columns = mk_list[1]

                        app_dict['metrics'][metric]['over_time'][origin_key] = {
                            'daily': {
                                'types' : mk_list_columns,
                                'data' : mk_list_int
                            }
                        }

                        app_dict['metrics'][metric]['aggregated']['data'][origin_key] = {}
                        for timeframe in timeframes:  
                            data_list = []
                            timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'
                            days = timeframe if timeframe != 'max' else 9999

                            ##for active addresses we cannot just sum up the values, we need to pull the hll data for each timeframe from our db
                            if metric == 'daa':
                                val = self.get_active_addresses_val(project, origin_key, days)
                                data_list.append(val)
                            else:
                                for metric_key in self.app_metrics[metric]['metric_keys']:
                                    val = self.aggregate_metric(df, origin_key, metric_key, days)
                                    data_list.append(val)
                        
                            app_dict['metrics'][metric]['aggregated']['data'][origin_key][timeframe_key] = data_list

            ## Contracts NEW
            app_dict['contracts_table'] = {}
            for timeframe in timeframes: 
                timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'
                days = timeframe if timeframe != 'max' else 9999
                
                contracts = self.get_app_contracts(project, self.chains_list_in_api_apps, days)
                contract_dict = {
                    'types': contracts.columns.to_list(),
                    'data': contracts.values.tolist()
                }
                app_dict['contracts_table'][timeframe_key] = contract_dict

            app_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            app_dict = fix_dict_nan(app_dict, f'apps/details/{project}')

            if self.s3_bucket == None:
                self.save_to_json(app_dict, f'apps/details/{project}')
            else:
                if is_all: ## in this case don't invalidate each file
                    upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/apps/details/{project}', app_dict, self.cf_distribution_id, invalidate=False)
                else:
                    upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/apps/details/{project}', app_dict, self.cf_distribution_id)
            print(f'DONE -- App details export for {project}')

        else:
            print(f'..skipped: App details export for {project}. No data found')
        return project

    def run_app_details_jsons(self, owner_projects:list, is_all=False):
        timeframes = [1,7,30,90,180,365,'max']
        counter = 1

        ## run steps in parallel
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.create_app_details_json, i, timeframes, is_all) for i in owner_projects]
            for future in as_completed(futures):
                project = future.result()
                print(f'..done with {project}. {counter}/{len(owner_projects)}')    
                counter += 1

    def run_app_details_jsons_all(self):
        print(f'Creating App details for all apps for chains: {self.chains_list_in_api_apps}')
        ## get all active projects with contracts assigned
        chains_str = ', '.join([f"'{chain}'" for chain in self.chains_list_in_api_apps])
        exec_string = f"""
                select distinct(owner_project) as name
                from vw_apps_contract_level_materialized
                where origin_key IN ({chains_str})
        """
        df_projects = pd.read_sql(exec_string, self.db_connector.engine.connect())
        projects = df_projects.name.to_list()
        print(f'..starting: App details export for all projects. Number of projects: {len(projects)}')

        self.run_app_details_jsons(projects, is_all=True)
        empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/apps/details/*')

    ########################################################################
    ### ETH AGG
    ########################################################################

    def create_eco_overview_json(self):
        traction_dict = {
            "data": {
                "count_layer2s" : {},
                "tps" : {
                    "layer_2s" : {},
                    "ethereum_mainnet" : {}
                },
                "stables" : {
                    "layer_2s" : {},
                    "ethereum_mainnet" : {}
                },
                "app_fees" : {
                    "layer_2s" : {},
                    "ethereum_mainnet" : {}
                },
                "meet_l2s" : {}
            }
        }

        ## Count Layer 2s
        df = execute_jinja_query(self.db_connector, "api/select_l2count_over_time.sql.j2", query_parameters={}, return_df=True)
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['date'])

        traction_dict["data"]["count_layer2s"]= {
            "daily": {
                "types": df.columns.tolist(),
                "values": df.values.tolist()
            }
        }         

        ## txcount layer 2s
        query_parameters = {
            "days": 9999,
            "rolling": 7,
            "metric_key": 'txcount',
        }

        df = execute_jinja_query(self.db_connector, "api/select_sum_metric_l2s_rolling.sql.j2", query_parameters, return_df=True)
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['date'])

        ## divide all values by 60*60*24 to get TPS
        df['value'] = df['value'] / (60*60*24)

        traction_dict["data"]["tps"]["layer_2s"]= {
            "daily": {
                "types": df.columns.tolist(),
                "values": df.values.tolist()
            }
        }         

        ## txcount ethereum mainnet
        query_parameters = {
            "days": 9999,
            "metric_key": 'txcount',
            "origin_key": 'ethereum',
            "rolling": 7
        }
        df = execute_jinja_query(self.db_connector, "api/select_fact_kpis_rolling.sql.j2", query_parameters, return_df=True)
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['date'])

        ## divide all values by 60*60*24 to get TPS
        df['value'] = df['value'] / (60*60*24)

        traction_dict["data"]["tps"]["ethereum_mainnet"]= {
            "daily": {
                "types": df.columns.tolist(),
                "values": df.values.tolist()
            }
        }      

        ## stables layers 2s
        query_parameters = {
            "days": 9999,
            "metric_key": 'stables_mcap',
        }
        df_usd = execute_jinja_query(self.db_connector, "api/select_sum_metric_l2s.sql.j2", query_parameters, return_df=True)
        query_parameters = {
            "days": 9999,
            "metric_key": 'stables_mcap_eth',
        }
        df_eth = execute_jinja_query(self.db_connector, "api/select_sum_metric_l2s.sql.j2", query_parameters, return_df=True)
        df = pd.merge(df_usd, df_eth, on=['date'], how='left', suffixes=('_usd', '_eth'))

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['date'])
        df.rename(columns={'value_usd': 'usd', 'value_eth': 'eth'}, inplace=True)

        traction_dict["data"]["stables"]["layer_2s"]= {
            "daily": {
                "types": df.columns.tolist(),
                "values": df.values.tolist()
            }
        }        

        ## stables ethereum mainnet
        query_parameters = {
            "days": 9999,
            "metric_key": 'stables_mcap',
            "origin_key": 'ethereum'
        }
        df_usd = execute_jinja_query(self.db_connector, "api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
        query_parameters = {
            "days": 9999,
            "metric_key": 'stables_mcap_eth',
            "origin_key": 'ethereum'
        }
        df_eth = execute_jinja_query(self.db_connector, "api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
        df = pd.merge(df_usd, df_eth, on=['date'], how='left', suffixes=('_usd', '_eth'))

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['date'])
        df.rename(columns={'value_usd': 'usd', 'value_eth': 'eth'}, inplace=True)

        traction_dict["data"]["stables"]["ethereum_mainnet"]= {
            "daily": {
                "types": df.columns.tolist(),
                "values": df.values.tolist()
            }
        }        

        ## App Fees layers 2s
        query_parameters = {
            "days": 9999,
            "rolling": 7,
            "metric_key": 'app_fees_usd',
        }
        df_usd = execute_jinja_query(self.db_connector, "api/select_sum_metric_l2s_rolling.sql.j2", query_parameters, return_df=True)
        
        query_parameters = {
            "days": 9999,
            "metric_key": 'app_fees_eth',
        }
        df_eth = execute_jinja_query(self.db_connector, "api/select_sum_metric_l2s_rolling.sql.j2", query_parameters, return_df=True)
        df = pd.merge(df_usd, df_eth, on=['date'], how='left', suffixes=('_usd', '_eth'))

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['date'])
        df.rename(columns={'value_usd': 'usd', 'value_eth': 'eth'}, inplace=True)

        traction_dict["data"]["app_fees"]["layer_2s"]= {
            "daily": {
                "types": df.columns.tolist(),
                "values": df.values.tolist()
            }
        }        

        ## App fees ethereum mainnet
        query_parameters = {
            "days": 9999,
            "metric_key": 'app_fees_usd',
            "origin_key": 'ethereum',
            "rolling": 7
        }
        df_usd = execute_jinja_query(self.db_connector, "api/select_fact_kpis_rolling.sql.j2", query_parameters, return_df=True)
        query_parameters = {
            "days": 9999,
            "metric_key": 'app_fees_eth',
            "origin_key": 'ethereum',
            "rolling": 7
        }
        df_eth = execute_jinja_query(self.db_connector, "api/select_fact_kpis_rolling.sql.j2", query_parameters, return_df=True)
        df = pd.merge(df_usd, df_eth, on=['date'], how='left', suffixes=('_usd', '_eth'))

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['date'])
        df.rename(columns={'value_usd': 'usd', 'value_eth': 'eth'}, inplace=True)

        traction_dict["data"]["app_fees"]["ethereum_mainnet"]= {
            "daily": {
                "types": df.columns.tolist(),
                "values": df.values.tolist()
            }
        }        

        ## meet layer 2s
        l2s_to_meet = ['optimism', 'arbitrum', 'base' , 'celo' , 'soneium']
        for l2 in l2s_to_meet:
            query_parameters = {
                "origin_key": l2
            }
            aa_total = execute_jinja_query(self.db_connector, "api/select_total_aa.sql.j2", query_parameters, return_df=True).iloc[0, 0]

            query_parameters = {
                "origin_key": l2,
                "metric_key": 'daa',
            }
            aa_yesterday = execute_jinja_query(self.db_connector, "api/select_fact_kpis_latest.sql.j2", query_parameters, return_df=True).iloc[0, 1]

            query_parameters = {
                "origin_key": l2,
                "metric_key": 'stables_mcap',
            }
            stables_mcap_usd = execute_jinja_query(self.db_connector, "api/select_fact_kpis_latest.sql.j2", query_parameters, return_df=True).iloc[0, 1]

            query_parameters = {
                "origin_key": l2,
                "metric_key": 'stables_mcap_eth',
            }
            stables_mcap_eth = execute_jinja_query(self.db_connector, "api/select_fact_kpis_latest.sql.j2", query_parameters, return_df=True).iloc[0, 1]

            query_parameters = {
                "origin_key": l2,
                "metric_key": 'txcount',
            }
            tps = execute_jinja_query(self.db_connector, "api/select_fact_kpis_latest.sql.j2", query_parameters, return_df=True).iloc[0, 1] / (24*60*60)  # convert to TPS
            
            if l2 == 'celo':
                top_apps_list = ['tetherto', 'circlefin', 'velodrome', 'uniswap', 'mento-protocol']
            else:
                query_parameters = {
                    "origin_key": l2,
                    "days": 7,
                    "limit": 5
                }
                top_apps = execute_jinja_query(self.db_connector, "api/select_top_apps.sql.j2", query_parameters, return_df=True)
                top_apps_list = top_apps['owner_project'].tolist()
            
            l2_dict = {
                "total_aa": int(aa_total),
                "yesterday_aa": int(aa_yesterday),
                "stables_mcap_usd": float(stables_mcap_usd),
                "stables_mcap_eth": float(stables_mcap_eth),
                "tps": float(tps),
                "top_apps": top_apps_list,
            }

            traction_dict["data"]["meet_l2s"][l2] = l2_dict

        traction_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        traction_dict = fix_dict_nan(traction_dict, 'ecosystem_overview')

        if self.s3_bucket == None:
            self.save_to_json(traction_dict, 'ecosystem_overview')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/ecosystem/overview', traction_dict, self.cf_distribution_id)
        print(f'DONE -- Ecosystem overview export')



    #######################################################################
    ### LABELS
    #######################################################################

    def create_labels_json(self, type='full'):
        if type == 'full':
            limit = 250000
        elif type == 'quick':
            limit = 100
        else:
            raise ValueError('type must be either "full" or "quick"')
        
        order_by = 'txcount'
        df = self.db_connector.get_labels_lite_db(limit=limit, order_by=order_by, origin_keys=self.chains_list_in_api_labels)
        df = db_addresses_to_checksummed_addresses(df, ['address'])
        df = string_addresses_to_checksummed_addresses(df, ['deployer_address'])
        df['deployment_tx'] = df['deployment_tx'].str.lower()
        
        df['gas_fees_usd'] = df['gas_fees_usd'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['txcount_change'] = df['txcount_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['gas_fees_usd_change'] = df['gas_fees_usd_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['daa_change'] = df['daa_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)

        ## if txcount change > 99 then set to 99
        df['txcount_change'] = df['txcount_change'].apply(lambda x: 99.99 if x > 99.99 else x)
        df['gas_fees_usd_change'] = df['gas_fees_usd_change'].apply(lambda x: 99.99 if x > 99.99 else x)
        df['daa_change'] = df['daa_change'].apply(lambda x: 99.99 if x > 99.99 else x)

        df['deployment_date'] = df['deployment_date'].apply(lambda x: str(x))
        df['deployment_date'] = df['deployment_date'].replace('NaT', None)

        df = df.replace({np.nan: None})        

        labels_dict = {
            'data': {
                'sort': {
                    'by': order_by,
                    'direction': 'desc'
                },
                'types': df.columns.to_list(),
                'data': df.values.tolist()
            }
        }

        labels_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        labels_dict = fix_dict_nan(labels_dict, f'labels-{type}')

        if self.s3_bucket == None:
            self.save_to_json(labels_dict, f'labels-{type}')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/{type}', labels_dict, self.cf_distribution_id)
        print(f'DONE -- labels {type} export')

    def create_labels_sparkline_json(self):
        df = self.db_connector.get_labels_page_sparkline(limit=10000, origin_keys=self.chains_list_in_api_labels)
        df = db_addresses_to_checksummed_addresses(df, ['address'])

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')

        ## Fill in missing dates for each address, origin_key combination
        # Find min and max dates for each address, origin_key combination
        min_dates = df.groupby(['address', 'origin_key'])['date'].min().reset_index()
        max_dates = df.groupby(['address', 'origin_key'])['date'].max().reset_index()
        # Merge min and max dates
        date_ranges = pd.merge(min_dates, max_dates, on=['address', 'origin_key'])
        date_ranges.columns = ['address', 'origin_key', 'min_date', 'max_date']
        # Create a DataFrame for all combinations of address, origin_key, and date
        all_combinations = []
        for _, row in date_ranges.iterrows():
            address = row['address']
            origin_key = row['origin_key']
            all_dates = pd.date_range(start=row['min_date'], end=row['max_date'])
            all_combinations.append(pd.DataFrame({
                'date': all_dates,
                'address': address,
                'origin_key': origin_key
            }))
        # Concatenate all combinations into a single DataFrame
        all_combinations_df = pd.concat(all_combinations, ignore_index=True)
        # Merge with the original dataframe
        merged_df = pd.merge(all_combinations_df, df, on=['date', 'address', 'origin_key'], how='left')
        # Fill NaN values with 0
        merged_df['txcount'].fillna(0, inplace=True)
        merged_df['gas_fees_usd'].fillna(0, inplace=True)
        merged_df['daa'].fillna(0, inplace=True)

        df = merged_df.copy()

        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)

        df['txcount'] = df['txcount'].astype(int)
        df['daa'] = df['daa'].astype(int)
        df['gas_fees_usd'] = df['gas_fees_usd'].round(2)

        sparkline_dict = {
                'data': {'types': ['unix', 'txcount', 'gas_fees_usd', 'daa'],}
        }

        for address, origin_key in df[['address', 'origin_key']].drop_duplicates().values:
            sparkline_dict['data'][f'{origin_key}_{address}'] = {
                    'sparkline': df[(df['address'] == address) & (df['origin_key'] == origin_key)][['unix', 'txcount', 'gas_fees_usd', 'daa']].values.tolist()
            }                     

        sparkline_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        sparkline_dict = fix_dict_nan(sparkline_dict, 'labels-sparkline')

        if self.s3_bucket == None:
            self.save_to_json(sparkline_dict, 'labels-sparkline')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/sparkline', sparkline_dict, self.cf_distribution_id)
        print(f'DONE -- sparkline export')

    def create_projects_json(self):        
        df = self.db_connector.get_active_projects(add_category=True)
        df = df.rename(columns={'name': 'owner_project'})
        df = df.replace({np.nan: None})        

        projects_dict = {
            'data': {
                'types': df.columns.to_list(),
                'data': df.values.tolist()
            }
        }

        projects_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        projects_dict = fix_dict_nan(projects_dict, f'projects')

        if self.s3_bucket == None:
            self.save_to_json(projects_dict, f'projects')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/projects', projects_dict, self.cf_distribution_id)
        print(f'DONE -- projects export')

    def create_projects_filtered_json(self):        
        df = self.db_connector.get_active_projects(add_category=True, filtered_by_chains=self.chains_list_in_api_apps)
        df = df.rename(columns={'name': 'owner_project'})
        df = df.replace({np.nan: None})        

        projects_dict = {
            'data': {
                'types': df.columns.to_list(),
                'data': df.values.tolist()
            }
        }

        projects_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        projects_dict = fix_dict_nan(projects_dict, f'projects_filtered')

        if self.s3_bucket == None:
            self.save_to_json(projects_dict, f'projects_filtered')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/projects_filtered', projects_dict, self.cf_distribution_id)
        print(f'DONE -- projects_filtered export')

    def create_top_contracts_json(self, limit, origin_key=None):
        if origin_key == None:
            origin_keys = self.chains_list_in_api_labels
        else:
            origin_keys = [origin_key]

        df = self.db_connector.get_labels_export_df(limit=limit, origin_keys=origin_keys, incl_aggregation=True)
        df = db_addresses_to_checksummed_addresses(df, ['address'])
        df = string_addresses_to_checksummed_addresses(df, ['deployer_address'])
        df['deployment_tx'] = df['deployment_tx'].str.lower()
        
        df = df.where(pd.notnull(df), None)
        df['deployment_date'] = df['deployment_date'].astype(str)        
        df['deployment_date'] = df['deployment_date'].replace('NaT', None)
        df = df.drop(columns=['origin_key']) ## chain_id is sufficient

        labels_dict = df.to_dict(orient='records')  

        if self.s3_bucket == None:
            self.save_to_json(labels_dict, f'export_labels')
        else:
            if origin_key == None:
                full_name = f'{self.api_version}/top_contracts/export_all'
            else:
                full_name = f'{self.api_version}/top_contracts/export_{origin_key}'

            upload_json_to_cf_s3(self.s3_bucket, full_name, labels_dict, self.cf_distribution_id)
        print(f'DONE -- top contracts {full_name} export')

    def run_top_contracts_jsons(self):
        ## run create_top_contracts_json for all chains that are in self.chains_list_in_api_labels
        print(f'Creating Top contracts for chains: {self.chains_list_in_api_labels}')
        for chain in self.chains_list_in_api_labels:
            print(f'..creating Top contracts for {chain}')
            self.create_top_contracts_json(100, chain)


    def create_export_labels_parquet(self, subset = 'top50k'):
        if subset == 'top50k':
            limit = 50000
        else:
            raise ValueError('subset must be either "top50k"')
        
        df = self.db_connector.get_labels_export_df(limit=limit, origin_keys=self.chains_list_in_api_labels)
        df = db_addresses_to_checksummed_addresses(df, ['address'])
        df = df.drop(columns=['origin_key']) ## chain_id is sufficient

        upload_parquet_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/export_labels_{subset}', df, self.cf_distribution_id)
        print(f'DONE -- labels export_labels_{subset}.parquet export')

    def create_export_oli_parquet(self):        
        exec_string = f"""
            SELECT concat('0x',encode(id, 'hex')) as id, attester, recipient, is_offchain, revoked, ipfs_hash, tx_id, decoded_data_json, "time", time_created, revocation_time
            FROM public.oli_label_pool_bronze;
        """
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())
        df = db_addresses_to_checksummed_addresses(df, ['attester', 'recipient'])

        upload_parquet_to_cf_s3(self.s3_bucket, f'{self.api_version}/oli/labels_raw', df, self.cf_distribution_id)
        print(f'DONE -- OLI labels_raw.parquet export')

        exec_string = f"""
            SELECT concat('0x',encode(id, 'hex')) as id, chain_id, address, tag_id, tag_value, attester, time_created, revocation_time, revoked, is_offchain
            FROM public.oli_label_pool_silver;
        """
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())
        df = db_addresses_to_checksummed_addresses(df, ['address', 'attester'])

        upload_parquet_to_cf_s3(self.s3_bucket, f'{self.api_version}/oli/labels_decoded', df, self.cf_distribution_id)
        print(f'DONE -- OLI labels_decoded.parquet export')


    #######################################################################
    ### EIM
    #######################################################################

    def create_eth_exported_json(self, df):
        entity_dict = {}    
        metric = 'eth_exported'

        ## add 'total' to the list of entities
        self.eth_exported_entities['total'] = {
            'name': 'Total ETH Exported',
            'type': 'total',
            'chains': ['all']
        }

        for entity in self.eth_exported_entities:
            if self.eth_exported_entities[entity]['type'] in ['Layer 2', 'Layer 1', 'Sidechain', 'total']:
                mk_list = self.generate_daily_list(df, metric, entity, metric_type='eim', start_date='2021-01-01')
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                entity_dict[entity] = {
                    'changes': self.create_changes_dict(df, metric, entity, metric_type='eim'),
                    'daily': {
                        'types' : mk_list_columns,
                        'data' : mk_list_int
                    },
                    'latest_usd': mk_list_int[-1][1],
                    'latest_eth': mk_list_int[-1][2],
                }

        #sort entity_dict by latest value desc
        entity_dict = dict(sorted(entity_dict.items(), key=lambda item: item[1]['latest_usd'], reverse=True))

        entity_metadata_dict = self.eth_exported_entities.copy()
        ## sort entity_metadata_dict same as entity_dict
        entity_metadata_dict = {k: entity_metadata_dict[k] for k in entity_dict.keys()}

        details_dict = {
            'data': {
                'metric_id': metric,
                'metric_name': self.eim_metrics[metric]['name'],
                'entities': entity_metadata_dict,
                'chart': entity_dict
            }
        }

        details_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        details_dict = fix_dict_nan(details_dict, f'metrics/{metric}')

        if self.s3_bucket == None:
            self.save_to_json(details_dict, f'eim/eth_exported')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/eim/eth_exported', details_dict, self.cf_distribution_id)
        print(f'DONE -- ETH exported export done')

    def create_eth_supply_json(self, df):
        metric_dict = {}
        metrics_list = ['eth_supply', 'eth_issuance_rate']
        ## keep only metrics from self.da_metrics that are in the metrics_list
        metrics = {key: value for key, value in self.eim_metrics.items() if key in metrics_list} 

        for metric in metrics:
            mk_list = self.generate_daily_list(df, metric, "ethereum", metric_type='eim')
            mk_list_int = mk_list[0]
            mk_list_columns = mk_list[1]

            metric_dict[metric] = {
                'daily': {
                    'types' : mk_list_columns,
                    'data' : mk_list_int
                }
            }        

        details_dict = {
            'data': {
                'chart': metric_dict,
                'events': self.ethereum_events['upgrades']
            }
        }

        details_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        details_dict = fix_dict_nan(details_dict, f'metrics/{metric}')

        if self.s3_bucket == None:
            self.save_to_json(details_dict, f'eim/eth_supply')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/eim/eth_supply', details_dict, self.cf_distribution_id)
        print(f'DONE -- ETH supply export done')

    def create_eth_holders_json(self):
        df = self.db_connector.get_holders_with_balances()
        ## sort df by eth_equivalent_balance_eth desc
        df = df.sort_values(by='eth_equivalent_balance_eth', ascending=False)

        ## rename holding_type column to tracking
        df = df.rename(columns={'holding_type': 'tracking_type'})

        ## order columns
        df = df[["holder_key", "name", "type", "tracking_type", "eth_equivalent_balance_usd", "eth_equivalent_balance_eth"]]
        holders_dict = {
            'data': {
                'sort': {
                    'by': 'eth_equivalent_balance_eth',
                    'direction': 'desc'
                },
                'types': df.columns.to_list(),
                'data': df.values.tolist()
            }
        }

        holders_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        holders_dict = fix_dict_nan(holders_dict, f'eim-eth_holders')

        if self.s3_bucket == None:
            self.save_to_json(holders_dict, f'eim/eth_holders')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/eim/eth_holders', holders_dict, self.cf_distribution_id)
        print(f'DONE -- ETH holders export done')


    #######################################################################
    ### API ENDPOINTS
    #######################################################################

    def create_fundamentals_json(self, df):
        df = df[['metric_key', 'origin_key', 'date', 'value']].copy()

        ## filter out all metric_keys that end with _eth
        df = df[~df.metric_key.str.endswith('_eth')]

        ## filter date to be in the last 365 days
        df = df.loc[df.date >= (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')]

        ## only keep metrics that are also in the metrics_list (based on metrics dict)
        df = df[df.metric_key.isin(self.metrics_list + ['aa_last7d'])]

        ## transform date column to string with format YYYY-MM-DD
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        ## filter based on settings in main_config
        for adapter in self.main_config:
            ## filter out origin_keys from df if in_api=false
            if adapter.api_in_main == False:
                #print(f"Filtering out origin_keys for adapter {adapter.name}")
                df = df[df.origin_key != adapter.origin_key]
            elif len(adapter.api_exclude_metrics) > 0:
                origin_key = adapter.origin_key
                for metric in adapter.api_exclude_metrics:
                    if metric != 'blockspace':
                        #print(f"Filtering out metric_keys {metric} for adapter {adapter.name}")
                        metric_keys = self.metrics[metric]['metric_keys']
                        df = df[~((df.origin_key == origin_key) & (df.metric_key.isin(metric_keys)))]
        
        ## put dataframe into a json string
        fundamentals_dict = df.to_dict(orient='records')
        fundamentals_dict = fix_dict_nan(fundamentals_dict, 'fundamentals')

        if self.s3_bucket == None:
            self.save_to_json(fundamentals_dict, 'fundamentals')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fundamentals', fundamentals_dict, self.cf_distribution_id)

        ## json for ethereum.org (just last 7 days)
        #df = df[df.metric_key.isin(['txcount', 'txcosts_median_usd', 'aa_last7d'])]
        df = df.loc[df.date >= (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')]

        eth_fundamentals_dict = df.to_dict(orient='records')
        eth_fundamentals_dict = fix_dict_nan(eth_fundamentals_dict, 'fundamentals_7d')

        if self.s3_bucket == None:
            self.save_to_json(eth_fundamentals_dict, 'fundamentals_7d')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fundamentals_7d', eth_fundamentals_dict, self.cf_distribution_id)
        print(f'DONE -- Fundamentals export done')

    
    def create_fundamentals_full_json(self, df):
        df = df[['metric_key', 'origin_key', 'date', 'value']].copy()

        ## only keep metrics that are also in the metrics_list (based on metrics dict)
        df = df[df.metric_key.isin(self.metrics_list + ['aa_last7d'])]

        ## transform date column to string with format YYYY-MM-DD
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        ## filter based on settings in main_config
        for adapter in self.main_config:
            ## filter out origin_keys from df if in_api=false
            if adapter.api_in_main == False:
                #print(f"Filtering out origin_keys for adapter {adapter.name}")
                df = df[df.origin_key != adapter.origin_key]
            elif len(adapter.api_exclude_metrics) > 0:
                origin_key = adapter.origin_key
                for metric in adapter.api_exclude_metrics:
                    if metric != 'blockspace':
                        #print(f"Filtering out metric_keys {metric} for adapter {adapter.name}")
                        metric_keys = self.metrics[metric]['metric_keys']
                        df = df[~((df.origin_key == origin_key) & (df.metric_key.isin(metric_keys)))]
        
        ## put dataframe into a json string
        fundamentals_dict = df.to_dict(orient='records')

        fundamentals_dict = fix_dict_nan(fundamentals_dict, 'fundamentals_full')

        if self.s3_bucket == None:
            self.save_to_json(fundamentals_dict, 'fundamentals_full')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fundamentals_full', fundamentals_dict, self.cf_distribution_id)

    def create_da_fundamentals_json(self):
        metrics_user_string = "'" + "','".join(self.da_metrics_list) + "'"
        chain_user_string = "'" + "','".join(self.da_layers_list) + "'"

        df = self.download_data(chain_user_string, metrics_user_string)
        df = df[['metric_key', 'origin_key', 'date', 'value']].copy()

        ## transform date column to string with format YYYY-MM-DD
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        ## put dataframe into a json string
        fundamentals_dict = df.to_dict(orient='records')

        fundamentals_dict = fix_dict_nan(fundamentals_dict, 'da_fundamentals')

        if self.s3_bucket == None:
            self.save_to_json(fundamentals_dict, 'da_fundamentals')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/da_fundamentals', fundamentals_dict, self.cf_distribution_id)

    def create_metrics_export_json(self, df):
        df = df[['metric_key', 'origin_key', 'date', 'value']].copy()
        ## transform date column to string with format YYYY-MM-DD
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        ## filter based on settings in main_config
        for adapter in self.main_config:
            ## filter out origin_keys from df if in_api=false
            if adapter.api_in_main == False:
                #print(f"Filtering out origin_keys for adapter {adapter.name}")
                df = df[df.origin_key != adapter.origin_key]
            elif len(adapter.api_exclude_metrics) > 0:
                origin_key = adapter.origin_key
                for metric in adapter.api_exclude_metrics:
                    if metric != 'blockspace':
                        #print(f"Filtering out metric_keys {metric} for adapter {adapter.name}")
                        metric_keys = self.metrics[metric]['metric_keys']
                        df = df[~((df.origin_key == origin_key) & (df.metric_key.isin(metric_keys)))]
        
        for metric in self.metrics:
            if self.metrics[metric]['fundamental'] == False:
                continue
            #print(f'...exporting metric {metric}')
            mks = self.metrics[metric]['metric_keys']
            df_metric = df[df['metric_key'].isin(mks)].copy()

            ## put dataframe into a json string
            metric_dict = df_metric.to_dict(orient='records')
            metric_dict = fix_dict_nan(metric_dict, f'metric_dict_{metric}')

            if self.s3_bucket == None:
                self.save_to_json(metric_dict, f'metric_{metric}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/export/{metric}', metric_dict, self.cf_distribution_id)
                
    def create_custom_metrics_json(self, custom_metrics):
        for metric in custom_metrics:
            df_main = pd.DataFrame()
            for origin_key in self.chains_list_in_api:
                query_parameters = {
                    'origin_key': origin_key,
                    'metric_key': metric,
                    'days': 9999
                }
                df = execute_jinja_query(self.db_connector, "api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
                df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC').astype(str)
                df['origin_key'] = origin_key
                df['metric_key'] = metric
                df['value'] = df['value'].astype(int)

                df_main = pd.concat([df_main, df], ignore_index=True)
            
            ## convert df_main to a dictionary 
            data_dict = {}
            
            data_dict['data'] = df_main.to_dict(orient='records')

            data_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            data_dict = fix_dict_nan(data_dict, 'custom_metrics')

            if self.s3_bucket == None:
                self.save_to_json(data_dict, f'custom_metrics {metric}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/export/{metric}', data_dict, self.cf_distribution_id)

    ## TODO: DEPRECATE
    def create_contracts_json(self):
        exec_string = f"""
            SELECT address, contract_name, project_name, sub_category_key, origin_key
            FROM public.blockspace_labels;
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())
        df = db_addresses_to_checksummed_addresses(df, ['address'])

        contracts_dict = df.to_dict(orient='records')
        
        contracts_dict = fix_dict_nan(contracts_dict, 'contracts')

        if self.s3_bucket == None:
            self.save_to_json(contracts_dict, 'contracts')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/contracts', contracts_dict, self.cf_distribution_id)

    def create_glo_json(self):
        glo = Glo(self.db_connector)
        
        df = glo.run_glo()
        df_mcap = self.db_connector.get_glo_mcap()

        current_mcap = df_mcap[(df_mcap['metric_key'] == 'market_cap_usd') & (df_mcap['date'] == df_mcap['date'].max())].value.max()
        df['share'] = df['balance'] / current_mcap

        df_mcap['date'] = pd.to_datetime(df_mcap['date']).dt.tz_localize('UTC')
        df_mcap['unix'] = df_mcap['date'].apply(lambda x: x.timestamp() * 1000)
        df_mcap = df_mcap.drop(columns=['date'])
        df_mcap = df_mcap.pivot(index='unix', columns='metric_key', values='value')
        df_mcap.reset_index(inplace=True)
        df_mcap.rename(columns={'market_cap_eth':'eth', 'market_cap_usd':'usd'}, inplace=True)
        df_mcap = df_mcap.sort_values(by='unix', ascending=True)

        glo_dict = {'holders_table':{}, 'chart':{}, 'source':[]}

        for index, row in df.iterrows():
            glo_dict['holders_table'][row['holder']] = {'balance':row['balance'], 'share':row['share'], 'website':row['website'], 'twitter':row['twitter']}

        glo_dict['chart']['types'] = df_mcap.columns.to_list()
        glo_dict['chart']['data'] = df_mcap.values.tolist()
        glo_dict['source'] = ["Dune"]

        glo_dict = fix_dict_nan(glo_dict, 'GLO Dollar')

        if self.s3_bucket == None:
            self.save_to_json(glo_dict, 'GLO Dollar')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/glo_dollar', glo_dict, self.cf_distribution_id)

    def create_all_jsons(self):
        df = self.get_all_data()
        self.create_master_json(df)
        self.create_landingpage_json(df)

        self.create_chain_details_jsons(df)
        self.create_metric_details_jsons(df)

        self.create_economics_json(df)
        
        self.create_fundamentals_json(df)
        self.create_fundamentals_full_json(df)
        self.create_contracts_json()

    ## JSON removal
    ## connect to s3 bucket and output list of files
    
    ## this function will remove all app files that are older than 5 days
    def clean_app_files(self):
        df = get_files_df_from_s3('growthepie', 'v1/apps/details')

        ## filter df_raw where last_modified more than 3 days ago
        df['last_modified'] = pd.to_datetime(df['last_modified'])
        df['last_modified'] = df['last_modified'].dt.tz_localize(None)
        df['days_since_last_modified'] = (pd.Timestamp.now() - df['last_modified']).dt.days

        df_to_remove = df[df['days_since_last_modified'] > 4]

        if df_to_remove.empty:
            print('No App files to remove')
        else:
            for index, row in df_to_remove.iterrows():
                key = row['key']
                remove_file_from_s3('growthepie', key)
import os
import json
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.db_connector import DbConnector
from src.config import gtp_units, gtp_metrics_new, levels_dict
from src.main_config import MainConfig, get_main_config
from src.da_config import get_da_config
from src.misc.helper_functions import fix_dict_nan, upload_json_to_cf_s3, empty_cloudfront_cache, highlights_prep, db_addresses_to_checksummed_addresses, send_discord_message
from src.misc.jinja_helper import execute_jinja_query

# --- IMPORT SCHEMAS ---
from src.api.schemas import (
    MetricResponse, MetricDetails, 
    ChainOverviewResponse, ChainData, ChainAchievements,
    StreaksResponse, EcosystemResponse, 
    UserInsightsResponse, UserInsightsData,
    TreeMapResponse
)
from src.api.seo_json import SeoJsonBuilder, atomic_write_json

# --- Set up a proper logger ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
AGG_METHOD_SUM = 'sum'
AGG_METHOD_MEAN = 'mean'
AGG_METHOD_LAST = 'last'

AGG_CONFIG_SUM = 'sum'
AGG_CONFIG_AVG = 'avg'
AGG_CONFIG_MAA = 'maa'

METRIC_DAA = 'daa'
METRIC_WAA = 'waa'
METRIC_MAA = 'maa'
METRIC_QAA = 'qaa'
METRIC_AA_7D = 'aa_last7d'
METRIC_AA_30D = 'aa_last30d'

class JsonGen():
    def __init__(self, s3_bucket: str, cf_distribution_id: str, db_connector: DbConnector, api_version: str = 'v1'):
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        self.units = gtp_units
        self.metrics = gtp_metrics_new
        self.main_config = get_main_config(api_version=self.api_version)
        self.da_config = get_da_config(api_version=self.api_version)
        
        # Lazy load price to prevent crash on init if DB is flaky
        self._latest_eth_price = None

    @property
    def latest_eth_price(self):
        if self._latest_eth_price is None:
            self._latest_eth_price = self.db_connector.get_last_price_usd('ethereum')
        return self._latest_eth_price

    def _save_to_json(self, data: Dict, path: str):
        """Saves dictionary to JSON file."""
        full_path = f'output/{path}.json'
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w') as fp:
            # Pydantic dump cleans data, but we use allow_nan=False to force error or manual fix if needed.
            # Using custom fix_dict_nan before this call usually.
            json.dump(data, fp, ignore_nan=True)

    def _save_to_json_atomic(self, data: Dict, path: str):
        """Saves dictionary to JSON file via an atomic local move."""
        atomic_write_json(path, data, lambda payload, fp: json.dump(payload, fp, ignore_nan=True))
    
    def _get_metric_source(self, metric_id: str, origin_key: str, level: str) -> List[str]:
        query_params = {"metric_id": metric_id, "origin_key": origin_key, "level": level}
        df = execute_jinja_query(self.db_connector, "api/select_metric_source.sql.j2", query_params, return_df=True)
        if df.empty:
            return []
        sources: List[str] = []
        for source in df["source"].tolist():
            if pd.isna(source):
                continue
            source_str = str(source).strip()
            if source_str:
                sources.append(source_str)
        return sources

    def _alert_missing_metric_source(self, metric_id: str, origin_key: str, level: str):
        try:
            send_discord_message(
                f"[metrics] Missing source for metric_id='{metric_id}', origin_key='{origin_key}', level='{level}'."
            )
        except Exception as exc:
            logging.warning(f"Failed to send Discord alert for missing metric source: {exc}")

    def _get_raw_data_single_ok(self, origin_key: str, metric_key: str, days: Optional[int] = None) -> pd.DataFrame:
        """Get fact kpis from the database for a specific metric key (daily)."""
        logging.debug(f"Fetching raw data for origin_key={origin_key}, metric_key={metric_key}, days={days}")
        query_parameters = {'origin_key': origin_key, 'metric_key': metric_key, 'days': days}
        df = self.db_connector.execute_jinja("api/select_fact_kpis.sql.j2", query_parameters, load_into_df=True)
        
        if df.empty:
            return pd.DataFrame()
            
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['metric_key'] = metric_key
        # Ensure numeric comparisons and aggregations are safe even if DB returns strings.
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        
        # NOTE: Ideally move these calcs to SQL or Config
        if metric_key == 'gas_per_second':
            # Convert gas_per_second from gas units to millions of gas units for easier readability
            df['value'] = df['value'] / 1_000_000
        if metric_key == 'da_data_posted_bytes':
            # Convert da_data_posted_bytes from bytes to gigabytes for easier readability
            df['value'] = df['value'] / 1024 / 1024 / 1024
        return df
    
    def _get_raw_data_single_ok_granular(self, origin_key: str, metric_key: str, days: Optional[int] = None, granularity: str = 'hourly') -> pd.DataFrame:
        """Get fact kpis from the database for a specific metric key (granular)."""
        logging.debug(f"Fetching granular data for origin_key={origin_key}, metric_key={metric_key}, days={days}, granularity={granularity}")
        query_parameters = {
            'origin_key': origin_key,
            'metric_key': metric_key,
            'days': days,
            'granularity': granularity
        }
        df = self.db_connector.execute_jinja("api/select_fact_kpis_granular.sql.j2", query_parameters, load_into_df=True)
        
        if df.empty:
            return pd.DataFrame()
            
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['metric_key'] = metric_key
        # Ensure numeric comparisons and aggregations are safe even if DB returns strings.
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # NOTE: Ideally move these calcs to SQL or Config
        if metric_key == 'gas_per_second':
            # Convert gas_per_second from gas units to millions of gas units for easier readability
            df['value'] = df['value'] / 1_000_000
        if metric_key == 'da_data_posted_bytes':
            # Convert da_data_posted_bytes from bytes to gigabytes for easier readability
            df['value'] = df['value'] / 1024 / 1024 / 1024

        return df

    @staticmethod
    def _prepare_metric_key_data(df: pd.DataFrame, metric_key: str, max_date_fill: bool = True) -> pd.DataFrame:
        """Prepares metric data by trimming leading zeros and filling missing dates."""
        logging.debug(f"Preparing metric key data for {metric_key}. DataFrame shape: {df.shape}")
        if df.empty:
            return df
            
        # Trim leading zeros for a cleaner chart start
        df = df.loc[df["value"].ne(0).idxmax():].copy()
        if df.empty:
            return df
        logging.debug(f"After trimming leading zeros, DataFrame shape: {df.shape}")

        if max_date_fill:
            # Fill missing rows until yesterday with 0 for continuous time-series
            yesterday = pd.Timestamp.now(timezone.utc).normalize() - pd.Timedelta(days=1)
            start_date = min(df['date'].min(), yesterday)
            
            all_dates = pd.date_range(start=start_date, end=yesterday, freq='D', tz='UTC')
            df = df.set_index('date').reindex(all_dates)
            # Only fill numeric columns; avoid forcing string columns to 0.
            df['value'] = pd.to_numeric(df['value'], errors='coerce').fillna(0)
            df = df.reset_index().rename(columns={'index': 'date'})
            df['metric_key'] = metric_key
            logging.debug(f"After filling missing dates, DataFrame shape: {df.shape}")
            
        return df

    def _get_prepared_timeseries_df(self, origin_key: str, metric_keys: List[str], start_date: Optional[str], max_date_fill: bool) -> pd.DataFrame:
        """
        Fetches, prepares, and pivots the timeseries data for given metric keys,
        returning a single DataFrame.
        """
        now_utc = pd.Timestamp.now(timezone.utc)
        start_dt = pd.to_datetime(start_date or '2020-01-01').tz_localize('UTC') if not pd.to_datetime(start_date or '2020-01-01').tzinfo else pd.to_datetime(start_date)
        days = (now_utc - start_dt).days

        df_list = [
            self._prepare_metric_key_data(self._get_raw_data_single_ok(origin_key, mk, days), mk, max_date_fill)
            for mk in metric_keys
        ]

        valid_dfs = [df for df in df_list if not df.empty]
        if not valid_dfs:
            return pd.DataFrame()

        df_full = pd.concat(valid_dfs, ignore_index=True)
        df_pivot = df_full.pivot(index='date', columns='metric_key', values='value').sort_index()
        return df_pivot

    def _get_prepared_timeseries_df_granular(self, origin_key: str, metric_keys: List[str], days: int, granularity: str = 'hourly') -> pd.DataFrame:
        """
        Fetches, prepares, and pivots the granular timeseries data for given metric keys,
        returning a single DataFrame.
        """
        df_list = [
            self._prepare_metric_key_data(self._get_raw_data_single_ok_granular(origin_key, mk, days, granularity), mk, False)
            for mk in metric_keys
        ]

        valid_dfs = [df for df in df_list if not df.empty]
        if not valid_dfs:
            return pd.DataFrame()

        df_full = pd.concat(valid_dfs, ignore_index=True)
        df_pivot = df_full.pivot(index='date', columns='metric_key', values='value').sort_index()
        return df_pivot
        
    def _format_df_for_json(self, df: pd.DataFrame, units: List[str]) -> Tuple[List[List], List[str]]:
        """
        Takes a DataFrame, renames columns based on units, converts index to unix timestamp,
        and formats it into a list of lists and a list of column types for the JSON output.
        """
        if df.empty:
            return [], []
        
        df_formatted = df.reset_index()
        df_formatted.rename(columns={'date': 'unix'}, inplace=True)
        
        rename_map = {}
        value_cols = [col for col in df_formatted.columns if col != 'unix']
        if 'usd' in units or 'eth' in units:
            for col in value_cols:
                rename_map[col] = 'eth' if col.endswith('_eth') else 'usd'
        elif value_cols:
            rename_map[value_cols[0]] = 'value'
        
        df_formatted = df_formatted.rename(columns=rename_map)

        # Convert to milliseconds (13-digit unix), preserving tz-aware data
        unix_dt = pd.to_datetime(df_formatted['unix'], utc=True)
        unix_naive = unix_dt.dt.tz_convert('UTC').dt.tz_localize(None)
        # Force nanosecond resolution so we don't accidentally get day-level integers.
        unix_ns = unix_naive.astype('datetime64[ns]').astype('int64')
        df_formatted['unix'] = unix_ns // 1_000_000
        
        base_order = ['unix']
        present_cols = [col for col in ['usd', 'eth', 'value'] if col in df_formatted.columns]
        column_order = base_order + present_cols
        
        final_df = df_formatted[column_order]
        # Replace NaN with None for valid JSON (Pydantic handles this mostly, but good for safety)
        final_df = final_df.astype("object").where(pd.notna(final_df), None)
        
        return final_df.values.tolist(), final_df.columns.to_list()

    def _create_changes_dict(self, df: pd.DataFrame, metric_id: str, level: str, periods: Dict[str, int], agg_window: int, agg_method: str) -> Dict:
        """
        Calculates percentage change based on rolling aggregate windows over daily data.
        
        Args:
            df (pd.DataFrame): Input DataFrame with daily data, DatetimeIndex, and value columns.
            metric_id (str): The ID of the metric for config lookup.
            level (str): The level of the data (e.g., 'chains').
            periods (Dict[str, int]): Maps change key (e.g., '30d') to lookback period in days (e.g., 30).
            agg_window (int): The size of the aggregation window in days (e.g., 30 for monthly).
            agg_method (str): The aggregation method ('sum', 'mean', or 'last' for pre-aggregated values like MAA).
        """
        if df.empty:
            # Return empty structure with None
            return {'types': [], **{key: [] for key in periods}}

        metric_dict = self.metrics[level][metric_id]
        units = metric_dict.get('units', [])
        reverse_rename_map, final_types_ordered = self._get_column_type_mapping(df.columns.tolist(), units)

        changes_dict = {
            'types': final_types_ordered,
            **{key: [] for key in periods.keys()}
        }

        for final_type in final_types_ordered:
            original_col = reverse_rename_map.get(final_type)
            series = df[original_col]

            for key, period in periods.items():
                change_val = None
                if len(series) >= period + agg_window:
                    if agg_method == AGG_METHOD_LAST:
                        cur_val = series.iloc[-1]
                        prev_val = series.iloc[-(1 + period)]
                    else:
                        cur_val = series.iloc[-agg_window:].agg(agg_method)
                        prev_val = series.iloc[-(agg_window + period) : -period].agg(agg_method)

                    if pd.notna(cur_val) and pd.notna(prev_val) and prev_val > 0 and cur_val >= 0:
                        change = (cur_val - prev_val) / prev_val
                        change_val = round(change, 4)
                        if change_val > 100: 
                            change_val = 99.99
                
                changes_dict[key].append(change_val)
        return changes_dict

    def _get_column_type_mapping(self, df_columns: List[str], units: List[str]) -> Tuple[Dict[str, str], List[str]]:
        """Helper to map dataframe columns to final JSON types ('usd', 'eth', 'value')."""
        reverse_rename_map = {}
        if 'usd' in units or 'eth' in units:
            for col in df_columns:
                reverse_rename_map['eth' if col.endswith('_eth') else 'usd'] = col
        elif df_columns:
            reverse_rename_map['value'] = df_columns[0]
            
        final_types_ordered = []
        if 'usd' in reverse_rename_map: final_types_ordered.append('usd')
        if 'eth' in reverse_rename_map: final_types_ordered.append('eth')
        if 'value' in reverse_rename_map: final_types_ordered.append('value')
        return reverse_rename_map, final_types_ordered
    
    def _create_summary_values_dict(self, df: pd.DataFrame, metric_id: str, level: str, agg_window: int, agg_method: str) -> Dict:
        """
        Calculates a single aggregated value for the most recent period (e.g., last 30 days).
        """
        metric_dict = self.metrics[level][metric_id]
        units = metric_dict.get('units', [])
        reverse_rename_map, final_types_ordered = self._get_column_type_mapping(df.columns.tolist(), units)

        if df.empty or len(df) < agg_window:
            return {'types': final_types_ordered, 'data': [None] * len(final_types_ordered)}

        if agg_method == AGG_METHOD_LAST:
            aggregated_series = df.iloc[-1]
        else:
            aggregated_series = df.iloc[-agg_window:].agg(agg_method)
        
        data_list = []
        for t in final_types_ordered:
            val = aggregated_series.get(reverse_rename_map.get(t))
            data_list.append(val if pd.notna(val) else None)
            
        return {'types': final_types_ordered, 'data': data_list}

    def create_metric_per_chain_dict(self, origin_key: str, metric_id: str, level: str = 'chains', start_date: Optional[str] = None) -> Optional[Dict]:
        """Creates a Pydantic Model for metric/chain."""
        metric_dict = self.metrics[level][metric_id]
        daily_df = self._get_prepared_timeseries_df(origin_key, metric_dict['metric_keys'], start_date, metric_dict.get('max_date_fill', False))

        if daily_df.empty:
            return None

        agg_config = metric_dict.get('monthly_agg')
        
        if agg_config == AGG_CONFIG_SUM: agg_method = AGG_METHOD_SUM
        elif agg_config == AGG_CONFIG_AVG: agg_method = AGG_METHOD_MEAN
        elif agg_config == AGG_CONFIG_MAA: agg_method = AGG_METHOD_LAST
        else: raise ValueError(f"Invalid monthly_agg config '{agg_config}'")
            
        # --- AGGREGATIONS ---
        if agg_config == AGG_CONFIG_MAA:
            weekly_df = self._get_prepared_timeseries_df(origin_key, [METRIC_WAA], start_date, metric_dict.get('max_date_fill', False))
            monthly_df = self._get_prepared_timeseries_df(origin_key, [METRIC_MAA], start_date, metric_dict.get('max_date_fill', False))
            quarterly_df = self._get_prepared_timeseries_df(origin_key, [METRIC_QAA], start_date, metric_dict.get('max_date_fill', False))
        else:
            weekly_df = daily_df.resample('W-MON', label='left', closed='left').agg(agg_method)
            monthly_df = daily_df.resample('MS').agg(agg_method)
            quarterly_df = daily_df.resample('QS').agg(agg_method)

        daily_7d_dict = None
        if metric_dict.get('avg', False):
            rolling_avg_df = daily_df.rolling(window=7).mean().dropna(how='all')
            d7_data, d7_cols = self._format_df_for_json(rolling_avg_df, metric_dict['units'])
            daily_7d_dict = {'types': d7_cols, 'data': d7_data}
        
        # --- FORMATTING TIMESERIES ---
        daily_list, daily_cols = self._format_df_for_json(daily_df, metric_dict['units'])
        weekly_list, weekly_cols = self._format_df_for_json(weekly_df, metric_dict['units'])
        monthly_list, monthly_cols = self._format_df_for_json(monthly_df, metric_dict['units'])
        quarterly_list, quarterly_cols = self._format_df_for_json(quarterly_df, metric_dict['units'])

        hourly_timeseries = None
        hourly_changes = None
        if metric_dict.get('hourly_available', False):
            hourly_df = self._get_prepared_timeseries_df_granular(origin_key, metric_dict['metric_keys'], days=14, granularity='hourly')
            hourly_list, hourly_cols = self._format_df_for_json(hourly_df, metric_dict['units'])
            hourly_timeseries = {'types': hourly_cols, 'data': hourly_list}

            hourly_periods = {'1d': 24, '3d': 72, '7d': 168}
            hourly_changes = self._create_changes_dict(hourly_df, metric_id, level, hourly_periods, 1, AGG_METHOD_LAST)
            last_1h = self._create_summary_values_dict(hourly_df, metric_id, level, 1, AGG_METHOD_LAST)

        # --- CHANGES ---
        daily_periods = {'1d': 1, '7d': 7, '30d': 30, '90d': 90, '180d': 180, '365d': 365}
        weekly_periods = {'7d': 7, '28d': 28, '84d': 84, '365d': 365}
        monthly_periods = {'30d': 30, '90d': 90, '180d': 180, '365d': 365}

        daily_changes = self._create_changes_dict(daily_df, metric_id, level, daily_periods, 1, AGG_METHOD_LAST)
        
        if metric_id == METRIC_DAA:
            df_aa_weekly = self._get_prepared_timeseries_df(origin_key, [METRIC_AA_7D], start_date, metric_dict.get('max_date_fill', False))
            df_aa_monthly = self._get_prepared_timeseries_df(origin_key, [METRIC_AA_30D], start_date, metric_dict.get('max_date_fill', False))
            weekly_changes = self._create_changes_dict(df_aa_weekly, metric_id, level, weekly_periods, 7, AGG_METHOD_LAST)
            monthly_changes = self._create_changes_dict(df_aa_monthly, metric_id, level, monthly_periods, 30, AGG_METHOD_LAST)
        else:
            weekly_changes = self._create_changes_dict(daily_df, metric_id, level, weekly_periods, 7, agg_method)
            monthly_changes = self._create_changes_dict(daily_df, metric_id, level, monthly_periods, 30, agg_method)

        # --- SUMMARY ---
        if metric_id == METRIC_DAA:
            last_1d = self._create_summary_values_dict(daily_df, metric_id, level, 1, AGG_METHOD_LAST)
            last_7d = self._create_summary_values_dict(df_aa_weekly, metric_id, level, 7, AGG_METHOD_LAST)
            last_30d = self._create_summary_values_dict(df_aa_monthly, metric_id, level, 30, AGG_METHOD_LAST)
        else:
            last_1d = self._create_summary_values_dict(daily_df, metric_id, level, 1, agg_method)
            last_7d = self._create_summary_values_dict(daily_df, metric_id, level, 7, agg_method)
            last_30d = self._create_summary_values_dict(daily_df, metric_id, level, 30, agg_method)

        # --- BUILD PYDANTIC MODEL ---
        try:
            timeseries_dict = {
                'daily': {'types': daily_cols, 'data': daily_list},
                'weekly': {'types': weekly_cols, 'data': weekly_list},
                'monthly': {'types': monthly_cols, 'data': monthly_list},
                'quarterly': {'types': quarterly_cols, 'data': quarterly_list},
                'daily_7d_rolling': daily_7d_dict
            }
            if hourly_timeseries is not None:
                timeseries_dict['hourly'] = hourly_timeseries

            changes_dict = {
                'daily': daily_changes,
                'weekly': weekly_changes,
                'monthly': monthly_changes,
                # 'quarterly': None (Dictionaries allow omitting optional keys)
            }
            if hourly_changes is not None:
                changes_dict['hourly'] = hourly_changes

            response_model = MetricResponse(
                details=MetricDetails(
                    metric_id=metric_id,
                    metric_name=metric_dict['name'],
                    
                    # Update 1: Pass Dicts directly
                    timeseries=timeseries_dict,
                    
                    # Update 2: Pass Dicts directly
                    changes=changes_dict,
                    
                    # Update 3: Pass Dicts directly
                    summary={
                        'last_1h': last_1h if metric_dict.get('hourly_available', False) else {'types': last_1d['types'], 'data': [None] * len(last_1d['types'])},
                        'last_1d': last_1d,
                        'last_7d': last_7d,
                        'last_30d': last_30d
                    }
                ),
                last_updated_utc=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            )
            
            # Dump to dict and run nan fixer just to be 100% safe against invalid JSON
            output_dict = response_model.model_dump(mode='json')
            return fix_dict_nan(output_dict, f'metrics/{origin_key}/{metric_id}')
            
        except Exception as e:
            logging.error(f"Validation failed for {origin_key} - {metric_id}: {e}")
            return None
    
    def _process_and_save_metric(self, origin_key: str, metric_id: str, level: str, start_date: str, update_cadence: str = 'daily'):
        """
        Worker function to process and save/upload a single metric-chain combination.
        This is designed to be called by the ThreadPoolExecutor.
        """
        logging.debug(f"Processing: {origin_key} - {metric_id}")
        metric_dict = self.create_metric_per_chain_dict(origin_key, metric_id, level, start_date)

        if metric_dict:
            sources = self._get_metric_source(metric_id, origin_key, level)
            if sources:
                metric_dict.setdefault('details', {})['source'] = sources
            else:
                self._alert_missing_metric_source(metric_id, origin_key, level)

            s3_path = f'{self.api_version}/metrics/{level}/{origin_key}/{metric_id}'
            if self.s3_bucket is None:
                self._save_to_json(metric_dict, s3_path)
            else:
                if update_cadence == 'hourly':
                    # For hourly updates, we can skip invalidation to save costs, and rather set cache-control headers for a shorter TTL
                    upload_json_to_cf_s3(self.s3_bucket, s3_path, metric_dict, self.cf_distribution_id, invalidate=False, cache_control="public, max-age=60")
                    logging.info(f"SUCCESS: Exported {origin_key} - {metric_id} (with hourly cache control, no invalidation) ")
                else:
                    upload_json_to_cf_s3(self.s3_bucket, s3_path, metric_dict, self.cf_distribution_id, invalidate=False)
                    logging.info(f"SUCCESS: Exported {origin_key} - {metric_id}")
        else:
            logging.warning(f"NO DATA: Skipped export for {origin_key} - {metric_id}")
            
    def create_metric_jsons(self, metric_ids: Optional[List[str]] = None, origin_keys: Optional[List[str]] = None, level: str = 'chains', start_date='2020-01-01', max_workers: int = 5, invalidate=None):
        tasks = []
        if metric_ids:
            logging.info(f"Filtering tasks for specific metric IDs: {metric_ids}")
        else:
            logging.info(f"Generating task list for ALL metric IDs: {list(self.metrics[level].keys())}")
        
        if level == 'chains':
            logging.info(f"Running task list for Chains")
            config = self.main_config
            update_cadence = 'hourly'
        elif level == 'data_availability':
            logging.info(f"Running task list for Data Availability")
            config = self.da_config
            update_cadence = 'daily'
        else:
            raise ValueError(f"Invalid level '{level}'. Must be 'chains' or 'data_availability'.")
        
        if origin_keys:
            logging.info(f"Filtering tasks for specific origin keys: {origin_keys}")
        else:
            logging.info(f"Generating task list for ALL origin keys: {list(chain.origin_key for chain in config)}")
        
        for metric_id in self.metrics[level].keys():
            if metric_ids and metric_id not in metric_ids: continue
            if not self.metrics[level][metric_id].get('fundamental', False): continue

            for chain in config:
                if origin_keys and chain.origin_key not in origin_keys: continue
                if not chain.api_in_main: continue
                if metric_id in chain.api_exclude_metrics: continue
                
                tasks.append((chain.origin_key, metric_id))
        
        logging.info(f"Starting parallel processing for {len(tasks)} tasks with max_workers={max_workers}...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(self._process_and_save_metric, ok, mid, level, start_date, update_cadence): (ok, mid) for ok, mid in tasks}
            for future in as_completed(future_to_task):
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f'Task {future_to_task[future]} generated an exception: {exc}')

        if self.s3_bucket and self.cf_distribution_id and (update_cadence != 'hourly' or invalidate):
            logging.info("Invalidating CloudFront cache for all metrics...")
            empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/metrics/{level}/*')

    def get_chain_highlights_dict(self, origin_key: str, days: int = 7, limit: int = 5) -> List[Dict]:
        query_params = {"origin_key": origin_key, "days" : days, "limit": limit}
        df = execute_jinja_query(self.db_connector, 'api/select_highlights.sql.j2', query_params, return_df=True)
        if not df.empty:
            highlights =  highlights_prep(df, gtp_metrics_new)
            ## remove highlights that are excluded for this chain
            chain = next((c for c in self.main_config if c.origin_key == origin_key), None)
            for highlight in highlights:
                if chain:
                    metric_id = highlight['metric_id']
                    if metric_id in chain.api_exclude_metrics:
                        highlight['excluded'] = True
            highlights = [h for h in highlights if not h.get('excluded', False)]
            return highlights
                
        return []

    def get_chain_ranking_dict(self, origin_key):
        ranking_dict = {}
        for metric_id, metric in self.metrics['chains'].items():
            if metric["ranking_bubble"] == True:
                # Optimized: could move this check out
                comparison_oks = [
                    c.origin_key for c in self.main_config
                    if metric_id not in c.api_exclude_metrics
                    and c.api_in_main
                    and (self.api_version == 'dev' or c.api_deployment_flag == "PROD")
                ]
                
                mks = self.metrics['chains'][metric_id]['metric_keys']
                metric_key = [x for x in mks if not x.endswith('_eth')][0]

                df_rankings = self.db_connector.execute_jinja("api/select_rankings.sql.j2", {'comparison_oks': comparison_oks, 'metric_key': metric_key, 'origin_key': origin_key}, load_into_df=True)
                
                if df_rankings.empty:
                    ranking_dict[metric_id] = {'rank': -1, 'out_of': -1, 'color_scale': 0} # Placeholder to satisfy Schema if strictly needed, or Optional
                    continue

                rank = int(df_rankings['rank'].values[0])
                rank_max = int(df_rankings['out_of'].values[0])
                
                # Build dict based on schema structure
                item = {
                    'rank': rank, 
                    'out_of': rank_max, 
                    'color_scale': round(rank/rank_max, 2),
                }

                if 'usd' in self.metrics['chains'][metric_id]['units'].keys():
                    value_usd = float(df_rankings['value'].values[0])
                    item['value_usd'] = value_usd
                    item['value_eth'] = value_usd / self.latest_eth_price
                else:
                    item['value'] = float(df_rankings['value'].values[0])
                    
                ranking_dict[metric_id] = item
        return ranking_dict
    
    def get_kpi_cards_dict(self, chain: MainConfig):
        kpi_cards_dict = {}
        ordered_metrics = ['daa', 'throughput', 'stables_mcap', 'fees', 'app_revenue', 'fdv']
        
        # Filter relevant metrics
        relevant_metrics = [m for m in ordered_metrics if m in self.metrics['chains'] and m not in chain.api_exclude_metrics]
        
        # We can add others that are marked ranking_bubble but not in ordered list
        for m, meta in self.metrics['chains'].items():
            if meta['ranking_bubble'] and m != 'txcosts' and m not in chain.api_exclude_metrics and m not in relevant_metrics:
                relevant_metrics.append(m)

        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
        for metric_id in relevant_metrics:
             metric_dict = self.metrics['chains'][metric_id]
             
             daily_df = self._get_prepared_timeseries_df(chain.origin_key, metric_dict['metric_keys'], start_date, metric_dict.get('max_date_fill', False))
             daily_list, daily_cols = self._format_df_for_json(daily_df, metric_dict['units'])
             
             if not daily_list: continue

             current_values = daily_list[-1][1:] # Skip Unix
             
             wow_change = 0.00
             if len(daily_list) > 8:
                 last_week_values = daily_list[-8][1:]
                 # This logic creates a list of float changes? Or just one? 
                 # Original code: [(x-y)/y ...] -> This is a list.
                 # Schema needs to support List or Float.
                 wow_data = [(x - y) / y if y != 0 else 0 for x, y in zip(current_values, last_week_values)]
                 wow_change = {'types': daily_cols[1:], 'data': wow_data}
             else:
                 wow_change = {'types': ["value"], 'data': [0.0]}

             kpi_cards_dict[metric_id] = {
                 'sparkline': {'types': daily_cols, 'data': daily_list},
                 'current_values': {'types': daily_cols[1:], 'data': current_values},
                 'wow_change': wow_change
             }
             
        return kpi_cards_dict
    
    def get_blockspace_dict(self, chain: MainConfig):
        if 'blockspace' not in chain.api_exclude_metrics:
            df = execute_jinja_query(self.db_connector, "api/select_blockspace_main_categories.sql.j2", {"origin_key": chain.origin_key, "days": 7}, return_df=True) 
            return { "types": df.columns.tolist(), "data": df.values.tolist()}
        return {"types": [], "data": []}

    def create_blockspace_tree_map_json(self):
        chain_list = [
            chain.origin_key
            for chain in self.main_config
            if chain.api_in_main
            and chain.api_in_apps
            and (self.api_version == 'dev' or chain.api_deployment_flag == 'PROD')
        ]

        if not chain_list:
            logging.warning("No chains eligible for blockspace tree map export.")
            return

        query = f"""
            WITH base AS (
                SELECT 
                    fact.date,
                    concat('0x', encode(fact.address, 'hex')) AS address,
                    fact.origin_key,
                    oli.contract_name,
                    coalesce(oli.owner_project, 'unlabeled') AS owner_project,
                    coalesce(cat.main_category_id, 'unlabeled') AS main_category_key,
                    coalesce(oli.usage_category, 'unlabeled') AS sub_category_key,
                    sum(fact.txcount) AS txcount,
                    sum(fact.gas_fees_eth) AS fees_paid_eth,
                    sum(fact.gas_fees_usd) AS fees_paid_usd,
                    sum(fact.daa) AS daa
                FROM blockspace_fact_contract_level fact
                LEFT JOIN vw_oli_label_pool_gold_pivoted_cross_chain_v1 oli
                    ON fact.address = oli.address
                    AND fact.origin_key = oli.origin_key
                LEFT JOIN oli_categories cat
                    ON oli.usage_category = cat.category_id::text
                WHERE fact.date >= current_date - interval '7 days'
                    AND fact.origin_key IN ({','.join([f"'{chain}'" for chain in chain_list])})
                GROUP BY 1,2,3,4,5,6,7
                ),
                unlabeled AS (
                SELECT * FROM base
                WHERE sub_category_key = 'unlabeled'
                ),
                ranked AS (
                SELECT
                    *,
                    row_number() OVER (PARTITION BY date, origin_key ORDER BY txcount DESC) AS rn
                FROM unlabeled
                ),
                top20 AS (
                SELECT * FROM ranked WHERE rn <= 20
                ),
                others AS (
                SELECT
                    date,
                    'all others' AS address,
                    origin_key,
                    'Unlabeled (Others)' AS contract_name,
                    'unlabeled' AS owner_project,
                    'unlabeled' AS main_category_key,
                    'unlabeled' AS sub_category_key,
                    sum(txcount) AS txcount,
                    sum(fees_paid_eth) AS fees_paid_eth,
                    sum(fees_paid_usd) AS fees_paid_usd,
                    sum(daa) AS daa
                FROM ranked
                WHERE rn > 20
                GROUP BY 1,3
                ),
                labeled AS (
                SELECT * FROM base
                WHERE sub_category_key <> 'unlabeled'
                )
                SELECT * FROM labeled
                UNION ALL
                SELECT date, address, origin_key, contract_name, owner_project, main_category_key, sub_category_key, txcount, fees_paid_eth, fees_paid_usd, daa
                FROM top20
                UNION ALL
                SELECT * FROM others;
        """

        df = self.db_connector.execute_query(query, load_df=True)
        if df.empty:
            logging.warning("No data returned for blockspace tree map export.")
            return

        df['date'] = df['date'].astype(str)

        try:
            response_model = TreeMapResponse(
                data={'types': df.columns.to_list(), 'data': df.values.tolist()},
                last_updated_utc=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            )
            tree_map_dict = fix_dict_nan(response_model.model_dump(mode='json'), 'blockspace/tree_map', False)
        except Exception as e:
            logging.error(f"Tree map validation failed: {e}")
            return

        s3_path = f'{self.api_version}/blockspace/tree_map'
        if self.s3_bucket:
            upload_json_to_cf_s3(self.s3_bucket, s3_path, tree_map_dict, self.cf_distribution_id, invalidate=False)
        else:
            self._save_to_json(tree_map_dict, s3_path)

        logging.info('DONE -- Tree Map')

    def temp_megaeth_apps_export(self): 
        query = """
            SELECT "date", owner_project, sum(txcount) as txcount
            FROM public.vw_apps_contract_level_materialized
            where origin_key = 'megaeth'
            group by 1,2
            order by 1 desc
        """

        df =self.db_connector.execute_query(query, True)
        
        if df.empty:
            logging.warning("No data returned for blockspace tree map export.")
            return

        df['date'] = df['date'].astype(str)
        
        data_dict = {
             "data": {
                 'types': df.columns.to_list(), 
                  'data': df.values.tolist()
            },
            "last_updated_utc": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        }
        data_dict = fix_dict_nan(data_dict, 'export/megaeth_apps')

        s3_path = f'{self.api_version}/export/megaeth_apps'
        if self.s3_bucket:
            upload_json_to_cf_s3(self.s3_bucket, s3_path, data_dict, self.cf_distribution_id, invalidate=False)
        else:
            self._save_to_json(data_dict, s3_path)

        logging.info('DONE -- MegaETH Apps Export')

    def create_contract_owner_project_mappings_json(self):
        chain_list = [
            chain.origin_key
            for chain in self.main_config
            if chain.api_in_apps
        ]

        if not chain_list:
            logging.warning("No chains eligible for contract owner_project mappings export.")
            return

        query = f"""
            SELECT
                '0x' || encode(address, 'hex') AS address,
                max(owner_project) AS owner_project
            FROM public.vw_oli_label_pool_gold_pivoted_cross_chain_v1
            WHERE owner_project IS NOT NULL
                AND origin_key IN ({','.join([f"'{chain}'" for chain in chain_list])})
            GROUP BY 1
            ORDER BY address
        """

        df = self.db_connector.execute_query(query, load_df=True)
        if df.empty:
            logging.warning("No data returned for contract owner_project mappings export.")
            return

        data_dict = {
            "data": {
                "types": df.columns.to_list(),
                "data": df.values.tolist()
            },
            "last_updated_utc": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        }
        data_dict = fix_dict_nan(data_dict, 'apps/contract_mappings')

        s3_path = f'{self.api_version}/apps/contract_mappings'
        if self.s3_bucket:
            upload_json_to_cf_s3(self.s3_bucket, s3_path, data_dict, self.cf_distribution_id, invalidate=False)
        else:
            self._save_to_json(data_dict, s3_path)

        logging.info('DONE -- Contract Owner Project Mappings Export')

    def get_ecosystem_dict(self, origin_keys: List[str]) -> dict:
        top_apps = execute_jinja_query(self.db_connector, "api/select_top_apps.sql.j2", {"origin_keys": origin_keys, "days": 7, "limit": 5000}, return_df=True)
        if top_apps.empty: return {} # Handle empty case

        top_apps['txcount'] = top_apps['txcount'].astype(int)
        
        active_apps = execute_jinja_query(self.db_connector, "api/select_count_apps.sql.j2", {"origin_keys": origin_keys, "days": 7}, return_df=True)
        active_apps_count = int(active_apps.values[0][0]) if not active_apps.empty else 0
        
        return {
            "active_apps": {"count": active_apps_count},
            "apps": {"types": top_apps.columns.tolist(), "data": top_apps.values.tolist()},
        }
    
    def get_lifetime_achievements_dict(self, chain: str) -> dict:
        achievements = {}
        metrics_sum = ['txcount', 'fees', 'profit', 'rent_paid']
        metric_keys_map = {mk: metric for metric in metrics_sum for mk in gtp_metrics_new['chains'][metric]['metric_keys']}
        
        mk_list = [mk.replace('txcount', 'txcount_plain') for mk in metric_keys_map.keys()]
        
        df = execute_jinja_query(self.db_connector, "api/select_fact_kpis_achievements.sql.j2", {'metric_keys': mk_list, 'origin_key': chain}, return_df=True)
        
        if not df.empty:
            df['metric_key'] = df['metric_key'].str.replace('txcount_plain', 'txcount')
            for row in df.to_dict(orient='records'):
                mk = row['metric_key']
                if mk in levels_dict:
                    ach = self._calculate_achievement(row['total_value'], levels_dict[mk])
                    if ach:
                        mid = metric_keys_map.get(mk)
                        if mid:
                            if mid not in achievements: achievements[mid] = {}
                            unit = 'value' if len(gtp_metrics_new['chains'][mid]['units']) == 1 else mk[-3:]
                            achievements[mid][unit] = ach

        # DAA Separate
        df_daa = execute_jinja_query(self.db_connector, "api/select_total_aa.sql.j2", {'origin_key': chain}, return_df=True)
        if not df_daa.empty:
            if df_daa.iloc[0]['value']:            
                ach = self._calculate_achievement(df_daa.iloc[0]['value'], levels_dict['daa'])
                if ach:
                    achievements['daa'] = {'value': ach}

        return achievements

    def _calculate_achievement(self, total_value: float, levels: dict) -> Optional[Dict]:
        sorted_levels = sorted(levels.items(), key=lambda x: x[1], reverse=True)
        for level, threshold in sorted_levels:
            if total_value >= threshold:
                max_level = max(levels.keys())
                if level == max_level:
                    percent = None
                else:
                    next_thresh = levels[level + 1]
                    percent = (total_value - threshold) / (next_thresh - threshold) * 100 if next_thresh > threshold else 100.0
                
                return {'level': level, 'total_value': total_value, 'percent_to_next_level': round(percent, 2) if percent is not None else None}
        return None
    
    def get_streaks_dict(self, origin_key: str) -> dict:
        df = execute_jinja_query(self.db_connector, "api/select_streak_length.sql.j2", {'origin_key': origin_key}, return_df=True)
        streaks = {}
        if df.empty: return streaks

        for metric_id in gtp_metrics_new['chains']:
            for metric_key in gtp_metrics_new['chains'][metric_id]['metric_keys']:
                if metric_key in df['metric_key'].values:
                    row = df.loc[df['metric_key'] == metric_key].iloc[0]
                    raw_y = row['yesterdays_value'] if 'yesterdays_value' in df.columns else None
                    if raw_y is None or pd.isna(raw_y):
                        send_discord_message(f"JSON GEN: Missing yesterdays_value for streaks: chain={origin_key} metric_key={metric_key}")
                        continue

                    try:
                        y_val = float(raw_y)
                    except (TypeError, ValueError):
                        logging.warning(
                            "Skipping invalid yesterdays_value for chain=%s metric_key=%s value=%r",
                            origin_key, metric_key, raw_y
                        )
                        send_discord_message(f"JSON GEN: Invalid yesterdays_value for streaks: chain={origin_key} metric_key={metric_key} value={raw_y}")
                        continue

                    if metric_id not in streaks: streaks[metric_id] = {}
                    unit = 'value' if len(gtp_metrics_new['chains'][metric_id]['units']) == 1 else metric_key[-3:]
                    streaks[metric_id][unit] = {
                        'streak_length': int(row['current_streak_length']),
                        'yesterday_value': round(y_val, 4)
                    }
        return streaks
    
    def create_chains_dict(self, origin_key:str) -> Optional[Dict]:
        chain = next((c for c in self.main_config if c.origin_key == origin_key), None) 
        if not chain: 
            print(f"Chain with origin_key '{origin_key}' not found in configuration.")
            return None

        try:
            # Gather Data
            highlights = self.get_chain_highlights_dict(origin_key, days=5, limit=4)
            ranking = self.get_chain_ranking_dict(origin_key)
            kpi_cards = self.get_kpi_cards_dict(chain)
            #streaks = self.get_streaks_dict(origin_key)
            streaks = {}
            lifetime = self.get_lifetime_achievements_dict(origin_key)
            blockspace = self.get_blockspace_dict(chain)
            ecosystem = self.get_ecosystem_dict([chain.origin_key]) if chain.api_in_apps else {}

            # Create Pydantic Model
            response_model = ChainOverviewResponse(
                data=ChainData(
                    chain_id=chain.origin_key,
                    chain_name=chain.name,
                    highlights=highlights,
                    events=chain.events,
                    ranking=ranking,
                    kpi_cards=kpi_cards,
                    achievements=ChainAchievements(streaks=streaks, lifetime=lifetime),
                    blockspace=blockspace,
                    ecosystem=ecosystem
                ),
                last_updated_utc=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            )
            
            output_dict = response_model.model_dump(mode='json')
            return fix_dict_nan(output_dict, f'chains/{origin_key}/overview')

        except Exception as e:
            logging.error(f"Chain overview validation failed for {origin_key}: {e}")
            return None
    
    def _process_and_save_chain_overview(self, origin_key: str):
        chain_dict = self.create_chains_dict(origin_key)
        if chain_dict:
            s3_path = f'{self.api_version}/chains/{origin_key}/overview'
            if self.s3_bucket is None:
                self._save_to_json(chain_dict, s3_path)
            else:
                upload_json_to_cf_s3(self.s3_bucket, s3_path, chain_dict, self.cf_distribution_id, invalidate=False)
            logging.info(f"SUCCESS: Exported {origin_key}")
        else:
            logging.warning(f"NO DATA: Skipped export for {origin_key}")

    def create_chains_jsons(self, origin_keys:Optional[list[str]]=None, max_workers: int = 5):
        tasks = []
        for chain in self.main_config:
            if origin_keys and chain.origin_key not in origin_keys: continue
            if not chain.api_in_main: continue
            tasks.append((chain.origin_key))
        
        logging.info(f"Processing {len(tasks)} chains...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(self._process_and_save_chain_overview, ok): ok for ok in tasks}
            for future in as_completed(future_to_task):
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f'Task generated an exception: {exc}')

        if self.s3_bucket and self.cf_distribution_id:
            empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/chains/*')

    def create_seo_jsons(self):
        logging.info("Generating SEO summary JSON artifacts...")
        builder = SeoJsonBuilder(
            db_connector=self.db_connector,
            main_config=self.main_config,
            api_version=self.api_version,
            logger=logging.getLogger(__name__),
            alert_func=send_discord_message,
        )
        payloads = builder.build_all()

        for family, payload in payloads.items():
            s3_path = f'{self.api_version}/seo/{family}'
            if self.s3_bucket:
                upload_json_to_cf_s3(
                    self.s3_bucket,
                    s3_path,
                    payload,
                    self.cf_distribution_id,
                    invalidate=False,
                    cache_control="public, max-age=300, s-maxage=86400, stale-while-revalidate=3600, stale-if-error=86400",
                )
            else:
                self._save_to_json_atomic(payload, s3_path)
            logging.info("SUCCESS: Exported SEO JSON %s", family)

        if self.s3_bucket and self.cf_distribution_id:
            empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/seo/*')

    def create_streaks_today_json(self):
        logging.info("Generating streaks_today JSON...")
        streaks_today_dict = {}

        for chain in self.main_config:
            if chain.api_in_main and chain.origin_key not in ['loopring', 'megaeth', 'polygon_pos']:
                params = {'origin_key': chain.origin_key, 'custom_gas': chain.origin_key in ['mantle', 'metis', 'gravity', 'plume', 'celo']}
                df = execute_jinja_query(self.db_connector, "api/select_streak_today.sql.j2", params, return_df=True)
                
                if not df.empty:
                    streaks_today_dict[chain.origin_key] = {}
                    for metric_id in gtp_metrics_new['chains']:
                        for metric_key in gtp_metrics_new['chains'][metric_id]['metric_keys']:
                            for col in df.columns:
                                if metric_key == col:
                                    if metric_id not in streaks_today_dict[chain.origin_key]:
                                        streaks_today_dict[chain.origin_key][metric_id] = {}
                                    unit = 'value' if len(gtp_metrics_new['chains'][metric_id]['units']) == 1 else metric_key[-3:]
                                    val = df[col].values[0]
                                    streaks_today_dict[chain.origin_key][metric_id][unit] = float(val) if val is not None else 0.0

        if not streaks_today_dict: return

        try:
            response = StreaksResponse(
                data=streaks_today_dict, 
                last_updated_utc=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            )
            output = response.model_dump(mode='json')
            s3_path = f'{self.api_version}/chains/all/streaks_today'
            if self.s3_bucket:
                upload_json_to_cf_s3(self.s3_bucket, s3_path, output, self.cf_distribution_id, invalidate=False, cache_control="public, max-age=60")
            else:
                self._save_to_json(output, s3_path)
        except Exception as e:
            logging.error(f"Streaks today validation failed: {e}")

    # def create_ecosystem_builders_json(self):
    #     logging.info("Generating ecosystem_apps JSON...")
    #     origin_keys = [c.origin_key for c in self.main_config if c.api_in_apps]
    #     ecosystem_dict = self.get_ecosystem_dict(origin_keys)
        
    #     if not ecosystem_dict: return

    #     try:
    #         response = EcosystemResponse(
    #             data={'ecosystem': ecosystem_dict},
    #             last_updated_utc=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    #         )
    #         output = response.model_dump(mode='json')
    #         s3_path = f'{self.api_version}/ecosystem/builders'
    #         if self.s3_bucket:
    #             upload_json_to_cf_s3(self.s3_bucket, s3_path, output, self.cf_distribution_id, invalidate=True)
    #         else:
    #             self._save_to_json(output, s3_path)
    #     except Exception as e:
    #         logging.error(f"Ecosystem builders validation failed: {e}")

    def create_user_insights_json(self, origin_keys:Optional[list[str]]=None):
        for chain in self.main_config:
            if origin_keys and chain.origin_key not in origin_keys: continue
            if not chain.api_in_main or not chain.api_in_user_insights: continue
            
            logging.info(f"Generating user_insights JSON for {chain.origin_key}...")
            
            # New User Contracts
            nuc_dict = {}
            for days in [1, 7]:
                df = execute_jinja_query(self.db_connector, 'api/select_new_user_contracts.sql.j2', {"days": days, "origin_key": chain.origin_key, "limit": 10}, return_df=True)
                if not df.empty:
                    df = db_addresses_to_checksummed_addresses(df, ['address'])
                    nuc_dict[f'{days}d'] = {"types": df.columns.tolist(), "data": df.values.tolist()}
                else:
                    nuc_dict[f'{days}d'] = {"types": [], "data": []}

            # Cross Chain Addresses
            cca_dict = {}
            for days in [1, 7]:
                df = execute_jinja_query(self.db_connector, "api/select_cross_chain_addresses.sql.j2", {'origin_key': chain.origin_key, 'days': days, 'limit': 11}, return_df=True)

                mk = 'aa_last7d' if days == 7 else 'daa'
                total_q = f"select value from fact_kpis where origin_key = '{chain.origin_key}' and metric_key = '{mk}' order by date desc limit 1"
                total = self.db_connector.execute_query(total_q, load_df=True).value.values[0]
                
                df = pd.concat([df, pd.DataFrame([{'cross_chain': 'total', 'current': total, 'previous': 0, 'change': 0, 'change_percent': 0}])], ignore_index=True)
                df['share_of_users'] = df['current'] / total
                cca_dict[f'{days}d'] = {"types": df.columns.tolist(), "data": df.values.tolist()}
            
            try:
                response = UserInsightsResponse(
                    data=UserInsightsData(new_user_contracts=nuc_dict, cross_chain_addresses=cca_dict),
                    last_updated_utc=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                )
                output = fix_dict_nan(response.model_dump(mode='json'), f'chains/{chain.origin_key}/user_insights')
                
                s3_path = f'{self.api_version}/chains/{chain.origin_key}/user_insights'
                if self.s3_bucket:
                    upload_json_to_cf_s3(self.s3_bucket, s3_path, output, self.cf_distribution_id, invalidate=False)
                else:
                    self._save_to_json(output, s3_path)
            except Exception as e:
                logging.error(f"User insights validation failed for {chain.origin_key}: {e}")

        if self.s3_bucket:
             empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/chains/*')

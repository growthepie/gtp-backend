import os
import math
import simplejson as json
import pandas as pd
from datetime import datetime, timezone

from src.main_config import get_main_config, get_all_l2_config
from src.misc.helper_functions import upload_json_to_cf_s3, db_addresses_to_checksummed_addresses, fix_dict_nan, empty_cloudfront_cache, send_discord_message
from src.db_connector import DbConnector

class BlockspaceJSONCreation():

    def __init__(self, s3_bucket, cf_distribution_id, db_connector:DbConnector, api_version, drop_daily_7d_rolling:bool=False):
        ## Constants
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        self.main_conf = get_main_config()
        self.all_l2_config = get_all_l2_config()

        ## Feature flag — when ON, the SMALL category_comparison split files omit `daily_7d_rolling`
        ## (the client recomputes it from `daily`). Default OFF; only flip ON after the frontend PR
        ## that computes the rolling mean client-side has shipped. The large category_comparison.json
        ## is never touched by this. Honors env var so ops can flip it without a code change.
        self.drop_daily_7d_rolling = drop_daily_7d_rolling or os.getenv("BLOCKSPACE_DROP_DAILY_7D_ROLLING", "false").strip().lower() in ("1", "true", "yes")

    def _fillna_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = df.select_dtypes(include="number").columns
        if len(numeric_cols) > 0:
            df[numeric_cols] = df[numeric_cols].fillna(0)
        return df
    
    def get_blockspace_overview_daily_data(self, chain_keys):
        where_origin_key = f"AND bs_scl.origin_key = '{chain_keys[0]}'" if len(chain_keys) == 1 else "AND bs_scl.origin_key IN ('" + "','".join(chain_keys) + "')"
        select_origin_key = f"bs_scl.origin_key as chain_key" if len(chain_keys) == 1 else "'all_l2s' as chain_key"
        group_by_origin_key = f"bs_scl.origin_key" if len(chain_keys) == 1 else "chain_key"
        
        exec = f"""
            WITH totals AS (
                SELECT
                    bs_scl.date,
                    {select_origin_key},
                    SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                    SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                    SUM(bs_scl.txcount) AS txcount
                FROM
                    public.blockspace_fact_category_level bs_scl
                WHERE
                    bs_scl."date" < date_trunc('day', NOW())
                    AND bs_scl.category_id != 'total_usage'
                    {where_origin_key}
                GROUP BY
                    bs_scl."date",
                    {group_by_origin_key}
            )
            SELECT
                bs_scl.date,
                {select_origin_key},
                bs_cm.main_category_id as main_category_key,
                SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                SUM(bs_scl.txcount) AS txcount,
                SUM(bs_scl.gas_fees_eth) / NULLIF(totals.gas_fees_eth, 0) AS gas_fees_share_eth,
                SUM(bs_scl.gas_fees_usd) / NULLIF(totals.gas_fees_usd, 0) AS gas_fees_share_usd,
                SUM(bs_scl.txcount) / NULLIF(totals.txcount, 0) AS txcount_share
            FROM
                public.blockspace_fact_category_level bs_scl
            LEFT JOIN 
                vw_oli_category_mapping bs_cm 
                ON lower(bs_scl.category_id) = lower(bs_cm.category_id) 
            JOIN 
                totals 
                ON bs_scl.date = totals.date
            WHERE
                bs_scl."date" < date_trunc('day', NOW())
                AND bs_scl.category_id != 'total_usage'
                {where_origin_key}
            GROUP BY
                bs_scl."date",
                {group_by_origin_key},
                bs_cm.main_category_id,
                totals.gas_fees_eth,
                totals.gas_fees_usd,
                totals.txcount
            ORDER BY
                bs_scl."date" ASC
        """

        df = pd.read_sql(exec, self.db_connector.engine.connect())

        # date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df.drop(columns=['date'], inplace=True)
        self._fillna_numeric(df)

        return df
    
    def get_blockspace_overview_timeframe_overview(self, chain_keys, days):
        date_where = f"AND bs_scl.date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ""
        where_origin_key = f"AND bs_scl.origin_key = '{chain_keys[0]}'" if len(chain_keys) == 1 else "AND bs_scl.origin_key IN ('" + "','".join(chain_keys) + "')"
        select_origin_key = f"bs_scl.origin_key as chain_key" if len(chain_keys) == 1 else "'all_l2s' as chain_key"
        group_by_origin_key = f"bs_scl.origin_key" if len(chain_keys) == 1 else "chain_key"
        
        exec = f"""
            WITH totals AS (
                SELECT
                    {select_origin_key},
                    SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                    SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                    SUM(bs_scl.txcount) AS txcount
                FROM
                    public.blockspace_fact_category_level bs_scl
                WHERE
                    bs_scl."date" < date_trunc('day', NOW())
                    {date_where}
                    AND bs_scl.category_id != 'total_usage'
                    {where_origin_key}
                GROUP BY
                    {group_by_origin_key}
            )

                
            SELECT
                {select_origin_key},
                bs_cm.main_category_id as main_category_key,
                SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                SUM(bs_scl.txcount) AS txcount,
                SUM(bs_scl.gas_fees_eth) / NULLIF(totals.gas_fees_eth, 0) AS gas_fees_share_eth,
                SUM(bs_scl.gas_fees_usd) / NULLIF(totals.gas_fees_usd, 0) AS gas_fees_share_usd,
                SUM(bs_scl.txcount) / NULLIF(totals.txcount, 0) AS txcount_share
            FROM
                public.blockspace_fact_category_level bs_scl
            LEFT JOIN 
                vw_oli_category_mapping bs_cm 
                ON lower(bs_scl.category_id) = lower(bs_cm.category_id)
            JOIN
                totals
                ON chain_key = totals.chain_key
            WHERE
                bs_scl."date" < DATE_TRUNC('day', NOW())
                {date_where}
                -- ignore total_usage
                and bs_scl.category_id != 'total_usage'
                {where_origin_key}
            GROUP BY
                bs_cm.main_category_id,
                {group_by_origin_key},
                totals.gas_fees_eth,
                totals.gas_fees_usd,
                totals.txcount
            ORDER BY
                2 ASC
        """

        df = pd.read_sql(exec, self.db_connector.engine.connect())

        # fill NaN values with 0
        self._fillna_numeric(df)

        return df
    
    def get_blockspace_totals_timeframe(self, chain_keys, days):
        date_where = f"AND bs_scl.date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ""
        select_origin_key = f"bs_scl.origin_key as chain_key" if len(chain_keys) == 1 else "'all_l2s' as chain_key"
        where_origin_key = f"AND bs_scl.origin_key = '{chain_keys[0]}'" if len(chain_keys) == 1 else "AND bs_scl.origin_key IN ('" + "','".join(chain_keys) + "')"
    
        exec = f"""
            SELECT
                {select_origin_key},
                SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                SUM(bs_scl.txcount) AS txcount
            FROM public.blockspace_fact_category_level bs_scl
            WHERE bs_scl."date" < DATE_TRUNC('day', NOW())
                {date_where}
                and bs_scl.category_id = 'total_usage'
                {where_origin_key}
            GROUP BY 1
        """
        df = pd.read_sql(exec, self.db_connector.engine.connect())

        # fill NaN values with 0
        self._fillna_numeric(df)

        return df

    def get_category_mapping(self):
        exec_string = "SELECT * FROM vw_oli_category_mapping"
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())
        return df
    
    ##### FILE HANDLERS #####
    def save_to_json(self, data, path, minify=False):
        #create directory if not exists
        os.makedirs(os.path.dirname(f'output/{self.api_version}/{path}.json'), exist_ok=True)
        ## save to file
        with open(f'output/{self.api_version}/{path}.json', 'w') as fp:
            if minify:
                json.dump(data, fp, ignore_nan=True, separators=(",", ":"))
            else:
                json.dump(data, fp, ignore_nan=True)

    @staticmethod
    def _round_sig(x, sig=6):
        ## round a float to `sig` significant figures (not decimal places), keeping the
        ## meaningful digits of small shares while shedding trailing float64 noise on large values.
        if x == 0 or not math.isfinite(x):
            return x
        return round(x, -int(math.floor(math.log10(abs(x)))) + (sig - 1))

    @classmethod
    def _normalize_numbers(cls, o, sig=6):
        ## Display-lossless precision normalization applied to the SMALL JSONs only:
        ##   - integer-valued floats -> int (drops the trailing `.0`, e.g. 211515.0 -> 211515)
        ##   - all other floats -> `sig` significant figures
        ## Strings (addresses, names, category keys) and ints are left untouched. Builds new
        ## containers, so the source dict (and therefore the already-written large file) is unaffected.
        if isinstance(o, bool):
            return o
        if isinstance(o, float):
            if o.is_integer():
                return int(o)
            return cls._round_sig(o, sig)
        if isinstance(o, list):
            return [cls._normalize_numbers(v, sig) for v in o]
        if isinstance(o, dict):
            return {k: cls._normalize_numbers(v, sig) for k, v in o.items()}
        return o

    def _save_or_upload_json(self, data, path, invalidate=True, normalize=True):
        ## single entry point that mirrors the local-vs-s3 branching used by every export.
        ## `path` is relative to the api_version root, e.g. 'blockspace/overview/base'.
        ## Small-JSON exports are precision-normalized and minified here; the large combined files
        ## are written via save_to_json / upload_json_to_cf_s3 directly and are NOT affected.
        if normalize:
            data = self._normalize_numbers(data)
        if self.s3_bucket == None:
            self.save_to_json(data, path, minify=True)
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/{path}', data, self.cf_distribution_id, invalidate=invalidate, minify=True)

    ##### ANSWER (precomputed) JSON HELPERS #####
    @staticmethod
    def _resolve_value(types, row, name):
        ## resolve a value out of a (types, row) pair by the column NAME, never a fixed index,
        ## so an upstream reorder of `types` cannot silently corrupt the value. Returns None if
        ## the name is absent or the row is malformed.
        if not isinstance(types, list) or not isinstance(row, (list, tuple)):
            return None
        try:
            idx = types.index(name)
        except ValueError:
            return None
        if idx >= len(row):
            return None
        return row[idx]

    @staticmethod
    def _is_finite_positive(v):
        return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v > 0

    @staticmethod
    def _num(v):
        ## coerce to a finite number, else 0 (used for L1/L2 totals where a missing slice == 0).
        return v if (isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)) else 0

    @staticmethod
    def _generated_at_iso():
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    def _get_l2_universe(self):
        ## The curated "L2 universe" shared by the answer endpoints, derived from main_config
        ## (equivalent to master.json's deployment/bucket fields). Include a chain iff:
        ##   - api_deployment_flag == 'PROD'  (drops DEV / ZIRCUIT)
        ##   - bucket not in {'Layer 1', '-'}
        ##   - origin_key not in {ethereum, polygon_pos, all_l2s, multiple}
        ## Returns (universe_keys_sorted, excluded_non_l2_keys_sorted). excludedNonL2Keys = PROD,
        ## non-special chains dropped by the bucket filter (keys, not display names).
        special = {'ethereum', 'polygon_pos', 'all_l2s', 'multiple'}
        non_l2_buckets = {'Layer 1', '-'}
        universe = []
        excluded = []
        for chain in self.main_conf:
            key = chain.origin_key
            if key in special:
                continue
            if getattr(chain, 'api_deployment_flag', None) != 'PROD':
                continue
            if getattr(chain, 'bucket', None) in non_l2_buckets:
                excluded.append(key)
                continue
            universe.append(key)
        return sorted(universe), sorted(excluded)

    ##### JSON GENERATION METHODS #####

    def create_blockspace_overview_json(self):
        overview_dict = {
            "data": {
                "metric_id": "overview",
                "chains": {}
            }
        }

        # define the timeframes in days for the overview data
        overview_timeframes = [7, 30, 90, 180, 365, "max"]

        # get main_category_keys in the blockspace_df
        category_mapping = self.get_category_mapping()

        main_category_keys = category_mapping['main_category_id'].unique().tolist()

        ## put all origin_keys in a list where in_api is True
        chain_keys = [chain.origin_key for chain in self.main_conf if chain.api_in_main == True and 'blockspace' not in chain.api_exclude_metrics]

        #for chain_key in config:
        for chain in self.all_l2_config:
            origin_key = chain.origin_key

            if chain.api_in_main == False:
                print(f'-- SKIPPED -- Chain blockspace overview export for {origin_key}. API is set to False')
                continue

            if 'blockspace' in chain.api_exclude_metrics:
                print(f'-- SKIPPED -- Chain blockspace overview export for {origin_key}. Blockspace is in exclude_metrics')
                continue

            print(f"Processing {origin_key}")

            # get daily data (multiple rows per day - one for each main_category_key)
            if origin_key == 'all_l2s':
                chain_df = self.get_blockspace_overview_daily_data(chain_keys)
            else:
                chain_df = self.get_blockspace_overview_daily_data([origin_key])

            chain_timeframe_overview_dfs = {}
            chain_timeframe_totals_dfs = {}
            for timeframe in overview_timeframes:
                if origin_key == 'all_l2s':
                    chain_timeframe_overview_dfs[timeframe] = self.get_blockspace_overview_timeframe_overview(chain_keys, timeframe)
                    chain_timeframe_totals_dfs[timeframe] = self.get_blockspace_totals_timeframe(chain_keys, timeframe)
                else:
                    chain_timeframe_overview_dfs[timeframe] = self.get_blockspace_overview_timeframe_overview([origin_key], timeframe)
                    chain_timeframe_totals_dfs[timeframe] = self.get_blockspace_totals_timeframe([origin_key], timeframe)

            chain_name = chain.name

            # create dict for each chain
            chain_dict = {
                "chain_name": chain_name,
                "daily": {
                    "types": [
                        "unix",
                        "gas_fees_eth_absolute",
                        "gas_fees_usd_absolute",
                        "txcount_absolute",
                        "gas_fees_share_eth",
                        "gas_fees_share_usd",
                        "txcount_share"
                    ],
                    # data for main categories will be added in the for loop below
                },
                "overview": {
                    "types": [
                        "gas_fees_eth_absolute",
                        "gas_fees_usd_absolute",
                        "txcount_absolute",
                        "gas_fees_share_eth",
                        "gas_fees_share_usd",
                        "txcount_share"
                    ],
                    # data for timeframes will be added in the for loop below
                },
                "totals": {
                    "types": [
                        "gas_fees_eth_absolute",
                        "gas_fees_usd_absolute",
                        "txcount_absolute"
                    ],
                    # data for timeframes will be added in the for loop below
                }
            }

            for main_category_key in main_category_keys:
                # filter the dataframes accordingly, we'll use the chain_totals_df to calculate the gas_fees_share and txcount_share
                main_category_df = chain_df.loc[(chain_df.main_category_key == main_category_key)]

                # # create dict for each main_category_key
                # chain_dict["daily"][main_category_key] = {
                #     "data": []
                # }

                # # create list of lists for each main_category_key
                # mk_list = main_category_df[[
                #     'unix', 'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share_eth', 'gas_fees_share_usd', 'txcount_share']].values.tolist()

                # # add the list of lists to the main_category_key dict
                # chain_dict["daily"][main_category_key]['data'] = mk_list

                for timeframe in overview_timeframes:
                    timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'

                    # create dict for each timeframe
                    if timeframe_key not in chain_dict["overview"]:
                        chain_dict["overview"][timeframe_key] = {}

                    if main_category_key not in chain_dict["overview"][timeframe_key]:
                        chain_dict["overview"][timeframe_key][main_category_key] = {}

                    # get the averages for timeframe and main_category_key
                    averages = chain_timeframe_overview_dfs[timeframe].loc[(chain_timeframe_overview_dfs[timeframe].main_category_key == main_category_key)][[
                        'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share_eth', 'gas_fees_share_usd', 'txcount_share']]

                    # if we have any non-zero values, add list of averages for each timeframe
                    if averages.any().any():
                        chain_dict["overview"][timeframe_key][main_category_key]['data'] = averages.values.tolist()[0]
                    
                    ## only add contracts to all field in dicts
                    if origin_key == 'all_l2s':
                        print(f"..adding contracts for {timeframe_key} and {main_category_key}")
                        top_contracts_gas = self.db_connector.get_contracts_overview(main_category_key, timeframe, chain_keys)
                        # convert address to checksummed string 
                        top_contracts_gas = db_addresses_to_checksummed_addresses(top_contracts_gas, ['address'])

                        chain_dict["overview"][timeframe_key][main_category_key]['contracts'] = {
                            "data": top_contracts_gas[
                                ['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", "txcount"]
                            ].values.tolist(),
                            "types": ["address", "project_name", "name", "main_category_key", "sub_category_key", "chain", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                        }
            
            ## add totals per chain
            for timeframe in overview_timeframes:
                timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'

                if timeframe_key not in chain_dict["totals"]:
                    chain_dict["totals"][timeframe_key] = {}

                chain_dict["totals"][timeframe_key]['data'] = chain_timeframe_totals_dfs[timeframe][['gas_fees_eth', 'gas_fees_usd', 'txcount']].values.tolist()[0]

            overview_dict['data']['chains'][origin_key] = chain_dict

        #order the dict by origin_key asc alphabetically
        overview_dict['data']['chains'] = dict(sorted(overview_dict['data']['chains'].items()))
        overview_dict = fix_dict_nan(overview_dict, 'blockspace/overview', False)

        if self.s3_bucket == None:
            self.save_to_json(overview_dict, f'blockspace/overview')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/blockspace/overview', overview_dict, self.cf_distribution_id)
        print(f'-- DONE -- Blockspace export for overview')

        ## also write the same content split into one file per chain (+ an index)
        self._write_blockspace_overview_split(overview_dict)

        ## precomputed answer endpoint derived from the same overview dict (finance category only)
        self._write_answer_defi_l1_vs_l2(overview_dict)

    def _write_answer_defi_l1_vs_l2(self, overview_dict):
        ## /v1/answers/defi-l1-vs-l2.json — replaces frontend derivation over the full overview.json.
        ## Source slice: chains.{chain}.overview[window].finance.data (resolved via overview.types).
        ## L1 = ethereum, L2 = all_l2s; plus top-5 L2 contributors by finance txcount over 30d.
        chains = overview_dict.get('data', {}).get('chains', {})
        windows = ['7d', '30d', '90d', '180d', '365d', 'max']

        def finance_metrics(chain_key, window):
            ch = chains.get(chain_key, {})
            ov = ch.get('overview', {})
            types = ov.get('types')
            fin = ov.get(window, {}).get('finance', {})
            row = fin.get('data') if isinstance(fin, dict) else None
            tx = self._resolve_value(types, row, 'txcount_absolute')
            usd = self._resolve_value(types, row, 'gas_fees_usd_absolute')
            return tx, usd

        out = {
            "generatedAtIso": self._generated_at_iso(),
            "byWindow": {},
            "topL2Contributors30d": []
        }

        for w in windows:
            l1_tx, l1_usd = finance_metrics('ethereum', w)
            l2_tx, l2_usd = finance_metrics('all_l2s', w)
            l1_tx, l1_usd, l2_tx, l2_usd = self._num(l1_tx), self._num(l1_usd), self._num(l2_tx), self._num(l2_usd)
            tot_tx = l1_tx + l2_tx
            tot_usd = l1_usd + l2_usd
            out['byWindow'][w] = {
                "l1Txcount": l1_tx,
                "l1GasFeesUsd": l1_usd,
                "l2Txcount": l2_tx,
                "l2GasFeesUsd": l2_usd,
                "l2ShareTxcount": (l2_tx / tot_tx) if tot_tx > 0 else None,
                "l2ShareGasFeesUsd": (l2_usd / tot_usd) if tot_usd > 0 else None,
            }

        ## top L2 contributors over 30d (every chain except the aggregates / L1s)
        exclude = {'ethereum', 'all_l2s', 'multiple', 'polygon_pos'}
        contribs = []
        for key in chains:
            if key in exclude:
                continue
            tx, usd = finance_metrics(key, '30d')
            if self._is_finite_positive(tx):
                contribs.append({
                    "chainKey": key,
                    "chainName": chains[key].get('chain_name'),
                    "txcount": tx,
                    "gasFeesUsd": self._num(usd),
                })
        contribs_total = sum(c['txcount'] for c in contribs)
        for c in contribs:
            c['shareOfL2Txs'] = (c['txcount'] / contribs_total) if contribs_total > 0 else None
        contribs.sort(key=lambda x: x['txcount'], reverse=True)
        out['topL2Contributors30d'] = contribs[:5]

        out = fix_dict_nan(out, 'answers/defi-l1-vs-l2', False)
        self._save_or_upload_json(out, 'answers/defi-l1-vs-l2')
        print('-- DONE -- Answer export for defi-l1-vs-l2')

    def _write_blockspace_overview_split(self, overview_dict):
        ## Splits the combined blockspace/overview.json into one file per chain so the
        ## frontend can fetch a single chain instead of the full payload.
        ## Each split mirrors the combined shape exactly (drop-in subset):
        ##   blockspace/overview/{origin_key}.json -> {"data": {"metric_id", "chains": {origin_key: ...}}}
        ## plus blockspace/overview/index.json listing the available chains.
        metric_id = overview_dict['data'].get('metric_id', 'overview')
        chains = overview_dict['data']['chains']

        index = {
            "data": {
                "metric_id": metric_id,
                "chains": {}
            }
        }

        for origin_key, chain_dict in chains.items():
            chain_file = {
                "data": {
                    "metric_id": metric_id,
                    "chains": {origin_key: chain_dict}
                }
            }
            # invalidate=False here; we issue one wildcard invalidation at the end instead
            self._save_or_upload_json(chain_file, f'blockspace/overview/{origin_key}', invalidate=False)
            index['data']['chains'][origin_key] = {"chain_name": chain_dict.get('chain_name')}

        self._save_or_upload_json(index, f'blockspace/overview/index', invalidate=False)

        if self.s3_bucket != None:
            empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/blockspace/overview/*')
        print(f'-- DONE -- Blockspace split export for overview ({len(chains)} chains)')

    def create_blockspace_single_chain_json(self, origin_keys:list=None):
        # define the timeframes in days for the overview data
        overview_timeframes = [1, 7, 30, 180, "max"]
        # get main_category_keys in the blockspace_df
        category_mapping = self.get_category_mapping()

        main_category_keys = category_mapping['main_category_id'].unique().tolist()
        
        if origin_keys != None:
            ## create main_conf_filtered that only contains chains that are in the origin_keys list
            main_config_filtered = [chain for chain in self.main_conf if chain.origin_key in origin_keys]
        else:
            main_config_filtered = self.main_conf

        #for chain_key in config:
        for chain in main_config_filtered:
            origin_key = chain.origin_key
            if chain.api_in_main == False:
                print(f'-- SKIPPED -- Single chain blockspace export for {origin_key}. API is set to False')
                continue

            if 'blockspace' in chain.api_exclude_metrics:
                print(f'-- SKIPPED -- Single chain blockspace export for {origin_key}. Blockspace is in exclude_metrics')
                continue

            print(f"Processing {origin_key}")
            
            try:

                # get daily data (multiple rows per day - one for each main_category_key)
                chain_df = self.get_blockspace_overview_daily_data([origin_key])

                chain_timeframe_overview_dfs = {}
                chain_timeframe_totals_dfs = {}
                for timeframe in overview_timeframes:
                    chain_timeframe_overview_dfs[timeframe] = self.get_blockspace_overview_timeframe_overview([origin_key], timeframe)
                    chain_timeframe_totals_dfs[timeframe] = self.get_blockspace_totals_timeframe([origin_key], timeframe)

                chain_name = chain.name

                # create dict for each chain
                chain_dict = {
                    "chain_name": chain_name,
                    "daily": {
                        "types": [
                            "unix",
                            "gas_fees_eth_absolute",
                            "gas_fees_usd_absolute",
                            "txcount_absolute",
                            "gas_fees_share_eth",
                            "gas_fees_share_usd",
                            "txcount_share"
                        ],
                        # data for main categories will be added in the for loop below
                    },
                    "overview": {
                        "types": [
                            "gas_fees_eth_absolute",
                            "gas_fees_usd_absolute",
                            "txcount_absolute",
                            "gas_fees_share_eth",
                            "gas_fees_share_usd",
                            "txcount_share"
                        ],
                        # data for timeframes will be added in the for loop below
                    },
                    "totals": {
                        "types": [
                            "gas_fees_eth_absolute",
                            "gas_fees_usd_absolute",
                            "txcount_absolute"
                        ],
                        # data for timeframes will be added in the for loop below
                    }
                }

                for main_category_key in main_category_keys:
                    # filter the dataframes accordingly, we'll use the chain_totals_df to calculate the gas_fees_share and txcount_share
                    main_category_df = chain_df.loc[(chain_df.main_category_key == main_category_key)]

                    # create dict for each main_category_key
                    chain_dict["daily"][main_category_key] = {
                        "data": []
                    }

                    # create list of lists for each main_category_key
                    mk_list = main_category_df[[
                        'unix', 'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share_eth', 'gas_fees_share_usd', 'txcount_share']].values.tolist()

                    # add the list of lists to the main_category_key dict
                    chain_dict["daily"][main_category_key]['data'] = mk_list

                    for timeframe in overview_timeframes:
                        timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'

                        # create dict for each timeframe
                        if timeframe_key not in chain_dict["overview"]:
                            chain_dict["overview"][timeframe_key] = {}

                        if main_category_key not in chain_dict["overview"][timeframe_key]:
                            chain_dict["overview"][timeframe_key][main_category_key] = {}

                        # get the averages for timeframe and main_category_key
                        averages = chain_timeframe_overview_dfs[timeframe].loc[(chain_timeframe_overview_dfs[timeframe].main_category_key == main_category_key)][[
                            'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share_eth', 'gas_fees_share_usd', 'txcount_share']]

                        # if we have any non-zero values, add list of averages for each timeframe
                        if averages.any().any():
                            chain_dict["overview"][timeframe_key][main_category_key]['data'] = averages.values.tolist()[0]
                        

                        ## add contracts 
                        top_contracts_gas = self.db_connector.get_contracts_overview(main_category_key, timeframe, [origin_key], contract_limit=50)
                        # convert address to checksummed string 
                        top_contracts_gas = db_addresses_to_checksummed_addresses(top_contracts_gas, ['address'])

                        chain_dict["overview"][timeframe_key][main_category_key]['contracts'] = {
                            "data": top_contracts_gas[
                                ['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", "txcount"]
                            ].values.tolist(),
                            "types": ["address", "project_name", "name", "main_category_key", "sub_category_key", "chain", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                        }
                    
                ## add totals per chain
                for timeframe in overview_timeframes:
                    timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'

                    if timeframe_key not in chain_dict["totals"]:
                        chain_dict["totals"][timeframe_key] = {}

                    chain_dict["totals"][timeframe_key]['data'] = chain_timeframe_totals_dfs[timeframe][['gas_fees_eth', 'gas_fees_usd', 'txcount']].values.tolist()[0]

                chain_dict = fix_dict_nan(chain_dict, f'chains/blockspace/{origin_key}', False)

                if self.s3_bucket == None:
                    self.save_to_json(chain_dict, f'chains/blockspace/{origin_key}')
                else:
                    upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/chains/blockspace/{origin_key}', chain_dict, self.cf_distribution_id, invalidate=False)
                
                print(f'-- DONE -- Single chain blockspace export for {origin_key}')
            except Exception as e:
                print(f'-- ERROR -- Single chain blockspace export for {origin_key} failed: {e}')
                send_discord_message(f'Error: Single Chain Blockspace JSON export failed for {origin_key} failed: {e}')
                
        empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/chains/blockspace/*')

    def get_comparison_aggregate_data_day(self, days, category_type, origin_keys:list):
        date_where = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ""
    
        sub_category_key_str = ""
        sub_category_key_str2 = ""
        if category_type == 'sub_category':
            sub_category_key_str = "category_id as sub_category_key, "         
            sub_category_key_str2 = "sub_category_key, " 

        exec = f"""
            -- Total values for each chain (origin_key) over specified days
            WITH totals AS (
                SELECT 
                    origin_key, 
                    SUM(gas_fees_eth) AS total_gas_fees_eth,
                    SUM(txcount) AS total_txcount
                FROM blockspace_fact_category_level
                WHERE date < DATE_TRUNC('day', NOW())
                    {date_where}
                    -- ignore total_usage
                    and category_id != 'total_usage'
                    and origin_key IN ('{"','".join(origin_keys)}')
                GROUP BY origin_key
            )

            -- Aggregate for each sub_category_key and chain over specified days
            SELECT 
                origin_key, 
                bcm.main_category_id as main_category_key,
                {sub_category_key_str}
                SUM(gas_fees_eth) AS gas_fees_eth,
                SUM(gas_fees_usd) AS gas_fees_usd,
                SUM(txcount) AS txcount,
                (SUM(gas_fees_eth) / t.total_gas_fees_eth) AS gas_fees_share,
                (SUM(txcount) / t.total_txcount) AS txcount_share
            FROM blockspace_fact_category_level bs_scl
            JOIN totals t USING (origin_key)
            INNER JOIN 
                vw_oli_category_mapping bcm USING (category_id)
            WHERE date < DATE_TRUNC('day', NOW())
                {date_where}
                -- ignore total_usage
                and category_id != 'total_usage'
            GROUP BY origin_key, main_category_key, {sub_category_key_str2} t.total_gas_fees_eth, t.total_txcount
        """

        df = pd.read_sql(exec, self.db_connector.engine)

        # fill NaN values with 0
        self._fillna_numeric(df)

        return df

    def get_comparison_daily_data(self, days, category_type, origin_keys:list):
        date_where = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ""
        
        sub_category_key_str = ""
        sub_category_key_str2 = ""
        if category_type == 'sub_category':
            sub_category_key_str = "category_id as sub_category_key, " 
            sub_category_key_str2 = "sub_category_key, "

        exec = f"""
            -- Total values for each chain over the specified days
            WITH totals AS (
                SELECT 
                    date,
                    origin_key, 
                    SUM(gas_fees_eth) AS total_gas_fees_eth,
                    SUM(txcount) AS total_txcount
                FROM blockspace_fact_category_level
                WHERE date < DATE_TRUNC('day', NOW())
                    {date_where}
                    -- ignore total_usage
                    and category_id != 'total_usage'
                    and origin_key IN ('{"','".join(origin_keys)}')
                GROUP BY origin_key, date
                
            ),
            -- Daily values for each chain over the specified days
            daily AS (
                SELECT 
                    origin_key, 
                    bcm.main_category_id as main_category_key,
                    {sub_category_key_str}
                    date,
                    SUM(gas_fees_eth) AS gas_fees_eth,
                    SUM(gas_fees_usd) AS gas_fees_usd,
                    SUM(txcount) AS txcount
                FROM blockspace_fact_category_level bs_scl
                INNER JOIN 
                    vw_oli_category_mapping bcm USING (category_id)
                WHERE date < DATE_TRUNC('day', NOW())
                    {date_where}
                    -- ignore total_usage
                    and category_id != 'total_usage'
                GROUP BY origin_key, main_category_key, {sub_category_key_str2} date
                
            )

            -- Aggregate for each sub_category_key and chain over specified days
            SELECT
                origin_key,
                main_category_key,
                {sub_category_key_str2}
                date,
                gas_fees_eth,
                gas_fees_usd,
                txcount,
                -- 0 if total_gas_fees_eth is 0
                CASE WHEN t.total_gas_fees_eth = 0 THEN 0 ELSE (gas_fees_eth / t.total_gas_fees_eth) END AS gas_fees_share,
                CASE WHEN t.total_txcount = 0 THEN 0 ELSE (txcount / t.total_txcount) END AS txcount_share
            FROM daily
            JOIN totals t USING (origin_key, date)
        """

        df = pd.read_sql(exec, self.db_connector.engine)

        # date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')

        # datetime to unix timestamp using timestamp() function
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)

        # fill NaN values with 0
        self._fillna_numeric(df)

        # set index to unix timestamp but keep unix column
        df.set_index('unix', inplace=True, drop=False)

        # sort by unix timestamp
        df.sort_index(inplace=True, ascending=True)

        return df

    def get_comparison_totals_per_chain_by_timeframe(self, timeframe):
        date_where = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{timeframe} days')" if timeframe != 'max' else ""

        exec = f"""
            -- Total values for each chain over the specified days
            SELECT 
                origin_key,
                SUM(gas_fees_eth) AS gas_fees_eth,
                SUM(txcount) AS txcount
            FROM blockspace_fact_category_level
            WHERE date < DATE_TRUNC('day', NOW())
                {date_where}
                -- ignore total_usage
                and category_id != 'total_usage'
            GROUP BY origin_key
        """

        df = pd.read_sql(exec, self.db_connector.engine)

        # fill NaN values with 0
        self._fillna_numeric(df)

        return df


    def create_blockspace_comparison_json(self):
        # create base dict
        comparison_dict = {
            'data': {
                # keys for main categories
            }
        }

        # get data for each timeframe
        timeframes = [7, 30, 180, "max"]

        ## put all origin_keys from main_conf in a list where in_api is True
        chain_keys = [chain.origin_key for chain in self.main_conf if chain.api_in_main == True and 'blockspace' not in chain.api_exclude_metrics]
        
        # daily data for max timeframe
        sub_cat_daily_df = self.get_comparison_daily_data(timeframes[-1], 'sub_category', chain_keys)
        main_cat_daily_df = self.get_comparison_daily_data(timeframes[-1], 'main_category', chain_keys)

        for timeframe in timeframes:
            print(f"...processing timeframe {timeframe} days")
            sub_cat_agg_df = self.get_comparison_aggregate_data_day(timeframe, 'sub_category', chain_keys)
            main_cat_agg_df = self.get_comparison_aggregate_data_day(timeframe, 'main_category', chain_keys)
            
            timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'

            # create dict for each main category
            for main_cat in main_cat_agg_df['main_category_key'].unique():
                print(f"...processing main category {main_cat} for {timeframe} days")
                if main_cat not in comparison_dict['data']:
                    comparison_dict['data'][main_cat] = {
                        "aggregated": {
                            # .. keys for timeframes
                        },
                        "subcategories": {
                            # keys for sub categories
                            "list": sub_cat_agg_df[sub_cat_agg_df['main_category_key'] == main_cat]['sub_category_key'].unique().tolist() if main_cat in sub_cat_agg_df['main_category_key'].unique() else [],
                        },
                        "daily": {
                            "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                            # daily data for each chain
                        },
                        "daily_7d_rolling":{
                            "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                            # daily data for each chain
                        },
                        "type": "main_category"
                    }
                
                comparison_dict['data'][main_cat]['aggregated'][timeframe_key] = {
                    "contracts": {
                        "data":[
                            # .. top contracts

                        ],
                        "types": ["address", "project_name", "name", "main_category_key", "sub_category_key", "chain", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                    
                    },
                    "data": {
                        "types": ["gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                        # .. data for each chain
                    }
                }

                # get top contracts for main category
                top_contracts_gas = self.db_connector.get_contracts_category_comparison(main_cat, timeframe, chain_keys)
                # convert address to checksummed string 
                top_contracts_gas = db_addresses_to_checksummed_addresses(top_contracts_gas, ['address'])

                if top_contracts_gas.shape[0] > 0:
                    ## Trim values (smaller json size)

                    # for values in columns "gas_fees_usd" keep only 2 decimals
                    top_contracts_gas['gas_fees_usd'] = top_contracts_gas['gas_fees_usd'].apply(lambda x: round(x, 2))
                    # for values in columns "gas_fees_eth" keep only 5 decimals
                    top_contracts_gas['gas_fees_eth'] = top_contracts_gas['gas_fees_eth'].apply(lambda x: round(x, 5))                                        

                    # add top contracts to dict
                    comparison_dict['data'][main_cat]['aggregated'][timeframe_key]['contracts']['data'] = top_contracts_gas[['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", "txcount"]].values.tolist()


                # add aggregate timeframe data for each chain for each main category
                for chain in main_cat_agg_df['origin_key'].unique():
                    chain_df = main_cat_agg_df[(main_cat_agg_df['origin_key'] == chain) & (main_cat_agg_df['main_category_key'] == main_cat)]
                    if chain_df.shape[0] > 0:
                        comparison_dict['data'][main_cat]['aggregated'][timeframe_key]['data'][chain] = chain_df[['gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share', 'txcount_share']].values.tolist()[0]

                # create dict for each sub category of main category
                for sub_cat in sub_cat_agg_df[sub_cat_agg_df['main_category_key'] == main_cat]['sub_category_key'].unique():
                    if sub_cat not in  comparison_dict['data'][main_cat]['subcategories']:
                        comparison_dict['data'][main_cat]['subcategories'][sub_cat] = {
                            "aggregated": {
                                # .. keys for timeframes
                            },
                            "daily": {
                                "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                                # daily data for each chain
                            },
                            "daily_7d_rolling":{
                                "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                                # daily data for each chain
                            },
                            "type": "sub_category"
                        }

                    comparison_dict['data'][main_cat]['subcategories'][sub_cat]['aggregated'][timeframe_key] = {
                        "data": {
                            "types": ["gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                            # .. data for each chain
                        }
                    }

                    # add aggregate timeframe data for each chain for each sub category
                    for chain in sub_cat_agg_df['origin_key'].unique():
                        chain_df = sub_cat_agg_df[(sub_cat_agg_df['origin_key'] == chain) & (sub_cat_agg_df['sub_category_key'] == sub_cat)]
                        if chain_df.shape[0] > 0:
                            comparison_dict['data'][main_cat]['subcategories'][sub_cat]['aggregated'][timeframe_key]['data'][chain] = chain_df[['gas_fees_eth', 'gas_fees_usd', 'gas_fees_share', 'txcount', 'txcount_share']].values.tolist()[0]
        
        # add daily data for each chain for each main category
        for chain in main_cat_daily_df['origin_key'].unique():
            for main_cat in main_cat_daily_df['main_category_key'].unique():
                # get daily data for each chain for each main category and sort by unix asc
                chain_df = main_cat_daily_df[(main_cat_daily_df['origin_key'] == chain) & (main_cat_daily_df['main_category_key'] == main_cat)].copy()

                if chain_df.shape[0] > 0:
                    ## for values in column 'gas_fees_eth' keep only 5 decimals
                    chain_df['gas_fees_eth'] = chain_df['gas_fees_eth'].apply(lambda x: round(x, 5))
                    ## for values in column 'gas_fees_usd' keep only 2 decimals
                    chain_df['gas_fees_usd'] = chain_df['gas_fees_usd'].apply(lambda x: round(x, 2))
                    
                    comparison_dict['data'][main_cat]['daily'][chain] = chain_df[['unix', 'gas_fees_eth', 'gas_fees_usd', 'txcount']].values.tolist()
                    
                    # calculate rolling 7d average
                    chain_df['7d_gas_fees_eth'] = chain_df['gas_fees_eth'].rolling(7).mean()
                    chain_df['7d_gas_fees_usd'] = chain_df['gas_fees_usd'].rolling(7).mean()
                    chain_df['7d_txcount'] = chain_df['txcount'].rolling(7).mean()

                    # drop nan values
                    chain_df.dropna(inplace=True)
                    
                    ## for values in column 'gas_fees_eth' keep only 5 decimals
                    chain_df['7d_gas_fees_eth'] = chain_df['7d_gas_fees_eth'].apply(lambda x: round(x, 5))
                    ## for values in column 'gas_fees_usd' keep only 2 decimals
                    chain_df['7d_gas_fees_usd'] = chain_df['7d_gas_fees_usd'].apply(lambda x: round(x, 2))
                    
                    comparison_dict['data'][main_cat]['daily_7d_rolling'][chain] = chain_df[['unix', '7d_gas_fees_eth', '7d_gas_fees_usd', '7d_txcount']].values.tolist()

                    # add daily data for each chain for each sub category of main category
                    for sub_cat in sub_cat_daily_df[sub_cat_daily_df['main_category_key'] == main_cat]['sub_category_key'].unique():
                        # print("daily sub category chain_df", chain, chain_df)
                        chain_df = sub_cat_daily_df[(sub_cat_daily_df['origin_key'] == chain) & (sub_cat_daily_df['sub_category_key'] == sub_cat)].copy()
                        
                        if chain_df.shape[0] > 0:
                            ## for values in column 'gas_fees_eth' keep only 5 decimals
                            chain_df['gas_fees_eth'] = chain_df['gas_fees_eth'].apply(lambda x: round(x, 5))
                            ## for values in column 'gas_fees_usd' keep only 2 decimals
                            chain_df['gas_fees_usd'] = chain_df['gas_fees_usd'].apply(lambda x: round(x, 2))

                            comparison_dict['data'][main_cat]['subcategories'][sub_cat]['daily'][chain] = chain_df[['unix', 'gas_fees_eth', 'gas_fees_usd', 'txcount']].values.tolist()
                            
                            # calculate rolling 7d average
                            chain_df['7d_gas_fees_eth'] = chain_df['gas_fees_eth'].rolling(7).mean()
                            chain_df['7d_gas_fees_usd'] = chain_df['gas_fees_usd'].rolling(7).mean()
                            chain_df['7d_txcount'] = chain_df['txcount'].rolling(7).mean()

                            # drop nan values
                            chain_df.dropna(inplace=True)

                            ## for values in column 'gas_fees_eth' keep only 5 decimals
                            chain_df['7d_gas_fees_eth'] = chain_df['7d_gas_fees_eth'].apply(lambda x: round(x, 5))
                            ## for values in column 'gas_fees_usd' keep only 2 decimals
                            chain_df['7d_gas_fees_usd'] = chain_df['7d_gas_fees_usd'].apply(lambda x: round(x, 2))

                            #comparison_dict['data'][main_cat]['subcategories'][sub_cat]['daily_7d_rolling'][chain] = chain_df[['unix', '7d_gas_fees_eth', '7d_gas_fees_usd', '7d_gas_fees_share', '7d_txcount', '7d_txcount_share']].values.tolist()
                            comparison_dict['data'][main_cat]['subcategories'][sub_cat]['daily_7d_rolling'][chain] = chain_df[['unix', '7d_gas_fees_eth', '7d_gas_fees_usd', '7d_txcount']].values.tolist()

        comparison_dict = fix_dict_nan(comparison_dict, f'blockspace/category_comparison', False)

        if self.s3_bucket == None:
            self.save_to_json(comparison_dict, f'blockspace/category_comparison')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/blockspace/category_comparison', comparison_dict, self.cf_distribution_id)
        print(f'-- DONE -- Blockspace export for category_comparison')

        ## also write the same content split into one file per main category (+ an index)
        self._write_blockspace_comparison_split(comparison_dict)

        ## precomputed answer endpoint derived from the same comparison dict (stablecoin subcat only)
        self._write_answer_stablecoin_activity(comparison_dict)

    def _write_answer_stablecoin_activity(self, comparison_dict):
        ## /v1/answers/stablecoin-activity.json — replaces frontend derivation over the full
        ## category_comparison.json. Source slice: data.token_transfers.subcategories.stablecoin.
        ## Two metrics x three periods, ranked top-5 over the curated L2 universe.
        universe, excluded = self._get_l2_universe()
        universe_set = set(universe)

        out = {
            "generatedAtIso": self._generated_at_iso(),
            "universeSize": len(universe),
            "universeKeys": universe,
            "excludedNonL2Keys": excluded,
            "byMetricByPeriod": {
                "txcount": {"daily": [], "weekly": [], "monthly": []},
                "gas_spent": {"daily": [], "weekly": [], "monthly": []},
            }
        }

        subcat = (comparison_dict.get('data', {})
                  .get('token_transfers', {})
                  .get('subcategories', {})
                  .get('stablecoin'))

        if isinstance(subcat, dict):
            # txcount -> txcount_absolute ; gas_spent -> gas_fees_absolute_usd (USD)
            metrics = {"txcount": "txcount_absolute", "gas_spent": "gas_fees_absolute_usd"}

            def ranked_top5(entries):
                entries.sort(key=lambda x: x['value'], reverse=True)
                return entries[:5]

            # weekly = aggregated 7d, monthly = aggregated 30d
            agg = subcat.get('aggregated', {})
            for period, tf in (("weekly", "7d"), ("monthly", "30d")):
                data_block = agg.get(tf, {}).get('data', {})
                types = data_block.get('types')
                for metric_key, col in metrics.items():
                    entries = []
                    for key, row in data_block.items():
                        if key == 'types' or key not in universe_set:
                            continue
                        v = self._resolve_value(types, row, col)
                        if self._is_finite_positive(v):
                            entries.append({"key": key, "value": v})
                    out['byMetricByPeriod'][metric_key][period] = ranked_top5(entries)

            # daily = last element of the .daily[key] series
            daily_block = subcat.get('daily', {})
            daily_types = daily_block.get('types')
            for metric_key, col in metrics.items():
                entries = []
                for key, series in daily_block.items():
                    if key == 'types' or key not in universe_set:
                        continue
                    if not isinstance(series, list) or len(series) == 0:
                        continue
                    v = self._resolve_value(daily_types, series[-1], col)
                    if self._is_finite_positive(v):
                        entries.append({"key": key, "value": v})
                out['byMetricByPeriod'][metric_key]['daily'] = ranked_top5(entries)

        out = fix_dict_nan(out, 'answers/stablecoin-activity', False)
        self._save_or_upload_json(out, 'answers/stablecoin-activity')
        print('-- DONE -- Answer export for stablecoin-activity')

    @staticmethod
    def _strip_daily_7d_rolling(main_cat_dict):
        ## Return a copy of one category's dict with `daily_7d_rolling` removed at the category
        ## level and within every subcategory. Non-mutating (does not touch comparison_dict), so the
        ## already-written large category_comparison.json and the answer JSONs are unaffected.
        out = {k: v for k, v in main_cat_dict.items() if k != 'daily_7d_rolling'}
        subs = main_cat_dict.get('subcategories')
        if isinstance(subs, dict):
            out['subcategories'] = {
                sk: ({k: v for k, v in sv.items() if k != 'daily_7d_rolling'} if isinstance(sv, dict) else sv)
                for sk, sv in subs.items()
            }
        return out

    def _write_blockspace_comparison_split(self, comparison_dict):
        ## Splits the combined blockspace/category_comparison.json into one file per main
        ## category so the frontend can fetch a single category instead of the full payload.
        ## Each split mirrors the combined shape exactly (drop-in subset):
        ##   blockspace/category_comparison/{main_category}.json -> {"data": {main_category: ...}}
        ## plus blockspace/category_comparison/index.json listing categories + their subcategories.
        categories = comparison_dict['data']

        index = {"data": {}}

        for main_cat, main_cat_dict in categories.items():
            # when the flag is ON, omit daily_7d_rolling from the split (client recomputes it from daily)
            if self.drop_daily_7d_rolling:
                main_cat_dict = self._strip_daily_7d_rolling(main_cat_dict)
            cat_file = {
                "data": {main_cat: main_cat_dict}
            }
            # invalidate=False here; we issue one wildcard invalidation at the end instead
            self._save_or_upload_json(cat_file, f'blockspace/category_comparison/{main_cat}', invalidate=False)

            # subcategory keys live under subcategories.list (everything else there is a sub_cat dict)
            subcategories = main_cat_dict.get('subcategories', {}).get('list', [])
            index['data'][main_cat] = {
                "type": main_cat_dict.get('type', 'main_category'),
                "subcategories": subcategories
            }

        self._save_or_upload_json(index, f'blockspace/category_comparison/index', invalidate=False)

        if self.s3_bucket != None:
            empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/blockspace/category_comparison/*')
        print(f'-- DONE -- Blockspace split export for category_comparison ({len(categories)} categories)')

    def create_all_jsons(self):
        self.create_blockspace_overview_json()
        self.create_blockspace_comparison_json()
        self.create_blockspace_single_chain_json()

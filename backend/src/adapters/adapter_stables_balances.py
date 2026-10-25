import os
import threading
import pandas as pd
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

from web3 import Web3
from eth_abi import encode
from src.adapters.abstract_adapters import AbstractAdapter
from src.gtp_dna import get_gtp_dna_file

#!# Logic requires 'first_block_of_day' data in fact_kpis (balances are read at the last block of each day).
#!# Tables must exist first: src/queries/postgres/stables/create_stables_balances.sql
#!# balanceOf() is the source of truth; transfers (Dune) are only used to discover addresses.

# Multicall3 is deployed at the same address on (almost) every EVM chain
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
MULTICALL3_ABI = [{
    "inputs": [{"components": [
        {"name": "target", "type": "address"},
        {"name": "allowFailure", "type": "bool"},
        {"name": "callData", "type": "bytes"}],
        "name": "calls", "type": "tuple[]"}],
    "name": "aggregate3",
    "outputs": [{"components": [
        {"name": "success", "type": "bool"},
        {"name": "returnData", "type": "bytes"}],
        "name": "returnData", "type": "tuple[]"}],
    "stateMutability": "payable",
    "type": "function",
}]
BALANCE_OF_SELECTOR = Web3.keccak(text="balanceOf(address)")[:4]

# saved Dune queries, SQL in src/queries/dune/
DUNE_HOLDER_DISCOVERY_QUERY_ID = 8915328 # stables_balances_holder_discovery.sql: top receivers per token, last n days
DUNE_TOP_HOLDERS_QUERY_ID = 8915430      # stables_balances_top_holders.sql: top holders per token by all-time net transfers


class AdapterStablecoinBalances(AbstractAdapter):
    """
    Adapter for measuring how much of each mapped stablecoin is held by tracked addresses (contracts and EOAs) on a chain.

    Two steps per chain:
        1. discover() -> candidate addresses into sys_stables_balances_candidates
        2. measure()  -> balanceOf via Multicall3 at end-of-day blocks into fact_stables_balances

    Measurement is a full scan (every mapped token) per address every `full_scan_every_days` days; in between only
    the (address, token) pairs that held a balance since the last full scan are read. Pairs a full scan finds newly
    funded are backfilled for the days since the previous full scan, so history stays complete. Historical backfills
    (dates before measured_from) use the same split in reverse, see measure().

    Args:
        adapter_params (dict): Optional keys:
        - batch_size (int): max balanceOf calls per Multicall3 eth_call. Default 1000; halved per chain (down to
          min_batch_size) when every RPC rejects a batch, e.g. response size limits on public RPCs.
        - min_batch_size (int): Default 100.
        - max_workers (int): Multicall batches run in parallel. Default 6.
        - full_scan_every_days (int): Default 7.
        - rpc_timeout_seconds (int): Default 30.
        - dune_api_key (str): Default env DUNE_API. Needed for the 'dune' discovery source.
        - blockscout_api_key (str): Default env BLOCKSCOUT_API_KEY_PRO (or BLOCKSCOUT_PRO). Needed for the 'blockscout' discovery source.
        db_connector: Database connector instance.
    """
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Stablecoin Balances", adapter_params, db_connector)
        self.batch_size = adapter_params.get('batch_size', 1000)
        self.min_batch_size = adapter_params.get('min_batch_size', 100)
        self.max_workers = adapter_params.get('max_workers', 6)
        self.full_scan_every_days = adapter_params.get('full_scan_every_days', 7)
        self.rpc_timeout_seconds = adapter_params.get('rpc_timeout_seconds', 30)
        self.dune_api_key = adapter_params.get('dune_api_key', os.getenv('DUNE_API'))
        self.blockscout_api_key = adapter_params.get('blockscout_api_key', os.getenv('BLOCKSCOUT_API_KEY_PRO') or os.getenv('BLOCKSCOUT_PRO'))

        # stablecoin mapping (same source as adapter_stables_v2)
        _config = {}
        exec(get_gtp_dna_file("stables/stables_config_v2.py"), _config)
        self.address_mapping = _config['address_mapping']

        self.rpc_calls = 0 # counts every eth_call, to keep an eye on costs
        self._rpcs = {}
        self._batch_sizes = {}
        self._lock = threading.Lock()

    def extract(self, extract_params: dict):
        """
        Run discovery and measurement for each chain.

        Args:
            extract_params (dict):
            - origin_keys (list): chains to run for, e.g. ['ethereum', 'polygon_pos', 'celo'].
              ['*'] = every chain with app metrics (runs_aggregate_apps in main_config), except non-EVM chains.
            - days (int): measure balances for the last n days (excl. today). Default 30.
            - discover (bool): run discovery step. Default True.
            - sources (list): discovery sources, any of 'activity', 'oli', 'dune', 'blockscout', 'dune_history', 'dune_holders'.
              Default ['activity', 'oli', 'dune', 'blockscout'].
              'dune' (top receivers, last 7 days) runs daily and catches addresses that become large holders.
              'blockscout' (current top holders) runs on a chain's first run and then weekly (blockscout_weekday).
              'dune_history' (top receivers per yearly window back to history_years) is a one-time seed for chains
              without Blockscout access (e.g. Polygon). 'dune_holders' (all-time net transfers) is a fallback only:
              ~60 credits on Ethereum and it times out on Polygon/Celo.
            - top_n (int): 'activity': top n most used addresses (by gas, txcount and daa). Default 1000.
            - oli_limit (int or None): 'oli': max OLI labeled addresses (with owner_project), ranked by 30d txcount.
              Default None = all of them (the goal is daily balances for every address with an owner_project).
            - dune_top_n (int): 'dune': top n receivers per token. Default 1000.
            - dune_days (int): 'dune': lookback for received amounts. Default 7.
            - dune_min_received_usd (float): 'dune': min USD received per (address, token) within dune_days. Default 100k.
            - blockscout_top_n (int): 'blockscout': top n holders per token. Default 1000.
            - blockscout_min_usd (float): 'blockscout': min balance in USD. Default 10k (Celo's top holders are mostly below 100k).
              Polygon is a 'featured chain' on Blockscout and needs a Builder/Business/Pro plan.
            - blockscout_min_supply_share (float): 'blockscout': only tokens with at least this share of the chain's stablecoin supply. Default 0.01.
            - blockscout_weekday (int or None): 'blockscout': weekday to rerun on (0 = Monday). Always runs on a chain's first run;
              None = every run. Default 0.
            - history_years (int): 'dune_history': how many years back from today. Default 6.
            - history_window_days (int): 'dune_history': length of each window (one Dune query each). Default 365.
              Shorter windows (e.g. 91 = quarterly) find more holders when the top_n cutoff of a window is still far above
              history_min_received_usd (logged as "cutoff reached"); high-volume tokens are dominated by routers and bots.
            - history_top_n (int): 'dune_history': top n receivers per token and window. Default 1000.
            - history_min_received_usd (float): 'dune_history': min USD received per (address, token) within a window. Default 100k.
            - history_token_ids (list): 'dune_history': only these token_ids (e.g. the tokens with the largest coverage gap). Default all mapped tokens.
              Example seed for a chain where top holders are missing (used for base, arbitrum and polygon_pos in Oct 2026):
              {'origin_keys': ['arbitrum'], 'days': 21, 'sources': ['dune_history'], 'history_years': 4, 'history_window_days': 91,
               'history_top_n': 10_000, 'history_token_ids': ['circlefin_usdc', 'tetherto_usdt0']}
            - holders_top_n (int): 'dune_holders': top n holders per token. Default 1000.
            - holders_min_supply_share (float): 'dune_holders': only tokens with at least this share of the chain's stablecoin supply. Default 0.01.
            - holders_min_usd (float): 'dune_holders': min approximate balance in USD. Default 100k.
            - holders_token_ids (list): 'dune_holders': rank holders of these token_ids instead of selecting by supply share.
            - prune (bool): stop measuring small candidates after measurement (see prune()). Default True.
            - prune_min_usd (float), prune_lookback_days (int), prune_min_measured_days (int): Defaults 10k, 30, 14.
        """
        days = extract_params.get('days', 30)
        origin_keys = extract_params['origin_keys']
        if origin_keys == ['*']:
            origin_keys = self.get_app_chains()
            print(f"Running for app metrics chains: {origin_keys}")
        for origin_key in origin_keys:
            if extract_params.get('discover', True):
                self.discover(origin_key, extract_params)
            self.measure(origin_key, days=days)
            if extract_params.get('prune', True):
                self.prune(origin_key, extract_params.get('prune_min_usd', 10_000), extract_params.get('prune_lookback_days', 30), extract_params.get('prune_min_measured_days', 14))
            print(f"{origin_key}: done. eth_calls so far: {self.rpc_calls}")

    # balances are read via Multicall3 / web3, which these chains don't support (Starknet needs its own path)
    NON_EVM_CHAINS = {'starknet'}

    def get_app_chains(self) -> list:
        """ chains with app metrics enabled (runs_aggregate_apps), minus non-EVM chains """
        return [c.origin_key for c in self._main_config() if c.runs_aggregate_apps and c.origin_key not in self.NON_EVM_CHAINS]

    def load(self, df):
        # measure() loads after every date so progress in sys_stables_balances_candidates stays consistent
        raise NotImplementedError("Data is loaded within measure().")


    #-#-#-# 1. Discovery #-#-#-#

    def discover(self, origin_key: str, params: dict = {}):
        """
        Collect candidate addresses and merge them into sys_stables_balances_candidates.
        Sources: 'activity' (most used addresses), 'oli' (labeled app addresses), 'dune' (largest receivers per token),
        'blockscout' (current top holders per token from the Blockscout API), 'dune_history' (largest receivers per token in
        yearly windows), 'dune_holders' (largest holders per token by all-time net transfers; expensive, fallback only).
        """
        sources = params.get('sources', ['activity', 'oli', 'dune', 'blockscout'])
        frames = []
        if 'activity' in sources:
            frames.append(self._discover_activity(origin_key, params.get('top_n', 1000)))
        if 'oli' in sources:
            frames.append(self._discover_oli(origin_key, params.get('oli_limit')))
        if 'dune' in sources:
            frames.append(self._discover_dune(origin_key, params.get('dune_top_n', 1000), params.get('dune_days', 7), params.get('dune_min_received_usd', 100_000)))
        if 'blockscout' in sources and self._blockscout_due(origin_key, params.get('blockscout_weekday', 0)):
            frames.append(self._discover_blockscout(origin_key, params.get('blockscout_top_n', 1000), params.get('blockscout_min_supply_share', 0.01), params.get('blockscout_min_usd', 10_000)))
        if 'dune_history' in sources:
            frames.append(self._discover_dune_history(origin_key, params.get('history_top_n', 1000), params.get('history_years', 6), params.get('history_min_received_usd', 100_000),
                                                      params.get('history_window_days', 365), params.get('history_token_ids')))
        if 'dune_holders' in sources:
            frames.append(self._discover_dune_holders(origin_key, params.get('holders_top_n', 1000), params.get('holders_min_supply_share', 0.01), params.get('holders_min_usd', 100_000), params.get('holders_token_ids')))
        self.merge_candidates(origin_key, frames)

    def merge_candidates(self, origin_key: str, frames: list):
        """ Merge discovered (address, source) frames into sys_stables_balances_candidates, keeping all sources per address. """
        frames = [f for f in frames if not f.empty]
        if not frames:
            print(f"{origin_key}: no candidates discovered.")
            return
        new = pd.concat(frames, ignore_index=True)
        new = new.groupby('address')['source'].apply(set).reset_index()

        # merge sources and dates with what we already have
        existing = self.get_candidates(origin_key)[['address', 'sources', 'first_discovered']]
        merged = new.merge(existing, on='address', how='left')
        today = date.today()
        merged['sources'] = merged.apply(
            lambda r: ','.join(sorted(r['source'] | set(r['sources'].split(',') if isinstance(r['sources'], str) else []))), axis=1)
        n_new = merged['first_discovered'].isna().sum()
        merged['first_discovered'] = merged['first_discovered'].fillna(today)
        merged['last_discovered'] = today
        merged['origin_key'] = origin_key

        self._upsert_candidates(merged[['origin_key', 'address', 'sources', 'first_discovered', 'last_discovered']])

        # a pruned address found again by a balance-based source (or OLI) is measured again; its full scan is forced
        # and the days it was skipped are backfilled by measure() (its measured range just continues)
        pruned = set(existing.merge(self.get_candidates(origin_key)[['address', 'pruned_on']], on='address')
                     .loc[lambda x: x['pruned_on'].notna(), 'address'])
        revive = [a for a, src in zip(new['address'], new['source']) if a in pruned and src & self.UNPRUNE_SOURCES]
        if revive:
            self._upsert_candidates(pd.DataFrame({'origin_key': origin_key, 'address': revive, 'pruned_on': None, 'last_full_scan': None}))
        print(f"{origin_key}: discovered {len(merged)} candidates ({n_new} new, {len(revive)} unpruned).")

    # sources that say something about balances (or ownership): finding a pruned address again unprunes it
    UNPRUNE_SOURCES = {'oli', 'dune', 'blockscout', 'dune_history', 'dune_holders'}

    def prune(self, origin_key: str, min_usd: float = 10_000, lookback_days: int = 30, min_measured_days: int = 14):
        """
        Stop measuring candidates that don't matter: no OLI owner_project, measured for at least min_measured_days,
        and a total stablecoin balance below min_usd on every day of the last lookback_days. Their history stays.
        Addresses with an OLI owner_project are never pruned.
        """
        df = self.db_connector.execute_query(f"""
            WITH daily AS (
                SELECT address, "date", SUM(balance_usd) AS usd
                FROM public.vw_fact_stables_balances_v1
                WHERE origin_key = '{origin_key}' AND "date" >= current_date - interval '{int(lookback_days)} days'
                GROUP BY 1, 2
            ),
            peak AS (SELECT address, MAX(usd) AS max_usd FROM daily GROUP BY 1)
            SELECT '0x' || encode(c.address, 'hex') AS address
            FROM public.sys_stables_balances_candidates c
            LEFT JOIN peak p ON p.address = c.address
            WHERE c.origin_key = '{origin_key}'
              AND c.pruned_on IS NULL
              AND c.measured_from <= current_date - interval '{int(min_measured_days)} days'
              AND COALESCE(p.max_usd, 0) < {float(min_usd)}
              AND NOT EXISTS (
                  SELECT 1 FROM public.vw_oli_label_pool_gold_pivoted_v2 l
                  WHERE l.address = c.address AND l.origin_key = c.origin_key AND l.owner_project IS NOT NULL
              )
        """, load_df=True)
        if not df.empty:
            self._upsert_candidates(pd.DataFrame({'origin_key': origin_key, 'address': df['address'], 'pruned_on': date.today()}))
        print(f"{origin_key}: pruned {len(df)} candidates (no OLI owner, < ${min_usd:,.0f} on every day of the last {lookback_days} days).")

    def _discover_activity(self, origin_key: str, top_n: int, days: int = 30) -> pd.DataFrame:
        # most used addresses by gas, txcount or daa (union of the three rankings)
        df = self.db_connector.get_most_used_contracts(top_n, origin_key, days)
        print(f"{origin_key}: 'activity' found {len(df)} addresses.")
        return pd.DataFrame({'address': df['address'].apply(self._to_hex), 'source': 'activity'})

    def _discover_oli(self, origin_key: str, oli_limit: int = None, days: int = 30) -> pd.DataFrame:
        # addresses mapped to an application in OLI, most active first
        query = f"""
            SELECT l.address
            FROM public.vw_oli_label_pool_gold_pivoted_v2 l
            LEFT JOIN (
                SELECT address, SUM(txcount) AS txcount
                FROM public.blockspace_fact_contract_level
                WHERE origin_key = '{origin_key}' AND "date" >= current_date - interval '{days} days'
                GROUP BY 1
            ) a USING (address)
            WHERE l.origin_key = '{origin_key}' AND l.owner_project IS NOT NULL
            ORDER BY a.txcount DESC NULLS LAST
            {f'LIMIT {int(oli_limit)}' if oli_limit else ''}
        """
        df = self.db_connector.execute_query(query, load_df=True)
        print(f"{origin_key}: 'oli' found {len(df)} addresses.")
        return pd.DataFrame({'address': df['address'].apply(self._to_hex), 'source': 'oli'})

    def _discover_dune(self, origin_key: str, top_n: int, days: int, min_received_usd: float, offset_days: int = 0, source: str = 'dune', token_ids: list = None) -> pd.DataFrame:
        """
        Largest receivers of each mapped stablecoin (or only `token_ids`) in a window of `days` days ending `offset_days` days ago
        (Dune erc20_<chain>.evt_Transfer), kept if they received at least min_received_usd of that token.
        """
        tokens = [t for t in self._get_tokens(origin_key) if token_ids is None or t['token_id'] in token_ids]
        if not tokens:
            print(f"{origin_key}: skipping '{source}' discovery, none of {token_ids} is mapped on this chain.")
            return pd.DataFrame(columns=['address', 'source'])
        df = self._run_dune(origin_key, source, DUNE_HOLDER_DISCOVERY_QUERY_ID, tokens, {'days': days, 'offset_days': offset_days, 'top_n': top_n})
        if df.empty:
            return pd.DataFrame(columns=['address', 'source'])
        df['usd'] = self._raw_to_usd(origin_key, df, 'received_raw', tokens)
        # a token whose top_n-th receiver still got far more than min_received_usd has more large receivers than top_n
        token_ids_by_address = {t['address'].lower(): t['token_id'] for t in tokens}
        for token_address, g in df.groupby(df['token_address'].str.lower()):
            if len(g) >= top_n and g['usd'].min() >= min_received_usd:
                print(f"{origin_key}: '{source}' {token_ids_by_address.get(token_address)} cutoff reached (top {top_n} all received >= ${g['usd'].min():,.0f}), shorter windows would find more.")
        df = df[df['usd'] >= min_received_usd]
        print(f"{origin_key}: '{source}' found {df['address'].nunique()} addresses with >= ${min_received_usd:,.0f} received in {days}d ending {offset_days}d ago.")
        return pd.DataFrame({'address': df['address'].str.lower(), 'source': source})

    def _discover_dune_history(self, origin_key: str, top_n: int, years: int, min_received_usd: float, window_days: int = 365, token_ids: list = None) -> pd.DataFrame:
        """
        Largest receivers per token in windows of `window_days` days back `years` years: a holder must have received its
        balance at some point, so this finds dormant large holders. One Dune query per window keeps each run far below Dune's
        30 minute limit (a full year on Polygon: ~2 minutes on the medium engine). One-time seed per chain.
        """
        n_windows = -(-years * 365 // window_days)
        frames = [self._discover_dune(origin_key, top_n, window_days, min_received_usd, offset_days=window_days * w, source='dune_history', token_ids=token_ids)
                  for w in range(n_windows)]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame(columns=['address', 'source'])
        df = pd.concat(frames).drop_duplicates('address')
        print(f"{origin_key}: 'dune_history' found {len(df)} addresses over {n_windows} windows of {window_days}d.")
        return df

    def _blockscout_due(self, origin_key: str, weekday) -> bool:
        """ Blockscout runs on a chain's first run (no candidate found by it yet) and then on `weekday` (None = always). """
        if weekday is None or date.today().weekday() == weekday:
            return True
        found_before = self.db_connector.execute_query(f"""
            SELECT 1 FROM public.sys_stables_balances_candidates
            WHERE origin_key = '{origin_key}' AND sources LIKE '%blockscout%' LIMIT 1
        """, load_df=True)
        if found_before.empty:
            return True
        print(f"{origin_key}: skipping 'blockscout' discovery (runs weekly, weekday {weekday}).")
        return False

    def _discover_dune_holders(self, origin_key: str, top_n: int, min_supply_share: float, min_usd: float, token_ids: list = None) -> pd.DataFrame:
        """
        Largest holders per stablecoin, ranked by all-time net transfers on Dune (an approximation, only used for ranking).
        Tokens: token_ids if given, else those with >= min_supply_share of the chain's stablecoin supply.
        One Dune query per token, so a single heavy token cannot push the run past Dune's 30 minute limit.
        Expensive (~30 credits per large token): run once per chain as a seed.
        """
        tokens = self._get_dominant_tokens(origin_key, min_supply_share) if token_ids is None else \
            [t for t in self._get_tokens(origin_key) if t['token_id'] in set(token_ids)]
        if not tokens:
            print(f"{origin_key}: 'dune_holders' found no matching token.")
            return pd.DataFrame(columns=['address', 'source'])

        addresses = set()
        for t in tokens:
            df = self._run_dune(origin_key, f"dune_holders {t['token_id']}", DUNE_TOP_HOLDERS_QUERY_ID, [t], {'top_n': top_n}, performance='large')
            if df.empty:
                continue
            df = df[self._raw_to_usd(origin_key, df, 'balance_raw', [t]) >= min_usd]
            # excluded balances (e.g. issuer treasuries) are never measured, so no need to track them
            df = df[~df['address'].str.lower().isin(t['exclude'])]
            addresses |= set(df['address'].str.lower())
            print(f"{origin_key}: 'dune_holders' {t['token_id']}: {len(df)} holders with >= ${min_usd:,.0f} approx. balance.")
        print(f"{origin_key}: 'dune_holders' found {len(addresses)} addresses.")
        return pd.DataFrame({'address': sorted(addresses), 'source': 'dune_holders'})

    def _discover_blockscout(self, origin_key: str, top_n: int, min_supply_share: float, min_usd: float) -> pd.DataFrame:
        """
        Current top holders of each stablecoin with >= min_supply_share of the chain's supply, from the Blockscout PRO API
        (holders come sorted by balance; paging stops at top_n or below min_usd). Free, so it can run every day.
        Blockscout's cached balances are only used for ranking; balances are always read via balanceOf().
        """
        import time
        import requests

        empty = pd.DataFrame(columns=['address', 'source'])
        caip2 = next((c.caip2 for c in self._main_config() if c.origin_key == origin_key), None)
        if not caip2 or not self.blockscout_api_key:
            print(f"{origin_key}: skipping 'blockscout' discovery (caip2: {caip2}, api key set: {bool(self.blockscout_api_key)}).")
            return empty
        url = f"https://api.blockscout.com/{caip2.split(':')[1]}/api/v2/tokens/{{token}}/holders"

        tokens = self._get_dominant_tokens(origin_key, min_supply_share)
        prices = self._get_prices(origin_key)
        addresses = set()
        for t in tokens:
            price = prices.get(t['token_id'], 0) or 0
            found, params, retries = [], {'apikey': self.blockscout_api_key}, 0
            try:
                while len(found) < top_n:
                    r = requests.get(url.format(token=t['address']), params=params, timeout=30)
                    if r.status_code == 429 and retries < 5:
                        retries += 1
                        time.sleep(2 ** retries)
                        continue
                    r.raise_for_status()
                    page = r.json()
                    usd = [int(i['value']) / 10 ** t['decimals'] * price for i in page.get('items', [])]
                    found += [i['address']['hash'].lower() for i, v in zip(page.get('items', []), usd) if v >= min_usd]
                    if not page.get('next_page_params') or not usd or usd[-1] < min_usd:
                        break
                    params = {**page['next_page_params'], 'apikey': self.blockscout_api_key}
            except Exception as e:
                # discovery is additive: an API failure must not block measuring known candidates
                # the API key is a URL param: never print it
                print(f"{origin_key}: 'blockscout' {t['token_id']} failed after {len(found)} holders, continuing: {str(e).replace(self.blockscout_api_key, '***')[:200]}")
            found = [a for a in found[:top_n] if a not in t['exclude']]
            addresses |= set(found)
            print(f"{origin_key}: 'blockscout' {t['token_id']}: {len(found)} holders with >= ${min_usd:,.0f}.")
        print(f"{origin_key}: 'blockscout' found {len(addresses)} addresses.")
        return pd.DataFrame({'address': sorted(addresses), 'source': 'blockscout'})

    def _get_dominant_tokens(self, origin_key: str, min_supply_share: float) -> list:
        """ mapped tokens with at least min_supply_share of the chain's stablecoin supply on the latest date """
        shares = self.db_connector.execute_query(f"""
            SELECT token_id, value_usd / SUM(value_usd) OVER () AS share
            FROM public.vw_fact_stables_mcap_v2
            WHERE origin_key = '{origin_key}' AND metric_key IN ('total_supply', 'track_on_l1') AND value_usd > 0
              AND "date" = (SELECT MAX("date") FROM public.vw_fact_stables_mcap_v2 WHERE origin_key = '{origin_key}')
        """, load_df=True)
        dominant = set(shares.loc[shares['share'] >= min_supply_share, 'token_id'])
        return [t for t in self._get_tokens(origin_key) if t['token_id'] in dominant]

    def _get_prices(self, origin_key: str) -> pd.Series:
        """ latest USD price per token_id on this chain, from vw_fact_stables_mcap_v2 """
        return self.db_connector.execute_query(f"""
            SELECT DISTINCT ON (token_id) token_id, value_usd / NULLIF(value, 0) AS price_usd
            FROM public.vw_fact_stables_mcap_v2
            WHERE origin_key = '{origin_key}' AND metric_key IN ('total_supply', 'track_on_l1') AND value > 0
            ORDER BY token_id, "date" DESC
        """, load_df=True).set_index('token_id')['price_usd']

    def _main_config(self):
        from src.main_config import get_main_config
        return get_main_config()

    def _run_dune(self, origin_key: str, source: str, query_id: int, tokens: list, params: dict, performance: str = 'medium') -> pd.DataFrame:
        """ Run a saved discovery query for this chain's tokens. Failures return an empty frame: discovery must not block measurement. """
        from dune_client.client import DuneClient
        from dune_client.query import QueryBase
        from dune_client.types import QueryParameter
        dune_alias = next((c.aliases_dune for c in self._main_config() if c.origin_key == origin_key), None)
        if not dune_alias or not self.dune_api_key:
            print(f"{origin_key}: skipping '{source}' discovery (dune alias: {dune_alias}, api key set: {bool(self.dune_api_key)}).")
            return pd.DataFrame()
        query = QueryBase(name=f'stables {source} {origin_key}', query_id=query_id, params=[
            QueryParameter.text_type('blockchain', dune_alias),
            QueryParameter.text_type('token_addresses', ','.join(t['address'].lower() for t in tokens)),
        ] + [QueryParameter.text_type(k, str(int(v))) for k, v in params.items()])
        try:
            return DuneClient(self.dune_api_key).refresh_into_dataframe(query, performance=performance)
        except Exception as e:
            print(f"{origin_key}: '{source}' discovery failed, continuing without it: {e}")
            return pd.DataFrame()

    def _raw_to_usd(self, origin_key: str, df: pd.DataFrame, raw_col: str, tokens: list) -> pd.Series:
        """ raw token amount -> USD, using decimals from the mapping and the latest price from vw_fact_stables_mcap_v2 """
        prices = self._get_prices(origin_key)
        by_address = {t['address'].lower(): t for t in tokens}
        token = df['token_address'].str.lower().map(by_address)
        return (df[raw_col] / token.map(lambda t: 10 ** t['decimals'])) * token.map(lambda t: prices.get(t['token_id'], 0)).fillna(0)


    #-#-#-# 2. Measurement #-#-#-#

    def measure(self, origin_key: str, days: int = 30):
        """
        Read balances for every candidate at the last block of each date it has not been measured for yet.
        Dates after measured_to are filled ascending, dates before measured_from descending, so each candidate's
        measured range stays contiguous. Both directions use the full/sparse scan split (see class docstring):
        backward (historical) dates full-scan on a fixed calendar (every full_scan_every_days by date ordinal) and in
        between read only the pairs with a balance in the following full_scan_every_days measured days; pairs a backward
        full scan finds that the sparse days missed are backfilled for the later dates since the previous backward full scan.
        A backfill is a run with a larger `days`, e.g. days=(date.today() - date(2026, 1, 1)).days.
        """
        tokens = self._get_tokens(origin_key)
        token_by_id = {t['token_id']: t for t in tokens}
        cand = self.get_candidates(origin_key)
        cand = cand[cand['pruned_on'].isna()]
        if cand.empty or not tokens:
            print(f"{origin_key}: nothing to measure (candidates: {len(cand)}, tokens: {len(tokens)}).")
            return

        target_dates = [date.today() - timedelta(days=d) for d in range(days, 0, -1)]
        # blocks also cover the days before the window, for backfilling pairs found by a full scan
        blocks = self._get_end_of_day_blocks(origin_key, target_dates[0] - timedelta(days=self.full_scan_every_days))
        target_dates = [d for d in target_dates if d in blocks]
        if not target_dates:
            print(f"ERROR {origin_key}: no 'first_block_of_day' data for the target range. Backfill it first.")
            return

        # state as dicts for fast in-memory updates
        ranges = {r.address: [self._as_date(r.measured_from), self._as_date(r.measured_to)] for r in cand.itertuples()}
        last_full = {r.address: self._as_date(r.last_full_scan) for r in cand.itertuples()}
        # backward state (set on the first backward date, after the forward pass): measured_from per address, last
        # backward full scan per address, and {date: {address: {token_id}}} with non-zero balances for the following dates
        start_from = {}
        prev_full_backward = {}
        later = None

        for direction, dates in (('forward', target_dates), ('backward', list(reversed(target_dates)))):
            for d in dates:
                if direction == 'forward':
                    todo = [a for a, (f, t) in ranges.items() if t is None or d > t]
                else:
                    todo = [a for a, (f, t) in ranges.items() if f is not None and d == f - timedelta(days=1)]
                if not todo:
                    continue
                active_tokens = [t for t in tokens if t['max_date'] is None or d <= t['max_date']]

                if direction == 'backward':
                    if later is None:
                        start_from = {a: f for a, (f, _) in ranges.items() if f is not None}
                        later = self._get_pairs_by_date(origin_key, min(start_from.values()), max(start_from.values()) + timedelta(days=self.full_scan_every_days - 1))
                    full = todo if d.toordinal() % self.full_scan_every_days == 0 else []
                    sparse = [] if full else todo
                    # pairs with a balance in the following full_scan_every_days days
                    active = {}
                    for n in range(1, self.full_scan_every_days + 1):
                        for a, tids in later.get(d + timedelta(days=n), {}).items():
                            active.setdefault(a, set()).update(tids)
                else:
                    full = [a for a in todo if last_full[a] is None or (d - last_full[a]).days >= self.full_scan_every_days]
                    full_set = set(full)
                    sparse = [a for a in todo if a not in full_set]
                    # pairs with a balance since each address's last full scan
                    active = self._get_active_pairs(origin_key, [a for a in todo if last_full[a] is not None], d, last_full)

                pairs = [(a, t) for t in active_tokens for a in full if a not in t['exclude']]
                pairs += [(a, token_by_id[tid]) for a in sparse for tid in active.get(a, set()) if tid in token_by_id and a not in token_by_id[tid]['exclude']]
                df = self._read_balances(origin_key, pairs, blocks[d])
                self._store_balances(origin_key, d, df)

                # backfill pairs that a full scan found newly funded, for the days since the previous full scan
                if direction == 'forward':
                    funded = df.groupby('address')['token_id'].apply(set).to_dict() if not df.empty else {}
                    gaps = {}
                    for a in full:
                        if last_full[a] is None:
                            continue
                        for tid in funded.get(a, set()) - active.get(a, set()):
                            for n in range(1, (d - last_full[a]).days):
                                gaps.setdefault(last_full[a] + timedelta(days=n), []).append((a, token_by_id[tid]))
                    for gap_date, gap_pairs in sorted(gaps.items()):
                        if gap_date in blocks:
                            self._store_balances(origin_key, gap_date, self._read_balances(origin_key, gap_pairs, blocks[gap_date]))
                    if gaps:
                        print(f"- {origin_key} {d}: backfilled {sum(len(p) for p in gaps.values())} newly funded pair-days since the last full scan.")
                    for a in full:
                        last_full[a] = d
                else:
                    self._add_pairs(later, d, df)
                    # backfill pairs this full scan found that the sparse days since the previous backward full scan skipped
                    funded = df.groupby('address')['token_id'].apply(set).to_dict() if not df.empty else {}
                    gaps = {}
                    for a in full:
                        until = prev_full_backward.get(a, start_from.get(a, d))
                        for tid in funded.get(a, set()) - active.get(a, set()):
                            for n in range(1, (until - d).days):
                                gaps.setdefault(d + timedelta(days=n), []).append((a, token_by_id[tid]))
                    for gap_date, gap_pairs in sorted(gaps.items()):
                        if gap_date in blocks:
                            gap_df = self._read_balances(origin_key, gap_pairs, blocks[gap_date])
                            self._store_balances(origin_key, gap_date, gap_df)
                            self._add_pairs(later, gap_date, gap_df)
                    if gaps:
                        print(f"- {origin_key} {d}: backfilled {sum(len(p) for p in gaps.values())} newly funded pair-days since the previous backward full scan.")
                    for a in full:
                        prev_full_backward[a] = d
                    # keep only the dates later backward dates still look at
                    for old in [k for k in later if k > d + timedelta(days=self.full_scan_every_days)]:
                        del later[old]

                # update progress only after balances are stored
                if direction == 'backward':
                    # only measured_from, so a concurrent forward run's measured_to/last_full_scan are never overwritten
                    for a in todo:
                        ranges[a][0] = d
                    self._upsert_candidates(pd.DataFrame({'origin_key': origin_key, 'address': todo, 'measured_from': d}))
                    print(f"{origin_key} {d} (block {blocks[d]}, {direction}): {len(full)} full + {len(sparse)} sparse addresses, {len(pairs)} pairs -> {len(df)} non-zero balances.")
                    continue
                for a in todo:
                    f, t = ranges[a]
                    if t is not None and d > t + timedelta(days=1):
                        f = None # stale candidate with a gap before this window: restart the contiguous range
                    ranges[a] = [d if f is None or d < f else f, d if t is None or d > t else t]
                self._upsert_candidates(pd.DataFrame({
                    'origin_key': origin_key,
                    'address': todo,
                    'measured_from': [ranges[a][0] for a in todo],
                    'measured_to': [ranges[a][1] for a in todo],
                    'last_full_scan': [last_full[a] for a in todo],
                }))
                print(f"{origin_key} {d} (block {blocks[d]}, {direction}): {len(full)} full + {len(sparse)} sparse addresses, {len(pairs)} pairs -> {len(df)} non-zero balances.")

    def _get_active_pairs(self, origin_key: str, addresses: list, d: date, last_full: dict) -> dict:
        """ {address: {token_id}} with a non-zero balance between the address's last full scan and d (exclusive) """
        if not addresses:
            return {}
        since = min(last_full[a] for a in addresses)
        df = self.db_connector.execute_query(f"""
            SELECT DISTINCT '0x' || encode(address, 'hex') AS address, token_id, "date"
            FROM public.fact_stables_balances
            WHERE origin_key = '{origin_key}' AND "date" >= '{since}' AND "date" < '{d}'
        """, load_df=True)
        active = {}
        for r in df.itertuples():
            if r.address in last_full and last_full[r.address] is not None and self._as_date(r.date) >= last_full[r.address]:
                active.setdefault(r.address, set()).add(r.token_id)
        return active

    def _get_pairs_by_date(self, origin_key: str, since: date, until: date) -> dict:
        """ {date: {address: {token_id}}} with a non-zero balance between since and until (inclusive) """
        df = self.db_connector.execute_query(f"""
            SELECT DISTINCT '0x' || encode(address, 'hex') AS address, token_id, "date"
            FROM public.fact_stables_balances
            WHERE origin_key = '{origin_key}' AND "date" BETWEEN '{since}' AND '{until}'
        """, load_df=True)
        by_date = {}
        for r in df.itertuples():
            by_date.setdefault(self._as_date(r.date), {}).setdefault(r.address, set()).add(r.token_id)
        return by_date

    @staticmethod
    def _add_pairs(by_date: dict, d: date, df: pd.DataFrame):
        for r in df.itertuples():
            by_date.setdefault(d, {}).setdefault(r.address, set()).add(r.token_id)

    def _store_balances(self, origin_key: str, d: date, df: pd.DataFrame):
        if df.empty:
            return
        df = df.copy()
        df['date'] = d
        df['origin_key'] = origin_key
        df['address'] = df['address'].str.replace('0x', '\\x', regex=False)
        self.db_connector.upsert_table('fact_stables_balances', df.set_index(['date', 'origin_key', 'token_id', 'address']))

    def _read_balances(self, origin_key: str, pairs: list, block_number: int) -> pd.DataFrame:
        """
        balanceOf for (address, token) pairs via Multicall3.aggregate3 with allowFailure=True, batches in parallel.
        A failed sub-call (e.g. token not yet deployed at this block) is skipped, never stored as 0.
        """
        size = self._batch_sizes.get(origin_key, self.batch_size)
        chunks = [pairs[i:i + size] for i in range(0, len(pairs), size)]

        # any batch failing at min_batch_size on all RPCs raises, so the date is not marked as measured
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            all_results = list(executor.map(lambda c: self._read_chunk(origin_key, c, block_number), chunks))

        rows = []
        failed = 0
        for chunk, results in zip(chunks, all_results):
            for (a, t), (success, data) in zip(chunk, results):
                if not success or len(data) < 32:
                    failed += 1
                    continue
                raw = int.from_bytes(data[:32], 'big')
                if raw > 0:
                    rows.append({'address': a, 'token_id': t['token_id'], 'balance': Decimal(raw).scaleb(-t['decimals'])})
        if failed:
            print(f"- {origin_key} block {block_number}: {failed}/{len(pairs)} balanceOf sub-calls failed (skipped).")
        return pd.DataFrame(rows, columns=['address', 'token_id', 'balance'])

    def _read_chunk(self, origin_key: str, chunk: list, block_number: int) -> list:
        """ One Multicall3 call; if every RPC rejects it, halve the chunk (and the chain's batch size) and retry. """
        calls = [(Web3.to_checksum_address(t['address']), True, BALANCE_OF_SELECTOR + encode(['address'], [Web3.to_checksum_address(a)]))
                 for a, t in chunk]
        try:
            return self._call_with_rpc_failover(
                origin_key, lambda w3: w3.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI).functions.aggregate3(calls).call(block_identifier=block_number))
        except RuntimeError:
            if len(chunk) <= self.min_batch_size:
                raise
            half = len(chunk) // 2
            with self._lock:
                if self._batch_sizes.get(origin_key, self.batch_size) > half:
                    self._batch_sizes[origin_key] = max(half, self.min_batch_size)
                    print(f"- {origin_key}: batch size reduced to {self._batch_sizes[origin_key]}.")
            return self._read_chunk(origin_key, chunk[:half], block_number) + self._read_chunk(origin_key, chunk[half:], block_number)


    #-#-#-# Helpers #-#-#-#

    def get_candidates(self, origin_key: str) -> pd.DataFrame:
        query = f"""
            SELECT '0x' || encode(address, 'hex') AS address, sources, first_discovered, last_discovered,
                   measured_from, measured_to, last_full_scan, pruned_on
            FROM public.sys_stables_balances_candidates
            WHERE origin_key = '{origin_key}'
        """
        return self.db_connector.execute_query(query, load_df=True)

    def _upsert_candidates(self, df: pd.DataFrame):
        # only the columns present in df are updated (pangres ON CONFLICT DO UPDATE)
        df = df.copy()
        df['address'] = df['address'].str.replace('0x', '\\x', regex=False)
        self.db_connector.upsert_table('sys_stables_balances_candidates', df.set_index(['origin_key', 'address']))

    def _get_tokens(self, origin_key: str) -> list:
        tokens = []
        for token_id, v in self.address_mapping.get(origin_key, {}).items():
            if isinstance(v, dict) and 'address' in v:
                tokens.append({
                    'token_id': token_id,
                    'address': v['address'],
                    'decimals': int(v['decimals']),
                    'max_date': pd.to_datetime(v['max_date']).date() if v.get('max_date') else None,
                    # addresses excluded from circulating supply in metrics_stables_v2 (e.g. issuer treasuries) are never measured
                    'exclude': {a.lower() for a in v.get('exclude_balances', [])},
                })
        return tokens

    def _get_end_of_day_blocks(self, origin_key: str, start_date: date) -> dict:
        """ {date: last block of that date} = first_block_of_day(date + 1) - 1, same convention as adapter_stables_v2 """
        first_blocks = self.db_connector.get_first_block_of_the_day_range(origin_key, start_date.isoformat())
        return {
            (datetime.strptime(d, '%Y-%m-%d').date() - timedelta(days=1)): b - 1
            for d, b in first_blocks.items() if b > 0
        }

    def _get_rpcs(self, origin_key: str) -> list:
        # non-special RPCs only (special_use RPCs are paid): active ones first, then inactive ones as fallback.
        # 'active' reflects keeping up with the chain head, which historical reads don't need.
        if origin_key not in self._rpcs:
            df = self.db_connector.execute_query(f"""
                SELECT url FROM public.sys_rpc_config
                WHERE origin_key = '{origin_key}' AND COALESCE(special_use, false) = false
                ORDER BY COALESCE(active, false) DESC, COALESCE(synced, false) DESC
            """, load_df=True)
            self._rpcs[origin_key] = list(dict.fromkeys(df['url']))
        if not self._rpcs[origin_key]:
            raise ValueError(f"No RPCs found for chain {origin_key}. Please add RPCs to sys_rpc_config.")
        return self._rpcs[origin_key]

    def _call_with_rpc_failover(self, origin_key: str, call_fn):
        last_error = None
        rpcs = self._get_rpcs(origin_key)
        with self._lock:
            candidates = list(rpcs)
        for rpc in candidates:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": self.rpc_timeout_seconds}))
                with self._lock:
                    self.rpc_calls += 1
                return call_fn(w3)
            except Exception as e:
                last_error = e
                print(f"RPC call failed on {origin_key} with {rpc[:40]}...: {str(e)[:200]}")
                # demote failing RPCs (e.g. pruned nodes without historical state) to the end of the list
                with self._lock:
                    if rpc in rpcs:
                        rpcs.remove(rpc)
                        rpcs.append(rpc)
        raise RuntimeError(f"RPC call failed on {origin_key} for all RPCs.") from last_error

    @staticmethod
    def _to_hex(address) -> str:
        if isinstance(address, (bytes, bytearray, memoryview)):
            return '0x' + bytes(address).hex()
        return str(address).lower().replace('\\x', '0x')

    @staticmethod
    def _as_date(value):
        return None if pd.isna(value) else pd.to_datetime(value).date()

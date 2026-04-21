import os
import asyncio
import pandas as pd
from datetime import date, datetime, timedelta

from web3 import Web3
from starknet_py.net.full_node_client import FullNodeClient # Starknet w3 equivalent
from starknet_py.contract import Contract
from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.adapter_SupplyReader import SupplyReaderAdapter
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

#!# Logic requires 'first_block_of_day' data to be available in fact_kpis.
#!# Everytime adapter is inititated, sys_stables_v2 table is updated
#!# If chains is ['*'], then only pulls data for chains that are flagged as ['PROD', 'ZIRCUIT', 'DEV']
#!# Tracking totalSupply through Dune events is not reliable for total_supply, as some developers decided not to emit events to save on gas.

class AdapterStablecoinSupply(AbstractAdapter):
    """
    Adapter for scraping stablecoin supply across different chains.

    Args:
        adapter_params (dict): Dictionary containing parameters for the adapter. Expected keys are:
        - nothing
        db_connector: Database connector instance for interacting with the database.
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Stablecoin Adapter v2", adapter_params, db_connector)
        
        # Store stablecoin metadata and mapping
        import requests
        _url = "https://raw.githubusercontent.com/growthepie/gtp-dna/main/stables/stables_config_v2.py"
        _response = requests.get(_url)
        _response.raise_for_status()
        _config = {}
        exec(_response.text, _config)
        self.address_mapping = _config['address_mapping']
        self.coin_mapping = _config['coin_mapping']

        # try to update sys_stables_v2
        self.update_sys_stables_v2(self.coin_mapping)

        # keep track of which chain is currently being used
        self.current_chain = None
        self.current_rpc = None
        self.list_of_rpcs = None
        self.current_rpc_index = 0
        self.rpc_timeout_seconds = 5

        # abi for reading totalSupply and balanceOf directly from ERC20 contracts
        self.erc20_abi = [
            {"constant": True, "inputs": [], "name": "totalSupply", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
            {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"}
        ]

        # load chain main config from db
        self.config = self.db_connector.get_table("sys_main_conf")
        self.config = self.config[['origin_key', 'deployed_supplyreader', 'aliases_dune']]


    def extract(self, extract_params: dict):
        """
        Extract stablecoin supply data for specified chains and token_ids.
        
        Args:
            extract_params (dict): Dictionary containing the parameters for extraction. Expected keys are:
            - origin_keys (list): List of chain names to extract data for.  Use ['*'] for all (chains that are in PROD, DEV or ZIRCUIT).
            - token_ids (list): List of token_ids to extract data for. Use ['*'] for all.
            - metric_keys (list): List of metric_keys to extract data for. Use ['*'] for all. Options are 'total_supply', 'exclude_balances', 'track_on_l1'
        """
        # all data combined
        df_all = None

        chains = extract_params.get('origin_keys', ['*'])
        token_ids = extract_params.get('token_ids', ['*'])
        metric_keys = extract_params.get('metric_keys', ['*'])

        print("Extracting stables data for chains:", chains, "and token_ids:", token_ids, "and metric_keys:", metric_keys)

        # exchange '*' for all chains in mapping
        if chains == ['*']:
            chains_mapping = list(self.address_mapping.keys())
            chains_conf = self.db_connector.get_all_L1_L2_origin_keys_prod(['PROD', 'ZIRCUIT', 'DEV'])
            # select only chains that are in both 
            chains = list(set(chains_mapping) & set(chains_conf))
            # print warning for chains that are in mapping but not in conf and vice versa
            chains_not_in_conf = list(set(chains_mapping) - set(chains_conf))
            chains_not_in_mapping = list(set(chains_conf) - set(chains_mapping))
            if len(chains_not_in_conf) > 0:
                print(f"The following chains are in address_mapping but 'ARCHIVE'd in main_config and will be skipped: {chains_not_in_conf}. If these chains should be pulled in, please check gtp_dna github and set the column api_deployment_flag to ('PROD', 'DEV' or 'ZIRCUIT').")
            if len(chains_not_in_mapping) > 0:
                print(f"The following chains are in main_config but not in the address_mapping: {chains_not_in_mapping}. Please add them to the mapping in src/stables_config_v2.py.")
        # exchange '*' for all token_ids in mapping
        if token_ids == ['*']:
            token_ids = [coin['token_id'] for coin in self.coin_mapping]
        # exchange '*' for all metric_keys
        if metric_keys == ['*']:
            metric_keys = ['total_supply', 'exclude_balances', 'track_on_l1'] # 'volume' and 'transactions' ... not implemented yet

        # get db_progress, DataFrame that keeps track of which coins, chains and metric_keys are up to date. Used to determine from which day onwards to pull data.
        db_progress = self.get_db_progress()

        # iterate through each chain and get data
        for chain in chains:

            # check if chain is in mapping, if not skip and log warning
            if chain not in self.address_mapping:
                print(f"No stablecoins in address mapping for chain '{chain}', skipping. Please check the file: src/stables_config_v2.py.")
                continue

            # get df with all data for the chain and merge into df_all
            df = self.extract_data_from_chain(chain, token_ids, metric_keys, db_progress)

            # merge df into df_all
            if df_all is None:
                df_all = df
            elif df is not None:
                df_all = pd.concat([df_all, df], ignore_index=True)

        # return the combined df with all data
        return df_all
    

    # load data into db
    def load(self, df, table_name: str="fact_stables_v2"):
        """
        Load the extracted data into the database.

        Args:
            df (DataFrame): DataFrame from extract as is.
            table_name (str): Name of the table to load the data into. Default is "fact_stables_v2".
        """
        # return early if df empty
        if df.empty:
            print("Nothing to load.")
            return
        # make sure address is lowercase
        df['address'] = df['address'].str.lower()
        # set index and upsert
        df = df.set_index(['date', 'origin_key', 'metric_key', 'token_id', 'address', 'decimals'])
        self.db_connector.upsert_table(table_name, df)
        print(f"Loaded {len(df)} records into {table_name}.")


    #-#-#-# Helper Functions #-#-#-#


    def extract_data_from_chain(self, chain, token_ids, metric_keys, db_progress, pretend_today_is=None):
        """
        Function to get stablecoin data for a specific chain and (multiple) token_ids and (multiple) metric_keys.
        
        Args:
            chain (str): The origin_key of the chain to get data for.
            token_ids (list): List of token_ids to get data for.
            metric_keys (list): List of metric_keys to extract data for.
            db_progress (DataFrame): DataFrame that keeps track of which coins, chains and metric_keys are up to date. Used to determine from which day onwards to pull data.
            pretend_today_is (pd.Timestamp, optional): A date string to pretend as today's date for testing purposes. Defaults to today. e.g. pd.Timestamp('2024-01-01').date()
        """
        # combine all data into one df
        df_all = pd.DataFrame()

        for metric_key in metric_keys:

            ## extract data for 'total_supply'
            if metric_key == 'total_supply':

                # pull total_supply data from RPCs (100% reliable method)
                df = self.total_supply_from_rpc(chain, token_ids, db_progress, pretend_today_is=pretend_today_is)
                
                # merge df into df_all
                if df.empty:
                    continue
                df['metric_key'] = 'total_supply'
                df_all = pd.concat([df_all, df], ignore_index=True)

            ## extract data for 'exclude_balances'
            if metric_key == 'exclude_balances':

                # pull exclude_balances from RPCs (100% reliable method)
                df = self.exclude_balances_from_rpc(chain, token_ids, db_progress, pretend_today_is=pretend_today_is)

                # merge df into df_all
                if df.empty:
                    continue
                df['metric_key'] = 'exclude_balances'
                df_all = pd.concat([df_all, df], ignore_index=True)

            ## extract data for 'track_on_l1'
            if metric_key == 'track_on_l1':

                # pull track_on_l1 data from RPCs, use Dune for backfill old data
                df = self.track_on_l1_from_rpc_or_dune(chain, token_ids, db_progress, pretend_today_is=pretend_today_is)

                # merge df into df_all
                if df.empty:
                    continue
                df['metric_key'] = 'track_on_l1'
                df_all = pd.concat([df_all, df], ignore_index=True)

            ## extract data for volume ...
            # if metric_key == 'volume': ...

        return df_all
                
    def total_supply_from_rpc(self, chain, token_ids, db_progress, pretend_today_is=None):
        # first we check if chain mapping is defined with 'total_supply' (or 'track_on_l1'), if not skip
        if 'total_supply' != self.check_supply_mapping_for_chain(chain, self.address_mapping):
            return pd.DataFrame()
        
        # check if SupplyReader is deployed on the chain
        chain_config = self.config[self.config['origin_key'] == chain]
        supplyreader_deployed_raw = chain_config['deployed_supplyreader'].iloc[0]
        is_supplyreader_deployed = pd.notna(supplyreader_deployed_raw) and supplyreader_deployed_raw != ''
        is_supplyreader_deployed_date = (pd.to_datetime(supplyreader_deployed_raw).date() if is_supplyreader_deployed else None)

        # create specific chain db_progress df (includes new coins and removes already upto-date coins)
        db_progress_filtered = self.get_total_supply_progress(chain, token_ids, db_progress)
        if db_progress_filtered.empty:
            print(f"WARNING {chain}: No token_ids found or already fully backfilled. Continuing.")
            return pd.DataFrame()
        print(f"Pulling 'total_supply' on {chain} for token_ids: {db_progress_filtered['token_id'].tolist()}")

        # replace NaN with a dummy old date
        db_progress_filtered = db_progress_filtered.fillna({'date': pd.Timestamp('2000-01-01').date()})
        block_date_mapping = self.get_last_block_of_day_from_db(chain, db_progress_filtered['date'].min().isoformat())
        if pretend_today_is is not None:
            block_date_mapping = block_date_mapping[block_date_mapping['date'] <= pretend_today_is]
        # send warning & return if no block_date_mapping is found
        if len(block_date_mapping) == 0: # raise error if we have no 'first_block_of_day' data for the chain
            print(f"ERROR: Missing dates in block date mapping for chain {chain}. Can't pull in stablecoin supply via RPC. Please backfill 'first_block_of_day' first.")
            return pd.DataFrame()
        
        # keep track of everything
        df_supplies_all = pd.DataFrame()

        # going from newest to oldest date
        for index, row in block_date_mapping.iterrows():
            block_number = row['value']
            date = row['date']

            # can we use SupplyReader for this date to pull data?
            use_supply_reader = (is_supplyreader_deployed and is_supplyreader_deployed_date <= pd.to_datetime(date).date())

            # Yes, use SupplyReader :)
            if use_supply_reader:
                try: 
                    # skip rpc call if max_date is reached for that token_id only
                    db_progress_filtered_temp = db_progress_filtered[~(pd.to_datetime(db_progress_filtered['max_date']) < pd.to_datetime(date))]
                    if db_progress_filtered_temp.empty:
                        #print(f"- Skipping SupplyReader for all token_ids on {date} as they have all reached their max_date.")
                        continue

                    supplies = self.read_total_supplies_SupplyReader(
                        chain=chain,
                        token_addresses=db_progress_filtered_temp['address'].tolist(),
                        block_number=block_number,
                    )
                    print(f"- Used SupplyReader for {len(db_progress_filtered_temp)} token_ids, chain {chain}, {date} = block {block_number}: {supplies}")
                    df_supplies = db_progress_filtered_temp[['origin_key', 'token_id', 'address', 'decimals']].copy()
                    df_supplies['value'] = supplies / (10 ** df_supplies['decimals'])
                    df_supplies['date'] = date
                    df_supplies_all = pd.concat([df_supplies_all, df_supplies], ignore_index=True)

                    # remove coins from db_progress_filtered if supply is 0
                    if 0 in supplies:
                        to_be_removed_coins = db_progress_filtered[db_progress_filtered['token_id'].isin([db_progress_filtered['token_id'].iloc[i] for i, s in enumerate(supplies) if s == 0])]
                        db_progress_filtered = db_progress_filtered[~db_progress_filtered['token_id'].isin(to_be_removed_coins['token_id'])]
                        print(f"- Removed {len(to_be_removed_coins)} coin(s) with 0 supply from db_progress_filtered: {to_be_removed_coins['token_id'].tolist()}")
                        db_progress_filtered = db_progress_filtered.reset_index(drop=True)
                    # remove coins from db_progress_filtered if backfill date is reached
                    if db_progress_filtered['date'].max() >= date - timedelta(days=1):
                        to_be_removed_coins = db_progress_filtered[db_progress_filtered['date'] >= date - timedelta(days=1)]
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['date'] < date - timedelta(days=1)]
                        print(f"- Removed {len(to_be_removed_coins)} coin(s) from db_progress_filtered based on date: {to_be_removed_coins['token_id'].tolist()}")
                        db_progress_filtered = db_progress_filtered.reset_index(drop=True)
                    # remove coins from db_progress_filtered if min_date is reached
                    to_be_removed_coins = db_progress_filtered[pd.to_datetime(db_progress_filtered['min_date']) >= pd.to_datetime(date)]
                    if len(to_be_removed_coins) > 0:
                        db_progress_filtered = db_progress_filtered[~db_progress_filtered['token_id'].isin(to_be_removed_coins['token_id'])]
                        print(f"- Removed {len(to_be_removed_coins)} coin(s) from db_progress_filtered based on min_date: {to_be_removed_coins['token_id'].tolist()}")
                        db_progress_filtered = db_progress_filtered.reset_index(drop=True)
                    # break if db_progress_filtered is empty after removals
                    if len(db_progress_filtered) == 0:
                        print("- All coins fully processed, exiting loop.")
                        break
                    continue
                except Exception as e:
                    print(f"SupplyReader failed for chain {chain} on block {block_number} and date {date}. Falling back to single RPC calls: {e}")

            # No, single RPC calls :(
            for index, row in db_progress_filtered.iterrows():
                origin_key = row['origin_key']
                token_id = row['token_id']
                address = row['address']
                decimals = row['decimals']

                # skip rpc call if max_date is reached for that token_id only
                if pd.notna(row['max_date']) and pd.to_datetime(date) > pd.to_datetime(row['max_date']):
                    #print(f"- Skipping RPC call for {token_id} on {date} as it has reached its max_date ({row['max_date']}).")
                    continue

                try:
                    supply = self.read_total_supply_rpc(
                        chain=chain,
                        address=address,
                        block_number=block_number,
                        decimals=decimals,
                    )
                    df_supplies_all = pd.concat([df_supplies_all, pd.DataFrame({
                        'origin_key': [origin_key],
                        'token_id': [token_id],
                        'address': [address],
                        'decimals': [decimals],
                        'value': [supply],
                        'date': [date]
                    })], ignore_index=True)
                    print(f"- Used single RPC call for {token_id}, chain {origin_key}, {date} = block {block_number}: {supply}")

                    # remove coin from db_progress_filtered if supply is 0
                    if supply == 0:
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        print(f"- Removed {token_id} from db_progress_filtered for chain {chain} on date {date} due to 0 supply.")
                    # remove coin from db_progress_filtered if backfill date is reached
                    elif db_progress_filtered[db_progress_filtered['token_id'] == token_id]['date'].max() >= date - timedelta(days=1):
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        print(f"- Removed {token_id} from db_progress_filtered for chain {chain} on date {date} due to backfill date reached.")
                    # remove coin from db_progress_filtered if min_date is reached
                    elif pd.notna(db_progress_filtered[db_progress_filtered['token_id'] == token_id]['min_date'].iloc[0]) and db_progress_filtered[db_progress_filtered['token_id'] == token_id]['min_date'].iloc[0] >= date:
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        print(f"- Removed {token_id} from db_progress_filtered for chain {chain} on date {date} due to min_date reached.")
                    # break if db_progress_filtered is empty after removals
                    if len(db_progress_filtered) == 0:
                        print("- All coins fully processed, exiting loop.")
                        break
                except Exception as e:
                    # remove coins from db_progress_filtered if contract not yet deployed
                    if self._is_contract_not_deployed_error(e):
                        print(f"- Removed {token_id} from db_progress_filtered for chain {chain} on date {date} due to contract not deployed yet.")
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        if len(db_progress_filtered) == 0:
                            break
                    else:
                        print(f"RPC call failed for chain {chain}, token {token_id} on block {block_number} and date {date}: {e}")

        # return and print report
        if df_supplies_all.empty:
            print(f"No new stablecoin supply data for chain '{chain}' found using RPC calls.")
        else:
            df_supplies_all['method'] = 'rpc'
            print(f"Successfully pulled stablecoin supply data for chain '{chain}' using RPC calls for {len(df_supplies_all)} records.")
        return df_supplies_all
    
    def exclude_balances_from_rpc(self, chain, token_ids, db_progress, pretend_today_is=None):
        # check if chain mapping has any token_ids with 'exclude_balances', if not skip
        if 'total_supply' != self.check_supply_mapping_for_chain(chain, self.address_mapping):
            exclude_balances = next(
                (
                    token_data['exclude_balances']
                    for token_data in self.address_mapping.get(chain, {}).values()
                    if 'exclude_balances' in token_data
                ),
                None
            )
            if exclude_balances is None:
                return pd.DataFrame()

        # check if SupplyReader is deployed on the chain
        chain_config = self.config[self.config['origin_key'] == chain]
        supplyreader_deployed_raw = chain_config['deployed_supplyreader'].iloc[0]
        is_supplyreader_deployed = pd.notna(supplyreader_deployed_raw) and supplyreader_deployed_raw != ''
        is_supplyreader_deployed_date = (pd.to_datetime(supplyreader_deployed_raw).date() if is_supplyreader_deployed else None)

        # create specific chain db_progress df (includes new coins and removes already upto-date coins)
        db_progress_filtered = self.get_exclude_balances_progress(chain, token_ids, db_progress)
        if db_progress_filtered.empty:
            print(f"WARNING {chain}: No token_ids found or already fully backfilled. Continuing.")
            return pd.DataFrame()
        print(f"Pulling 'exclude_balances' on {chain} for token_ids: {db_progress_filtered['token_id'].tolist()}")
    
        # replace NaN with a dummy old date
        db_progress_filtered = db_progress_filtered.fillna({'date': pd.Timestamp('2000-01-01').date()})
        block_date_mapping = self.get_last_block_of_day_from_db(chain, db_progress_filtered['date'].min().isoformat())
        if pretend_today_is is not None:
            block_date_mapping = block_date_mapping[block_date_mapping['date'] <= pretend_today_is]
        # send warning & return if no block_date_mapping is found
        if len(block_date_mapping) == 0: # raise error if we have no 'first_block_of_day' data for the chain
            print(f"ERROR: Missing dates in block date mapping for chain {chain}. Can't pull in stablecoin supply via RPC. Please backfill 'first_block_of_day' first.")
            return pd.DataFrame()
        
        # keep track of everything
        df_balances_all = pd.DataFrame()

        # going from newest to oldest date
        for index, row in block_date_mapping.iterrows():
            block_number = row['value']
            date = row['date']

            # can we use SupplyReader for this date to pull data?
            use_supply_reader = (is_supplyreader_deployed and is_supplyreader_deployed_date <= pd.to_datetime(date).date())

            # single RPC calls: SupplyReader not yet implemented (no huge speed increases expected)
            for index, row in db_progress_filtered.iterrows():
                origin_key = row['origin_key']
                token_id = row['token_id']
                address = row['address']
                decimals = row['decimals']

                # skip rpc call if max_date is reached for that token_id only
                if pd.notna(row['max_date']) and pd.to_datetime(date) > pd.to_datetime(row['max_date']):
                    #print(f"- Skipping RPC call for {token_id} on {date} as it has reached its max_date ({row['max_date']}).")
                    continue

                try:
                    supply = self.read_balance_rpc(
                        chain=chain,
                        address=address,
                        token_address=self.address_mapping[chain][token_id]['address'],
                        block_number=block_number,
                        decimals=decimals,
                    )
                    df_balances_all = pd.concat([df_balances_all, pd.DataFrame({
                        'origin_key': [origin_key],
                        'token_id': [token_id],
                        'address': [address],
                        'decimals': [decimals],
                        'value': [supply],
                        'date': [date]
                    })], ignore_index=True)
                    print(f"- Used RPC on {token_id} for balance of {address}, chain {origin_key}, {date} = block {block_number}: {supply}")

                    # remove coin from db_progress_filtered if supply is 0
                    if supply == 0:
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        print(f"- Removed {token_id} for balance of {address}, from db_progress_filtered for chain {chain} on date {date} due to 0 supply.")
                    # remove coin from db_progress_filtered if backfill date is reached
                    elif db_progress_filtered[db_progress_filtered['token_id'] == token_id]['date'].max() >= date - timedelta(days=1):
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        print(f"- Removed {token_id} for balance of {address}, from db_progress_filtered for chain {chain} on date {date} due to backfill date reached.")
                    # remove coin from db_progress_filtered if min_date is reached
                    elif pd.notna(db_progress_filtered[db_progress_filtered['token_id'] == token_id]['min_date'].iloc[0]) and db_progress_filtered[db_progress_filtered['token_id'] == token_id]['min_date'].iloc[0] >= date:
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        print(f"- Removed {token_id} for balance of {address}, from db_progress_filtered for chain {chain} on date {date} due to min_date reached.")
                    # break if db_progress_filtered is empty after removals
                    if len(db_progress_filtered) == 0:
                        print("- All coins fully processed, exiting loop.")
                        break
                except Exception as e:
                    # remove coins from db_progress_filtered if contract not yet deployed
                    if self._is_contract_not_deployed_error(e):
                        print(f"- Removed {token_id} for balance of {address}, from db_progress_filtered for chain {chain} on date {date} due to contract not deployed yet.")
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        if len(db_progress_filtered) == 0:
                            break
                    else:
                        print(f"RPC call failed for chain {chain}, token {token_id} for balance of {address} on block {block_number} and date {date}: {e}")

        # return and print report
        if df_balances_all.empty:
            print(f"No new stablecoin balance data for chain '{chain}' found using RPC calls.")
        else:
            df_balances_all['method'] = 'rpc'
            print(f"Successfully pulled stablecoin balance data for chain '{chain}' using RPC calls for {len(df_balances_all)} records.")
        return df_balances_all


    def track_on_l1_from_rpc_or_dune(self, chain, token_ids, db_progress, min_amount=9999, pretend_today_is=None):
        # first we check if chain mapping is defined with 'track_on_l1', if not skip
        if 'track_on_l1' != self.check_supply_mapping_for_chain(chain, self.address_mapping):
            return pd.DataFrame()

        # stores all the extracted data
        df_all = pd.DataFrame()

        # get db_progress_filtered, DataFrame that keeps track of bridges are up to date
        db_progress_filtered = self.get_track_on_l1_progress(chain, self.address_mapping)
        
        # fill in missing dates with '2025-01-01'
        #db_progress_filtered = db_progress_filtered.fillna({'date': '2026-01-01'})

        token_address_df = db_progress[db_progress['origin_key'] == 'ethereum'][['token_id', 'address', 'decimals']]
        token_address_df = token_address_df[token_address_df['token_id'].isin(token_ids)].reset_index(drop=True)

        # use dune to backfill old data (where date in db_progress_filtered is null)
        if db_progress_filtered['date'].isnull().any():
            
            print(f"Pulling track_on_l1 data for chain {chain} from Dune for {len(db_progress_filtered)} bridge addresses, full history...")

            # extract dune data (full history)
            from src.adapters.adapter_dune import AdapterDune
            dune = AdapterDune({'api_key' : os.getenv("DUNE_API")}, self.db_connector)
            load_params = {
                'queries': [
                    {
                        'name': 'stablecoin_balances_bridges',
                        'query_id': 6688616,
                        'params': {
                            'chain': 'ethereum',
                            'decimals': ','.join(token_address_df['decimals'].astype(str).tolist()),
                            'eoa_addresses': ','.join(db_progress_filtered['address'].tolist()),
                            'erc20_addresses': ','.join(token_address_df['address'].tolist()),
                            'token_ids': ','.join(token_address_df['token_id'].tolist()),
                            'min_amount': min_amount
                        }
                    }
                ]
            }
            df = dune.extract(load_params)

            # adjust df and return (as we just pulled everything from dune, we can skip the rest of the function)
            df = df.rename(columns={'totalSupply': 'value', 'eoa_address': 'address'})
            df['method'] = 'dune'
            df['origin_key'] = chain
            return df
        
        # check if SupplyReader is deployed on ethereum and from which date
        chain_config_eth = self.config[self.config['origin_key'] == 'ethereum']
        supplyreader_eth_raw = chain_config_eth['deployed_supplyreader'].iloc[0] if not chain_config_eth.empty else None
        is_eth_supplyreader = pd.notna(supplyreader_eth_raw) and supplyreader_eth_raw != ''
        eth_supplyreader_date = (pd.to_datetime(supplyreader_eth_raw).date() if is_eth_supplyreader else None)

        # use rpc to backfill recent dates
        for index, row in db_progress_filtered.iterrows():
            bridge = row['address']

            # get block date map for ethereum
            block_date_mapping = self.get_last_block_of_day_from_db('ethereum', row['date'])
            block_date_mapping = block_date_mapping[block_date_mapping['date'] <= (pd.to_datetime('today').date() if pretend_today_is is None else pretend_today_is)]

            # send warning & return if no block_date_mapping is found
            if len(block_date_mapping) == 0: # raise error if we have no 'first_block_of_day' data for the chain
                print(f"ERROR: Missing dates in block date mapping for chain {chain}. Can't pull in stablecoin track_on_l1 data via RPC or Dune. Please backfill 'first_block_of_day' first.")
                continue

            # pull in with RPC for each date in block_date_mapping
            for index, row in block_date_mapping.iterrows():
                block_number = row['value']
                date = row['date']

                use_supply_reader = is_eth_supplyreader and eth_supplyreader_date <= pd.to_datetime(date).date()

                if use_supply_reader:
                    # batch read via SupplyReader contract
                    balances = self.read_tokensBalance_SupplyReader('ethereum', bridge, token_address_df['address'].tolist(), block_number)
                    token_address_df['value'] = balances
                    token_address_df['value'] = token_address_df['value'] / (10 ** token_address_df['decimals'])
                else:
                    # fall back to individual balanceOf calls when SupplyReader not yet deployed on ethereum
                    token_address_df['value'] = [
                        self.read_balance_rpc('ethereum', bridge, r['address'], block_number, r['decimals'])
                        for _, r in token_address_df.iterrows()
                    ]

                token_address_df = token_address_df[token_address_df['value'] > min_amount].reset_index(drop=True) # minimum threshold for it to start be tracked
                df_day = token_address_df.copy()
                df_day['date'] = date
                df_day['address'] = bridge # we set the address to the bridge address, so we can track which bridge this token is sitting on
                df_day['method'] = 'rpc'

                # merge pulled data into df_all
                df_all = pd.concat([df_all, df_day], ignore_index=True)
                print(f"Pulled track_on_l1 data for bridge {bridge} on date {date} and block {block_number} using {'SupplyReader' if use_supply_reader else 'RPC'}, found {len(df_day)} token_ids.")

                # break in case we do not find any balances above the minimum threshold
                if len(token_address_df) == 0:
                    print(f"No tokens above the minimum threshold of {min_amount} found on bridge {bridge} for date {date}. Stopping further backfill for this bridge address.")
                    break

        # add origin_key
        df_all['origin_key'] = chain

        return df_all



    #-#-#-# Raw Helper Functions #-#-#-#

    def _is_contract_not_deployed_error(self, error: Exception) -> bool:
        error_str = str(error).lower()
        return (
            "could not decode contract function call" in error_str
            or "contract not found" in error_str
            or "contract error" in error_str
        )

    def _call_with_rpc_failover(self, chain: str, call_fn):
        """
        Execute a read-only chain call and rotate through chain RPCs on retryable failures.
        """
        self.get_new_w3(chain)  # sets self.list_of_rpcs
        max_attempts = len(self.list_of_rpcs)
        last_error = None

        for attempt in range(max_attempts):
            w3 = self.get_new_w3(chain, rotate=(attempt > 0))
            try:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(call_fn, w3)
                    return future.result(timeout=self.rpc_timeout_seconds)
            except FuturesTimeoutError:
                last_error = TimeoutError(f"RPC timed out after {self.rpc_timeout_seconds}s")
                print(f"RPC timed out on chain {chain} with RPC {self.current_rpc} (attempt {attempt + 1}/{max_attempts})")
            except Exception as e:
                if self._is_contract_not_deployed_error(e):
                    raise e
                last_error = e
                print(f"RPC call failed on chain {chain} with RPC {self.current_rpc} (attempt {attempt + 1}/{max_attempts}): {e}")

        raise RuntimeError(
            f"RPC call failed on chain {chain} after trying {max_attempts} RPC endpoints."
        ) from last_error

    def read_total_supplies_SupplyReader(self, chain: str, token_addresses: list, block_number: int = None):
        """
        Read raw total supplies using the SupplyReader contract with RPC failover.
        """
        def _call_fn(w3):
            self.SupplyReader = SupplyReaderAdapter(w3)
            return self.SupplyReader.get_total_supplies(token_addresses, block_number)

        return self._call_with_rpc_failover(chain, _call_fn)

    def read_tokensBalance_SupplyReader(self, chain: str, address: str, token_addresses: list, block_number: int = None):
        """
        Read raw total supplies using the SupplyReader contract with RPC failover.
        """
        def _call_fn(w3):
            self.SupplyReader = SupplyReaderAdapter(w3)
            return self.SupplyReader.tokens_balance(address, token_addresses, block_number)

        return self._call_with_rpc_failover(chain, _call_fn)

    def read_tokenBalances_SupplyReader(self, chain: str, addresses: str, token_address: list, block_number: int = None):
        """
        Read raw total supplies using the SupplyReader contract with RPC failover.
        """
        def _call_fn(w3):
            self.SupplyReader = SupplyReaderAdapter(w3)
            return self.SupplyReader.token_balances(addresses, token_address, block_number)

        return self._call_with_rpc_failover(chain, _call_fn)

    def read_total_supply_rpc(self, chain: str, address: str, block_number: int, decimals: int = 18) -> float:
        """
        Read token totalSupply via direct ERC20 contract call with RPC failover.
        """
        if chain == 'starknet': # special case for starknet!
            def _call_fn(w3):
                async def _read_total_supply():
                    contract = await Contract.from_address(address=int(address, 16), provider=w3)
                    result = await contract.functions["total_supply"].call(block_number=block_number)
                    value = result[0] if isinstance(result, (list, tuple)) else result
                    return value / (10 ** decimals)
                return asyncio.run(_read_total_supply())
            return self._call_with_rpc_failover(chain, _call_fn)
        else: # all other evm chains
            def _call_fn(w3):
                contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=self.erc20_abi)
                total_supply = contract.functions.totalSupply().call(block_identifier=block_number)
                return total_supply / (10 ** decimals)
            return self._call_with_rpc_failover(chain, _call_fn)

    def read_balance_rpc(self, chain: str, address: str, token_address: str, block_number: int, decimals: int = 18) -> float:
        """
        Read token balanceOf via direct ERC20 contract call with RPC failover.
        """
        if chain == 'starknet': # special case for starknet!
            def _call_fn(w3):
                async def _read_balance():
                    contract = await Contract.from_address(address=int(token_address, 16), provider=w3)
                    result = await contract.functions["balanceOf"].call(int(address, 16), block_number=block_number)
                    value = result[0] if isinstance(result, (list, tuple)) else result
                    return value / (10 ** decimals)
                return asyncio.run(_read_balance())
            return self._call_with_rpc_failover(chain, _call_fn)
        else: # all other evm chains
            def _call_fn(w3):
                contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=self.erc20_abi)
                balance = contract.functions.balanceOf(Web3.to_checksum_address(address)).call(block_identifier=block_number)
                return balance / (10 ** decimals)
            return self._call_with_rpc_failover(chain, _call_fn)

    def get_new_w3(self, chain: str, rotate: bool = False):
        """
        Create a new Web3 instance for a chain and optionally rotate to the next RPC.
        """
        if self.current_chain != chain or self.list_of_rpcs is None:
            primary_rpc = self.db_connector.get_special_use_rpc(chain)
            fallback_rpcs = self.db_connector.get_all_rpcs_for_chain(chain)
            all_rpcs = [primary_rpc] + fallback_rpcs

            # remove nulls and duplicates while preserving order
            self.list_of_rpcs = [rpc for rpc in dict.fromkeys(all_rpcs) if rpc]
            if len(self.list_of_rpcs) == 0:
                raise ValueError(f"No RPCs found for chain {chain}. Please add RPCs to your database for this chain.")

            self.current_chain = chain
            self.current_rpc_index = 0
            self.current_rpc = self.list_of_rpcs[self.current_rpc_index]
            print(f"Switched w3 instance to new chain: {chain} with RPC: {self.current_rpc}")
        elif rotate:
            self.current_rpc_index = (self.current_rpc_index + 1) % len(self.list_of_rpcs)
            self.current_rpc = self.list_of_rpcs[self.current_rpc_index]
            print(f"Rotating w3 instance to new RPC for chain: {chain} to RPC: {self.current_rpc}")

        if chain == 'starknet':
            return FullNodeClient(node_url=self.current_rpc)
        else:
            return Web3(Web3.HTTPProvider(
                self.current_rpc,
                request_kwargs={"timeout": (self.rpc_timeout_seconds, self.rpc_timeout_seconds)},
            ))

    def update_sys_stables_v2(self, coin_mapping):
        """
        Function to update the sys_stables_v2 table with the latest stablecoin metadata from the config file.
        """
        try:
            self.db_connector.delete_all_rows("sys_stables_v2")
            df = pd.DataFrame(coin_mapping).set_index('token_id')
            self.db_connector.upsert_table("sys_stables_v2", df)
            print(f"{len(df)} rows inserted into sys_stables_v2.")
        except Exception as e:
            print(f"Error updating sys_stables_v2 table: {e}")

    def get_last_block_of_day_from_db(self, chain: str, start_date: str):
        query = f"""
            SELECT
                DATE("date" - INTERVAL '1 day') as "date",
                CAST(value - 1 AS INTEGER) as value
            FROM public.fact_kpis
            WHERE
                metric_key = 'first_block_of_day'
                AND origin_key = '{chain}'
                AND "date" > '{start_date}'
                AND CAST(value - 1 AS INTEGER) >= 0 
            ORDER BY "date" DESC
        """
        result = self.db_connector.execute_query(query, load_df=True)
        return result
    
    def get_db_progress(self):
        """
        Function to check what stablecoin data we already have in our database and filled until which date.
        """
        query = f"""
            SELECT DISTINCT ON (f.origin_key, f.metric_key, f.token_id, f.address, f.decimals)
                f.date,
                f.origin_key,
                f.metric_key,
                f.token_id,
                f.address,
                f.decimals,
                f.method,
                f.value,
                s.owner_project,
                s.symbol,
                s.bridged_origin_chain,
                s.bridged_origin_token_id,
                s.coingecko_id,
                s.fiat,
                s.logo
            FROM public.fact_stables_v2 f
            LEFT JOIN public.sys_stables_v2 s ON f.token_id = s.token_id
            ORDER BY f.origin_key, f.metric_key, f.token_id, f.address, f.decimals, f.date DESC;
        """
        result = self.db_connector.execute_query(query, load_df=True)
        return result
    
    # function to know if a chain should be tracked via total_supply or track_on_l1
    def check_supply_mapping_for_chain(self, chain: str, address_mapping: dict):
        if chain not in address_mapping:
            print(f"No stablecoins in address mapping for chain '{chain}', skipping. Please check the file: src/stables_config_v2.py.")
            return
        if 'track_on_l1' in address_mapping[chain]:
            return 'track_on_l1'
        else:
            return 'total_supply'

    def get_total_supply_progress(self, chain: str, token_ids: list, db_progress: pd.DataFrame):
        # filter db_progress for specific chain and metric_key
        db_progress_filtered = db_progress[(db_progress['origin_key'] == chain) & (db_progress['metric_key'] == 'total_supply')]
        # filter token_ids down to what is actully deployed on this chain
        chain_token_ids = [token for token in self.address_mapping[chain] if token in token_ids]
        # see if we have new coins
        new_coins = [token for token in chain_token_ids if token not in db_progress_filtered['token_id'].unique()]
        # add the new coins to db_progress_filtered
        db_progress_filtered = pd.concat([db_progress_filtered, pd.DataFrame({
            'date': [None] * len(new_coins),
            'origin_key': [chain] * len(new_coins),
            'metric_key': ['total_supply'] * len(new_coins),
            'token_id': new_coins,
            'address': [self.address_mapping[chain][token]['address'].lower() for token in new_coins],
            'decimals': [self.address_mapping[chain][token]['decimals'] for token in new_coins],
            'value': [None] * len(new_coins),
        })], ignore_index=True)
        # filter out coins which are already at yesterdays date (= up to date)
        date_yesterday = (datetime.now() - timedelta(days=1)).date()
        db_progress_filtered = db_progress_filtered[db_progress_filtered['date'] != date_yesterday]
        # add two columns to keep track of the max and min date for backfilling, defined in address_mapping
        db_progress_filtered['max_date'] = None
        db_progress_filtered['min_date'] = None
        db_progress_filtered['max_date'] = db_progress_filtered.apply(lambda row: self.address_mapping[chain][row['token_id']]['max_date'] if pd.isna(row['max_date']) and row['token_id'] in self.address_mapping[chain] and 'max_date' in self.address_mapping[chain][row['token_id']] else row['max_date'], axis=1)
        db_progress_filtered['min_date'] = db_progress_filtered.apply(lambda row: self.address_mapping[chain][row['token_id']]['min_date'] if pd.isna(row['min_date']) and row['token_id'] in self.address_mapping[chain] and 'min_date' in self.address_mapping[chain][row['token_id']] else row['min_date'], axis=1)
        db_progress_filtered['max_date'] = pd.to_datetime(db_progress_filtered['max_date']).dt.date
        db_progress_filtered['min_date'] = pd.to_datetime(db_progress_filtered['min_date']).dt.date
        # filter out already fully backfilled coins based on max_date
        db_progress_filtered = db_progress_filtered[~((db_progress_filtered['date'] >= db_progress_filtered['max_date']) & (pd.notna(db_progress_filtered['max_date'])))]
        return db_progress_filtered
    
    def get_exclude_balances_progress(self, chain: str, token_ids: list, db_progress: pd.DataFrame):
        # filter db_progress for specific chain and metric_key
        db_progress_filtered = db_progress[(db_progress['origin_key'] == chain) & (db_progress['metric_key'] == 'exclude_balances')]
        # filter token_ids down to what is actully deployed on this chain
        chain_token_ids = [token for token in self.address_mapping[chain] if token in token_ids and 'exclude_balances' in self.address_mapping[chain][token]]
        # build one row per (token_id, exclude_balance address)
        new_rows = []
        for token in chain_token_ids:
            token_data = self.address_mapping[chain][token]
            for exclude_address in token_data['exclude_balances']:
                # skip if this (token_id, address) combo already exists in db_progress
                already_exists = (
                    (db_progress_filtered['token_id'] == token) &
                    (db_progress_filtered['address'] == exclude_address.lower())
                ).any()
                if not already_exists:
                    new_rows.append({
                        'date': None,
                        'origin_key': chain,
                        'metric_key': 'exclude_balances',
                        'token_id': token,
                        'address': exclude_address.lower(),
                        'decimals': token_data['decimals'],
                        'method': token_data.get('method', 'rpc'),
                        'value': None,
                    })
        if new_rows:
            db_progress_filtered = pd.concat(
                [db_progress_filtered, pd.DataFrame(new_rows)],
                ignore_index=True
            )
        # filter out coins which are already at yesterdays date (= up to date)
        date_yesterday = (datetime.now() - timedelta(days=1)).date()
        db_progress_filtered = db_progress_filtered[db_progress_filtered['date'] != date_yesterday]
        # add two columns to keep track of the max and min date for backfilling, defined in address_mapping
        db_progress_filtered['max_date'] = None
        db_progress_filtered['min_date'] = None
        db_progress_filtered['max_date'] = db_progress_filtered.apply(lambda row: self.address_mapping[chain][row['token_id']]['max_date'] if pd.isna(row['max_date']) and row['token_id'] in self.address_mapping[chain] and 'max_date' in self.address_mapping[chain][row['token_id']] else row['max_date'], axis=1)
        db_progress_filtered['min_date'] = db_progress_filtered.apply(lambda row: self.address_mapping[chain][row['token_id']]['min_date'] if pd.isna(row['min_date']) and row['token_id'] in self.address_mapping[chain] and 'min_date' in self.address_mapping[chain][row['token_id']] else row['min_date'], axis=1)
        db_progress_filtered['max_date'] = pd.to_datetime(db_progress_filtered['max_date']).dt.date
        db_progress_filtered['min_date'] = pd.to_datetime(db_progress_filtered['min_date']).dt.date
        # filter out already fully backfilled coins based on max_date
        db_progress_filtered = db_progress_filtered[~((db_progress_filtered['date'] >= db_progress_filtered['max_date']) & (pd.notna(db_progress_filtered['max_date'])))]
        return db_progress_filtered

    def get_track_on_l1_progress(self, chain: str, address_mapping: dict):
        # get all bridge addresses for a chain
        bridge_addresses = [address_mapping[origin_key] for origin_key in address_mapping if 'track_on_l1' in address_mapping[origin_key] and chain == origin_key][0]['track_on_l1']

        # check if we have data in db for these bridged addresses
        query = f"""
            SELECT
                origin_key,
                address,
                'track_on_l1' AS metric_key,
                MAX(date) AS date
            FROM public.fact_stables_v2
            WHERE
                address IN ({', '.join([f"'{addr.lower()}'" for addr in bridge_addresses])})
                AND metric_key = 'track_on_l1'
                AND origin_key = '{chain}'
            GROUP BY 1, 2
        """
        df = self.db_connector.execute_query(query, load_df=True)

        # if not we need to use dune to scrape it all
        if df.empty:
            print(f"No existing track_on_l1 data found in db for {chain}. Scraping balances of all bridge addresses from day one.")
            # add the new coins to df
            df = pd.DataFrame({
                'date': [None] * len(bridge_addresses),
                'origin_key': [chain] * len(bridge_addresses),
                'metric_key': ['track_on_l1'] * len(bridge_addresses),
                'address': bridge_addresses
            })
        else:
            # filter out bridge addresses which are already at yesterdays date (= up to date)
            date_yesterday = (datetime.now() - timedelta(days=1)).date()
            df = df[df['date'] != date_yesterday]
        return df

import time
import random
import pandas as pd
from web3 import Web3
import datetime
from web3.middleware import ExtraDataToPOAMiddleware
from requests.exceptions import HTTPError

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract
from src.stables_config import stables_metadata, stables_mapping
from sqlalchemy import text

## TODO: add days 'auto' functionality. if blocks are missing, fetch all. If tokens are missing, fetch all
## This should also work for new tokens being added etc
## TODO: add functionality that for some chains we don't need block data (we only need it if we have direct tokens)
## TODO: use get_erc20_balance function from helper_functions to get balances

class AdapterStablecoinSupply(AbstractAdapter):
    """
    Adapter for tracking stablecoin supply across different chains.
    
    This adapter tracks two types of stablecoins:
    1. Bridged stablecoins: Tokens that are locked in bridge contracts on the source chain
    2. Direct stablecoins: Tokens that are natively minted on the target chain
    
    adapter_params require the following fields:
        origin_keys: list (optional) - Specific chains to process
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Stablecoin Supply", adapter_params, db_connector)
        
        # Store stablecoin metadata and mapping
        self.stables_metadata = stables_metadata
        self.stables_mapping = stables_mapping
        
        # Initialize web3 connections to different chains
        self.connections = {}
        self.supported_chains = []
        
        # Add L2 chains that are in the mapping
        for chain_name in self.stables_mapping.keys():
            if chain_name not in self.supported_chains:
                self.supported_chains.append(chain_name)

        self.chains = adapter_params.get('origin_keys', self.supported_chains)

        connection_chains = self.chains.copy()
        # Always include Ethereum as source chain for block data
        if 'ethereum' not in connection_chains:
            connection_chains.append('ethereum')
        
        # Create connections to each chain
        for chain in connection_chains:
            try:
                rpc_url = self.db_connector.get_special_use_rpc(chain)
                w3 = Web3(Web3.HTTPProvider(rpc_url))
                
                # Apply middleware for PoA chains if needed
                if chain != 'ethereum':
                    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

                ## Test the connection
                if w3.is_connected():
                    print(f"Connected to {chain}")
                    self.connections[chain] = w3
                else:   
                    raise ValueError(f"Failed to connect to {chain} with RPC URL {rpc_url}")
            except Exception as e:
                print(f"Failed to connect to {chain} using RPC URL {rpc_url}: {e}")
        
        print_init(self.name, self.adapter_params)

    def retry_balance_call(self, func, *args, max_retries=8, initial_wait=1.0, **kwargs):
        """
        Retry a balance call with exponential backoff for handling rate limits and network issues.
        
        Args:
            func: Function to call (e.g., token_contract.functions.balanceOf(...).call)
            *args: Arguments to pass to the function
            max_retries: Maximum number of retry attempts
            initial_wait: Initial wait time in seconds
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            Result of the function call
            
        Raises:
            Exception: If all retries are exhausted
        """
        retries = 0
        wait_time = initial_wait
        
        while retries < max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_str = str(e).lower()
                
                # Check if it's a rate limiting error (429)
                is_rate_limit = (
                    "429" in error_str or 
                    "too many requests" in error_str or 
                    "rate limit" in error_str
                )
                
                # Check if it's a network/connection error
                is_network_error = (
                    "connection" in error_str or
                    "timeout" in error_str or
                    "network" in error_str
                )
                
                # Check if it's a contract execution error (don't retry these)
                is_contract_error = (
                    "execution reverted" in error_str or
                    "could not decode contract function call" in error_str
                )
                
                # Don't retry contract execution errors
                if is_contract_error:
                    raise e
                
                # Retry for rate limits and network errors
                if is_rate_limit or is_network_error:
                    retries += 1
                    
                    # Extract Retry-After header if available (for HTTP 429 errors)
                    retry_after = None
                    if hasattr(e, 'response') and hasattr(e.response, 'headers'):
                        retry_after = e.response.headers.get("Retry-After")
                    
                    if retry_after and retry_after.isdigit():
                        wait_time = int(retry_after) + random.uniform(0, 1)
                    else:
                        # Exponential backoff with jitter
                        wait_time = min((2 ** retries) * initial_wait + random.uniform(0, 1), 60)
                    
                    if retries < max_retries:
                        print(f"Rate limit/network error detected. Retrying ({retries}/{max_retries}) in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                    else:
                        print(f"Max retries ({max_retries}) reached for rate limiting/network errors")
                        raise e
                else:
                    # For other errors, don't retry
                    raise e
        
        raise Exception(f"Failed after {max_retries} retries")

    def extract(self, load_params:dict, update=False):
        """
        Extract stablecoin data based on load parameters.
        
        load_params require the following fields:
            days: int - Days of historical data to load
            load_type: str - Type of data to load ('block_data', 'bridged_supply', 'direct_supply', 'total_supply')
            stablecoins: list (optional) - Specific stablecoins to track
        """
        self.days = load_params['days']
        self.load_type = load_params['load_type']
        self.stablecoins = load_params.get('stablecoins', list(self.stables_metadata.keys()))
        
        if self.load_type == 'block_data':
            df = self.get_block_data(update=update)
        elif self.load_type == 'bridged_supply':
            df = self.get_bridged_supply(update=update)
        elif self.load_type == 'direct_supply':
            df = self.get_direct_supply(update=update)
        elif self.load_type == 'locked_supply':
            df = self.get_locked_supply(update=update)
        elif self.load_type == 'total_supply':
            df = self.get_total_supply()
        else:
            raise ValueError(f"load_type {self.load_type} not supported for this adapter")

        print_extract(self.name, load_params, df.shape)
        return df
    
    def load(self, df:pd.DataFrame):
        """Load processed data into the database with validation to prevent corruption"""
        
        # CRITICAL VALIDATION: Check for mismatched metric_keys to prevent data corruption
        if not df.empty:
            if 'metric_key' in df.index.names:
                metric_keys = df.index.get_level_values('metric_key').unique()
            elif 'metric_key' in df.columns:
                metric_keys = df['metric_key'].unique()
            else:
                metric_keys = []
            
            # Validate that block data doesn't go to fact_stables
            if 'first_block_of_day' in metric_keys and self.load_type not in ['block_data', 'total_supply']:
                raise ValueError(f"CRITICAL ERROR: Attempting to load first_block_of_day data with load_type='{self.load_type}'. This would corrupt fact_stables!")
            
            # Validate that stables data doesn't go to fact_kpis
            stables_metrics = ['supply_bridged', 'supply_direct', 'locked_supply', 'supply_bridged_exceptions']
            if any(metric in metric_keys for metric in stables_metrics) and self.load_type in ['block_data', 'total_supply']:
                raise ValueError(f"CRITICAL ERROR: Attempting to load stables data with load_type='{self.load_type}'. This would corrupt fact_kpis!")
        
        if self.load_type == 'block_data' or self.load_type == 'total_supply':
            tbl_name = 'fact_kpis'
        else:
            tbl_name = 'fact_stables'
        upserted = self.db_connector.upsert_table(tbl_name, df)
        print_load(self.name, upserted, tbl_name)

    #### Helper functions ###
    def get_block_date(self, w3: Web3, block_number: int):
        """
        Get the date of a block based on its block number.

        :param w3: Web3 object to connect to the EVM blockchain.
        :param block_number: Block number to find the date for.
        :return: Date of the block or None if the block is not found.
        """
        day_unix = w3.eth.get_block(block_number)['timestamp']
        day = datetime.datetime.utcfromtimestamp(day_unix)
        return day
        
    def get_first_block_of_day(self, w3: Web3, target_date: datetime.date):
        """
        Finds the first block of a given day using binary search based on the timestamp.
        Includes simple optimizations to reduce RPC calls.

        :param w3: Web3 object to connect to Ethereum blockchain.
        :param target_date: The target date to find the first block of the day (in UTC).
        :return: Block object of the first block of the day or None if not found.
        """
        # Initialize cache if not exists
        if not hasattr(self, '_block_cache'):
            self._block_cache = {}  # Simple cache: {(chain_id, date_str): block}
            self._timestamp_cache = {}  # Cache: {(chain_id, block_num): timestamp}
        
        # Get chain ID for cache lookup
        chain_id = w3.eth.chain_id
        date_str = target_date.strftime("%Y-%m-%d")
        
        # Check cache first
        cache_key = (chain_id, date_str)
        if cache_key in self._block_cache:
            print(f"Using cached block for {date_str}")
            return self._block_cache[cache_key]
        
        # Calculate start timestamp for target day
        start_of_day = datetime.datetime.combine(target_date, datetime.time(0, 0), tzinfo=datetime.timezone.utc)
        start_timestamp = int(start_of_day.timestamp())

        # Get latest block
        latest_block = w3.eth.get_block('latest')
        latest_number = latest_block['number']
        
        # Cache the latest block timestamp
        self._timestamp_cache[(chain_id, latest_number)] = latest_block['timestamp']
        
        # Early exit if chain didn't exist yet
        if latest_block['timestamp'] < start_timestamp:
            return None

        low, high = 0, latest_number

        # Binary search to find the first block with timestamp >= start_timestamp
        while low < high:
            mid = (low + high) // 2
            
            # Check cache before making RPC call
            if (chain_id, mid) in self._timestamp_cache:
                mid_timestamp = self._timestamp_cache[(chain_id, mid)]
            else:
                try:
                    mid_block = w3.eth.get_block(mid)
                    mid_timestamp = mid_block['timestamp']
                    # Cache this result
                    self._timestamp_cache[(chain_id, mid)] = mid_timestamp
                    time.sleep(0.1)  # Sleep to avoid rate limiting
                except Exception as e:
                    print(f"Error getting block {mid}: {e}")
                    # On error, adjust bounds to try a different block
                    high = mid - 1
                    continue
            
            if mid_timestamp < start_timestamp:
                low = mid + 1
            else:
                high = mid

        # Get the final block
        try:
            result_block = w3.eth.get_block(low)
            # Cache for future use
            self._block_cache[cache_key] = result_block
            return result_block if result_block['timestamp'] >= start_timestamp else None
        except Exception as e:
            print(f"Error getting final block {low}: {e}")
            return None
        
    def get_block_numbers(self, w3, days: int = 7):
        """
        Retrieves the first block of each day for the past 'days' number of days and returns a DataFrame 
        with the block number and timestamp for each day.
        
        Processes dates from newest to oldest for consistency with get_block_data method.

        :param w3: Web3 object to connect to the Ethereum blockchain.
        :param days: The number of days to look back from today (default is 7).
        :return: DataFrame containing the date, block number, and block timestamp for each day.
        """
        current_date = datetime.datetime.now().date()  # Get the current date (no time)
        
        # Initialize an empty list to hold the data
        block_data = []
        found_zero_block = False

        # Process dates from newest to oldest
        for i in range(days):
            target_date = current_date - datetime.timedelta(days=i)
            
            # If we found a zero block already, skip older dates
            if found_zero_block:
                print(f"..skipping {target_date} as earlier date had zero block")
                continue
                
            # Retrieve the first block of the day for the target date
            block = self.get_first_block_of_day(w3, target_date)

            # Error handling in case get_first_block_of_day returns None
            if block is None:
                print(f"ERROR: Could not retrieve block for {target_date}")
            else:
                # Log the block number and timestamp
                print(f'..block number for {target_date}: {block["number"]}')
                
                # Check if this is a zero block (chain didn't exist yet)
                if block["number"] == 0:
                    found_zero_block = True
                    print(f"..found zero block at {target_date}, will skip older dates")

                # Append the result as a dictionary to the block_data list
                block_data.append({
                    'date': str(target_date),
                    'block': block['number'],
                    'block_timestamp': block['timestamp']
                })

        # Convert the collected block data into a DataFrame
        df = pd.DataFrame(block_data)
        
        # Handle empty dataframe case
        if df.empty:
            return df
            
        # Convert block to string
        df['block'] = df['block'].astype(str)
        return df

    def get_block_data(self, update=False):
        """
        Get block data for all chains
        
        First checks if data already exists in the database (fact_kpis table) to avoid
        unnecessary RPC calls. Only fetches missing data from the blockchain.
        
        Optimizations:
        1. Uses existing data from database when available
        2. Only fetches missing dates
        3. Stops processing older dates once block 0 is found (chain wasn't active)
        4. Filters out all but the latest date with block 0
        """
        df_main = pd.DataFrame()
        print(f"Getting block data for {self.days} days and update set to {update}")

        block_chains = self.chains.copy()
        # Always include Ethereum as source chain for block data
        if 'ethereum' not in block_chains:
            block_chains.append('ethereum')
        
        for chain in block_chains:
            print(f"Processing {chain} block data")
            ## check if chain dict has a key "direct" and if it contains data
            if chain != 'ethereum' and (self.stables_mapping[chain].get("direct") is None or len(self.stables_mapping[chain]["direct"]) == 0):
                print(f"Skipping {chain} as it doesn't have direct tokens")
                continue
            
            # First check if we already have this data in the database
            existing_data = self.db_connector.get_data_from_table(
                "fact_kpis", 
                filters={
                    "metric_key": "first_block_of_day",
                    "origin_key": chain
                },
                days=self.days
            )
            
            # Check if we already have complete data in the database
            if not existing_data.empty and len(existing_data) >= self.days:
                print(f"...using existing block data for {chain} from database")
                # Rename 'value' column to match expected format
                existing_data = existing_data.reset_index()
                
                # Handle zero blocks - keep only the latest date with block 0
                if 0 in existing_data['value'].astype(int).values:
                    existing_data_with_zeroes = existing_data[existing_data['value'].astype(int) == 0]
                    latest_zero_date = existing_data_with_zeroes['date'].max()
                    
                    # Keep all non-zero blocks and only the latest zero block
                    existing_data = existing_data[
                        (existing_data['value'].astype(int) != 0) | 
                        (existing_data['date'] == latest_zero_date)
                    ]
                    
                    print(f"...filtered out old zero blocks, keeping only latest at {latest_zero_date}")
                
                df_chain = existing_data[['metric_key', 'origin_key', 'date', 'value']]
                df_main = pd.concat([df_main, df_chain])
                continue
            
            # If we don't have complete data, check what dates we're missing
            existing_dates = set()
            known_zero_date = None
            
            if not existing_data.empty:
                print(f"...found partial data for {chain}, fetching missing dates only")
                existing_data = existing_data.reset_index()
                
                # Check if we have any dates with block 0 already
                zero_blocks = existing_data[existing_data['value'].astype(int) == 0]
                if not zero_blocks.empty:
                    known_zero_date = zero_blocks['date'].max()
                    print(f"...found existing zero block at {known_zero_date}, will skip older dates")
                
                existing_dates = set(existing_data['date'].dt.strftime('%Y-%m-%d'))
            
            # Check if chain connection is available
            if chain not in self.connections:
                print(f"...skipping {chain} - no connection available")
                continue
                
            # Create set of all dates we need
            current_date = datetime.datetime.now().date()
            all_dates = set()
            for i in range(self.days):
                date = current_date - datetime.timedelta(days=i)
                all_dates.add(date.strftime('%Y-%m-%d'))
            
            # Find missing dates, sorted from newest to oldest
            missing_dates = sorted(all_dates - existing_dates, reverse=True)
            
            if not missing_dates:
                print(f"...no missing dates for {chain}")
                continue
                
            print(f"...fetching {len(missing_dates)} missing dates for {chain}")
            
            # Get web3 connection for this chain
            w3 = self.connections[chain]
            
            # Only fetch missing dates
            missing_blocks_data = []
            found_zero_block = False
            oldest_zero_date = None
            
            for date_str in missing_dates:  # Already sorted newest to oldest
                target_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                
                # Skip dates in the future
                if target_date > current_date:
                    continue
                    
                # Skip dates older than our known zero block date (if we have one)
                if known_zero_date and target_date < known_zero_date.date():
                    #print(f"...skipping {date_str} as it's older than known zero block date {known_zero_date.date()}")
                    continue
                    
                # If we already found a zero block in this run, skip older dates
                if found_zero_block and target_date < oldest_zero_date:
                    #print(f"...skipping {date_str} as it's older than discovered zero block date {oldest_zero_date}")
                    continue
                    
                # Get first block of this day
                block = self.get_first_block_of_day(w3, target_date)
                
                if block:
                    block_number = block['number']
                    
                    # Check if this is a zero block
                    if block_number == 0:
                        found_zero_block = True
                        oldest_zero_date = target_date
                        print(f"...found zero block at {date_str}, will skip older dates")
                    
                    missing_blocks_data.append({
                        'date': date_str,
                        'block': block_number,
                        'block_timestamp': block['timestamp'],
                        'origin_key': chain,
                        'metric_key': 'first_block_of_day'
                    })
                else:
                    print(f"...couldn't get block for {date_str} on {chain}")
            
            # Create dataframe from missing blocks
            if missing_blocks_data:
                df_missing = pd.DataFrame(missing_blocks_data)
                
                # Convert block to string and date to datetime
                df_missing['block'] = df_missing['block'].astype(str)
                df_missing['date'] = pd.to_datetime(df_missing['date'])
                
                # Rename block column to value and drop block_timestamp
                df_missing.rename(columns={'block': 'value'}, inplace=True)
                if 'block_timestamp' in df_missing.columns:
                    df_missing.drop(columns=['block_timestamp'], inplace=True)
                
                # Combine with existing data if present
                if not existing_data.empty:
                    df_chain = pd.concat([existing_data, df_missing])
                else:
                    df_chain = df_missing
                
                # Handle zero blocks - keep only the latest date with block 0
                if 0 in df_chain['value'].astype(int).values:
                    df_chain_with_zeroes = df_chain[df_chain['value'].astype(int) == 0]
                    latest_zero_date = df_chain_with_zeroes['date'].max()
                    
                    # Keep all non-zero blocks and only the latest zero block
                    df_chain = df_chain[
                        (df_chain['value'].astype(int) != 0) | 
                        (df_chain['date'] == latest_zero_date)
                    ]
                    
                    print(f"...filtered out old zero blocks, keeping only latest at {latest_zero_date}")
                
                if update:
                    df_chain.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
                    df_chain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)

                    # If col index in df_main, drop it
                    if 'index' in df_chain.columns:
                        df_chain.drop(columns=['index'], inplace=True)
                    
                    # CRITICAL FIX: Explicitly load block data to fact_kpis to prevent corruption
                    upserted = self.db_connector.upsert_table('fact_kpis', df_chain)
                    print_load(self.name, upserted, 'fact_kpis')

                df_main = pd.concat([df_main, df_chain])
        
        # Remove duplicates and set index
        if not df_main.empty:
            df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
            df_main.set_index(['metric_key', 'origin_key', 'date'], inplace=True)

            # If col index in df_main, drop it
            if 'index' in df_main.columns:
                df_main.drop(columns=['index'], inplace=True)
        
        return df_main
    
    def get_bridged_supply(self, update=False):
        """
        Get supply of stablecoins locked in bridge contracts on source chain (currently only Ethereum).
        """
        df_main = pd.DataFrame()
        
        # Process each L2 chain
        for chain in self.chains:
            if chain == 'ethereum':
                continue  # Skip Ethereum as it's the source chain
            
            if chain not in self.stables_mapping:
                print(f"No mapping found for {chain}, skipping")
                continue
            
            print(f"Processing bridged stablecoins for {chain}")
            
            # Check if chain has bridged tokens
            if 'bridged' not in self.stables_mapping[chain]:
                print(f"No bridged tokens defined for {chain}")
                continue
            
            # Get bridge contracts for this chain
            bridge_config = self.stables_mapping[chain]['bridged']

            # Get date of first block of this chain
            # Only needed for chains with direct tokens; bridged-only chains will query all historical data
            first_block_date = None
            chain_has_direct_tokens = self.stables_mapping[chain].get("direct") is not None and len(self.stables_mapping[chain]["direct"]) > 0
            
            if chain_has_direct_tokens:
                # Chain has direct tokens, so we need its RPC connection for the first block date
                if chain not in self.connections:
                    raise ValueError(f"Chain {chain} not connected to RPC, please add RPC connection (assign special_use in sys_rpc_config)")
                first_block_date = self.get_block_date(self.connections[chain], 1)
                print(f"First block date for {chain}: {first_block_date}")
            else:
                # Chain only has bridged tokens, no need for first block date filtering
                print(f"Chain {chain} only has bridged tokens, will query all historical data from Ethereum bridges")
                first_block_date = None

            # Process each source chain (usually Ethereum)
            ## TODO: add support for other source chains
            for source_chain, bridge_addresses in bridge_config.items():
                if source_chain != 'ethereum':
                    print(f"Skipping source chain {source_chain} for {chain} - only Ethereum supported so far")
                    continue

                if source_chain not in self.connections:
                    print(f"Source chain {source_chain} not connected, skipping")
                    continue

                # Get block numbers for source chain
                df_blocknumbers = self.db_connector.get_data_from_table(
                        "fact_kpis", 
                        filters={
                            "metric_key": "first_block_of_day",
                            "origin_key": source_chain
                        },
                        days=self.days
                    )
                
                if df_blocknumbers.empty:
                    print(f"No block data for source chain {source_chain}")
                    raise ValueError("No block data for source chain")
                
                df_blocknumbers['block'] = df_blocknumbers['value'].astype(int)
                df_blocknumbers.drop(columns=['value', 'origin_key'], inplace=True)
                df_blocknumbers = df_blocknumbers.sort_values(by='block', ascending=True)
                df_blocknumbers['block'] = df_blocknumbers['block'].astype(str)
                
                # Get web3 connection for source chain
                w3 = self.connections[source_chain]
                
                # Check balances for all stablecoins in all bridge addresses
                for stablecoin_id in self.stablecoins:
                    if stablecoin_id not in self.stables_metadata:
                        print(f"Stablecoin {stablecoin_id} not in metadata, skipping")
                        continue

                    if source_chain not in self.stables_metadata[stablecoin_id]['addresses']:
                        print(f"Stablecoin {stablecoin_id} not available on {source_chain}, skipping")
                        continue
                    
                    stable_data = self.stables_metadata[stablecoin_id]
                    symbol = stable_data['symbol']
                    decimals = stable_data['decimals']
                    
                    print(f"Checking {symbol} locked in {chain} bridges")
                    
                    # Surce chain token address (e.g., USDT on Ethereum)
                    token_address = self.stables_metadata[stablecoin_id]['addresses'][source_chain]
                    
                    # Basic ERC20 ABI for balanceOf function
                    token_abi = [
                        {"constant":True,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}
                    ]
                    
                    # Create a DataFrame for this stablecoin
                    df = df_blocknumbers.copy()
                    df['origin_key'] = chain
                    df['token_key'] = stablecoin_id
                    df['value'] = 0.0  # Initialize balance column

                    ## check for exceptions 
                    start_date = None
                    mk_value = 'supply_bridged'
                    if self.stables_metadata[stablecoin_id].get('exceptions') is not None:
                        exceptions = self.stables_metadata[stablecoin_id]['exceptions']
                        if source_chain in exceptions and chain in exceptions[source_chain]:
                            start_date = exceptions[source_chain][chain]['start_date']
                            start_date = pd.Timestamp(start_date)
                            print(f"Exceptions found for {symbol} on {chain}, using bridge addresses until {start_date}")
                    
                    # Create contract instance
                    try:
                        token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=token_abi)
                    except Exception as e:
                        print(f"Failed to create contract instance for {stablecoin_id}: {e}")
                        continue
                    
                    # Check balance in each bridge address for each block
                    contract_deployed = True
                    for i in range(len(df)-1, -1, -1):  # Go backwards in time
                        date = df['date'].iloc[i]
                        if first_block_date and date < first_block_date:
                            print(f"Reached first block date ({first_block_date}) for {chain}, stopping")
                            break  # Stop if we reach the first block date

                        if start_date and date < start_date:
                            print(f"Exception END: changing metric_key for {symbol} on {chain} after end date {start_date}")
                            mk_value = 'supply_bridged_exceptions'

                        block = df['block'].iloc[i]
                        print(f"...retrieving bridged balance for {symbol} at block {block} ({date})")
                        
                        total_balance = 0
                        
                        # Sum balances across all bridge addresses
                        for bridge_address in bridge_addresses:
                            try:
                                # Call balanceOf function with retry logic
                                balance = self.retry_balance_call(
                                    token_contract.functions.balanceOf(
                                        Web3.to_checksum_address(bridge_address)
                                    ).call,
                                    block_identifier=int(block)
                                )
                                
                                # Convert to proper decimal representation
                                adjusted_balance = balance / (10 ** decimals)
                                total_balance += adjusted_balance
                                
                            except Exception as e:
                                print(f"....Error getting balance for {symbol} in {bridge_address} at block {block}: {e}")
                                if 'execution reverted' in str(e) or 'Could not decode contract function call' in str(e):
                                    # Contract might not be deployed yet
                                    contract_deployed = False
                                    break
                        
                        if not contract_deployed:
                            print(f"Contract for {symbol} not deployed at block {block}, stopping")
                            break
                        
                        df.loc[df.index[i], 'value'] = total_balance
                        df.loc[df.index[i], 'metric_key'] = mk_value
                    
                    # Drop unneeded columns
                    df.drop(columns=['block'], inplace=True)

                    df_main = pd.concat([df_main, df])
                    
                    if update and not df.empty and 'value' in df.columns:
                        df = df[df['value'] != 0]
                        df = df.dropna()
                        df.drop_duplicates(subset=['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
                        df.set_index(['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)

                        # If col index in df_main, drop it
                        if 'index' in df.columns:
                            df.drop(columns=['index'], inplace=True)
                        self.load(df)                    
        
        # Clean up data
        if not df_main.empty:
            df_main = df_main[df_main['value'] != 0]
            df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
            df_main = df_main.dropna()
            df_main.set_index(['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
        else:
            # Return empty dataframe with correct structure
            df_main = pd.DataFrame(columns=['metric_key', 'origin_key', 'date', 'token_key', 'value']).set_index(['metric_key', 'origin_key', 'date', 'token_key'])
        return df_main
    
    def get_direct_supply(self, update=False):
        """
        Get supply of stablecoins that are natively minted on L2 chains
        """
        df_main = pd.DataFrame()
        
        # Process each chain
        for chain in self.chains:
            if chain not in self.stables_mapping:
                print(f"No mapping found for {chain}, skipping")
                continue
            
            if chain not in self.connections:
                print(f"Chain {chain} not connected, skipping")
                continue
                
            # Check if chain has direct tokens
            if 'direct' not in self.stables_mapping[chain]:
                print(f"No direct tokens defined for {chain}")
                continue
                
            print(f"Processing direct stablecoins for {chain}")
            
            # Get block numbers for this chain
            df_blocknumbers = self.db_connector.get_data_from_table(
                "fact_kpis", 
                filters={
                    "metric_key": "first_block_of_day",
                    "origin_key": chain
                },
                days=self.days
            )

            df_blocknumbers['block'] = df_blocknumbers['value'].astype(int)
            df_blocknumbers.drop(columns=['value', 'origin_key'], inplace=True)
            df_blocknumbers = df_blocknumbers.sort_values(by='block', ascending=True)
            df_blocknumbers['block'] = df_blocknumbers['block'].astype(str)
            
            # Get web3 connection for this chain
            w3 = self.connections[chain]
            
            # Get direct token configs for this chain
            direct_config = self.stables_mapping[chain]['direct']
            
            # Process each stablecoin
            for stablecoin_id, token_config in direct_config.items():
                if stablecoin_id not in self.stables_metadata:
                    print(f"Stablecoin {stablecoin_id} not in metadata, skipping")
                    continue
                
                if stablecoin_id not in self.stablecoins:
                    print(f"Stablecoin {stablecoin_id} not in requested stablecoins, skipping")
                    continue
                
                stable_data = self.stables_metadata[stablecoin_id]
                symbol = stable_data['symbol']
                decimals = stable_data['decimals']
                
                print(f"Getting supply for {symbol} on {chain}")
                
                # Extract token details
                token_address = token_config['token_address']
                method_name = token_config['method_name']
                
                # Basic ABI for totalSupply and decimals
                token_abi = [
                    {"constant":True,"inputs":[],"name":method_name,"outputs":[{"name":"","type":"uint256"}],"type":"function"},
                    {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}
                ]
                
                # Create a DataFrame for this stablecoin
                df = df_blocknumbers.copy()
                df['origin_key'] = chain
                df['token_key'] = stablecoin_id
                df['value'] = 0.0
                
                # Create contract instance
                try:
                    token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=token_abi)
                except Exception as e:
                    print(f"Failed to create contract instance for {stablecoin_id} on {chain}: {e}")
                    continue
                
                # Query total supply for each block
                contract_deployed = True
                for i in range(len(df)-1, -1, -1):  # Go backwards in time
                    date = df['date'].iloc[i]
                    block = df['block'].iloc[i]
                    print(f"...retrieving direct supply for {symbol} at block {block} ({date})")
                    
                    try:
                        # Call totalSupply function (or custom method name) with retry logic
                        supply_func = getattr(token_contract.functions, method_name)
                        total_supply = self.retry_balance_call(
                            supply_func().call,
                            block_identifier=int(block)
                        )
                        
                        # Convert to proper decimal representation
                        adjusted_supply = total_supply / (10 ** decimals)
                        df.loc[df.index[i], 'value'] = adjusted_supply
                        
                    except Exception as e:
                        print(f"....Error getting total supply for {symbol} at block {block}: {e}")
                        if 'execution reverted' in str(e) or 'Could not decode contract function call' in str(e):
                            # Contract might not be deployed yet
                            contract_deployed = False
                            break
                
                if not contract_deployed:
                    print(f"Contract for {symbol} not deployed at that time, skipping older blocks")
                    # Still add what we have so far

                df['metric_key'] = 'supply_direct'
                # Drop unneeded columns
                df.drop(columns=['block'], inplace=True)

                df_main = pd.concat([df_main, df])

                if update and not df.empty and 'value' in df.columns:
                    df = df[df['value'] != 0]
                    df = df.dropna()
                    df.drop_duplicates(subset=['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
                    df.set_index(['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)

                    # If col index in df_main, drop it
                    if 'index' in df.columns:
                        df.drop(columns=['index'], inplace=True)
                    self.load(df)

                
                        

        # Clean up data
        if not df_main.empty:
            df_main = df_main[df_main['value'] != 0]
            df_main = df_main.dropna()
            df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
            df_main.set_index(['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
        else:
            # Return empty dataframe with correct structure
            df_main = pd.DataFrame(columns=['metric_key', 'origin_key', 'date', 'token_key', 'value']).set_index(['metric_key', 'origin_key', 'date', 'token_key'])
        return df_main
    

    ## Logic is very similar to get_bridged_supply, could potentialy be refactored and simplified
    def get_locked_supply(self, update=False):
        """
        Get supply of stablecoins locked in treasury contracts (not actually in supply)
        """
        df_main = pd.DataFrame()
        
        # Process each L2 chain
        for chain in self.chains:
            if chain not in self.stables_mapping:
                print(f"No mapping found for {chain}, skipping")
                continue
            
            # Check if chain has bridged tokens
            if 'locked_supply' not in self.stables_mapping[chain]:
                print(f"No locked tokens defined for {chain}")
                continue

            print(f"Processing locked (to be subtracted) stablecoins for {chain}")
            
            # Get bridge contracts for this chain
            locked_supply_config = self.stables_mapping[chain]['locked_supply']

            # Get date of first block of this chain
            # Only needed for chains with direct tokens; bridged-only chains will query all historical data
            first_block_date = None
            chain_has_direct_tokens = self.stables_mapping[chain].get("locked_supply") is not None and len(self.stables_mapping[chain]["locked_supply"]) > 0
            
            if chain_has_direct_tokens:
                # Chain has direct tokens, so we need its RPC connection for the first block date
                if chain not in self.connections:
                    raise ValueError(f"Chain {chain} not connected to RPC, please add RPC connection (assign special_use in sys_rpc_config)")
                first_block_date = self.get_block_date(self.connections[chain], 1)
                print(f"First block date for {chain}: {first_block_date}")
            else:
                # Chain only has locked supply tokens, no need for first block date filtering
                print(f"Chain {chain} doesn't have direct tokens, will query all historical data for locked supply")
                first_block_date = None

            # Process each source chain
            for stablecoin_id in locked_supply_config:
                if stablecoin_id not in self.stables_metadata:
                    print(f"Stablecoin {stablecoin_id} not in metadata, skipping")
                    continue
                for source_chain in locked_supply_config[stablecoin_id].keys():
                    if source_chain not in self.connections:
                        print(f"Source chain {source_chain} not connected, skipping")
                        continue

                    # Get block numbers for source chain
                    df_blocknumbers = self.db_connector.get_data_from_table(
                            "fact_kpis", 
                            filters={
                                "metric_key": "first_block_of_day",
                                "origin_key": source_chain
                            },
                            days=self.days
                        )
                    
                    if df_blocknumbers.empty:
                        print(f"No block data for source chain {source_chain}")
                        raise ValueError("No block data for source chain")
                    
                    df_blocknumbers['block'] = df_blocknumbers['value'].astype(int)
                    df_blocknumbers.drop(columns=['value', 'origin_key'], inplace=True)
                    df_blocknumbers = df_blocknumbers.sort_values(by='block', ascending=True)
                    df_blocknumbers['block'] = df_blocknumbers['block'].astype(str)
                    
                    # Get web3 connection for source chain
                    w3 = self.connections[source_chain]
                
                    stable_data = self.stables_metadata[stablecoin_id]
                    symbol = stable_data['symbol']
                    decimals = stable_data['decimals']
                    
                    print(f"Checking {symbol} locked in {chain} contracts")
                    
                    # Surce chain token address (e.g., USDT on Ethereum)
                    token_address = self.stables_metadata[stablecoin_id]['addresses'][source_chain]
                    
                    # Basic ERC20 ABI for balanceOf function
                    token_abi = [
                        {"constant":True,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}
                    ]
                    
                    # Create a DataFrame for this stablecoin
                    df = df_blocknumbers.copy()
                    df['origin_key'] = chain
                    df['token_key'] = stablecoin_id
                    df['value'] = 0.0  # Initialize balance column
                    
                    # Create contract instance
                    try:
                        token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=token_abi)
                    except Exception as e:
                        print(f"Failed to create contract instance for {stablecoin_id}: {e}")
                        continue
                    
                    # Check balance in each bridge address for each block
                    contract_deployed = True
                    for i in range(len(df)-1, -1, -1):  # Go backwards in time
                        date = df['date'].iloc[i]
                        if first_block_date and date < first_block_date:
                            print(f"Reached first block date ({first_block_date}) for {chain}, stopping")
                            break  # Stop if we reach the first block date

                        block = df['block'].iloc[i]
                        print(f"...retrieving locked balance for {symbol} at block {block} ({date})")
                        
                        total_balance = 0
                            
                        # Check balances for defined stablecoin in locked contract
                        for address in locked_supply_config[stablecoin_id][source_chain]:
                            try:
                                # Call balanceOf function with retry logic
                                balance = self.retry_balance_call(
                                    token_contract.functions.balanceOf(
                                        Web3.to_checksum_address(address)
                                    ).call,
                                    block_identifier=int(block)
                                )
                                
                                # Convert to proper decimal representation
                                adjusted_balance = balance / (10 ** decimals)
                                total_balance += adjusted_balance
                                
                            except Exception as e:
                                print(f"....Error getting balance for {symbol} in {address} at block {block}: {e}")
                                if 'execution reverted' in str(e) or 'Could not decode contract function call' in str(e):
                                    # Contract might not be deployed yet
                                    contract_deployed = False
                                    break
                        
                        if not contract_deployed:
                            print(f"Contract for {symbol} not deployed at block {block}, stopping")
                            break
                        
                        df.loc[df.index[i], 'value'] = total_balance * -1  # Subtract from supply
                    
                    df['metric_key'] = 'locked_supply'
                    # Drop unneeded columns
                    df.drop(columns=['block'], inplace=True)

                    df_main = pd.concat([df_main, df])
                    
                    if update and not df.empty and 'value' in df.columns:
                        df = df[df['value'] != 0]
                        df = df.dropna()
                        df.drop_duplicates(subset=['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
                        df.set_index(['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)

                        # If col index in df_main, drop it
                        if 'index' in df.columns:
                            df.drop(columns=['index'], inplace=True)
                        self.load(df)                    
        
        # Clean up data
        if not df_main.empty:
            df_main = df_main[df_main['value'] != 0]
            df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
            df_main = df_main.dropna()
            df_main.set_index(['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
        else:
            # Return empty dataframe with correct structure
            df_main = pd.DataFrame(columns=['metric_key', 'origin_key', 'date', 'token_key', 'value']).set_index(['metric_key', 'origin_key', 'date', 'token_key'])
        return df_main
    
    def get_exchange_rates(self, currencies, target_date=None):
        """
        Get exchange rates for specified currencies from fact_kpis table.
        
        Args:
            currencies (list): List of currency codes (e.g., ['eur', 'brl'])
            target_date (str, optional): Specific date to get rates for. If None, gets latest rates.
            
        Returns:
            dict: Currency code -> exchange rate mapping
        """
        if not currencies:
            return {}
            
        rates = {}
        
        try:
            with self.db_connector.engine.connect() as conn:
                for currency in currencies:
                    if currency == 'usd':
                        rates[currency] = 1.0
                        continue
                        
                    origin_key = f'fiat_{currency}'
                    
                    if target_date:
                        # Get rate for specific date
                        query = text("""
                            SELECT value 
                            FROM fact_kpis 
                            WHERE metric_key = 'price_usd' 
                              AND origin_key = :origin_key 
                              AND date = :target_date
                            ORDER BY date DESC
                            LIMIT 1
                        """)
                        result = conn.execute(query, {
                            'origin_key': origin_key,
                            'target_date': target_date
                        })
                    else:
                        # Get latest rate
                        query = text("""
                            SELECT value 
                            FROM fact_kpis 
                            WHERE metric_key = 'price_usd' 
                              AND origin_key = :origin_key
                            ORDER BY date DESC
                            LIMIT 1
                        """)
                        result = conn.execute(query, {'origin_key': origin_key})
                    
                    row = result.fetchone()
                    if row:
                        rates[currency] = float(row[0])
                        print(f"Found exchange rate for {currency.upper()}: {rates[currency]:.6f}")
                    else:
                        print(f"Warning: No exchange rate found for {currency.upper()}, skipping conversion")
                        rates[currency] = None
                        
        except Exception as e:
            print(f"Error fetching exchange rates: {e}")
            # Return None for all currencies to indicate failure
            return {currency: None for currency in currencies}
            
        return rates
    
    def convert_to_usd(self, df, exchange_rates_dataframes):
        """
        Convert non-USD stablecoin values to USD using date-specific exchange rates via DataFrame merges.
        
        Args:
            df (pd.DataFrame): DataFrame with stablecoin data including token_key and date
            exchange_rates_dataframes (dict): Currency -> DataFrame mapping with date-indexed exchange rates
            
        Returns:
            pd.DataFrame: DataFrame with USD-converted values
        """
        if df.empty or not exchange_rates_dataframes:
            return df
            
        # Create a copy to avoid modifying original data
        df_converted = df.copy()
        
        # Ensure date column is datetime
        if 'date' in df_converted.columns:
            df_converted['date'] = pd.to_datetime(df_converted['date'])
        
        # Add currency column to identify which tokens need conversion
        df_converted['fiat_currency'] = df_converted['token_key'].map(
            lambda token: self.stables_metadata.get(token, {}).get('fiat', 'usd') if token else 'usd'
        )
        
        # Track conversions for reporting
        conversions_applied = 0
        original_rows = len(df_converted)
        
        # Process each currency that needs conversion
        for fiat_currency, rates_df in exchange_rates_dataframes.items():
            if rates_df.empty:
                print(f"Warning: Empty exchange rate data for {fiat_currency.upper()}")
                continue
            
            # Filter rows that need this currency conversion
            currency_mask = df_converted['fiat_currency'] == fiat_currency
            rows_to_convert = currency_mask.sum()
            
            if rows_to_convert == 0:
                continue
                
            print(f"Converting {rows_to_convert} {fiat_currency.upper()} stablecoin records...")
            
            # Prepare exchange rates DataFrame for merge
            rates_for_merge = rates_df.reset_index()
            rates_for_merge.rename(columns={'exchange_rate': f'{fiat_currency}_rate'}, inplace=True)
            
            # Perform the merge operation - this is where the magic happens!
            df_with_rates = df_converted[currency_mask].merge(
                rates_for_merge, 
                on='date', 
                how='left'
            )
            
            # Handle missing rates with forward fill (use closest available rate)
            rate_column = f'{fiat_currency}_rate'
            missing_rates = df_with_rates[rate_column].isnull().sum()
            
            if missing_rates > 0:
                # Forward fill missing rates
                df_with_rates[rate_column] = df_with_rates[rate_column].fillna(method='ffill').fillna(method='bfill')
                print(f"Warning: {missing_rates} dates had missing {fiat_currency.upper()} rates, used closest available rates")
            
            # Apply currency conversion: original_value * exchange_rate = usd_value
            conversion_mask = df_with_rates[rate_column].notnull()
            df_with_rates.loc[conversion_mask, 'value'] = (
                df_with_rates.loc[conversion_mask, 'value'] * 
                df_with_rates.loc[conversion_mask, rate_column]
            )
            
            # Update the main DataFrame with converted values
            df_converted.loc[currency_mask, 'value'] = df_with_rates['value']
            
            converted_count = conversion_mask.sum()
            conversions_applied += converted_count
            
            if converted_count > 0:
                sample_rate = df_with_rates.loc[conversion_mask, rate_column].iloc[0]
                print(f"Converted {converted_count} {fiat_currency.upper()} records to USD (sample rate: {sample_rate:.6f})")
        
        # Clean up temporary column
        df_converted = df_converted.drop(columns=['fiat_currency'])
        
        if conversions_applied > 0:
            print(f"Currency conversion complete: {conversions_applied}/{original_rows} records converted to USD")
                    
        return df_converted
    
    def get_total_supply(self, days=None):
        """
        Calculate the total stablecoin supply (bridged + direct - locked) per chain
        
        This method:
        1. Retrieves bridged supply data from Ethereum bridge contracts
        2. Retrieves direct supply data from L2 native tokens
        3. Retrieves locked supply data from treasury contracts
        4. Applies currency conversion for non-USD stablecoins
        5. Combines them to get total stablecoin supply by chain and stablecoin
        6. Also calculates a total across all stablecoins for each chain
        """

        days = days if days is not None else 9999
        
        print(f"Filtering data for origin_keys present in stables_config: {self.chains}")

        # Check if we have existing data in the database
        df_bridged = self.db_connector.get_data_from_table("fact_stables",
                    filters={
                        "metric_key": "supply_bridged",
                        "origin_key": self.chains
                    },
                    days=days
                )
        df_direct = self.db_connector.get_data_from_table("fact_stables",
                    filters={
                        "metric_key": "supply_direct",
                        "origin_key": self.chains
                    },
                    days=days
                )
        df_locked = self.db_connector.get_data_from_table("fact_stables",
                    filters={
                        "metric_key": "locked_supply",
                        "origin_key": self.chains
                    },
                    days=days
                )
        
        df_bridged_exceptions = self.db_connector.get_data_from_table("fact_stables",
                    filters={
                        "metric_key": "supply_bridged_exceptions",
                        "origin_key": self.chains
                    },
                    days=days
                )
        
        # Pre-fetch exchange rate DataFrames for all supported non-USD currencies
        print("Pre-fetching exchange rate data for currency conversion...")
        from src.adapters.adapter_currency_conversion import AdapterCurrencyConversion
        currency_adapter = AdapterCurrencyConversion({}, self.db_connector)
        
        # Get all possible non-USD currencies from stables metadata
        all_non_usd_currencies = set()
        for token_key, metadata in self.stables_metadata.items():
            fiat = metadata.get('fiat', 'usd')
            if fiat != 'usd':
                all_non_usd_currencies.add(fiat)
        
        # Fetch exchange rate DataFrames for all non-USD currencies
        exchange_rates_dataframes = {}
        for currency in all_non_usd_currencies:
            try:
                rates_df = currency_adapter.get_exchange_rates_dataframe(currency, days=days)
                if not rates_df.empty:
                    exchange_rates_dataframes[currency] = rates_df
                    print(f"Pre-fetched {len(rates_df)} exchange rate records for {currency.upper()}")
                else:
                    print(f"No exchange rate data found for {currency.upper()}")
            except Exception as e:
                print(f"Error pre-fetching exchange rates for {currency}: {e}")
        
        # Reset index to work with the dataframes
        if not df_bridged.empty:
            df_bridged = df_bridged.reset_index()
        if not df_direct.empty:
            df_direct = df_direct.reset_index()
        if not df_locked.empty:
            df_locked = df_locked.reset_index()
        if not df_bridged_exceptions.empty:
            df_bridged_exceptions.reset_index()

        df_bridged_all = pd.concat([df_bridged, df_bridged_exceptions])

        ## sum up bridged supply for for all chains that aren't Ethereum
        df_bridged_l2s = df_bridged_all[df_bridged_all['origin_key'] != 'ethereum']
        df_bridged_l2s.drop(columns=['origin_key'], inplace=True)
        df_bridged_l2s = df_bridged_l2s.groupby(['date'])['value'].sum().reset_index()
        df_bridged_l2s['metric_key'] = 'stables_mcap'
        
        # Combine datasets
        df = pd.concat([df_bridged, df_direct, df_locked])
        
        if df.empty:
            print("No data available for total supply calculation")
            # Return empty dataframe with correct structure
            return pd.DataFrame(columns=['metric_key', 'origin_key', 'date', 'token_key', 'value']).set_index(['metric_key', 'origin_key', 'date', 'token_key'])
        
        # Apply currency conversion for non-USD stablecoins
        print("Checking for non-USD stablecoins to convert...")
        
        # Reset index to work with the dataframe
        df_reset = df.reset_index()
        
        # Find all unique non-USD currencies that need conversion
        non_usd_currencies = set()
        if 'token_key' in df_reset.columns:
            for token_key in df_reset['token_key'].unique():
                if token_key and token_key in self.stables_metadata:
                    fiat = self.stables_metadata[token_key].get('fiat', 'usd')
                    if fiat != 'usd':
                        non_usd_currencies.add(fiat)
        
        if non_usd_currencies:
            print(f"Found non-USD stablecoins with currencies: {list(non_usd_currencies)}")
            
            # Filter pre-fetched exchange rates to only the currencies we need
            needed_exchange_rates = {
                currency: exchange_rates_dataframes[currency] 
                for currency in non_usd_currencies 
                if currency in exchange_rates_dataframes
            }
            
            if needed_exchange_rates:
                print("Applying date-specific currency conversion to stablecoin values...")
                df_reset = self.convert_to_usd(df_reset, needed_exchange_rates)
                
                # Set index back
                if 'token_key' in df_reset.columns:
                    df = df_reset.set_index(['metric_key', 'origin_key', 'date', 'token_key'])
                else:
                    df = df_reset.set_index(['metric_key', 'origin_key', 'date'])
            else:
                print("Warning: No exchange rate data available for needed currencies, using original values")
                df = df_reset.set_index(['metric_key', 'origin_key', 'date', 'token_key'])
        else:
            print("No non-USD stablecoins found, no conversion needed")
            df = df_reset.set_index(['metric_key', 'origin_key', 'date', 'token_key'])
        
        # Also create total across all stablecoins
        df_total = df.groupby(['origin_key', 'date'])['value'].sum().reset_index()
        df_total['metric_key'] = 'stables_mcap'

        df_total_ethereum = df_total[df_total['origin_key'] == 'ethereum'].copy()
        df_total = df_total[df_total['origin_key'] != 'ethereum']

        ## join df_total_ethereum with df_bridged_l2s
        df_total_ethereum = df_total_ethereum.merge(df_bridged_l2s, on='date', how='left', suffixes=('', '_l2s'))
        df_total_ethereum['value'] = df_total_ethereum['value'] - df_total_ethereum['value_l2s'].fillna(0)
        df_total_ethereum.drop(columns=['value_l2s', 'metric_key_l2s'], inplace=True)

        # Combine total Ethereum with other chains
        df_total = pd.concat([df_total, df_total_ethereum])
        
        # Set index and return
        df_total.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return df_total
import ast
from datetime import datetime, date
import numpy as np
import boto3
import botocore
import pandas as pd
import os
import random
import time
from src.adapters.rpc_funcs.web3 import Web3CC
from sqlalchemy import text
from src.main_config import get_main_config 
from src.adapters.rpc_funcs.chain_configs import chain_configs
from src.adapters.rpc_funcs.gcs_utils import connect_to_gcs, check_gcs_connection, save_data_for_range
from eth_account import Account
from eth_utils import keccak, to_checksum_address
import rlp

# ---------------- Utility Functions ---------------------
def safe_float_conversion(x):
    """
    Safely converts the input value to a float. If the input is a hexadecimal string, 
    it is first converted to an integer before converting to float.
    
    Args:
        x: The input value to convert (can be string, int, etc.).

    Returns:
        float: The converted float value or NaN if the conversion fails.
    """
    try:
        if isinstance(x, str) and x.startswith('0x'):
            return float(int(x, 16))
        return float(x)
    except (ValueError, TypeError):
        return np.nan

def hex_to_int(hex_str):
    """
    Converts a hexadecimal string to an integer.
    
    Args:
        hex_str (str): The hexadecimal string to convert.

    Returns:
        int: The integer value or None if conversion fails.
    """
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return None

def convert_input_to_boolean(df):
    """
    Converts 'input_data' or 'empty_input' columns in the DataFrame to boolean values based on the presence of data.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'input_data' or 'empty_input' column.

    Returns:
        pd.DataFrame: DataFrame with the converted boolean values.
    """
    if 'input_data' in df.columns:
        df['empty_input'] = df['input_data'].apply(
            lambda x: True if x in ['0x', '', b'\x00', b''] else False
        ).astype(bool)
    elif 'empty_input' in df.columns:
        df['empty_input'] = df['empty_input'].apply(
            lambda x: True if x in ['0x', '', b'\x00', b''] else False
        ).astype(bool)
    return df

def handle_l1_gas_price(df):
    """
    Safely converts the 'l1_gas_price' column to float and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_gas_price' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_gas_price' converted to float.
    """
    if 'l1_gas_price' in df.columns:
        df['l1_gas_price'] = df['l1_gas_price'].apply(safe_float_conversion)
        df['l1_gas_price'] = df['l1_gas_price'].astype('float64')
        df['l1_gas_price'].fillna(0, inplace=True)
    return df

def handle_l1_fee(df):
    """
    Safely converts the 'l1_fee' column to float and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_fee' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_fee' converted to float.
    """
    if 'l1_fee' in df.columns:
        df['l1_fee'] = df['l1_fee'].apply(safe_float_conversion)
        df['l1_fee'] = df['l1_fee'].astype('float64')
        df['l1_fee'].fillna(0, inplace=True)
    
    return df

def handle_l1_blob_base_fee(df):
    """
    Safely converts the 'handle_l1_blob_base_fee' column to float and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'handle_l1_blob_base_fee' column.

    Returns:
        pd.DataFrame: DataFrame with 'handle_l1_blob_base_fee' converted to float.
    """
    if 'l1_blob_base_fee' in df.columns:
        df['l1_blob_base_fee'] = df['l1_blob_base_fee'].apply(safe_float_conversion)
        df['l1_blob_base_fee'] = df['l1_blob_base_fee'].astype('float64')
        df['l1_blob_base_fee'].fillna(0, inplace=True)
    
    return df

def handle_l1_fee_scalar(df):
    """
    Fills NaN values in the 'l1_fee_scalar' column with '0'.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_fee_scalar' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_fee_scalar' column processed.
    """
    if 'l1_fee_scalar' in df.columns:
        df['l1_fee_scalar'].fillna('0', inplace=True)
    return df

def handle_l1_gas_used(df):
    """
    Converts the 'l1_gas_used' column from hexadecimal to integer and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_gas_used' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_gas_used' column processed.
    """
    if 'l1_gas_used' in df.columns:
        df['l1_gas_used'] = df['l1_gas_used'].apply(hex_to_int)
        df['l1_gas_used'].fillna(0, inplace=True)
    return df

def handle_l1_base_fee_scalar(df):
    """
    Converts the 'l1_base_fee_scalar' column from hexadecimal to integer and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_base_fee_scalar' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_base_fee_scalar' column processed.
    """
    if 'l1_base_fee_scalar' in df.columns:
        df['l1_base_fee_scalar'] = df['l1_base_fee_scalar'].apply(hex_to_int)
        df['l1_base_fee_scalar'].fillna(0, inplace=True)
    return df

def handle_l1_blob_base_fee_scalar(df):
    """
    Converts the 'l1_blob_base_fee_scalar' column from hexadecimal to integer and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_blob_base_fee_scalar' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_blob_base_fee_scalar' column processed.
    """
    if 'l1_blob_base_fee_scalar' in df.columns:
        df['l1_blob_base_fee_scalar'] = df['l1_blob_base_fee_scalar'].apply(hex_to_int)
        df['l1_blob_base_fee_scalar'].fillna(0, inplace=True)
    return df

def calculate_tx_fee(df):
    """
    Calculates the transaction fee based on gas price, gas used, and L1 fee if applicable.
    - If 'fee_currency' is present -> call handle_celo_fee(df).
    - Otherwise, do normal chains logic.

    Args:
        df (pd.DataFrame): The input DataFrame containing transaction data.

    Returns:
        pd.DataFrame: DataFrame with a new 'tx_fee' column added.
    """
    # Check if this is a Celo-style tx: tx_type == 123 or fee_currency is present and not all NaN
    is_celo_tx = (
        ("tx_type" in df.columns and (df["tx_type"] == 123).any()) and
        ("fee_currency" in df.columns and not df["fee_currency"].isnull().all())
    )

    if is_celo_tx:
        return handle_celo_fee(df)

    # Else, normal OP-chains logic:
    # 1) If we have 'gas_price', 'gas_used', and 'l1_fee', do:
    if all(col in df.columns for col in ["gas_price", "gas_used", "l1_fee"]):
        df["tx_fee"] = ((df["gas_price"] * df["gas_used"]) + df["l1_fee"]) / 1e18
    elif all(col in df.columns for col in ["gas_price", "gas_used"]):
        df["tx_fee"] = (df["gas_price"] * df["gas_used"]) / 1e18
    else:
        df["tx_fee"] = None
    return df

def handle_celo_fee(df):
    """
    Computes both raw_tx_fee (in original token) and tx_fee (in CELO) for Celo transactions,
    using cached fee currency data.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing transaction data.
        
    Returns:
        pd.DataFrame: DataFrame with all original columns plus 'raw_tx_fee' and 'tx_fee'.
    """
    from src.misc.celo_handler import CeloFeeCache

    # Get cached fee currency data
    fee_cache = CeloFeeCache()
    decimals_map, rate_map = fee_cache.get_cached_data()

    # If rate_map is empty, try forcing a refresh once
    if not rate_map:
        print("Exchange rate cache is empty. Forcing refresh...")
        fee_cache.force_refresh()
        decimals_map, rate_map = fee_cache.get_cached_data()

    # Track missing rates to report at the end
    missing_rates = set()

    # Normalize fee_currency addresses to lowercase
    if 'fee_currency' in df.columns:
        df["fee_currency"] = df["fee_currency"].astype(str).str.lower()
        df["fee_currency"] = df["fee_currency"].replace('nan', None)

    def compute_fees(row):
        # Get the fee currency address used for this transaction
        fee_currency = row.get("fee_currency")
        
        # If fee_currency is None or NaN, calculate as a standard transaction
        if pd.isna(fee_currency) or fee_currency is None or fee_currency == 'none':
            standard_tx_fee = ((row["gas_price"] * row["gas_used"]) + row.get("l1_fee", 0)) / 1e18
            return pd.Series({
                'raw_tx_fee': standard_tx_fee,
                'tx_fee': standard_tx_fee
            })
        
        # For transactions with a fee currency, do CELO conversion
        decimals = decimals_map.get(fee_currency, 18)

        # Calculate base gas fee in the token's denomination
        base_fee = (row["gas_price"] * row["gas_used"]) / (10 ** decimals)

        # Add L1 fee if present, also accounting for token decimals
        l1_fee_val = row.get("l1_fee", 0)
        raw_tx_fee = base_fee + (l1_fee_val / (10 ** decimals))

        # Convert to CELO using exchange rate
        rate = rate_map.get(fee_currency)                
        celo_tx_fee = raw_tx_fee / rate if rate is not None else None

        return pd.Series({
            'raw_tx_fee': raw_tx_fee,
            'tx_fee': celo_tx_fee
        })

    # Apply the fee calculations to each row
    fee_columns = df.apply(compute_fees, axis=1)
    
    # Add the new columns to the DataFrame while preserving all existing columns
    df['raw_tx_fee'] = fee_columns['raw_tx_fee']
    df['tx_fee'] = fee_columns['tx_fee']

    # Handle any failed conversions (where rate wasn't available)
    failed_conversions = df["tx_fee"].isnull().sum()
    if failed_conversions > 0:
        print(f"Warning: Failed to convert {failed_conversions} transaction fees to CELO due to missing exchange rates")
        if missing_rates:
            print(f"Missing rates for tokens: {', '.join([f'{addr}' for addr in missing_rates])}")

    return df

def handle_tx_hash(df, column_name='tx_hash'):
    """
    Converts the 'tx_hash' column values into the proper '\\x' hex format if applicable.
    Ensures transaction hashes are consistently 32 bytes (64 hex characters).
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'tx_hash' column.
        column_name (str): The name of the column to process (default is 'tx_hash').

    Returns:
        pd.DataFrame: DataFrame with 'tx_hash' column processed.
    """
    if column_name in df.columns:
        df[column_name] = df[column_name].apply(
            lambda tx_hash: format_tx_hash(tx_hash) if pd.notnull(tx_hash) else None
        )
    return df

def format_tx_hash(tx_hash):
    """
    Formats transaction hash to ensure it's properly formatted for PostgreSQL bytea storage.
    Ensures the hash is the full 32 bytes (64 hex characters).
    
    Args:
        tx_hash: The transaction hash to format (can be string, bytes, etc.)
        
    Returns:
        str: Properly formatted transaction hash with '\\x' prefix
    """
    # If it's already a properly formatted PostgreSQL bytea
    if isinstance(tx_hash, str) and tx_hash.startswith('\\x'):
        hex_part = tx_hash[2:]  # Remove \\x prefix
        # Ensure it's a full-length hash (64 hex characters)
        if len(hex_part) == 64:
            return tx_hash
        else:
            # This shouldn't happen for transaction hashes, but log for debugging
            print(f"Warning: Found tx_hash with unexpected length: {tx_hash}")
            return tx_hash
    
    # If it's a hex string with 0x prefix (common Ethereum format)
    if isinstance(tx_hash, str) and tx_hash.startswith('0x'):
        hex_part = tx_hash[2:]  # Remove 0x prefix
        if len(hex_part) == 64:
            return '\\x' + hex_part
        else:
            print(f"Warning: Found tx_hash with unexpected length: {tx_hash}")
            return '\\x' + hex_part
    
    # If it's bytes (common from Web3 libraries)
    if isinstance(tx_hash, bytes):
        hex_part = tx_hash.hex()
        return '\\x' + hex_part
    
    # If it's a plain hex string without prefix
    if isinstance(tx_hash, str) and all(c in '0123456789abcdefABCDEF' for c in tx_hash):
        if len(tx_hash) == 64:
            return '\\x' + tx_hash
        else:
            print(f"Warning: Found tx_hash with unexpected length: {tx_hash}")
            return '\\x' + tx_hash
    
    # For any other string format, first try to clean it up
    if isinstance(tx_hash, str):
        cleaned = tx_hash.replace('0x', '').replace('\\x', '')
        if cleaned.startswith("b'") or cleaned.startswith('b"'):
            cleaned = cleaned[2:]
        if cleaned.endswith("'") or cleaned.endswith('"'):
            cleaned = cleaned[:-1]
            
        if all(c in '0123456789abcdefABCDEF' for c in cleaned) and len(cleaned) == 64:
            return '\\x' + cleaned
    
    # Fallback: return the cleaned hex string
    return '\\x' + str(tx_hash).replace('0x', '').replace('\\x', '')

def handle_tx_hash_polygon_zkevm(df, column_name='tx_hash'):
    """
    Processes 'tx_hash' values for the Polygon zkEVM chain by converting them to '\\x' hex format.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'tx_hash' column.
        column_name (str): The name of the column to process (default is 'tx_hash').

    Returns:
        pd.DataFrame: DataFrame with 'tx_hash' values processed for Polygon zkEVM.
    """
    if column_name in df.columns:
        df[column_name] = df[column_name].apply(
            lambda x: '\\x' + ast.literal_eval(x).hex() if pd.notnull(x) else None
        )
    return df

def handle_bytea_columns(df, bytea_columns):
    """
    Processes columns in a DataFrame to handle PostgreSQL bytea types by ensuring proper formatting.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        bytea_columns (list): List of column names that need to be formatted as bytea.

    Returns:
        pd.DataFrame: DataFrame with bytea columns processed.
    """
    for col in bytea_columns:
        if col in df.columns:
            df[col] = df[col].replace(['nan', 'None', 'NaN'], np.nan)
            
            # Process each value depending on its type
            df[col] = df[col].apply(lambda x: process_bytea_value(x) if pd.notna(x) else x)
    
    return df

def process_bytea_value(value):
    """
    Process a value for PostgreSQL bytea format, handling various input formats.
    
    Args:
        value: The value to process (can be string, bytes, etc.)
        
    Returns:
        str: Properly formatted string for PostgreSQL bytea type.
    """
    # If it's already in PostgreSQL bytea format
    if isinstance(value, str) and value.startswith('\\x'):
        return value
        
    # If it's a hex string with 0x prefix
    if isinstance(value, str) and value.startswith('0x'):
        return '\\x' + value[2:]
        
    # If it's Python bytes object
    if isinstance(value, bytes):
        return '\\x' + value.hex()
        
    # If it's a string representation of bytes (like "b'\x01\x02'")
    if isinstance(value, str) and (value.startswith("b'") or value.startswith('b"')) and '\\x' in value:
        try:
            import ast
            byte_obj = ast.literal_eval(value)
            if isinstance(byte_obj, bytes):
                return '\\x' + byte_obj.hex()
        except (SyntaxError, ValueError):
            hex_parts = []
            parts = value.split('\\x')
            for i, part in enumerate(parts):
                if i > 0:
                    if len(part) >= 2:
                        hex_parts.append(part[:2])
            if hex_parts:
                return '\\x' + ''.join(hex_parts)
    
    # Special case for plain string representation of bytes without hex codes
    if isinstance(value, str) and (value.startswith("b'") or value.startswith('b"')):
        try:
            import ast
            byte_obj = ast.literal_eval(value)
            if isinstance(byte_obj, bytes):
                return '\\x' + byte_obj.hex()
        except (SyntaxError, ValueError):
            pass
    
    # For any string, try to ensure it's properly formatted
    if isinstance(value, str):
        return str(value).replace('0x', '\\x')
    
    # For any other type, convert to string
    return str(value).replace('0x', '\\x')

def handle_status(df, status_mapping):
    """
    Maps the 'status' column in the DataFrame to corresponding values using a status mapping dictionary.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'status' column.
        status_mapping (dict): Dictionary mapping status values.

    Returns:
        pd.DataFrame: DataFrame with 'status' column processed.
    """
    if 'status' in df.columns:
        default_value = status_mapping.get("default", -1)
        df['status'] = df['status'].apply(lambda x: status_mapping.get(str(x), default_value))
    return df

def handle_address_columns(df, address_columns):
    """
    Processes specified address columns, replacing missing values and handling specific cases like contract addresses.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing address columns.
        address_columns (list): List of column names to process.

    Returns:
        pd.DataFrame: DataFrame with address columns processed.
    """
    for col in address_columns:
        if col in df.columns:
            df[col] = df[col].replace('None', np.nan).fillna('')

            if col == 'receipt_contract_address':
                df[col] = df[col].apply(lambda x: None if not x or x.lower() == 'none' or x.lower() == '4e6f6e65' else x)

            if col == 'to_address':
                df[col] = df[col].replace('', np.nan)
                
    return df

def handle_effective_gas_price(df):
    """
    Replaces 'gas_price' with 'effective_gas_price' where available, and drops the 'effective_gas_price' column.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing gas price columns.

    Returns:
        pd.DataFrame: DataFrame with effective gas prices handled.
    """
    if 'effective_gas_price' in df.columns and 'gas_price' in df.columns:
        df['gas_price'] = df['effective_gas_price'].fillna(df['gas_price'])
        df.drop(['effective_gas_price'], axis=1, inplace=True)
    return df

def convert_columns_to_numeric(df, numeric_columns):
    """
    Converts specified columns in a DataFrame to numeric types, handling any errors gracefully.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        numeric_columns (list): List of columns to convert to numeric.

    Returns:
        pd.DataFrame: DataFrame with numeric columns converted.
    """
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def convert_columns_to_eth(df, value_conversion):
    """
    Converts specified columns from their original units to Ether by dividing them with the provided divisor.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        value_conversion (dict): A dictionary with column names as keys and divisors as values.

    Returns:
        pd.DataFrame: DataFrame with values converted to Ether.
    """
    for col, divisor in value_conversion.items():
        if col in df.columns:
            df[col] = df[col].astype(float) / divisor
    return df

def shorten_input_data(df):
    """
    Trims 'input_data' column values to 10 characters for storage optimization.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'input_data' column.

    Returns:
        pd.DataFrame: DataFrame with shortened 'input_data' column.
    """
    if 'input_data' in df.columns:
        df['input_data'] = df['input_data'].apply(
            lambda x: process_bytea_value(x)[:10] if x else None
        )
    return df

# Custom operation for Scroll
def handle_l1_fee_scroll(df):
    """
    Custom handler for Scroll chain: converts 'l1_fee' to Ether and calculates total transaction fee.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing transaction data for the Scroll chain.

    Returns:
        pd.DataFrame: DataFrame with 'l1_fee' and transaction fees processed.
    """
    if 'l1_fee' in df.columns:
        df['l1_fee'] = df['l1_fee'].apply(
            lambda x: int(x, 16) / 1e18 if isinstance(x, str) and x.startswith('0x') else float(x) / 1e18
        )
    
    if 'gas_price' in df.columns and 'gas_used' in df.columns:
        df['tx_fee'] = (df['gas_price'] * df['gas_used']) / 1e18

    if 'l1_fee' in df.columns and 'tx_fee' in df.columns:
        df['tx_fee'] += df['l1_fee']
    
    return df

def calculate_priority_fee(df):
    """
    Calculates the priority fee per gas by subtracting 'base_fee_per_gas' from 'max_fee_per_gas' and converting to Ether.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing gas price columns.

    Returns:
        pd.DataFrame: DataFrame with 'priority_fee_per_gas' calculated.
    """
    if 'max_fee_per_gas' in df.columns and 'base_fee_per_gas' in df.columns:
        df['priority_fee_per_gas'] = (df['max_fee_per_gas'] - df['base_fee_per_gas']) / 1e18
        df.drop('base_fee_per_gas', axis=1, inplace=True)
    else:
        df['priority_fee_per_gas'] = np.nan
    return df

def handle_max_fee_per_blob_gas(df):
    """
    Processes the 'max_fee_per_blob_gas' column by converting its values from hexadecimal to Ether.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing 'max_fee_per_blob_gas' column.

    Returns:
        pd.DataFrame: DataFrame with 'max_fee_per_blob_gas' processed.
    """
    if 'max_fee_per_blob_gas' in df.columns:
        df['max_fee_per_blob_gas'] = df['max_fee_per_blob_gas'].apply(
            lambda x: int(x, 16) / 1e18 if isinstance(x, str) and x.startswith('0x') else float(x) / 1e18
        ).astype(float)
    return df

# ---------------- Connection Functions ------------------
def connect_to_node(rpc_config):
    """
    Establishes a connection to the Ethereum node using the provided RPC configuration.
    
    Args:
        rpc_config (dict): RPC configuration details for connecting to the node.

    Returns:
        Web3CC: The Web3CC object for interacting with the blockchain, or raises a ConnectionError if connection fails.
    """
    try:
        return Web3CC(rpc_config)
    except ConnectionError as e:
        print(f"ERROR: failed to connect to the node with config {rpc_config}: {e}")
        raise

# ---------------- Generic Preparation Function ------------------
def prep_dataframe_new(df, chain):
    """
    Prepares the given DataFrame for storage and processing based on chain-specific configurations.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing transaction data.
        chain (str): The name of the blockchain chain.

    Returns:
        pd.DataFrame: The prepared DataFrame with necessary columns, data types, and operations applied.
    """
    op_chains = [
        'zora', 'base', 'optimism', 'gitcoin_pgn', 'mantle', 'mode', 'blast',
        'redstone', 'orderly', 'derive', 'karak', 'ancient8', 'kroma', 'fraxtal',
        'cyber', 'worldchain', 'mint', 'ink', 'soneium', 'swell', 'zircuit',
        'lisk', 'unichain'
    ]
    default_chains = ['manta', 'metis']
    arbitrum_nitro_chains = ['arbitrum', 'gravity', 'real', 'arbitrum_nova', 'plume']
    
    chain_lower = chain.lower()

    if chain_lower in op_chains:
        config = chain_configs.get('op_chains')
    elif chain_lower in arbitrum_nitro_chains:
        config = chain_configs.get('arbitrum_nitro')
    elif chain_lower in chain_configs:
        config = chain_configs[chain_lower]
    elif chain_lower in default_chains:
        config = chain_configs['default']
    else:
        raise ValueError(f"Chain '{chain}' is not listed in the supported chains.")

    # Ensure the required columns exist, filling with default values
    required_columns = config.get('required_columns', [])
    for col in required_columns:
        if col not in df.columns:
            df[col] = 0

    # Map columns
    column_mapping = config.get('column_mapping', {})
    existing_columns = [col for col in column_mapping.keys() if col in df.columns]
    df = df[existing_columns]
    df = df.rename(columns=column_mapping)

    # Ensure 'block_date' column exists and populate it
    if "block_timestamp" in df.columns:
        df["block_date"] = pd.to_datetime(df["block_timestamp"], unit="s").dt.date

    # Convert columns to numeric
    numeric_columns = config.get('numeric_columns', [])
    df = convert_columns_to_numeric(df, numeric_columns)

    # Apply special operations
    special_operations = config.get('special_operations', [])
    for operation_name in special_operations:
        operation_function = globals().get(operation_name)
        if operation_function:
            df = operation_function(df)
        else:
            print(f"Warning: Special operation '{operation_name}' not found.")

    # Apply custom operations if any
    custom_operations = config.get('custom_operations', [])
    for op_name in custom_operations:
        operation_function = globals().get(op_name)
        if operation_function:
            df = operation_function(df)
        else:
            print(f"Warning: Custom operation '{op_name}' not found.")

    # Fill NaN values with specified defaults
    fillna_values = config.get('fillna_values', {})
    for col, value in fillna_values.items():
        if col in df.columns:
            df[col].fillna(value, inplace=True)
            
    # Convert date columns
    date_columns = config.get('date_columns', {})
    for col, unit in date_columns.items():
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit=unit)

    # Map status values
    status_mapping = config.get('status_mapping', {})
    df = handle_status(df, status_mapping)

    # Handle address columns
    address_columns = config.get('address_columns', [])
    df = handle_address_columns(df, address_columns)

    # Handle bytea columns
    bytea_columns = config.get('bytea_columns', [])
    df = handle_bytea_columns(df, bytea_columns)

    # Value conversions
    value_conversion = config.get('value_conversion', {})
    df = convert_columns_to_eth(df, value_conversion)

    # Any additional custom steps
    if chain.lower() == 'ethereum':
        df = shorten_input_data(df)

    return df

# ---------------- Error Handling -----------------------
class MaxWaitTimeExceededException(Exception):
    pass

def handle_retry_exception(current_start, current_end, base_wait_time, rpc_url):
    max_wait_time = 60  # Maximum wait time in seconds
    wait_time = min(max_wait_time, 2 * base_wait_time)

    # Check if max_wait_time is reached and raise an exception
    if wait_time >= max_wait_time:
        raise MaxWaitTimeExceededException(f"For {rpc_url}: Maximum wait time exceeded for blocks {current_start} to {current_end}")

    # Add jitter
    jitter = random.uniform(0, wait_time * 0.1)
    wait_time += jitter
    formatted_wait_time = format(wait_time, ".2f")

    print(f"RETRY: for blocks {current_start} to {current_end} after {formatted_wait_time} seconds. RPC: {rpc_url}")
    time.sleep(wait_time)

    return wait_time

# ---------------- Database Interaction ------------------
def check_db_connection(db_connector):
    """
    Checks if the connection to the database is valid.
    
    Args:
        db_connector: The database connector object.

    Returns:
        bool: True if the connection is valid, False otherwise.
    """
    return db_connector is not None

# ---------------- Data Interaction --------------------
def extract_authorization_list(transaction_details, block_timestamp, block_date, block_number):
    """
    Extracts authorization list data from type 4 transactions and returns it as a DataFrame.
    
    Args:
        transaction_details (list): List of transaction dictionaries
        block_timestamp (datetime): Block timestamp
        block_date (date): Block date
        block_number (int): Block number
        
    Returns:
        pd.DataFrame: DataFrame containing authorization list data for type 4 transactions
    """
    auth_list_data = []
    
    for tx in transaction_details:
        # Check if transaction has type 4 and authorizationList
        if tx.get('type') == 4 and 'authorizationList' in tx and tx['authorizationList']:
            tx_hash = tx['hash']
            
            for index, auth in enumerate(tx['authorizationList']):
                try:
                    # Convert auth to dict if it's not already
                    auth_dict = dict(auth) if hasattr(auth, 'items') else auth
                    
                    # Calculate the authority (EOA) from r,s,v values
                    sender = to_checksum_address(auth_dict['address'])
                    chain_id = int(auth_dict['chainId'], 16) if isinstance(auth_dict['chainId'], str) else auth_dict['chainId']
                    nonce = int(auth_dict['nonce'], 16) if isinstance(auth_dict['nonce'], str) else auth_dict['nonce']
                    v = 27 + int(auth_dict['yParity'], 16) if isinstance(auth_dict['yParity'], str) else 27 + auth_dict['yParity']
                    r = int(auth_dict['r'], 16) if isinstance(auth_dict['r'], str) else auth_dict['r']
                    s = int(auth_dict['s'], 16) if isinstance(auth_dict['s'], str) else auth_dict['s']
                    
                    # Hash message per EIP-7702
                    # The message is: MAGIC || rlp([chain_id, address, nonce])
                    magic = b'\x05'  # EIP-7702 magic constant
                    auth_tuple = [chain_id, bytes.fromhex(sender[2:]), nonce]
                    rlp_encoded = rlp.encode(auth_tuple)
                    message_hash = keccak(magic + rlp_encoded)
                    
                    # Recover signer address
                    eoa_address = Account._recover_hash(message_hash, vrs=(v, r, s))
                    
                    # Create authorization list record matching table schema
                    auth_record = {
                        'tx_hash': tx_hash,
                        'tx_index': index,
                        'eoa_address': eoa_address,
                        'contract_address': sender,
                        'origin_key': None,  # Will be set to actual chain name in processing
                        'chain_ids': [chain_id],
                        'block_number': block_number,
                        'block_timestamp': block_timestamp,
                        'block_date': block_date
                    }
                    
                    auth_list_data.append(auth_record)
                    
                except Exception as e:
                    print(f"Error processing authorization list for tx {tx_hash}, index {index}: {e}")
                    continue
    
    return pd.DataFrame(auth_list_data) if auth_list_data else pd.DataFrame()

def process_authorization_list_data(auth_df, chain):
    """
    Processes authorization list DataFrame for database insertion.
    
    Args:
        auth_df (pd.DataFrame): The raw authorization list DataFrame
        chain (str): The blockchain chain name
        
    Returns:
        pd.DataFrame: Processed DataFrame ready for database insertion
    """
    if auth_df.empty:
        return auth_df
    
    # Set origin_key to the actual chain name
    auth_df['origin_key'] = chain.lower()
    
    # Handle bytea columns (tx_hash, eoa_address, contract_address)
    bytea_columns = ['tx_hash', 'eoa_address', 'contract_address']
    for col in bytea_columns:
        if col in auth_df.columns:
            auth_df[col] = auth_df[col].apply(lambda x: process_bytea_value(x) if pd.notnull(x) else None)
    
    # Ensure block_timestamp is datetime
    if 'block_timestamp' in auth_df.columns:
        auth_df['block_timestamp'] = pd.to_datetime(auth_df['block_timestamp'], unit='s')
    
    # Ensure block_date is a proper date object
    if 'block_date' in auth_df.columns:
        # Check if block_date is already a date object, if not convert it
        if not auth_df['block_date'].apply(lambda x: isinstance(x, date) if pd.notnull(x) else True).all():
            # If it's a timestamp, convert it
            auth_df['block_date'] = pd.to_datetime(auth_df['block_date'], unit='s').dt.date
        # If it's already a date object, leave it as is
    
    # Ensure proper integer types to match table schema
    if 'tx_index' in auth_df.columns:
        auth_df['tx_index'] = auth_df['tx_index'].astype('int32')  # matches int4 in PostgreSQL
    
    if 'block_number' in auth_df.columns:
        auth_df['block_number'] = auth_df['block_number'].astype('int64')  # matches int8 in PostgreSQL
    
    # Handle chain_ids array column - format as PostgreSQL array string for pangres compatibility
    if 'chain_ids' in auth_df.columns:
        def format_pg_array(x):
            """Convert Python list to PostgreSQL array format string"""
            if isinstance(x, list):
                # Convert list to PostgreSQL array format: {1,2,3}
                int_list = [int(item) for item in x]
                return '{' + ','.join(map(str, int_list)) + '}'
            else:
                # Single value as array: {1}
                return '{' + str(int(x)) + '}'
        
        auth_df['chain_ids'] = auth_df['chain_ids'].apply(format_pg_array)
    
    return auth_df

def get_latest_block(w3):
    """
    Retrieves the latest block number from the connected Ethereum node.
    
    Args:
        w3: The Web3 instance for interacting with the blockchain.

    Returns:
        int: The latest block number, or None if the retrieval fails after retries.
    """
    retries = 0
    while retries < 3:
        try:
            return w3.eth.block_number
        except Exception as e:
            print("RETRY: occurred while fetching the latest block, but will retry in 3s:", str(e))
            retries += 1
            time.sleep(3)

    print("ERROR: Failed to fetch the latest block after 3 retries.")
    return None
    
def fetch_block_transaction_details(w3, block):
    """
    Fetches detailed information for all transactions in a given block.
    Tries to use `eth_getBlockReceipts` first, and falls back to individual calls if not available.

    Args:
        w3: The Web3 instance for interacting with the blockchain.
        block (dict): The block data containing transactions.

    Returns:
        list: A list of dictionaries containing transaction details.
    """
    # Static set to track which RPCs we've already logged messages for
    if not hasattr(fetch_block_transaction_details, "_logged_rpcs"):
        fetch_block_transaction_details._logged_rpcs = set()

    transaction_details = []
    block_timestamp = block['timestamp']  # Get the block timestamp
    base_fee_per_gas = block['baseFeePerGas'] if 'baseFeePerGas' in block else None  # Fetch baseFeePerGas from the block
    block_hash = block['hash']
    txs = block['transactions']

    def _convert_bytes_to_hex(d: dict, key: str):
        """If d[key] is bytes or string representation of bytes, convert it to a hex string."""
        if key not in d:
            return

        value = d[key]

        if isinstance(value, bytes):
            d[key] = '0x' + value.hex()

        elif isinstance(value, str) and value.startswith("b'") and '\\x' in value:
            try:
                import ast
                byte_obj = ast.literal_eval(value)
                if isinstance(byte_obj, bytes):
                    d[key] = '0x' + byte_obj.hex()
            except (SyntaxError, ValueError):
                if value.startswith("b'") or value.startswith('b"'):
                    d[key] = '0x' + value[2:-1].replace('\\x', '')

    byte_fields = ['input', 'input_data', 'data']
    
    # Get the RPC URL
    rpc_url = w3.get_rpc_url()
    
    # Skip batch receipt fetch if this RPC is known not to support it
    if not Web3CC.is_method_supported(rpc_url, "get_block_receipts"):
        if rpc_url not in fetch_block_transaction_details._logged_rpcs:
            print(f"Skipping batch receipt fetch for RPC {rpc_url} (known to not support get_block_receipts)")
            fetch_block_transaction_details._logged_rpcs.add(rpc_url)
        use_fallback = True
    else:
        use_fallback = False

    if not use_fallback:
        try:
            # Primary method: get all receipts in one call
            receipts = w3.eth.get_block_receipts(block_hash)

            if len(receipts) != len(txs):
                raise ValueError("Mismatch between number of receipts and transactions")

            for tx, receipt in zip(txs, receipts):
                tx = dict(tx)
                receipt = dict(receipt)

                for field in byte_fields:
                    _convert_bytes_to_hex(receipt, field)
                    _convert_bytes_to_hex(tx, field)

                merged = {**receipt, **tx}

                if isinstance(tx['hash'], bytes):
                    merged['hash'] = '0x' + tx['hash'].hex()
                elif isinstance(tx['hash'], str) and not tx['hash'].startswith('0x'):
                    merged['hash'] = '0x' + tx['hash']
                else:
                    merged['hash'] = tx['hash']
                    
                merged['block_timestamp'] = block_timestamp
                if base_fee_per_gas:
                    merged['baseFeePerGas'] = base_fee_per_gas
                transaction_details.append(merged)

        except Exception as e:
            print(f"[Fallback] Block receipt fetch failed: {str(e)}")
            print(f"Falling back to per-transaction receipt fetch using RPC: {rpc_url}")
            use_fallback = True
            
            # If this is a method not supported error, mark this RPC as not supporting get_block_receipts
            if any(x in str(e).lower() for x in ["method not found", "not supported", "method not supported", "not implemented", 
                                                "hex number > 64 bits", "method handler crashed"]):
                Web3CC.method_not_supported(rpc_url, "get_block_receipts")

    if use_fallback:
        for tx in txs:
            try:
                tx = dict(tx)
                receipt = w3.eth.get_transaction_receipt(tx['hash'])
                receipt = dict(receipt)

                for field in byte_fields:
                    _convert_bytes_to_hex(receipt, field)
                    _convert_bytes_to_hex(tx, field)

                merged = {**receipt, **tx}

                if isinstance(tx['hash'], bytes):
                    merged['hash'] = '0x' + tx['hash'].hex()
                elif isinstance(tx['hash'], str) and not tx['hash'].startswith('0x'):
                    merged['hash'] = '0x' + tx['hash']
                else:
                    merged['hash'] = tx['hash']

                merged['block_timestamp'] = block_timestamp
                if base_fee_per_gas:
                    merged['baseFeePerGas'] = base_fee_per_gas
                transaction_details.append(merged)
            except Exception as inner_e:
                print(f"Failed to fetch receipt for tx {tx['hash'].hex()}: {str(inner_e)}")

    return transaction_details
    
def fetch_data_for_range(w3, block_start, block_end):
    """
    Fetches transaction data for a range of blocks and returns it as a DataFrame.
    Also extracts authorization list data from type 4 transactions.
    
    Args:
        w3: The Web3 instance for interacting with the blockchain.
        block_start (int): The starting block number.
        block_end (int): The ending block number.

    Returns:
        tuple: (transaction_df, auth_list_df) - DataFrames containing transaction data and authorization list data
    """
    all_transaction_details = []
    all_auth_list_data = []
    
    try:
        # Loop through each block in the range
        for block_num in range(block_start, block_end + 1):
            block = w3.eth.get_block(block_num, full_transactions=True)
            
            # Fetch transaction details for the block using the new function
            transaction_details = fetch_block_transaction_details(w3, block)
            all_transaction_details.extend(transaction_details)
            
            # Extract authorization list data from type 4 transactions
            if transaction_details:
                block_timestamp = block.get('timestamp', 0)
                block_date = pd.to_datetime(block_timestamp, unit='s').date() if block_timestamp else None
                
                auth_list_df = extract_authorization_list(transaction_details, block_timestamp, block_date, block_num)
                if not auth_list_df.empty:
                    all_auth_list_data.append(auth_list_df)

        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(all_transaction_details)
        
        # Combine all authorization list data
        auth_df = pd.concat(all_auth_list_data, ignore_index=True) if all_auth_list_data else pd.DataFrame()
        
        # if df doesn't have any records, then handle it gracefully
        if df.empty:
            print(f"...no transactions found for blocks {block_start} to {block_end}.")
            return None, auth_df  # Return None for transactions, but still return auth data if any
        else:
            return df, auth_df

    except Exception as e:
        raise e

def fetch_and_process_range(current_start, current_end, chain, w3, table_name, bucket_name, db_connector, rpc_url):
    """
    Fetches and processes transaction data for a range of blocks, saves it to GCS, and inserts it into the database.
    Also processes authorization list data from type 4 transactions.
    Retries the operation on failure with exponential backoff.

    Args:
        current_start (int): The starting block number.
        current_end (int): The ending block number.
        chain (str): The name of the blockchain chain.
        w3: The Web3 instance for interacting with the blockchain.
        table_name (str): The database table name to insert data into.
        bucket_name (str): The name of the GCS bucket.
        db_connector: The database connector object.
        rpc_url (str): The RPC URL used for fetching data.

    Raises:
        MaxWaitTimeExceededException: If the operation exceeds the maximum wait time.
    """
    base_wait_time = 3   # Base wait time in seconds
    start_time = time.time()
    while True:
        try:
            elapsed_time = time.time() - start_time

            df, auth_df = fetch_data_for_range(w3, current_start, current_end)

            # Check if df is None or empty, and if so, return early without further processing.
            if df is None or df.empty:
                print(f"...skipping blocks {current_start} to {current_end} due to no data.")
                # Still process authorization list data if present
                if not auth_df.empty:
                    try:
                        auth_df_prep = process_authorization_list_data(auth_df, chain)
                        auth_df_prep.drop_duplicates(subset=['tx_hash', 'tx_index'], inplace=True)
                        auth_df_prep.set_index(['tx_hash', 'tx_index'], inplace=True)
                        
                        db_connector.upsert_table('authorizations_7702', auth_df_prep, if_exists='update')
                        print(f"...authorization list data inserted for blocks {current_start} to {current_end}: {auth_df_prep.shape[0]} records")
                    except Exception as e:
                        print(f"ERROR: {rpc_url} - inserting authorization list data for blocks {current_start} to {current_end}: {e}")
                return

            save_data_for_range(df, current_start, current_end, chain, bucket_name)

            df_prep = prep_dataframe_new(df, chain)

            df_prep.drop_duplicates(subset=['tx_hash'], inplace=True)
            df_prep.set_index('tx_hash', inplace=True)
            df_prep.index.name = 'tx_hash'

            try:
                db_connector.upsert_table(table_name, df_prep, if_exists='update')  # Use DbConnector for upserting data
                rows_uploaded = df_prep.shape[0]
                print(f"...data inserted for blocks {current_start} to {current_end} successfully. Uploaded rows: {df_prep.shape[0]}. RPC: {w3.get_rpc_url()}")
                
                # Process and insert authorization list data if present
                if not auth_df.empty:
                    try:
                        auth_df_prep = process_authorization_list_data(auth_df, chain)
                        auth_df_prep.drop_duplicates(subset=['tx_hash', 'tx_index'], inplace=True)
                        auth_df_prep.set_index(['tx_hash', 'tx_index'], inplace=True)
                        
                        db_connector.upsert_table('authorizations_7702', auth_df_prep, if_exists='update')
                        print(f"...authorization list data inserted for blocks {current_start} to {current_end}: {auth_df_prep.shape[0]} records")
                    except Exception as e:
                        print(f"ERROR: {rpc_url} - inserting authorization list data for blocks {current_start} to {current_end}: {e}")
                        # Don't fail the whole operation if auth list insertion fails
                
                return rows_uploaded  # Return the number of rows uploaded
            except Exception as e:
                print(f"ERROR: {rpc_url} - inserting data for blocks {current_start} to {current_end}: {e}")
                raise e

        except Exception as e:
            print(f"ERROR: {rpc_url} - processing blocks {current_start} to {current_end}: {e}")
            base_wait_time = handle_retry_exception(current_start, current_end, base_wait_time, rpc_url)
            # Check if elapsed time exceeds 15 minutes (extended since we handle RPC-level timeouts separately)
            if elapsed_time >= 900:
                raise MaxWaitTimeExceededException(f"For {rpc_url}: Maximum wait time exceeded for blocks {current_start} to {current_end}")

def get_chain_config(db_connector, chain_name):
    """
    Retrieves the RPC configuration and batch size for the specified blockchain chain.
    
    Args:
        db_connector: The database connector object.
        chain_name (str): The name of the blockchain chain.

    Returns:
        tuple: A tuple containing the list of RPC configurations and the batch size.
    """
    # Determine the SQL query based on the chain name
    if chain_name.lower() == "celestia" or chain_name.lower() == "starknet":
        raw_sql = text(
            "SELECT url, workers, max_requests, max_tps "
            "FROM sys_rpc_config "
            "WHERE origin_key = :chain_name AND active = TRUE "
        )
    else:
        raw_sql = text(
            "SELECT url, workers, max_requests, max_tps "
            "FROM sys_rpc_config "
            "WHERE active = TRUE AND origin_key = :chain_name AND synced = TRUE"
        )

    with db_connector.engine.connect() as connection:
        result = connection.execute(raw_sql, {"chain_name": chain_name})
        rows = result.fetchall()

    config_list = []

    for row in rows:
        config = {"url": row['url']}
        # Add other keys only if they are not None
        if row['workers'] is not None:
            config['workers'] = row['workers']
        if row['max_requests'] is not None:
            config['max_req'] = row['max_requests']
        if row['max_tps'] is not None:
            config['max_tps'] = row['max_tps']
        
        config_list.append(config)

    # Retrieve batch_size
    batch_size = 10
    main_conf = get_main_config()
    for chain in main_conf:
        if chain.origin_key == chain_name:
            if chain.backfiller_batch_size > 0:
                batch_size = chain.backfiller_batch_size
            break

    return config_list, batch_size
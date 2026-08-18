import yaml
import pandas as pd
from web3 import Web3
import datetime
import time
import io
import requests
import zipfile

def _download_zip_with_retries(zip_url: str, attempts: int = 3, timeout: int = 30) -> io.BytesIO:
    last_error = None

    for attempt in range(1, attempts + 1):
        response = None
        try:
            response = requests.get(zip_url, timeout=timeout)
            response.raise_for_status()
            zip_content = io.BytesIO(response.content)

            if not zipfile.is_zipfile(zip_content):
                preview = response.content[:120].decode("utf-8", errors="replace")
                content_type = response.headers.get("Content-Type", "unknown")
                raise zipfile.BadZipFile(
                    f"Response from {zip_url} is not a zip file "
                    f"(status={response.status_code}, content_type={content_type}, "
                    f"bytes={len(response.content)}, preview={preview!r})"
                )

            zip_content.seek(0)
            return zip_content
        except (requests.RequestException, zipfile.BadZipFile) as exc:
            last_error = exc
            if attempt == attempts:
                break

            time.sleep(2 ** (attempt - 1))

    raise RuntimeError(f"Failed to download valid zip from {zip_url} after {attempts} attempts: {last_error}") from last_error

def get_eim_yamls(file_names:list):
    """
    Retrieves the YAML data from the EIM data repository on GitHub.

    :param file_names: List of file names to retrieve from the repository.
    :return: List of dictionaries containing the YAML data for each file.
    """    
    yaml_dicts = []

    repo_url = "https://github.com/etherguild/ethismoney-data/tree/main/"
    _, _, _, owner, repo_name, _, branch, *path = repo_url.split('/')
    path = '/'.join(path)
    zip_url = f"https://github.com/{owner}/{repo_name}/archive/{branch}.zip"
    zip_content = _download_zip_with_retries(zip_url)

    path_start = 'ethismoney-data-main/'
    with zipfile.ZipFile(zip_content) as zip_ref:
        nameslist = zip_ref.namelist()
        for file_name in file_names:
            yaml_dict = None
            for path in nameslist:  
                if path == f'{path_start}{file_name}.yml': 
                    with zip_ref.open(path) as file:
                        content = file.read().decode('utf-8')
                        yaml_dict = yaml.safe_load(content)
                        
            yaml_dicts.append(yaml_dict)
    return yaml_dicts

def get_eth_balance(w3: Web3, address, at_block='latest'):
    """
    Retrieves the ETH balance for a given address at a specified block.

    :param w3: Web3 object to connect to EVM blockchain.
    :param address: EVM address to check the balance.
    :param at_block: Block number to get the balance (default is 'latest').
    :return: Balance in Ether (ETH) for the specified address.
    """
    if not Web3.is_address(address):
        print(f"Invalid Ethereum address: {address}")
        raise ValueError(f"Invalid Ethereum address: {address}")

    try:
        checksum_address = Web3.to_checksum_address(address)
        balance = w3.eth.get_balance(checksum_address, block_identifier=at_block) / 10**18
    except Exception as e:
        print(f"Error retrieving ETH balance for {address} with block {at_block}: {e}")
        raise e
    return balance

def call_contract_function(w3: Web3, contract_address: str, abi: dict, function_name: str, *args, at_block='latest'):
    """
    Calls a specific function of a contract and handles errors gracefully.

    :param w3: Web3 object to connect to the EVM blockchain.
    :param contract_address: Address of the contract.
    :param abi: ABI of the contract.
    :param function_name: Name of the function to call on the contract.
    :param args: Arguments to pass to the contract function.
    :param at_block: Block identifier to execute the call.
    :return: The result of the contract function or None if an error occurs.
    """

    # check if the contract was deployed at the given address
    code = w3.eth.get_code(contract_address, block_identifier=at_block)
    time.sleep(0.1)  # Sleep to avoid rate limiting
    if code == b'':  # Contract not deployed by this block
        print(f"Contract not deployed at address {contract_address} with block {at_block}")
        return None

    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=abi)
        time.sleep(0.1)  # Sleep to avoid rate limiting
        function = getattr(contract.functions, function_name)
        return function(*args).call(block_identifier=int(at_block))
    except Exception as e:
        print(f"Error calling function {function_name} with args {args} on contract {contract_address} with block_identifier {at_block}: {e}")
        ## print datatypes of all variables
        print(f"contract_address: {type(contract_address)}")
        print(f"abi: {type(abi)}")
        print(f"function_name: {type(function_name)}")
        print(f"at_block: {type(at_block)}")
        ## print all args types
        for i in range(len(args)):
            print(f"args[{i}]: {type(args[i])}")

        raise e

def get_erc20_balance_ethereum(w3: Web3, token_contract: str, token_abi: dict, address, at_block='latest'):
    """
    Retrieves the ERC20 token balance for a given token contract and address at a specified block.

    :param w3: Web3 object to connect to EVM blockchain.
    :param token_contract: Address of the ERC20 token contract.
    :param token_abi: ABI of the ERC20 token contract.
    :param address: EVM address to check the balance.
    :param at_block: Block number to get the balance (default is 'latest').
    :return: Token balance for the specified address.
    """
    if not Web3.is_address(address):
        print(f"Invalid Ethereum address: {address}")
        raise ValueError(f"Invalid Ethereum address: {address}")

    result = call_contract_function(w3, token_contract, token_abi, 'balanceOf', Web3.to_checksum_address(address), at_block=at_block)
    if result is None:  # Check for None to avoid division by zero
        print(f"Error retrieving ERC20 balance for {address} with block {at_block}")
        return None
    
    return result / 10**18

def get_validator_balance(validator_ids: list, slot='head'):
    """
    Retrieves the validator balances for a list of validator IDs at a specific slot.

    :param validator_ids: List of validator IDs to retrieve balances.
    :param slot: Slot number to retrieve the balances (default is 'head').
    :return: Total balance of the validators in Gwei.
    """
    url = f'https://ethereum-beacon-api.publicnode.com/eth/v1/beacon/states/{slot}/validators'
    total_balance = 0

    for i in range(0, len(validator_ids), 100):
        batch = validator_ids[i:i+100]
        params = {'id': ','.join(map(str, batch))}
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Extract and sum up balances
        balances = [int(validator['balance']) for validator in data['data']]
        total_balance += sum(balances)

    return total_balance

def get_first_block_of_day(w3: Web3, target_date: datetime.date):
    """
    Finds the first block of a given day using binary search based on the timestamp.

    :param w3: Web3 object to connect to Ethereum blockchain.
    :param target_date: The target date to find the first block of the day (in UTC).
    :return: Block object of the first block of the day or None if not found.
    """
    start_of_day = datetime.datetime.combine(target_date, datetime.time(0, 0), tzinfo=datetime.timezone.utc)
    start_timestamp = int(start_of_day.timestamp())

    latest_block = w3.eth.get_block('latest')['number']
    low, high = 0, latest_block

    # Binary search to find the first block with a timestamp >= start_timestamp
    while low < high:
        mid = (low + high) // 2
        mid_block = w3.eth.get_block(mid)
        time.sleep(0.1)  # Sleep to avoid rate limiting
        if mid_block['timestamp'] < start_timestamp:
            low = mid + 1
        else:
            high = mid

    first_block_of_day = w3.eth.get_block(low)
    return first_block_of_day if first_block_of_day['timestamp'] >= start_timestamp else None

def get_block_numbers(w3, days: int = 7):
    """
    Retrieves the first block of each day for the past 'days' number of days and returns a DataFrame 
    with the block number and timestamp for each day.

    :param w3: Web3 object to connect to the Ethereum blockchain.
    :param days: The number of days to look back from today (default is 7).
    :return: DataFrame containing the date, block number, and block timestamp for each day.
    """
    current_date = datetime.datetime.now().date()  # Get the current date (no time)
    start_date = current_date - datetime.timedelta(days=days)  # Calculate the start date

    # Initialize an empty list to hold the data
    block_data = []

    # Loop over each day from start_date to current_date
    while current_date > start_date:
        # Calculate the next day's date for which we want to find the first block
        target_date = start_date + datetime.timedelta(days=1)

        # Retrieve the first block of the day for the target date
        new_block = get_first_block_of_day(w3, target_date)

        # Error handling in case get_first_block_of_day returns None
        if new_block is None:
            print(f"ERROR: Could not retrieve block for {target_date}")
        else:
            # Log the block number and timestamp
            print(f'..block number for {target_date}: {new_block["number"]}')

            # Append the result as a dictionary to the block_data list
            block_data.append({
                'date': str(target_date),
                'block': new_block['number'],
                'block_timestamp': new_block['timestamp']
            })

        # Move to the next date
        start_date = target_date

    # Convert the collected block data into a DataFrame
    df = pd.DataFrame(block_data)

    # block as string
    df['block'] = df['block'].astype(str)
    return df

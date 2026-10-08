import pandas as pd
from web3 import Web3, HTTPProvider
from sqlalchemy import exc
import threading
from web3.middleware import ExtraDataToPOAMiddleware
from src.db_connector import DbConnector
import sqlalchemy as sa
import time

from src.adapters.rpc_funcs.utils import get_latest_block

def connect_to_node(url):
    """
    Connects to an Ethereum node at the given URL using Web3.
    Retries the connection up to 5 times in case of failure.

    Args:
        url (str): The URL of the Ethereum node.

    Returns:
        Web3: The connected Web3 instance or None if the connection fails after retries.
    """
    retries = 5
    delay = 5
    w3 = Web3(HTTPProvider(url))
    
    # Inject POA middleware for chains that need it (safe to inject even if not needed)
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    
    for attempt in range(1, retries + 1):
        if w3.is_connected():
            return w3
        else:
            if attempt < retries:
                #print(f"...attempt {attempt} failed for {w3.provider.endpoint_uri}, retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"...attempt {attempt} failed for {w3.provider.endpoint_uri}. No more retries left.")
    return None
        
def fetch_rpc_urls(db_connector, chain_name):
    """
    Fetches active RPC URLs for a specific blockchain chain from the database.

    Args:
        db_connector: Database connector used to execute the query.
        chain_name (str): The name of the blockchain chain.

    Returns:
        pd.DataFrame: A DataFrame containing the active RPC URLs for the specified chain.
    """
    query = f"""
    SELECT url
    FROM sys_rpc_config
    WHERE origin_key = '{chain_name}'
    AND active = true;
    """
    try:
        with db_connector.engine.connect() as conn:
            result = pd.read_sql(query, conn)
        print(f"...RPC data fetched successfully for chain: {chain_name}")
        return result
    except exc.SQLAlchemyError as e:
        print(f"ERROR: fetching data for chain: {chain_name}")
        print(e)
        return pd.DataFrame()
    
def fetch_block(url, results, full_block_results=None):
    """
    Fetches the latest block number from the specified Ethereum node URL.
    If the connection fails, it returns 0 for that URL.

    Args:
        url (str): The URL of the Ethereum node.
        results (dict): A dictionary to store the block number for the URL.
        full_block_results (dict, optional): If given, also checks whether the node can return a
            block with full transactions (needed by the raw adapters) and stores True/False for the URL.
    """
    web3_instance = None
    try:
        web3_instance = connect_to_node(url)
        if web3_instance is not None:
            block = get_latest_block(web3_instance)
        else:
            block = None
    except Exception as e:
        print(f"ERROR: Failed to connect to {url}: {str(e)}")
        block = None

    results[url] = block if block is not None else 0

    if full_block_results is not None:
        full_block_results[url] = False
        if block:
            try:
                web3_instance.eth.get_block(block - 5, full_transactions=True)
                full_block_results[url] = True
            except Exception as e:
                print(f"ERROR: {url} cannot return full blocks: {str(e)[:200]}")


def fetch_all_blocks(rpc_urls, full_block_results=None):
    """
    Fetches the latest block number from all provided RPC URLs in parallel using threading.

    Args:
        rpc_urls (pd.DataFrame): DataFrame containing the RPC URLs.
        full_block_results (dict, optional): Passed to fetch_block to also check full block support.

    Returns:
        dict: A dictionary mapping each RPC URL to its latest block number.
    """
    threads = []
    results = {}
    for index, rpc in rpc_urls.iterrows():
        thread = threading.Thread(target=fetch_block, args=(rpc['url'], results, full_block_results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return results

def check_sync_state(blocks, block_threshold):
    """
    Checks the synchronization state of all nodes by comparing their block heights.
    Identifies nodes that are either too far behind or not responding.

    Args:
        blocks (dict): A dictionary mapping RPC URLs to their block numbers.
        block_threshold (int): The maximum allowed difference between the highest block and a node's block before it is considered unsynced.

    Returns:
        list: A list of URLs for nodes that are unsynced.
    """
    max_block = max(blocks.values())
    notsynced_nodes = []
    for url, block in blocks.items():
        if block == 0:
            print(f"UNSYNCED: Node {url} is not responding (block == 0).")
            notsynced_nodes.append(url)
        elif max_block - block > block_threshold:
            print(f"UNSYNCED: Node {url} is too far behind. Max block: {max_block} // Node block: {block} // Behind by: {max_block - block}")
            notsynced_nodes.append(url)
    return notsynced_nodes

def update_sync_state(db_connector, chain_name, synced_nodes, notsynced_nodes):
    """
    Writes the sync state of all checked nodes in a single transaction, so readers never see
    a state where every node is temporarily marked as synced.

    Args:
        db_connector: Database connector used to execute the query.
        chain_name (str): The name of the blockchain chain.
        synced_nodes (list): URLs of nodes to mark as synced.
        notsynced_nodes (list): URLs of nodes to mark as unsynced.
    """
    query = """
    UPDATE sys_rpc_config
    SET synced = :synced
    WHERE origin_key = :origin_key AND url IN :urls;
    """
    try:
        with db_connector.engine.begin() as conn:
            if synced_nodes:
                conn.execute(sa.text(query), {"synced": True, "origin_key": chain_name, "urls": tuple(synced_nodes)})
            if notsynced_nodes:
                conn.execute(sa.text(query), {"synced": False, "origin_key": chain_name, "urls": tuple(notsynced_nodes)})
        print(f"...{len(synced_nodes)} nodes set to synced.")
        if notsynced_nodes:
            print(f"UNSYNCED Nodes: {tuple(notsynced_nodes)} set to unsynced.")
    except sa.exc.SQLAlchemyError as e:
        print("ERROR: updating nodes' synced status.")
        print(e)


def get_chains_available(db_connector):
    """
    Retrieves a list of unique blockchain chain names from the database.

    Args:
        db_connector: Database connector used to execute the query.

    Returns:
        list: A list of distinct blockchain chain names.
    """
    try:
        with db_connector.engine.connect() as conn:
            query = """
            SELECT DISTINCT origin_key FROM sys_rpc_config;
            """
            result = conn.execute(sa.text(query))
            origin_keys = [row[0] for row in result]
            return origin_keys
    except sa.exc.SQLAlchemyError as e:
        print("ERROR: retrieving unique origin_keys.")
        print(e)
        return []

def sync_check():
    """
    Performs a synchronization check for all blockchain chains.
    Fetches the latest block numbers, checks sync state and full block support, then writes the
    synced flag for all nodes of a chain in one transaction.

    The function sets a block threshold of 100 for 'arbitrum' and 30 for other chains.
    """
    db_connector = DbConnector()

    chains = get_chains_available(db_connector)
    for chain_name in chains:
        if chain_name == 'arbitrum':
            block_threshold = 100
        else:
            block_threshold = 30
            
        print(f"START: processing chain: {chain_name}")
        rpc_urls = fetch_rpc_urls(db_connector, chain_name)

        if rpc_urls.empty:
            print(f"...no RPC urls found for chain: {chain_name} to process.")
        else:
            full_block_ok = {}
            blocks = fetch_all_blocks(rpc_urls, full_block_ok)
            notsynced_nodes = check_sync_state(blocks, block_threshold)

            # Nodes that report a block height but can't serve full blocks (e.g. plan-restricted
            # endpoints) would fail every raw run. Only enforce this if at least one node passes,
            # so chains where the check doesn't apply (non-EVM) keep their nodes.
            no_full_blocks = [url for url in blocks if url not in notsynced_nodes and not full_block_ok.get(url)]
            if no_full_blocks and len(no_full_blocks) < len(blocks) - len(notsynced_nodes):
                print(f"UNSYNCED: Nodes {no_full_blocks} cannot return full blocks.")
                notsynced_nodes += no_full_blocks
            elif no_full_blocks:
                print(f"WARNING: no node for {chain_name} passed the full block check, ignoring it for this chain.")

            synced_nodes = [url for url in blocks if url not in notsynced_nodes]
            update_sync_state(db_connector, chain_name, synced_nodes, notsynced_nodes)
        print(f"DONE: processing chain: {chain_name}")
        
    print("FINISHED: All chains processed.")
        
if __name__ == "__main__":
    sync_check()
import requests
import base64
import json
from queue import Queue
from threading import Thread
import time
import pandas as pd
from src.adapters.rpc_funcs.funcs_backfill import check_and_record_missing_block_ranges
from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.adapters.rpc_funcs.utils import connect_to_gcs, save_data_for_range, handle_retry_exception

class AdapterCelestia(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Celestia", adapter_params, db_connector)
        self.chain = adapter_params['chain']
        self.rpc_list = adapter_params['rpc_list']
        self.table_name = f'{self.chain}_tx'   
        self.db_connector = db_connector
        
        # Initialize GCS connection
        self.gcs_connection, self.bucket_name = connect_to_gcs()
        
    def extract_raw(self, load_params:dict):
        self.block_start = load_params['block_start']
        self.batch_size = load_params['batch_size']
        self.run(self.block_start, self.batch_size)
        print(f"FINISHED loading raw tx data for {self.chain}.")
        
    def run(self, block_start, batch_size):
        latest_block = self.get_latest_block()
        if latest_block is None:
            print("Could not fetch the latest block.")
            raise ValueError("Could not fetch the latest block.")
        if block_start == 'auto':
            block_start = self.db_connector.get_max_block(self.table_name)  
        else:
            block_start = int(block_start)

        print(f"Running with start block {block_start} and latest block {latest_block}")
        block_start = int(block_start)
        latest_block = int(latest_block)
        batch_size = int(batch_size)
        
        # Initialize the block range queue
        block_range_queue = Queue()
        self.enqueue_block_ranges(block_start, latest_block, batch_size, block_range_queue)
        print(f"Enqueued {block_range_queue.qsize()} block ranges.")

        # Manage threads to process block ranges from the queue
        self.manage_threads(block_range_queue)

    def enqueue_block_ranges(self, block_start, latest_block, batch_size, queue):
        # Enqueue block ranges into the queue for processing
        for start in range(block_start, latest_block + 1, batch_size):
            end = min(start + batch_size - 1, latest_block)
            queue.put((start, end))
            
    def manage_threads(self, block_range_queue):
        thread_list = []
        for rpc_endpoint in self.rpc_list:
            t = Thread(target=self.process_block_ranges, args=(block_range_queue, rpc_endpoint))
            t.start()
            thread_list.append(t)

        # Wait for all threads to complete
        for thread in thread_list:
            thread.join()

    def process_block_ranges(self, block_range_queue, rpc_endpoint):
        retry_limit = 3
        while not block_range_queue.empty():
            block_start, block_end = block_range_queue.get()
            attempt = 0
            while attempt < retry_limit:
                try:
                    self.fetch_and_process_range(block_start, block_end, self.chain, self.table_name, self.bucket_name, self.db_connector, rpc_endpoint)
                    break
                except Exception as e:
                    attempt += 1
                    print(f"Retry {attempt} for range {block_start}-{block_end} failed: {e}")
                    time.sleep(5)
            
            if attempt == retry_limit:
                print(f"Max retries reached for range {block_start}-{block_end}. Re-queuing for another try.")
                time.sleep(10)
                block_range_queue.put((block_start, block_end))
                   
    def fetch_data_for_range(self, block_start, block_end, preferred_endpoint=None):
        df = pd.DataFrame()
        for block_number in range(block_start, block_end + 1):
            #print(f"Fetching data for block {block_number}")
            block_df = self.retrieve_block_data(block_number, preferred_endpoint)
            df = pd.concat([df, block_df], ignore_index=True)
        return df

    def request_rpc(self, payload, headers, preferred_endpoint=None):
        # Try the endpoint this worker thread was assigned first, so a single dead
        # RPC doesn't get hammered on every call from every thread - each thread
        # only falls through to the rest of the list when its own pick is down.
        if preferred_endpoint and preferred_endpoint in self.rpc_list:
            ordered_endpoints = [preferred_endpoint] + [
                rpc for rpc in self.rpc_list if rpc != preferred_endpoint
            ]
        else:
            ordered_endpoints = self.rpc_list

        for rpc_endpoint in ordered_endpoints:
            try:
                response = requests.post(rpc_endpoint, json=payload, headers=headers, timeout=10)
                if response.status_code == 200:
                    response_json = response.json()
                    return response_json
                else:
                    print(f"Response from {rpc_endpoint} returned status code {response.status_code}")
            except ValueError:
                print(f"Failed to decode JSON from {rpc_endpoint}")
            except Exception as e:
                print(f"RPC failed at {rpc_endpoint} with error: {e}")
        print("All RPC endpoints failed.")
        return None

    def get_latest_block(self):
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "header",
            "params": {}
        }
        response = self.request_rpc(payload, headers)
        if response and 'result' in response and 'header' in response['result']:
            block_number = response['result']['header']['height']
            return block_number
        print("Failed to retrieve the latest block number.")
        return None

    def retrieve_block_data(self, block_number, preferred_endpoint=None):
        df = pd.DataFrame()
        page = 1
        total_tx_count = 0  # Track total transactions
        all_txs = []  # Store all transactions

        while True:
            tx_search = self.fetch_block_transaction_details(block_number, page, preferred_endpoint)

            # Handle case where no transactions are found or the RPC request fails
            if not tx_search or 'result' not in tx_search or 'txs' not in tx_search['result']:
                print(f"Failed to fetch transactions for block {block_number}, page {page}. Stopping.")
                break

            txs = tx_search['result']['txs']
            tx_count = len(txs)
            total_tx_count += tx_count
            all_txs.extend(txs)  # Store transactions from each page

            df = pd.concat([df, self.prep_dataframe_celestia(tx_search, preferred_endpoint)], ignore_index=True)

            # Stop fetching pages if fewer than 100 transactions are returned
            if tx_count < 100:
                break

            page += 1  # Increment to fetch the next page

        print(f"Total Transactions for Block {block_number}: {total_tx_count}")

        if not df.empty:
            df = df.astype("object").where(pd.notna(df), None)
            json_columns = ['blob_sizes', 'namespaces']
            for col in json_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else json.dumps(None))
                else:
                    df[col] = json.dumps(None)
        else:
            print(f"No transactions found for block number {block_number}")
        
        return df
     
    def get_block_timestamp(self, block_number, preferred_endpoint=None):
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "method": "block",
            "params": [str(block_number)],
            "id": 1
        }
        response = self.request_rpc(payload, headers, preferred_endpoint)
        if response and 'result' in response and 'block' in response['result'] and 'header' in response['result']['block']:
            return response['result']['block']['header']['time']
        print(f"Failed to fetch block timestamp for block {block_number}.")
        return None

    def fetch_block_transaction_details(self, block_number, page=1, preferred_endpoint=None):
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "method": "tx_search",
            "params": {
                "query": f"tx.height={str(block_number)}",
                "prove": True,
                "page": str(page),
                "per_page": "100",
                "order_by": "asc"
            },
            "id": 1
        }

        tx_search = self.request_rpc(payload, headers, preferred_endpoint)
        if tx_search and 'result' in tx_search and 'txs' in tx_search['result']:
            return tx_search
        else:
            print(f"Failed to fetch transaction details for block {block_number}.")
            return False, {"error": "No transactions found or RPC request failed"}
    
    def prep_dataframe_celestia(self, tx, preferred_endpoint=None):
        if tx['result']['txs'] == None or tx['result']['txs'] == []:
            print('No transactions found in this block!')
            return pd.DataFrame()

        data = []
        txs = tx['result']['txs']
        block = txs[0]['height']
        timestamp = self.get_block_timestamp(block, preferred_endpoint)
        for trx in txs:
            decoded_trx = decode_base64(trx)
            row = {}
            row['block_timestamp'] = timestamp
            row['block_number'] = block
            
            # Format tx_hash for bytea storage in PostgreSQL
            if trx['hash'].startswith('0x'):
                row['tx_hash'] = '\\x' + trx['hash'][2:]  # Remove '0x' and prepend '\\x'
            else:
                row['tx_hash'] = '\\x' + trx['hash']  # Prepend '\\x' directly if there's no '0x'
            
            row['gas_wanted'] = safe_int_conversion(trx['tx_result']['gas_wanted'])
            row['gas_used'] = safe_int_conversion(trx['tx_result']['gas_used'])
            attributes = [i['attributes'] for i in decoded_trx['tx_result']['events']]
            for a in attributes:
                for attr in a:
                    key = attr['key']
                    value = attr['value']
                    if key in ['spender', 'sender', 'receiver', 'recipient']:
                        row[key] = value
                    elif key == 'acc_seq':
                        # Check if there is a '/' and split to get the number after it
                        if '/' in value:
                            row[key] = value.split('/')[1]
                        else:
                            row[key] = value
                    elif key == 'fee':
                        if value is not None:
                            row[key] = safe_fee_conversion(value)
                            row['fee_payer'] = a[1]['value']
                        else:
                            print(f"Warning: 'value' is None for key 'fee' in attributes {attributes}")
                            row[key] = 0
                    elif key == 'action':
                        row[key] = value[1:]
                    elif key == 'signature':
                        row[key] = value
                    elif key == 'blob_sizes':
                        row[key] = safe_list_conversion(value)
                        row['namespaces'] = [i[1:-1] for i in a[1]['value'][1:-1].split(',')]
                        row['signer'] = a[2]['value'][1:-1]

            data.append(row)

        return pd.DataFrame(data)

    def fetch_and_process_range(self, current_start, current_end, chain, table_name, bucket_name, db_connector, preferred_endpoint=None):
        base_wait_time = 5   # Base wait time in seconds
        print(f"...processing blocks {current_start} to {current_end}...")
        while True:
            try:
                # Fetching Celestia block data for the specified range
                df = self.fetch_data_for_range(current_start, current_end, preferred_endpoint)

                # Check if df is None or empty, and return early without further processing.
                if df is None or df.empty:
                    print(f"Skipping blocks {current_start} to {current_end} due to no data.")
                    return

                # Save data to GCS
                # save_data_for_range(df, current_start, current_end, chain, bucket_name)

                # Remove duplicates and set index
                df.drop_duplicates(subset=['tx_hash'], inplace=True)
                df.set_index('tx_hash', inplace=True)
                df.index.name = 'tx_hash'
                if 'signer' not in df.columns:
                    df['signer'] = None
                    
                # Replace invalid values with None for PostgreSQL compatibility
                df.replace({pd.NA: None, 'null': None, 'None': None, 'nan': None, float('nan'): None}, inplace=True)

                # Upsert data into the database
                try:
                    db_connector.upsert_table(table_name, df, if_exists='update')
                    print(f"...data inserted for blocks {current_start} to {current_end} successfully. Uploaded rows: {df.shape[0]}.")
                except Exception as e:
                    print(f"Error inserting data for blocks {current_start} to {current_end}: {e}")
                    raise e
                break  # Break out of the loop on successful execution

            except Exception as e:
                print(f"Error processing blocks {current_start} to {current_end}: {e}")
                base_wait_time = handle_retry_exception(current_start, current_end, base_wait_time, preferred_endpoint or self.rpc_list[0])

    def process_missing_blocks(self, missing_block_ranges, batch_size):
        # Convert list of ranges into a queue of block ranges for batch processing
        block_range_queue = Queue()
        for start, end in missing_block_ranges:
            for batch_start in range(start, end + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, end)
                block_range_queue.put((batch_start, batch_end))
        print(f"Enqueued {block_range_queue.qsize()} block ranges for processing.")

        # Process the enqueued block ranges using multiple threads
        self.manage_threads(block_range_queue)
        print("Finished processing all missing block batches.")
        
    def backfill_missing_blocks(self, start_block, end_block, batch_size):
        missing_block_ranges = check_and_record_missing_block_ranges(self.db_connector, self.table_name, start_block, end_block)
        self.process_missing_blocks(missing_block_ranges, batch_size)
        
def safe_int_conversion(value_str, default=0):
    """Safely convert a string to integer, handling Unicode characters"""
    if not isinstance(value_str, str):
        return default
    
    # Check if string contains only ASCII digits (and optionally negative sign)
    if value_str.lstrip('-').isdigit():
        try:
            return int(value_str)
        except ValueError:
            return default
    
    # If string contains non-ASCII characters, log and return default
    try:
        # Check if string is ASCII
        value_str.encode('ascii')
        # If it's ASCII but still not convertible, try int conversion
        return int(value_str)
    except (UnicodeEncodeError, ValueError) as e:
        print(f"Warning: Cannot convert '{repr(value_str)}' to integer, contains non-ASCII characters or invalid format. Using default value {default}. Error: {e}")
        return default

def safe_fee_conversion(value_str, default=0):
    """Safely convert a fee string (removing 'utia') to integer"""
    if not isinstance(value_str, str):
        return default
    
    try:
        cleaned_value = value_str.replace('utia', '')
        return safe_int_conversion(cleaned_value, default)
    except Exception as e:
        print(f"Warning: Cannot process fee value '{repr(value_str)}'. Using default value {default}. Error: {e}")
        return default

def safe_list_conversion(value_str, default=None):
    """Safely convert a string like '[1,2,3]' to list of integers"""
    if default is None:
        default = []
    
    if not isinstance(value_str, str):
        return default
    
    try:
        if value_str.startswith('[') and value_str.endswith(']'):
            inner_value = value_str[1:-1].strip()
            if not inner_value:  # Empty list
                return []
            
            result = []
            for item in inner_value.split(','):
                item = item.strip()
                if item:
                    converted = safe_int_conversion(item, None)
                    if converted is not None:
                        result.append(converted)
                    else:
                        print(f"Warning: Cannot convert list item '{repr(item)}' to integer, skipping")
            return result
        else:
            print(f"Warning: List format not recognized for '{repr(value_str)}', expected '[...]' format")
            return default
    except Exception as e:
        print(f"Warning: Cannot process list value '{repr(value_str)}'. Using default value {default}. Error: {e}")
        return default

def decode_base64(element):
    if isinstance(element, dict):
        new_element = {}
        for key, value in element.items():
            if key == 'height':
                # Validate and convert height to an integer if possible
                try:
                    new_element[key] = int(value)
                except ValueError:
                    # If conversion fails, set to a default value or handle appropriately
                    new_element[key] = None
            else:
                new_element[key] = decode_base64(value)
        return new_element
    elif isinstance(element, list):
        return [decode_base64(item) for item in element]
    elif isinstance(element, str):
        try:
            decoded_bytes = base64.b64decode(element, validate=True)
            decoded_str = decoded_bytes.decode('utf-8')
            return decoded_str
        except Exception:
            return element
    else:
        return element

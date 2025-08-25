import time

from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.adapters.rpc_funcs.funcs_backfill import check_and_record_missing_block_ranges, find_first_block_of_day, find_last_block_of_day, date_to_unix_timestamp
from queue import Queue, Empty
from threading import Thread, Lock
from src.adapters.rpc_funcs.utils import Web3CC, connect_to_gcs, check_db_connection, check_gcs_connection, get_latest_block, connect_to_node, fetch_and_process_range
from datetime import datetime


class NodeAdapter(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        """
        Initializes the NodeAdapter class with the given RPC configurations and database connector.

        Args:
            adapter_params (dict): Adapter parameters, including RPC configurations and chain.
            db_connector: Database connection object for interaction with the database.

        Raises:
            ValueError: If no RPC configurations are provided.
            ConnectionError: If connection to any RPC node fails.
        """
        super().__init__("RPC-Raw", adapter_params, db_connector)
        
        self.rpc_configs = adapter_params.get('rpc_configs', [])
        if not self.rpc_configs:
            raise ValueError("No RPC configurations provided.")
        
        self.rpc_stats = {rpc_config['url']: {'rows_loaded': 0, 'time_per_block': []} for rpc_config in self.rpc_configs}

        self.active_rpcs = set()  # Keep track of active RPC configurations
        self.rpc_timeouts = {}  # Track processing start times for timeout detection

        self.chain = adapter_params['chain']
        self.table_name = f'{self.chain}_tx'
        
        # Try to initialize Web3 connection with the provided RPC configs
        self.w3 = None
        for rpc_config in self.rpc_configs:
            try:
                self.w3 = Web3CC(rpc_config)
                print(f"Connected to RPC URL: {rpc_config['url']}")
                break
            except Exception as e:
                print(f"Failed to connect to RPC URL: {rpc_config['url']} with error: {e}")
        
        if self.w3 is None:
            raise ConnectionError("Failed to connect to any provided RPC node.")
        
        # Initialize GCS connection
        self.gcs_connection, self.bucket_name = connect_to_gcs()
            
    def extract_raw(self, load_params:dict):
        """
        Extracts raw transaction data by starting from a specific block and batch size.

        Args:
            load_params (dict): Dictionary containing block start and batch size parameters.

        Prints a message upon successful completion.
        """
        self.block_start = load_params['block_start']
        self.batch_size = load_params['batch_size']
        self.run(self.block_start, self.batch_size)
        print(f"FINISHED loading raw tx data for {self.chain}.")

    def update_rpc_stats(self, rpc_url, rows_loaded, time_taken):
        """
        Updates the statistics for a specific RPC URL.

        Args:
            rpc_url (str): The RPC URL to update stats for.
            rows_loaded (int): Number of rows loaded during the process.
            time_taken (float): Time taken to process the block range.
        """
        if rows_loaded is None:
            rows_loaded = 0
        if rpc_url in self.rpc_stats:
            self.rpc_stats[rpc_url]['rows_loaded'] += rows_loaded
            self.rpc_stats[rpc_url]['time_per_block'].append(time_taken)

    def log_stats(self):
        """
        Logs the collected stats for each RPC, including the number of workers and TPS (transactions per second).
        """
        print("===== RPC Statistics =====")
        for rpc_url, stats in self.rpc_stats.items():
            # Retrieve the number of workers from rpc_configs
            workers = next((rpc['workers'] for rpc in self.rpc_configs if rpc['url'] == rpc_url), None)
            
            # Calculate total time and TPS
            total_time = sum(stats['time_per_block'])
            avg_time = total_time / len(stats['time_per_block']) if stats['time_per_block'] else 0
            tps = stats['rows_loaded'] / total_time if total_time > 0 else 0
            
            # Log the stats
            print(f"RPC URL: {rpc_url}")
            print(f"  - Total Rows Loaded: {stats['rows_loaded']}")
            print(f"  - Average Time Per Block Range: {avg_time:.2f} seconds")
            print(f"  - Number of Workers: {workers}")
            print(f"  - Transactions Per Second (TPS): {tps:.2f}")
        print("==========================")
    
    def check_and_kick_slow_rpcs(self, rpc_errors, error_lock):
        """
        Checks for RPCs that are taking too long (>10 minutes) for a single block range.
        Kicks them out if there are other active RPCs available.
        
        Args:
            rpc_errors (dict): Dictionary tracking errors for each RPC configuration.
            error_lock (Lock): Thread lock to synchronize access to rpc_errors.
        """
        current_time = time.time()
        timeout_threshold = 600  # 10 minutes in seconds
        rpcs_to_kick = []
        
        # Check each RPC for timeouts
        for rpc_url, start_times in self.rpc_timeouts.items():
            if rpc_url not in self.active_rpcs:
                continue
                
            # Check if any block range has been processing for more than 10 minutes
            for block_range, start_time in start_times.items():
                if current_time - start_time > timeout_threshold:
                    # Only kick if there are other active RPCs
                    if len(self.active_rpcs) > 1:
                        rpcs_to_kick.append((rpc_url, block_range))
                        print(f"TIMEOUT DETECTED: {rpc_url} has been processing {block_range} for {(current_time - start_time):.1f}s (>{timeout_threshold}s)")
                    else:
                        print(f"TIMEOUT WARNING: {rpc_url} is slow ({(current_time - start_time):.1f}s) but it's the only active RPC")
        
        # Kick the slow RPCs
        with error_lock:
            for rpc_url, block_range in rpcs_to_kick:
                if rpc_url in self.active_rpcs:
                    print(f"KICKING SLOW RPC: Removing {rpc_url} due to timeout on {block_range}")
                    self.active_rpcs.discard(rpc_url)
                    # Remove from rpc_configs to prevent restart
                    self.rpc_configs = [rpc for rpc in self.rpc_configs if rpc['url'] != rpc_url]
                    # Clean up timeout tracking for this RPC
                    if rpc_url in self.rpc_timeouts:
                        del self.rpc_timeouts[rpc_url]
                    # Increment error count to ensure proper tracking
                    rpc_errors[rpc_url] = rpc_errors.get(rpc_url, 0) + 1000  # Large number to ensure removal

    def run(self, block_start, batch_size):
        """
        Runs the transaction extraction process, checking connections to the database and GCS.
        Fetches the latest block and enqueues block ranges for processing.

        Args:
            block_start (int/str): The block number to start processing, or 'auto' to start from the last processed block.
            batch_size (int): Number of blocks to process per batch.

        Raises:
            ConnectionError: If database or GCS connection is not established.
            ValueError: If the start block is higher than the latest block.
        """
        if not check_db_connection(self.db_connector):
            raise ConnectionError("Database is not connected.")
        else:
            print("Successfully connected to database.")

        if not check_gcs_connection(self.gcs_connection):
            raise ConnectionError("GCS is not connected.")
        else:
            print("Successfully connected to GCS.")

        latest_block = get_latest_block(self.w3)
        if latest_block is None:
            print("Could not fetch the latest block.")
            raise ValueError("Could not fetch the latest block.")

        if block_start == 'auto':
            block_start = self.db_connector.get_max_block(self.table_name)  
        else:
            block_start = int(block_start)

        if block_start > latest_block:
            raise ValueError("The start block cannot be higher than the latest block.")

        print(f"Running with start block {block_start} and latest block {latest_block}")

        # Initialize the block range queue
        block_range_queue = Queue()
        self.enqueue_block_ranges(block_start, latest_block, batch_size, block_range_queue)
        print(f"Enqueued {block_range_queue.qsize()} block ranges.")
        self.manage_threads(block_range_queue)     

    def enqueue_block_ranges(self, block_start, block_end, batch_size, queue):
        """
        Enqueues block ranges in the provided queue for parallel processing by worker threads.

        Args:
            block_start (int): Starting block number.
            block_end (int): Ending block number.
            batch_size (int): Size of block batches to enqueue.
            queue (Queue): The queue to enqueue block ranges.
        """
        for start in range(block_start, block_end + 1, batch_size):
            end = min(start + batch_size - 1, block_end)
            queue.put((start, end))

    def manage_threads(self, block_range_queue):
        """
        Manages worker threads to process the block ranges in parallel.
        Tracks and manages RPC configuration errors and thread lifecycle.

        Args:
            block_range_queue (Queue): Queue containing block ranges for processing.

        Starts the worker threads and a monitoring thread to oversee the process.
        """
        threads = []
        rpc_errors = {}  # Track errors for each RPC configuration
        error_lock = Lock()
        
        for rpc_config in self.rpc_configs:
            rpc_errors[rpc_config['url']] = 0
            self.active_rpcs.add(rpc_config['url'])  # Mark as active
            thread = Thread(target=lambda rpc=rpc_config: self.process_rpc_config(
                rpc, block_range_queue, rpc_errors, error_lock))
            threads.append((rpc_config['url'], thread))
            thread.start()
            print(f"Started thread for {rpc_config['url']}")

        # Start the monitoring thread with access to RPC configurations
        monitor_thread = Thread(target=self.monitor_workers, args=(
            threads, block_range_queue, self.rpc_configs, rpc_errors, error_lock))
        monitor_thread.start()
        print("Started monitoring thread.")
        
        monitor_thread.join()
        
        print("All worker and monitoring threads have completed.")

    def monitor_workers(self, threads, block_range_queue, rpc_configs, rpc_errors, error_lock):
        """
        Monitors active worker threads and block range processing, restarting workers if necessary.
        Ensures that workers continue processing as long as there are tasks.

        Args:
            threads (list): List of active worker threads.
            block_range_queue (Queue): Queue of block ranges for processing.
            rpc_configs (list): List of RPC configurations.
            rpc_errors (dict): Dictionary tracking errors per RPC configuration.
            error_lock (Lock): Thread lock to manage concurrent access to the error dictionary.
        """
        additional_threads = []
        while True:
            #active_threads = [thread for _, thread in threads if thread.is_alive()]
            active_threads = [(rpc_url, thread) for rpc_url, thread in threads if thread.is_alive()]
            active_rpc_urls = [rpc_url for rpc_url, _ in active_threads]
            active = bool(active_threads)

            # Check if the block range queue is empty and no threads are active
            if block_range_queue.empty() and not active:
                print("DONE: All workers have stopped and the queue is empty.")
                break
            
            # Check if there are no more block ranges to process, but threads are still active
            if block_range_queue.qsize() == 0 and active:
                combined_rpc_urls = ", ".join(active_rpc_urls)
                print(f"...no more block ranges to process. Waiting for workers to finish. Active RPCs: {combined_rpc_urls}")
            else:
                print(f"====> Block range queue size: {block_range_queue.qsize()}. #Active threads: {len(active_threads)}")
                
            if not block_range_queue.empty() and not active:
                print("Detected unfinished tasks with no active workers. Restarting worker.")

                # Check if there are any active RPCs
                active_rpcs = [rpc_config for rpc_config in rpc_configs if rpc_config['url'] in self.active_rpcs]
                if not active_rpcs:
                    raise Exception("No active RPCs available. Stopping the DAG.")

                # Restart workers only for active RPCs
                for rpc_config in active_rpcs:
                    print(f"Restarting workers for RPC URL: {rpc_config['url']}")
                    new_thread = Thread(target=lambda rpc=rpc_config: self.process_rpc_config(
                        rpc, block_range_queue, rpc_errors, error_lock))
                    threads.append((rpc_config['url'], new_thread))
                    additional_threads.append(new_thread)
                    new_thread.start()
                    break

            # Check for slow RPCs and kick them if necessary
            self.check_and_kick_slow_rpcs(rpc_errors, error_lock)
            
            time.sleep(5)  # Sleep to avoid high CPU usage

        # Join all initial threads
        for _, thread in threads:
            thread.join()
            print(f"Thread for RPC URL has completed.")

        # Join all additional threads if any were started
        for thread in additional_threads:
            thread.join()
            print("Additional worker thread has completed.")

        print("All worker and monitoring threads have completed.")
            
    def process_rpc_config(self, rpc_config, block_range_queue, rpc_errors, error_lock):
        """
        Processes a block range using a specific RPC configuration.
        Starts worker threads to handle block ranges and retries on RPC connection failure.

        Args:
            rpc_config (dict): RPC configuration used to connect to the node.
            block_range_queue (Queue): Queue containing block ranges to process.
            rpc_errors (dict): Dictionary tracking errors for each RPC configuration.
            error_lock (Lock): Thread lock to synchronize access to rpc_errors.
        """
        max_retries = 3
        retry_delay = 10
        attempt = 0

        # Try to connect to the node, with retries
        node_connection = None
        while attempt < max_retries:
            try:
                #print(f"Attempting to connect to {rpc_config['url']}, attempt {attempt + 1}")
                node_connection = connect_to_node(rpc_config)
                print(f"Successfully connected to {rpc_config['url']}")
                break
            except Exception as e:
                #print(f"Failed to connect to {rpc_config['url']}: {e}")
                attempt += 1
                if attempt >= max_retries:
                    #print(f"Failed to connect to {rpc_config['url']} after {max_retries} attempts. Skipping this RPC.")
                    return
                #print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

        if not node_connection:
            return  # If the connection failed after retries, exit the function.

        workers = []
        for _ in range(rpc_config['workers']):
            worker = Thread(target=self.worker_task, args=(rpc_config, node_connection, block_range_queue, rpc_errors, error_lock))
            workers.append(worker)
            worker.start()

        for worker in workers:
            worker.join()
            if not worker.is_alive():
                print(f"Worker for {rpc_config['url']} has stopped.")
            
    def worker_task(self, rpc_config, node_connection, block_range_queue, rpc_errors, error_lock):
        """
        Performs the task of fetching and processing a block range using the specified RPC configuration.
        Requeues the block range if processing fails and tracks errors.

        Args:
            rpc_config (dict): RPC configuration for connecting to the blockchain node.
            node_connection: The node connection object.
            block_range_queue (Queue): Queue of block ranges to process.
            rpc_errors (dict): Dictionary to track RPC errors.
            error_lock (Lock): Lock for managing concurrent error tracking.
        """
        while rpc_config['url'] in self.active_rpcs and not block_range_queue.empty():
            block_range = None
            block_range_key = None
            rpc_url = rpc_config['url']
            
            try:
                block_range = block_range_queue.get(timeout=5)
                block_range_key = f"{block_range[0]}-{block_range[1]}"
                
                print(f"...processing block range {block_range_key} from {rpc_url}")

                # Track start time for timeout detection
                start_time = time.time()
                if rpc_url not in self.rpc_timeouts:
                    self.rpc_timeouts[rpc_url] = {}
                self.rpc_timeouts[rpc_url][block_range_key] = start_time

                # Fetch and process the block range
                rows_loaded = fetch_and_process_range(
                    block_range[0], block_range[1], self.chain, node_connection,
                    self.table_name, self.bucket_name,
                    self.db_connector, rpc_url
                )

                # Calculate time taken for this block range
                time_taken = time.time() - start_time

                # Clean up timeout tracking for this block range
                if rpc_url in self.rpc_timeouts and block_range_key in self.rpc_timeouts[rpc_url]:
                    del self.rpc_timeouts[rpc_url][block_range_key]

                # Update RPC stats
                self.update_rpc_stats(rpc_url, rows_loaded, time_taken)

                print(f"SUCCESS: Processed block range {block_range_key} from {rpc_url}. "
                    f"Rows loaded: {rows_loaded}, Time taken: {time_taken:.2f}s")

            except Empty:
                print("DONE: no more blocks to process. Worker is shutting down.")
                return
            except Exception as e:
                # Clean up timeout tracking for this block range on error
                if block_range and rpc_url in self.rpc_timeouts and block_range_key in self.rpc_timeouts[rpc_url]:
                    del self.rpc_timeouts[rpc_url][block_range_key]
                
                with error_lock:
                    rpc_errors[rpc_config['url']] += 1  # Increment error count
                    # Check immediately if the RPC should be removed
                    if rpc_errors[rpc_config['url']] >= len([rpc['workers'] for rpc in self.rpc_configs if rpc['url'] == rpc_config['url']]):
                        print(f"All workers for {rpc_config['url']} failed. Removing this RPC from rotation.")
                        self.rpc_configs = [rpc for rpc in self.rpc_configs if rpc['url'] != rpc_config['url']]
                        self.active_rpcs.discard(rpc_config['url'])
                        # Clean up timeout tracking for this RPC
                        if rpc_config['url'] in self.rpc_timeouts:
                            del self.rpc_timeouts[rpc_config['url']]
                            
                if block_range:
                    print(f"ERROR: for {rpc_config['url']} on block range {block_range[0]}-{block_range[1]}: {e}")
                    block_range_queue.put(block_range)  # Re-queue the failed block range
                    print(f"RE-QUEUED: block range {block_range[0]}-{block_range[1]}")
                break
            finally:
                if block_range:
                    block_range_queue.task_done()

    def process_missing_blocks(self, missing_block_ranges, batch_size):
        """
        Processes missing block ranges by enqueuing them and managing threads for backfill.

        Args:
            missing_block_ranges (list): List of tuples representing missing block ranges.
            batch_size (int): Size of block batches to process at once.
        """
        block_range_queue = Queue()
        for start, end in missing_block_ranges:
            self.enqueue_block_ranges(start, end, batch_size, block_range_queue)
        self.manage_threads(block_range_queue)
    
    def fetch_block_transaction_count(self, w3, block_num):
        """
        Fetches the total number of transactions in a specific block.

        Args:
            w3: Web3 instance used to interact with the blockchain.
            block_num (int): Block number for which transaction count is retrieved.

        Returns:
            int: The number of transactions in the block.
        """
        block = w3.eth.get_block(block_num, full_transactions=False)
        return len(block['transactions'])

    def check_range_has_transactions(self, start, end):
        """
        Efficiently checks if a block range contains any transactions.
        For single blocks, checks directly. For ranges, samples blocks to determine if likely empty.

        Args:
            start (int): Starting block number.
            end (int): Ending block number.

        Returns:
            bool: True if the range likely contains transactions, False if likely empty.
        """
        if start == end:
            # Single block - check directly
            transaction_count = self.fetch_block_transaction_count(self.w3, start)
            return transaction_count > 0
        else:
            # Multi-block range - sample blocks to check
            range_size = end - start + 1
            if range_size <= 5:
                # Small range - check all blocks
                for block_num in range(start, end + 1):
                    if self.fetch_block_transaction_count(self.w3, block_num) > 0:
                        return True
                return False
            else:
                # Large range - sample up to 5 blocks strategically
                sample_blocks = [
                    start,                           # First block
                    start + range_size // 4,         # 25% through
                    start + range_size // 2,         # Middle
                    start + 3 * range_size // 4,     # 75% through
                    end                              # Last block
                ]
                
                # Remove duplicates and ensure all are within range
                sample_blocks = list(set([b for b in sample_blocks if start <= b <= end]))
                
                for block_num in sample_blocks:
                    try:
                        if self.fetch_block_transaction_count(self.w3, block_num) > 0:
                            return True
                    except Exception as e:
                        print(f"Warning: Could not check block {block_num}: {e}")
                        # If we can't check, assume it has transactions to be safe
                        return True
                
                return False

    def backfill_missing_blocks(self, start_block, end_block, batch_size):
        """
        Identifies and backfills missing blocks by processing the appropriate block ranges.

        Args:
            start_block (int): The starting block number for backfill.
            end_block (int): The ending block number for backfill.
            batch_size (int): Number of blocks to process per batch.

        Filters out block ranges with no transactions.
        """
        missing_block_ranges = check_and_record_missing_block_ranges(self.db_connector, self.table_name, start_block, end_block)
        if not missing_block_ranges:
            print("No missing block ranges found.")
            return
        print(f"Found {len(missing_block_ranges)} missing block ranges.")
        print("Filtering out ranges with 0 transactions...")
        filtered_ranges = []
        
        for start, end in missing_block_ranges:
            has_transactions = self.check_range_has_transactions(start, end)
            if has_transactions:
                filtered_ranges.append((start, end))
            else:
                print(f"...skipping empty range {start}-{end}")

        if len(filtered_ranges) == 0:
            print("No missing block ranges with transactions found.")
            return
        
        print(f"After filtering: {len(filtered_ranges)} ranges with transactions to process.")
        print("Backfilling missing blocks")
        print("Missing block ranges:")
        for start, end in filtered_ranges:
            print(f"{start}-{end}")
        self.process_missing_blocks(filtered_ranges, batch_size)
            
    def backfill_date_range(self, start_date, end_date, batch_size):
        """
        Backfills data for a specified date range without checking for missing blocks.

        Args:
            start_date (str): The start date for backfill in the format 'YYYY-MM-DD'.
            end_date (str): The end date for backfill in the format 'YYYY-MM-DD'.
            batch_size (int): Number of blocks to process per batch.

        Raises:
            ValueError: If the date range is invalid or blocks cannot be found.
        """
        # Parse and validate dates
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")

        if start_date_obj > end_date_obj:
            raise ValueError("Start date must be earlier than end date.")

        # Convert dates to Unix timestamps
        start_timestamp = date_to_unix_timestamp(
            start_date_obj.year, start_date_obj.month, start_date_obj.day
        )
        end_timestamp = date_to_unix_timestamp(
            end_date_obj.year, end_date_obj.month, end_date_obj.day
        )

        # Find the first and last blocks for the given date range
        print(f"Finding blocks for date range: {start_date} to {end_date}...")
        start_block = find_first_block_of_day(self.w3, start_timestamp)
        end_block = find_last_block_of_day(self.w3, end_timestamp)

        print(f"Start block: {start_block}, End block: {end_block}")

        if start_block > end_block:
            raise ValueError("No blocks found in the specified date range.")

        # Enqueue block ranges and process
        block_range_queue = Queue()
        self.enqueue_block_ranges(start_block, end_block, batch_size, block_range_queue)
        print(f"Enqueued {block_range_queue.qsize()} block ranges for backfill.")
        self.manage_threads(block_range_queue)

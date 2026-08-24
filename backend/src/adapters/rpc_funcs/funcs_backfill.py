import os
from dotenv import load_dotenv
from datetime import datetime
import time
from sqlalchemy import text


# Load environment variables
load_dotenv()

# ------------------ Batch Processing Functions ------------------
def check_and_record_missing_block_ranges(db_connector, table_name, start_block, end_block, block_column="block_number"):
    """
    Checks for missing block ranges in the specified table and records them as ranges.

    Args:
        db_connector: Database connector to execute queries.
        table_name (str): Name of the table to check for missing blocks.
        start_block (int): The starting block number for the check.
        end_block (int): The ending block number for the check.
        block_column (str): Name of the block-number column in the table.

    Returns:
        list: A list of tuples representing missing block ranges (start_block, end_block).
    """
    print(f"Checking and recording missing block ranges for table: {table_name}.{block_column}")

    # Ensure start_block is not less than the smallest block in the database
    smallest_block_query = text(f"SELECT MIN({block_column}) AS min_block FROM {table_name};")
    with db_connector.engine.connect() as connection:
        result = connection.execute(smallest_block_query).fetchone()
        db_min_block = result[0] if result[0] is not None else 0

    if start_block < db_min_block:
        start_block = db_min_block

    print(f"Starting check from block number: {start_block}, up to block number: {end_block}")

    # Adjusted query to start from the specified start block and go up to the specified end block
    query = text(f"""
        WITH RECURSIVE missing_blocks (block_number) AS (
            SELECT {start_block} AS block_number
            UNION ALL
            SELECT block_number + 1
            FROM missing_blocks
            WHERE block_number < {end_block}
        )
        SELECT mb.block_number
        FROM missing_blocks mb
        LEFT JOIN {table_name} t ON mb.block_number = t.{block_column}
        WHERE t.{block_column} IS NULL
        ORDER BY mb.block_number;
    """)

    with db_connector.engine.connect() as connection:
        missing_blocks_result = connection.execute(query).fetchall()

    if not missing_blocks_result:
        print(f"No missing block ranges found for table: {table_name}.")
        return []

    missing_ranges = []
    start_missing_range = None
    previous_block = None

    for row in missing_blocks_result:
        current_block = row[0]
        if start_missing_range is None:
            start_missing_range = current_block

        if previous_block is not None and current_block != previous_block + 1:
            missing_ranges.append((start_missing_range, previous_block))
            start_missing_range = current_block

        previous_block = current_block

    # Add the last range if it exists
    if start_missing_range is not None:
        missing_ranges.append((start_missing_range, previous_block))

    # # Save to JSON file
    # with open(missing_blocks_file, 'w') as file:
    #     json.dump(missing_ranges, file)

    # print(f"Missing block ranges saved to {missing_blocks_file}")
    return missing_ranges

def date_to_unix_timestamp(year, month, day):
    """
    Converts a specific date into a Unix timestamp.

    Args:
        year (int): The year of the date.
        month (int): The month of the date.
        day (int): The day of the date.

    Returns:
        int: The Unix timestamp for the provided date.
    """
    return int(time.mktime(datetime(year, month, day).timetuple()))

def is_block_not_found_error(error):
    """
    Returns True for RPC errors that mean the requested block number does not
    exist on the chain/RPC, not that the RPC is temporarily unavailable.
    """
    error_msg = str(error).lower()
    return (
        ("block" in error_msg and "not found" in error_msg)
        or "header not found" in error_msg
        or "unknown block" in error_msg
    )

def find_nearest_existing_block(w3, block_num, min_block, max_block, max_distance=10000):
    """
    Finds a nearby existing block around a missing block number.

    Some chains expose sparse block-number ranges through their RPCs. A binary
    search by timestamp must be able to step across those gaps without returning
    a boundary that cannot later be fetched.
    """
    max_distance = min(max_distance, max(max_block - min_block, 0))
    for offset in range(1, max_distance + 1):
        for candidate in (block_num + offset, block_num - offset):
            if candidate < min_block or candidate > max_block:
                continue
            try:
                return candidate, w3.eth.get_block(candidate)
            except Exception as e:
                if not is_block_not_found_error(e):
                    raise

    return None, None

def find_first_block_of_day(w3, target_timestamp):
    """
    Finds the first block of the day based on a target timestamp using a binary search.
    Includes error handling for missing blocks during the search.

    Args:
        w3: Web3 instance to interact with the Ethereum blockchain.
        target_timestamp (int): The Unix timestamp of the start of the target day.

    Returns:
        int: The block number corresponding to the first block of the day.
    """
    min_block = 0
    max_block = w3.eth.block_number
    
    def safe_get_block(block_num):
        """Safely get a block, handling cases where the block doesn't exist."""
        try:
            return w3.eth.get_block(block_num)
        except Exception as e:
            if is_block_not_found_error(e):
                print(f"Block {block_num} (0x{block_num:x}) not found: {e}")
                return None
            raise
    
    while min_block <= max_block:
        mid_block = (min_block + max_block) // 2
        
        # Try to get the block, handle missing blocks
        block = safe_get_block(mid_block)
        if block is None:
            missing_mid_block = mid_block
            mid_block, block = find_nearest_existing_block(w3, missing_mid_block, min_block, max_block)
            if block is None:
                raise ValueError(f"Could not find any valid blocks around {missing_mid_block} (0x{missing_mid_block:x})")
        
        mid_block_timestamp = block.timestamp

        if mid_block_timestamp < target_timestamp:
            min_block = mid_block + 1
        elif mid_block_timestamp > target_timestamp:
            max_block = mid_block - 1
        else:
            # Find the first block of the day
            while mid_block > 0:
                prev_block = safe_get_block(mid_block - 1)
                if prev_block is None or prev_block.timestamp < target_timestamp:
                    break
                mid_block -= 1
            return mid_block

    return min_block

def find_last_block_of_day(w3, target_timestamp):
    """
    Finds the last block of the day based on a target timestamp using a binary search.
    Includes error handling for missing blocks during the search.

    Args:
        w3: Web3 instance to interact with the Ethereum blockchain.
        target_timestamp (int): The Unix timestamp of the start of the target day.

    Returns:
        int: The block number corresponding to the last block of the day.
    """
    # Set the end of the day timestamp (23:59:59 of the target day)
    end_of_day_timestamp = target_timestamp + 86400 - 1  # 86400 seconds in a day, -1 to stay in the same day

    def safe_get_block(block_num):
        """Safely get a block, handling cases where the block doesn't exist."""
        try:
            return w3.eth.get_block(block_num)
        except Exception as e:
            if is_block_not_found_error(e):
                print(f"Block {block_num} (0x{block_num:x}) not found: {e}")
                return None
            raise

    min_block = 0
    max_block = w3.eth.block_number
    while min_block <= max_block:
        mid_block = (min_block + max_block) // 2
        
        # Try to get the block, handle missing blocks
        block = safe_get_block(mid_block)
        if block is None:
            missing_mid_block = mid_block
            mid_block, block = find_nearest_existing_block(w3, missing_mid_block, min_block, max_block)
            if block is None:
                raise ValueError(f"Could not find any valid blocks around {missing_mid_block} (0x{missing_mid_block:x})")
        
        mid_block_timestamp = block.timestamp

        if mid_block_timestamp < end_of_day_timestamp:
            min_block = mid_block + 1
        elif mid_block_timestamp > end_of_day_timestamp:
            max_block = mid_block - 1
        else:
            # Find the last block of the day
            while mid_block < max_block:
                next_block = safe_get_block(mid_block + 1)
                if next_block is None or next_block.timestamp > end_of_day_timestamp:
                    break
                mid_block += 1
            return mid_block

    return max_block  # The last block that is still within the day

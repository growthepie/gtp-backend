import os
import json
import time
import re
from google.cloud import storage
from google.oauth2 import service_account
import io
import numpy as np
from datetime import datetime

def connect_to_gcs():
    """
    Establishes a connection to Google Cloud Storage using credentials from environment variables.
    
    Returns:
        tuple: A tuple containing the GCS client object and the bucket name.

    Raises:
        ConnectionError: If the connection to GCS fails.
    """
    try:
        # Get the GCS bucket name from environment variables
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        
        if not bucket_name:
            raise EnvironmentError("GCS bucket name not found in environment variables.")
        
        # Parse the JSON credentials using the GOOGLE_CREDENTIALS environment variable
        credentials_json = os.getenv('GOOGLE_CREDENTIALS')
        
        # Fix the issue with escaped newlines only if needed
        if credentials_json and '\\\\n' in credentials_json:
            credentials_json = credentials_json.replace('\\\\n', '\\n')
        
        credentials_info = json.loads(credentials_json)
        
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Create a GCS client
        gcs = storage.Client(credentials=credentials)
        
        return gcs, bucket_name
    except Exception as e:
        print("ERROR: An error occurred while connecting to GCS:", str(e))
        raise ConnectionError(f"An error occurred while connecting to GCS: {str(e)}")

def check_gcs_connection(gcs_connection):
    """
    Checks if the connection to GCS is established.
    
    Args:
        gcs_connection: The GCS connection object.

    Returns:
        bool: True if the connection is valid, False otherwise.
    """
    return gcs_connection is not None

def save_data_for_range(df, block_start, block_end, chain, bucket_name):
    """
    Saves the transaction data for a range of blocks to a GCS bucket in parquet format.
    Uses the structure: gcs_bucket_name/{chain_name}/{YYYY-MM-DD}/{file}
    
    Args:
        df (pd.DataFrame): The DataFrame containing transaction data.
        block_start (int): The starting block number.
        block_end (int): The ending block number.
        chain (str): The name of the blockchain chain.
        bucket_name (str): The name of the GCS bucket.
    """
    # Convert any 'object' dtype columns to string
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].apply(str)
            except Exception as e:
                raise e

    # Generate the filename
    filename = f"{chain}_tx_{block_start}_{block_end}.parquet"
    
    # Determine which timestamp column to use
    timestamp_col = None
    
    # List of potential timestamp column names (in order of preference)
    timestamp_candidates = [
        'block_timestamp',
        'block_time', 
        'timestamp',
    ]
    
    # Check for timestamp column (case-insensitive)
    for candidate in timestamp_candidates:
        for col in df.columns:
            if col.lower() == candidate.lower():
                timestamp_col = col
                break
        if timestamp_col:
            break
    
    # Get date_str based on available timestamp column
    if timestamp_col and not df.empty:
        # Use the timestamp from the first row
        block_timestamp = df[timestamp_col].iloc[0]
        
        # Convert timestamp to date format YYYY-MM-DD
        try:
            # Handle different timestamp formats
            if isinstance(block_timestamp, (int, float, np.int64, np.float64)):
                # Unix timestamp (seconds since epoch)
                timestamp_value = float(block_timestamp)
                date_str = datetime.fromtimestamp(timestamp_value).strftime("%Y-%m-%d")
            else:
                # String timestamp or datetime object
                if isinstance(block_timestamp, str):
                    # Try to parse the string timestamp
                    try:
                        date_str = datetime.strptime(block_timestamp, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
                    except ValueError:
                        try:
                            # Try format with microseconds and UTC: '2023-05-23 12:06:15.000000 UTC'
                            date_str = datetime.strptime(block_timestamp, "%Y-%m-%d %H:%M:%S.%f UTC").strftime("%Y-%m-%d")
                        except ValueError:
                            # Try ISO format - normalize fractional seconds to microseconds
                            timestamp_str = block_timestamp.replace('Z', '+00:00')
                            
                            # Handle fractional seconds of varying precision
                            # Pattern: YYYY-MM-DDTHH:MM:SS.fractional+TZ
                            fractional_pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)(\+\d{2}:\d{2})'
                            match = re.match(fractional_pattern, timestamp_str)
                            
                            if match:
                                date_part = match.group(1)
                                fractional_part = match.group(2)
                                timezone_part = match.group(3)
                                
                                # Normalize fractional seconds to exactly 6 digits (microseconds)
                                if len(fractional_part) > 6:
                                    # Truncate to 6 digits (nanoseconds -> microseconds)
                                    fractional_part = fractional_part[:6]
                                elif len(fractional_part) < 6:
                                    # Pad with zeros to reach 6 digits
                                    fractional_part = fractional_part.ljust(6, '0')
                                
                                timestamp_str = f"{date_part}.{fractional_part}{timezone_part}"
                            
                            date_str = datetime.fromisoformat(timestamp_str).strftime("%Y-%m-%d")
                else:
                    # Assume it's already a datetime object
                    date_str = block_timestamp.strftime("%Y-%m-%d")
        except Exception as e:
            # Fallback to current date if there's an error parsing the timestamp
            print(f"Error parsing block timestamp, falling back to current date: {str(e)}")
            date_str = time.strftime("%Y-%m-%d")
    else:
        print(f"No suitable timestamp column found in DataFrame. Available columns: {list(df.columns)}")
        print(f"Searched for timestamp columns: {timestamp_candidates}")
        raise ValueError("No suitable timestamp column found in DataFrame")
    
    # Create GCS file path
    file_key = f"{chain}/{date_str}/{filename}"
    #print(f"Created file path: {file_key}")
    
    # Connect to GCS
    gcs, _ = connect_to_gcs()
    bucket = gcs.bucket(bucket_name)
    
    # Convert DataFrame to parquet and upload to GCS
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    # Upload to GCS
    blob = bucket.blob(file_key)
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    
    #print(f"...saved data to GCS: {file_key}")
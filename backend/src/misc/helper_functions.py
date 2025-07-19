import requests
import time
import sys
import json
import pandas as pd
import unicodedata
from datetime import datetime, timedelta, timezone
import boto3
import os
import eth_utils
import random
import numpy as np
import yaml
from openai import OpenAI
from web3 import Web3

import dotenv
dotenv.load_dotenv()

## API interaction functions
def api_get_call(url, sleeper=1, retries=15, header=None, _remove_control_characters=False, as_json=True, proxy=None):
    retry_counter = 0
    interupt = False

    while True:
        try:
            response = requests.request("GET", url, headers=header, proxies=proxy)
            if response.status_code == 200:
                break
            elif response.status_code == 400:
                print(f"400 error, Bad Request with: {url} and response: {response.text}") 
                return False
            elif response.status_code == 404:
                print(f"404 error, Not Found with: {url} and response: {response.text}")
                return False
            else:
                retry_counter += 1
                if retry_counter <= retries:
                    waiting_time = retry_counter * sleeper
                    print(f"-- Code: {response.status_code} -- sleep for {str(waiting_time)}s then retry API call #{str(retry_counter)} with: {url}")
                    print(response.reason)
                    for i in range(1, waiting_time):
                        time.sleep(1)
                else:
                    print("retrying failed more than " + str(retries) + " times - Canceling")
                    return False
        except KeyboardInterrupt:
            interupt = True
            break        
        except:
            print('request issue, will retry with: ' + url)
            time.sleep(retry_counter * sleeper)
    
    if interupt == True:
        print("Execution ended successfully in api_get_call")
        sys.exit()
    else:
        if _remove_control_characters == True:
            if as_json == True:
                return json.loads(remove_control_characters(response.text))
            else:
                return remove_control_characters(response.text)
        else:
            if as_json == True:
                if response.text == '' or response.text == '[]':
                    print(f"Empty response for {url}")
                    return {}
                return json.loads(response.text)
            else:
                return response.text

def api_post_call(url, payload, sleeper=0.5, retries=15, header=None, _remove_control_characters=False):
    retry_counter = 0
    interupt = False

    while True:
        try:
            response = requests.request("POST", url, data=payload, headers=header)
            if response.status_code == 200:
                break
            else:
                retry_counter += 1
                if retry_counter <= retries:
                    waiting_time = retry_counter ** 2
                    print(f"-- Code: {response.status_code} -- sleep for {str(waiting_time)}s then retry API call #{str(retry_counter)} with: {url}")
                    print(response.reason)
                    for i in range(1, waiting_time):
                        time.sleep(1)
                else:
                    print("retrying failed more than " + str(retries) + " times - start over")
                    return False
        except KeyboardInterrupt:
            interupt = True
            break        
        except:
            print('request issue, will retry with: ' + url)
            time.sleep(retry_counter * sleeper)
    
    if interupt == True:
        print("Execution ended successfully in api_post_call")
        sys.exit()
    else:
        if _remove_control_characters == True:
            return json.loads(remove_control_characters(response.text))
        else:
            return json.loads(response.text)

def remove_control_characters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")

## Adapter preparation functions
## this function checks if origin_keys exist in the projects:AdapterMapping and returns an error if one is missing
def check_projects_to_load(projects, origin_keys):
        if origin_keys is not None:
            for ok in origin_keys:
                ok_existing = False
                for p in projects:
                    if p.origin_key == ok:
                        ok_existing = True
                        break
                if ok_existing == False:
                    print(f'origin_key {ok} does not exist in AdapterMapping OR query doesnt exist.')
                    raise NotImplementedError

## this function checks if query_name exist in the zettablock_queries and returns an error if one is missing
def check_zb_queries_to_load(zb_queries, query_names):
        for qname in query_names:
            qname_existing = False
            for zb in zb_queries:
                if zb.query_name == qname:
                    qname_existing = True
                    break
            if qname_existing == False:
                print(f'query name {qname} does not exist in zettablock_queries.py')
                raise NotImplementedError

## this function filters the projects:AdapterMapping and only returns origin keys defined in origin_keys
def return_projects_to_load(projects, origin_keys):
        if origin_keys is not None:
            projects_to_load = [x for x in projects if x.origin_key in origin_keys]
        else:
            projects_to_load = projects
        return projects_to_load

## this function returns a dataframe for the fact_kpis table
def get_df_kpis():
    return pd.DataFrame(columns=['date', 'metric_key', 'origin_key', 'value'])

## this function returns a dataframe for the fact_kpis table with date column filled out
def get_df_kpis_with_dates(days):
    df = pd.DataFrame(columns=['date', 'metric_key', 'origin_key', 'value'])
    df['date'] = pd.date_range(end=datetime.today().date()-timedelta(days=1), periods=days).tolist()
    return df

## this function upserts a dataframe to the fact_kpis and returns the number of upserted rows
def upsert_to_kpis(df, db_connector):
    tbl_name = 'fact_kpis'
    upserted = db_connector.upsert_table(tbl_name, df)
    return upserted, tbl_name

## this function returns the number of days between today and the last entry in fact_kpis
def get_missing_days_kpis(db_connector, metric_key, origin_key):
    last_date = db_connector.get_max_date(metric_key, origin_key)
    if last_date == None:
        days = 9999
        print(f"...no entry detected in tbl_kpis_daily for metric_key: {metric_key} and origin_key: {origin_key}. Set days to {days}.")
    else:
        delta = datetime.today().date() - last_date
        days = delta.days + 10 #add 5 just for precaution (in case some data was missing etc.)
        print(f"...last entry in tbl_kpis_daily detected for metric_key: {metric_key} and origin_key: {origin_key} is on {last_date}. Set days to {days}.")
    return (days) 

def get_missing_days_blockspace(db_connector, origin_key):
    last_date = db_connector.get_blockspace_max_date(origin_key)
    if last_date == None:
        days = 9999
        print(f"...no blockspace entry detected for origin_key: {origin_key}. Set days to {days}.")
    else:
        delta = datetime.today().date() - last_date
        days = delta.days + 3 #add 3 just for precaution (in case some data was missing etc.)
        print(f"...last blockspace entry for origin_key: {origin_key} is on {last_date}. Set days to {days}.")
    return (days) 

## prepare df for kpis_daily with input df having day and value columns
def prepare_df_kpis(df, metric_key, origin_key):
        df['day'] = df['day'].apply(pd.to_datetime)
        df['date'] = df['day'].dt.date
        df.drop(['day'], axis=1, inplace=True)
        df['metric_key'] = metric_key
        df['origin_key'] = origin_key
        # max_date = df['date'].max()
        # df.drop(df[df.date == max_date].index, inplace=True)
        today = datetime.today().strftime('%Y-%m-%d')
        df.drop(df[df.date == today].index, inplace=True, errors='ignore')
        df.value.fillna(0, inplace=True)
        return df

## convert df address columns to checksummed addresses
def db_addresses_to_checksummed_addresses(df, address_cols):
    for col in address_cols:
        df[col] = df[col].apply(lambda x: eth_utils.to_checksum_address(bytes(x)))
    return df

def string_addresses_to_checksummed_addresses(df, address_cols):
    for col in address_cols:
        ##df[col] = df[col].apply(lambda x: eth_utils.to_checksum_address(str(x)))
        ## this is a workaround for the above line, as it throws an error when the address is not a string
        df[col] = df[col].apply(lambda x: eth_utils.to_checksum_address(str(x)) if x and isinstance(x, str) else x)

    return df

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

    if at_block != 'latest':
        block_number = int(at_block)
        # check if the contract was deployed at the given address
        code = w3.eth.get_code(contract_address, block_identifier=block_number)
        time.sleep(0.1)  # Sleep to avoid rate limiting
        if code == b'':  # Contract not deployed by this block
            print(f"Contract not deployed at address {contract_address} with block {block_number}")
            return None
    else:
        block_number = int(w3.eth.block_number)

    print(f"Calling function {function_name} on contract {contract_address} at block {block_number} with args: {args}")

    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=abi)
        time.sleep(0.1)  # Sleep to avoid rate limiting
        function = getattr(contract.functions, function_name)
        return function(*args).call(block_identifier=block_number)
    except Exception as e:
        print(f"Error calling function {function_name} with args {args} on contract {contract_address} with block_identifier {block_number}: {e}")
        ## print datatypes of all variables
        print(f"contract_address: {type(contract_address)}")
        print(f"abi: {type(abi)}")
        print(f"function_name: {type(function_name)}")
        print(f"at_block: {type(at_block)}")
        ## print all args types
        for i in range(len(args)):
            print(f"args[{i}]: {type(args[i])}")

        raise e
    
def get_erc20_balance(self, w3: Web3, token_contract: str, token_abi: dict, address, at_block='latest'):
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
        print(f"Invalid address: {address}")
        raise ValueError(f"Invalid address: {address}")

    result = call_contract_function(w3, token_contract, token_abi, 'balanceOf', Web3.to_checksum_address(address), at_block=at_block)
    if result is None:  # Check for None to avoid division by zero
        print(f"Error retrieving ERC20 balance for {address} with block {at_block}")
        return None
    
    return result / 10**18

## Some simple Adapter print functions
def clean_params(params:dict):
    if 'api_key' in params:
        params['api_key'] = '***'
    if 'infura_api' in params:
        params['infura_api'] = '***'
    return params

def print_init(name:str, params:dict):
    params = clean_params(params)
    print(f"Adapter {name} initialized with {params}.")

def print_extract(name:str, params:dict, df_shape):
    params = clean_params(params)
    print(f'{name} extract done for {params}. DataFrame shape: {df_shape}')

def print_extract_raw(name:str, df_shape):
    print(f'Extract {name} RAW done. DataFrame shape: {df_shape}')

def print_load(name:str, upserted:int, tbl_name:str):
    print(f'Load {name} done - {upserted} rows upserted in {tbl_name}')

def print_load_raw(name:str, upserted:int, tbl_name:str):
    print(f'Load {name} RAW done - {upserted} rows upserted in {tbl_name}')

def print_orchestration_raw_start(name:str):
    print(f'Orchestration {name} RAW started.')

def print_orchestration_raw_end(name:str):
    print(f'Orchestration {name} RAW finished.')

## Discord functions
def send_discord_message(message, webhook_url=None):
    data = {"content": message}
    if webhook_url is None:
        webhook_url = os.getenv('DISCORD_ALERTS')
    response = requests.post(webhook_url, json=data)

    # Check the response status code
    if response.status_code == 204:
        print("Message sent successfully")
    else:
        print(f"Error sending message: {response.text}")

## Binance functions
def date_string_to_unix(date_string: str) -> int:
    dt = datetime.strptime(date_string, '%Y-%m-%d')
    timestamp = int(dt.timestamp() * 1000)
    return timestamp

def get_binance_ohlc(symbol:str, start_time, end_time, interval='daily'):
    # Set API endpoint and parameters
    endpoint = 'https://api.binance.com/api/v3/klines'
    start = date_string_to_unix(start_time)
    end = date_string_to_unix(end_time)

    if interval == 'daily':
        interval_api = '1d'
        datapoints = (end - start) / (1000 * 60 * 60 * 24)
        second_calc = (1000 * 60 * 60 * 24 * 999)
    elif interval == 'hourly':
        interval_api = '1h'
        datapoints = (end - start) / (1000 * 60 * 60 * 24) * 24
        second_calc = (1000 * 60 * 60 * 24 * (999/24))
    
    dfMain = pd.DataFrame()
    while datapoints > 0:
        print(f'{int(datapoints)} datapoints left and current start time is {int(start)}')
        if start + second_calc > end:
            end_time = end
        else:
            end_time = start + second_calc

        params = {
            'symbol': symbol.upper()+'USDT',
            'interval': interval_api,
            'limit': 1000,
            'startTime': int(start),
            'endTime': int(end_time)
        }

        # Make GET request to API
        response = requests.get(endpoint, params=params)
        data = response.json()
        df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'num_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        dfMain = pd.concat([dfMain, df])

        start = start + second_calc
        
        datapoints -= 1000
        time.sleep(0.5)

    dfMain['Date'] = pd.to_datetime(dfMain['time'], unit='ms')
    dfMain.set_index('Date', inplace=True)
    dfMain = dfMain[['open', 'high', 'low', 'close', 'volume']]
    dfMain.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
    dfMain = dfMain.astype(float)
    return dfMain

## take binance ohlc input and output price_usd in correct db format
def prep_binance_ohlc(df, granularity, origin_key):
    df.reset_index(inplace=True)
    df = df.drop_duplicates(subset='Date', keep='first')

    df = df[['Date', 'Close']]
    df['metric_key'] = 'price_usd'

    df['origin_key'] = origin_key
    df['granularity'] = granularity

    ## rename columns Date to timestamp and Close to value
    df.rename(columns={'Date': 'timestamp', 'Close': 'value'}, inplace=True)

    df.set_index(['timestamp', 'metric_key', 'origin_key', 'granularity'], inplace=True)
    return df

## JSON helper functions
def replace_nan_with_none(obj):
    if isinstance(obj, dict):
        return {k: replace_nan_with_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_with_none(elem) for elem in obj]
    elif isinstance(obj, float) and np.isnan(obj):
        return None
    else:
        return obj

def count_nans_and_log_paths(obj, path=''):
    counter = 0
    paths = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else k
            count, new_paths = count_nans_and_log_paths(v, new_path)
            counter += count
            paths.extend(new_paths)
    elif isinstance(obj, list):
        for i, elem in enumerate(obj):
            new_path = f"{path}[{i}]"
            count, new_paths = count_nans_and_log_paths(elem, new_path)
            counter += count
            paths.extend(new_paths)
    elif isinstance(obj, float) and np.isnan(obj):
        counter += 1
        paths.append(path)
    return counter, paths

def fix_dict_nan(test_dict, dict_name, send_notification=True):
    nan_count, nan_paths = count_nans_and_log_paths(test_dict)
    discord_webhook = os.getenv('DISCORD_TX_CHECKER')

    if nan_count > 0:
        nan_paths_str = '\n'.join(nan_paths)
        if send_notification:
            msg = f"Found {nan_count} NaNs in {dict_name} for the following paths:\n\n{nan_paths_str[:500]}..."
            send_discord_message(msg , discord_webhook)
        test_dict = replace_nan_with_none(test_dict)
        print(f"..WARNING: replaced {nan_count} NaNs in {dict_name}")
    else:
        print(f"..no NaNs found in {dict_name}")
    
    return test_dict


## S3 functions
def empty_cloudfront_cache(distribution_id, path):
        cf = boto3.client('cloudfront')
        print("Creating invalidation for path: " + path)
        time.sleep(random.randint(1, 3))
        res = cf.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                'Paths': {
                    'Quantity': 1,
                    'Items': [
                        path
                    ]
                },
                'CallerReference': str(time.time()).replace(".", "")
            }
        )
        invalidation_id = res['Invalidation']['Id']
        #print("Invalidation created successfully with Id: " + invalidation_id)
        return invalidation_id

def upload_file_to_cf_s3(bucket, path_name, local_path, cf_distribution_id):
    # Initialize S3 client
    s3 = boto3.client("s3")
    # Upload the file to S3
    s3.upload_file(local_path, bucket, path_name)

    print(f'..uploaded to {path_name}')
    empty_cloudfront_cache(cf_distribution_id, f'/{path_name}')


def upload_json_to_cf_s3(bucket, path_name, details_dict, cf_distribution_id, invalidate=True):
    # Convert Dictionary to JSON String
    details_json = json.dumps(details_dict)

    # Upload JSON String to an S3 Object
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket, 
        Key=f'{path_name}.json',
        Body=details_json,
        ContentType='application/json'
    )

    print(f'..uploaded to {path_name}')
    if invalidate:
        empty_cloudfront_cache(cf_distribution_id, f'/{path_name}.json')

def remove_file_from_s3(bucket, path_name):
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    obj = s3.Object(bucket, path_name)
    obj.delete()
    print(f"Deleted {path_name}")

def get_files_df_from_s3(bucket_name, prefix):
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    bucket = s3.Bucket(bucket_name)
    files = []
    for obj in bucket.objects.filter(Prefix=prefix):
        files.append([obj.key, obj.last_modified])

    df = pd.DataFrame(files, columns=['key', 'last_modified'])
    df.sort_values(by='last_modified', ascending=False, inplace=True)
    return df

def upload_image_to_cf_s3(bucket, s3_path, local_path, cf_distribution_id, file_type):
    """
    Uploads an image (PNG or SVG) to an S3 bucket and invalidates the CloudFront cache.
    
    :param bucket: S3 bucket name
    :param s3_path: Destination path in S3 (without extension)
    :param local_path: Local file path (with extension)
    :param cf_distribution_id: CloudFront distribution ID
    :param file_type: Image file type ('png' or 'svg')
    """
    valid_types = {'png': 'image/png', 'svg': 'image/svg+xml'}
    if file_type not in valid_types:
        raise ValueError("Unsupported file type. Use 'png' or 'svg'.")

    s3_key = f"{s3_path}.{file_type}"
    content_type = valid_types[file_type]

    print(f'...uploading {file_type} from {local_path} to {s3_key} in bucket {bucket}')
    
    # Upload file to S3
    s3 = boto3.client('s3')
    with open(local_path, 'rb') as image:
        s3.put_object(
            Bucket=bucket, 
            Key=s3_key,
            Body=image.read(),
            ContentType=content_type
        )

    print(f'..uploaded to {s3_key}')
    
    # Invalidate CloudFront cache
    empty_cloudfront_cache(cf_distribution_id, f'/{s3_key}')


def upload_parquet_to_cf_s3(bucket, path_name, df, cf_distribution_id):
    s3_url = f"s3://{bucket}/{path_name}.parquet"
    df.to_parquet(s3_url)

    print(f'..uploaded to {path_name}')
    empty_cloudfront_cache(cf_distribution_id, f'/{path_name}.parquet')

def upload_polars_df_to_s3(df, file_name, bucket, key):
    df.write_parquet(file_name)

    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, key)
        os.remove(file_name)
    except FileNotFoundError:
        print(f'The file {file_name} was not found')

## This function uploads a dataframe to S3 longterm bucket as parquet file
def dataframe_to_s3(path_name, df):
    s3_url = f"s3://{os.getenv('S3_LONG_TERM_BUCKET')}/{path_name}.parquet"
    df.to_parquet(s3_url)

    print(f'...uploaded to S3 longterm in {path_name}')

## This function uploads a dataframe to GCS bucket as parquet file
def dataframe_to_gcs(path_name, df):
    import io
    from src.adapters.rpc_funcs.gcs_utils import connect_to_gcs
    
    # Connect to GCS
    gcs, bucket_name = connect_to_gcs()
    bucket = gcs.bucket(bucket_name)
    
    # Convert DataFrame to parquet and upload to GCS
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    # Upload to GCS
    file_key = f"{path_name}.parquet"
    blob = bucket.blob(file_key)
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    
    print(f'...uploaded to GCS in {path_name}')

# prompt chatgpt, requires openai library
def prompt_chatgpt(prompt, api_key, model="gpt-3.5-turbo"):
    client = OpenAI(api_key=api_key)
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"An error occurred while prompting chatgpt: {str(e)}"
    
# convert yml file object (dict) into a df
def convert_economics_mapping_into_df(data): # turns yml object of economics_mapping.yml into a dataframe
    table = [
        [
            L2,
            layers.get('name'), 
            settlement_layer, 
            f.get('from_address'), 
            f.get('to_address'), 
            f.get('method'), 
            f.get('namespace') if settlement_layer in ['celestia', 'eigenda'] else None
        ]
        for L2, layers in data.items()
        for settlement_layer, filters in layers.items() if isinstance(filters, list)
        for f in filters
    ]
    df = pd.DataFrame(table, columns=['origin_key', 'name', 'da_layer', 'from_address', 'to_address', 'method', 'namespace'])
    return df

# get all official Open Labels Initative tags from OLI Github
def get_all_oli_tags_from_github():
    url = "https://raw.githubusercontent.com/openlabelsinitiative/OLI/refs/heads/main/1_data_model/tags/tag_definitions.yml"
    response = requests.get(url)
    data = yaml.load(response.text, Loader=yaml.FullLoader)
    df = pd.DataFrame(data['tags'])
    df['tag_id'] = df['tag_id'].str.replace('oli.', '')
    return df

# get all official Open Labels Initative (OLI) tags + gtp internal tags
def get_all_gtp_tags(db_connector):
    df = db_connector.get_table('oli_tags')
    return df

# get the growthepie x Open Labels Initative (OLI) list of trusted entities (attester, tag_id, score)
def get_trusted_entities(db_connector):
    # copy Github data into df
    url = "https://raw.githubusercontent.com/growthepie/gtp-dna/refs/heads/main/oli/trusted_entities.yml" # TODO: change to oli_tags
    response = requests.get(url)
    data = yaml.load(response.text, Loader=yaml.FullLoader)
    df = pd.DataFrame(data['trusted_entities'])
    # explode the tags column
    df = df.explode('tags')   
    # Then extract the tag_id and score from each tag dictionary
    df['tag_id'] = df['tags'].apply(lambda x: x['tag_id'] if x else None)
    df['score'] = df['tags'].apply(lambda x: x['score'] if x else None)
    # Drop the original tags column
    df = df.drop(columns=['tags'])
    # get all_tag from oli github
    all_tags = get_all_gtp_tags(db_connector)['tag_id'].tolist()
    all_tags = [tag.replace('oli.', '') for tag in all_tags] # not needed anymore
    # make a list of tag_id, if tag_id is "*" then add all tags
    df['tag_id_list'] = df['tag_id'].apply(lambda x: all_tags if x == "*" else [x])
    # expand the list of tag_ids into separate rows
    df = df.explode('tag_id_list')
    # drop duplicates (only keep first occurrences)
    df = df.sort_values(by=['attester', 'tag_id_list', 'tag_id'], ascending=[True, True, False])
    df = df.drop_duplicates(subset=['attester', 'tag_id_list'])
    # drop column tag_id and rename tag_id_list to tag_id
    df = df.drop(columns=['tag_id'])
    df = df.rename(columns={'tag_id_list': 'tag_id'})
    # reset index
    df = df.reset_index(drop=True)
    return df

def get_files_from_github_folder_large(repo_name, path="", token=None):
    """
    Retrieves all file names in a single GitHub repository folder,
    using the Git Trees API which better handles large directories.
    
    Args:
        repo_name (str): Name of the repository (e.g., "owner/repo").
        path (str, optional): Path within the repository. Defaults to root.
        github_token (str, optional): Personal access token for GitHub API. Defaults to None.
    
    Returns:
        list: List of filenames in the specified folder
    """
    print(f"Getting files in {repo_name}/{path}...")
    # First, get the default branch
    repo_url = f"https://api.github.com/repos/{repo_name}"
    headers = {}
    if token:
        headers["Authorization"] = f"{token}"

    repo_response = requests.get(repo_url, headers=headers)
    
    if repo_response.status_code != 200:
        print(f"Error getting repository: {repo_response.status_code}")
        print(repo_response.text)
        return []
    
    default_branch = repo_response.json()["default_branch"]
    
    # Get the reference to the latest commit on the default branch
    ref_url = f"https://api.github.com/repos/{repo_name}/git/refs/heads/{default_branch}"
    ref_response = requests.get(ref_url, headers=headers)
    
    if ref_response.status_code != 200:
        print(f"Error getting reference: {ref_response.status_code}")
        return []
    
    commit_sha = ref_response.json()["object"]["sha"]
    
    # Get the tree using the recursive parameter
    tree_url = f"https://api.github.com/repos/{repo_name}/git/trees/{commit_sha}?recursive=1"
    tree_response = requests.get(tree_url, headers=headers)
    
    if tree_response.status_code != 200:
        print(f"Error getting tree: {tree_response.status_code}")
        return []
    
    # Filter for files in the specified path
    tree_data = tree_response.json()
    files = []
    
    # Normalize path to ensure consistent formatting
    if path and not path.endswith('/'):
        path += '/'
    
    for item in tree_data["tree"]:
        # Only include files (not trees/directories)
        if item["type"] == "blob":
            item_path = item["path"]
            
            # Check if the item is in the specified path
            if not path or item_path.startswith(path):
                # Extract just the filename from the path
                if path:
                    relative_path = item_path[len(path):]
                else:
                    relative_path = item_path
                
                # Only include items directly in the folder (no subdirectories)
                if '/' not in relative_path:
                    files.append(relative_path)
    
    print(f"Found {len(files)} files in {repo_name}/{path}:")
    return files

# Convert a pandas DataFrame into a SQL SELECT query.
def df_to_postgres_values(df, table_alias="df"):
    # Get column names from DataFrame
    columns = df.columns.tolist()
    column_names = ", ".join([f'"{col}"' for col in columns])
    # Generate VALUES rows
    rows = []
    for _, row in df.iterrows():
        values = []
        for val in row:
            if val is None:
                values.append("null")
            elif isinstance(val, (int, float)):
                values.append(str(val))
            else:
                # Escape single quotes in string values
                val_str = str(val).replace("'", "''")
                values.append(f"'{val_str}'")
        row_str = "(" + ", ".join(values) + ")"
        rows.append(row_str)
    values_str = ",\n    ".join(rows)
    # Build the full query with comments
    query = f"""SELECT 
        *
    FROM (
        VALUES -- {', '.join(columns)}
        {values_str}
    ) AS {table_alias} ({column_names})"""
    return query

## get app logos from Github
def get_app_logo_files(repo, file_path:str, days:int, branch='main'):
    # get the latest commits
    commits = repo.get_commits(path=file_path, sha=branch)

    # check if there was a new commit in the last d days hours
    if commits[0].commit.author.date < datetime.now(timezone.utc) - timedelta(days=days):
        print(f"No new commit found in the last {24*days} hours.")
    else:
        print(f"New commit found in the last {24*days} hours.")

    ## loop through the commits and only keep the commits that are in the last d days. In addition, start a list of the files that were changed
    files_to_upload = []
    files_to_remove = []
    for commit in commits:
        if commit.commit.author.date > datetime.now(timezone.utc) - timedelta(days=days):
            for file in commit.files:
                if file_path in file.filename:
                    if file.status in ["added", "modified"]:
                        files_to_upload.append(file)
                    elif file.status == "removed":
                        files_to_remove.append(file)
    return files_to_upload, files_to_remove

def upload_app_logo_files_to_s3(repo, files_to_upload, cf_bucket_name, cf_distribution_id, branch='main'):
    ## TODO:iterate over files to remove? files_to_remove
    ## for each file in files_to_upload, download the file, write to to a temp file and then push it to S3
    for file in files_to_upload:
        print(f"Attempting to download {file.filename} from GitHub and uploading to S3.")
        try:
            content = repo.get_contents(file.filename, ref=branch)
            content = content.decoded_content
            file_name = file.filename.split("/")[-1]
            file_type = file_name.split(".")[-1]
            file_name = file_name.split(".")[0]
            with open(f'temp_img.{file_type}', "wb") as f:
                f.write(content)

            upload_image_to_cf_s3(
                bucket=cf_bucket_name, 
                s3_path=f'v1/apps/logos/{file_name}', 
                local_path=f'temp_img.{file_type}', 
                cf_distribution_id = cf_distribution_id, 
                file_type = file_type)
        except Exception as e:
            print(f"Error uploading {file.filename} to S3: {str(e)}")
        
    print("All files uploaded to S3.")

## This function gets the current list of files in the folder and removes any duplicates that have .png in the filename
## It returns a pandas dataframe with the filenames and the logo_path
## TODO: if we have logos in our repo that aren't in OSS dir we will add a new row to our oli_oss_dir table which should be avoided
def get_current_file_list(repo_name, file_path, github_token):
    current_files = get_files_from_github_folder_large(repo_name, file_path, github_token)
    ## load the files into a pandas dataframe and add 2nd column called "owner_project" with the filename without the extension
    df = pd.DataFrame(current_files, columns=["logo_path"])
    df['name'] = df['logo_path'].str.split(".").str[:-1].str.join(".")

    ## in df check for duplicates in the "name" column and remove the one the duplicate that has .png in the "logo_path" column
    df_duplicates = df[df.duplicated(subset=["name"], keep=False)]
    df_duplicates = df_duplicates[df_duplicates["logo_path"].str.contains(".png")]
    df = df[~df["logo_path"].isin(df_duplicates["logo_path"])]
    print(f"..removed {len(df_duplicates)} duplicates with .png in the filename.")

    df.set_index("name", inplace=True)
    return df

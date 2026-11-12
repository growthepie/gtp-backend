from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(seconds=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='utility_raw_data_check',
    description='This DAG checks if our raw data load is behind',
    tags=['utility'],
    start_date=datetime(2025,8,14),
    schedule='*/30 * * * *'
)

def check():
    @task()
    def run_check():
        from src.db_connector import DbConnector
        from src.main_config import get_main_config
        from src.misc.helper_functions import send_discord_message
        from src.adapters.rpc_funcs.utils import get_chain_config
        from datetime import datetime
        from zoneinfo import ZoneInfo
        import requests

        ## chain head stale for longer than this -> chain might have stopped (alert even if we are in sync)
        chain_stopped_minutes = 24 * 60

        def get_rpc_head(origin_key):
            """Returns (block_number, block_timestamp as aware UTC datetime) of the highest head across the
            chain's active+synced RPCs, or None if no RPC answered (e.g. non-EVM chains, all RPCs down)."""
            try:
                rpc_configs, _ = get_chain_config(db_connector, origin_key)
            except Exception as e:
                print(f"- could not load RPC config: {e}")
                return None
            head = None
            for rpc in rpc_configs:
                try:
                    r = requests.post(
                        rpc['url'],
                        json={"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": ["latest", False]},
                        timeout=15,
                    )
                    block = r.json()['result']
                    number = int(block['number'], 16)
                    ts = datetime.fromtimestamp(int(block['timestamp'], 16), ZoneInfo("UTC"))
                    if head is None or number > head[0]:
                        head = (number, ts)
                except Exception as e:
                    print(f"- RPC head lookup failed for {rpc['url'][:40]}...: {e}")
            return head

        main_config = get_main_config()
        db_connector = DbConnector()

        for chain in main_config:
            if chain.api_deployment_flag != 'PROD' or chain.origin_key in ['megaeth', 'polygon_pos', 'ronin', 'robinhood']:
                continue

            ## arbitrum_nova: low activity, blocks can be ~60min apart -> only alert after 5h
            test_time = 300 if chain.origin_key == 'arbitrum_nova' else 60

            print(f"Processing chain: {chain.origin_key}")
            max_val = db_connector.get_max_value(f'{chain.origin_key}_tx', 'block_timestamp')

            # Ensure max_val is timezone aware in UTC
            max_val = max_val.replace(tzinfo=ZoneInfo("UTC"))

            # Get current UTC time as aware datetime
            current_time = datetime.now(ZoneInfo("UTC"))

            # Calculate diff in minutes
            time_diff = (current_time - max_val).total_seconds() / 60
            time_diff = round(time_diff,2)

            print(f"- Time difference to current UTC: {time_diff} minutes")
            if time_diff <= test_time:
                continue

            ## Blocks missing check: are we actually behind the chain's top block, or is the chain itself not producing blocks?
            head = get_rpc_head(chain.origin_key)
            if head is None:
                print(f"ISSSUE: {chain.origin_key} is {time_diff} minutes behind (RPC head unavailable)")
                send_discord_message(f"RAW DATA SYNC ISSUE: {chain.origin_key} is {time_diff} minutes behind (couldn't verify against RPC head).")
                continue

            head_block, head_ts = head
            db_block = db_connector.get_max_block(f'{chain.origin_key}_tx')
            head_age = round((current_time - head_ts).total_seconds() / 60, 2)
            print(f"- DB max block: {db_block}, RPC head: {head_block} ({head_age} minutes old)")

            ## behind = RPC has blocks we don't, and the head is fresh (a stalled chain whose last few blocks we missed isn't a sync issue)
            if db_block < head_block and head_age <= test_time:
                print(f"ISSSUE: {chain.origin_key} is {time_diff} minutes / {head_block - db_block} blocks behind")
                send_discord_message(f"RAW DATA SYNC ISSUE: {chain.origin_key} is {time_diff} minutes behind ({head_block - db_block} blocks behind RPC head {head_block}).")
            elif time_diff > chain_stopped_minutes:
                print(f"ISSSUE: {chain.origin_key} chain might have stopped, last RPC head block {head_block} is {head_age} minutes old")
                send_discord_message(f"CHAIN MIGHT HAVE STOPPED: {chain.origin_key} has no new blocks, last block {head_block} at {head_ts:%Y-%m-%d %H:%M} UTC ({round(time_diff / 60, 1)}h ago in our DB).")
            else:
                print(f"- {chain.origin_key} not behind RPC head (no new blocks on chain), no alert")

    run_check()
check()

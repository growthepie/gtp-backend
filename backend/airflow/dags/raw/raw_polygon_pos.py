from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

## Polygon PoS raw tx data from public RPCs (sys_rpc_config). Source of all Polygon PoS metrics since 2026-11-17
## (before that: Dune + BigQuery). Verified identical to Dune's polygon.transactions for 2026-11-17 (~/polygon_raw_vs_dune.py).
## Table DDL: backend/src/queries/raw/polygon_pos_tx_ddl.sql

@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='raw_polygon_pos',
    description='Load raw tx data from Polygon PoS',
    tags=['raw', 'near-real-time', 'rpc'],
    start_date=datetime(2026, 11, 16),
    schedule='3/10 * * * *' ## every 10 min, offset to the least used raw DAG slot
)

def adapter_rpc():
    @task(execution_timeout=timedelta(minutes=45))
    def run_polygon_pos():
        from src.adapters.adapter_raw_rpc import NodeAdapter
        from src.adapters.rpc_funcs.utils import MaxWaitTimeExceededException, get_chain_config
        from src.db_connector import DbConnector

        # Initialize DbConnector
        db_connector = DbConnector()

        chain_name = 'polygon_pos'

        active_rpc_configs, batch_size = get_chain_config(db_connector, chain_name)
        print(f"POLYGON_POS_CONFIG={active_rpc_configs}")

        adapter_params = {
            'rpc': 'local_node',
            'chain': chain_name,
            'rpc_configs': active_rpc_configs,
        }

        # Initialize NodeAdapter
        adapter = NodeAdapter(adapter_params, db_connector)

        # Start block: continue after the last loaded block. On an empty table 'auto' would start at block 0 (genesis),
        # so start at the first block of the latest day in fact_kpis (first_block_of_day, filled by utility_first_block) instead.
        block_start = 'auto'
        if db_connector.get_max_block(f'{chain_name}_tx') == 0:
            df = db_connector.execute_query(f"""
                SELECT date, value FROM fact_kpis
                WHERE metric_key = 'first_block_of_day' AND origin_key = '{chain_name}'
                ORDER BY date DESC LIMIT 1""", load_df=True)
            if df.empty:
                raise ValueError(f"{chain_name}_tx is empty and there is no first_block_of_day to start from. Set a start block manually.")
            block_start = int(df['value'].iloc[0])
            print(f"{chain_name}_tx is empty, starting at the first block of {df['date'].iloc[0]}: {block_start}")

        # Initial load parameters
        load_params = {
            'block_start': block_start,
            'batch_size': batch_size,
        }

        try:
            adapter.extract_raw(load_params)
        except MaxWaitTimeExceededException as e:
            print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
            raise e
        finally:
            adapter.log_stats()

    run_polygon_pos()
adapter_rpc()

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook, claude_fix_on_failure


@dag(
    default_args={
        'owner': 'ahoura',
        'retries': 1,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=10),
        'on_failure_callback': [alert_via_webhook, claude_fix_on_failure],
    },
    dag_id='oli_automated_labeler',
    description='AI-based labeling of unlabeled contracts, with OLI attestation',
    tags=['oli', 'daily', 'labeling'],
    start_date=datetime(2026, 4, 22),
    schedule='30 01 * * 1',  # 1:30am UTC every Monday — after oli_airtable (00:50) refreshes materialized views
)
def etl():

    @task()
    def label_and_attest():
        import asyncio
        import argparse
        import sys
        from pathlib import Path

        labeling_dir = Path(__file__).resolve().parents[3] / 'labeling'
        if str(labeling_dir) not in sys.path:
            sys.path.insert(0, str(labeling_dir))

        from automated_labeler import run_pipeline

        args = argparse.Namespace(
            chains=None,           # all api_in_main chains with blockspace data
            days=7,
            max_contracts=200,
            per_chain_limit=0,     # no per-chain cap in production
            min_txcount=0,
            concurrency=5,
            output_dir='/tmp/oli_labeler_output',  # ephemeral; Airflow captures stdout
            input_json=None,
            attest=True,
            confidence_threshold=0.5,
            use_v2_fetch=True,     # get_unlabelled_contracts_v2: 0.1% threshold + top-10/chain
            reclassify_airtable=False,
        )
        asyncio.run(run_pipeline(args))

    label_and_attest()

etl()

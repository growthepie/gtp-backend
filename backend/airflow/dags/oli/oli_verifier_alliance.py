from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.models.param import Param
from src.misc.airflow_utils import alert_via_webhook


@dag(
    dag_id="oli_verifier_alliance",
    description="Backfill missing Verifier Alliance labels, then sync daily to OLI",
    default_args={
        "owner": "lorenz",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
        "on_failure_callback": alert_via_webhook,
    },
    start_date=datetime(2026, 10, 1),
    schedule="15 6 * * *",  # Daily export; independent of the metrics publication window.
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["oli", "daily"],
    params={
        "dry_run": Param(False, type="boolean"),
        "max_batches": Param(1000, type="integer", minimum=1),
    },
)
def main():
    @task(execution_timeout=timedelta(hours=20))
    def sync_labels():
        import json
        import os

        from airflow.sdk import get_current_context
        from src.adapters.adapter_verifier_alliance import sync
        from src.db_connector import DbConnector

        params = get_current_context()["params"]
        # Explicit persistent location: losing it loses the retry outbox and dimension cache.
        directory = os.environ["VERIFIER_ALLIANCE_STATE_DIR"]
        oli = None
        notify = None
        if not params["dry_run"]:
            from oli import OLI
            from src.misc.helper_functions import send_discord_message

            webhook = os.getenv("DISCORD_CONTRACTS") or os.environ["DISCORD_ALERTS"]
            if not webhook:
                raise ValueError("Configure DISCORD_CONTRACTS or DISCORD_ALERTS before live sync")
            def notify(message):
                response = send_discord_message(message, webhook_url=webhook)
                response.raise_for_status()

            oli = OLI(private_key=os.environ["OLI_gtp_auto_pk"],
                      api_key=os.environ["OLI_API_KEY"])
        db = DbConnector(db_name="oli")
        try:
            return sync(directory, db.engine, oli, dry_run=params["dry_run"],
                        max_batches=params["max_batches"],
                        verifier_map=json.loads(os.environ["VERIFIER_ALLIANCE_VERIFIERS"])
                        if os.getenv("VERIFIER_ALLIANCE_VERIFIERS") else None,
                        notify=notify)
        finally:
            db.engine.dispose()

    sync_labels()


main()

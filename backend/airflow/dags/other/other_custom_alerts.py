from datetime import datetime, timedelta

from airflow.sdk import dag, task


def _alert_via_webhook(context):
    from src.misc.airflow_utils import alert_via_webhook

    return alert_via_webhook(context)


@dag(
    default_args={
        "owner": "mseidl",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": _alert_via_webhook,
    },
    dag_id="other_custom_alerts",
    description="Standalone custom alerts for quick bites and related milestone trackers.",
    tags=["other", "alerts"],
    start_date=datetime(2026, 4, 28),
    schedule="45 05 * * *",
)
def run_dag():
    @task
    def app_ath_alerts():
        from src.misc.custom_alerts import run_app_ath_alerts

        run_app_ath_alerts()

    @task
    def pectra_type4_alerts():
        from src.misc.custom_alerts import run_pectra_type4_ath_alerts

        run_pectra_type4_ath_alerts()

    @task
    def arbitrum_timeboost_alerts():
        from src.misc.custom_alerts import run_arbitrum_timeboost_alerts

        run_arbitrum_timeboost_alerts()

    @task
    def shopify_usdc_alerts():
        from src.misc.custom_alerts import run_shopify_usdc_alerts

        run_shopify_usdc_alerts()

    @task
    def ethereum_scaling_alerts():
        from src.misc.custom_alerts import run_ethereum_scaling_alerts

        run_ethereum_scaling_alerts()

    @task
    def eip8004_alerts():
        from src.misc.custom_alerts import run_eip8004_alerts

        run_eip8004_alerts()

    @task
    def fusaka_alerts():
        from src.misc.custom_alerts import run_fusaka_alerts

        run_fusaka_alerts()

    @task
    def linea_burn_alerts():
        from src.misc.custom_alerts import run_linea_burn_alerts

        run_linea_burn_alerts()

    @task
    def robinhood_alerts():
        from src.misc.custom_alerts import run_robinhood_alerts

        run_robinhood_alerts()

    app_ath_alerts()
    pectra_type4_alerts()
    arbitrum_timeboost_alerts()
    shopify_usdc_alerts()
    ethereum_scaling_alerts()
    eip8004_alerts()
    fusaka_alerts()
    linea_burn_alerts()
    robinhood_alerts()


run_dag()

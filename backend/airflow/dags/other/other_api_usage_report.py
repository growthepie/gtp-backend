from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 1,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='other_api_usage_report',
    description='Weekly summary of who uses the public API (CloudFront logs) sent to Discord.',
    tags=['other', 'weekly'],
    start_date=datetime(2026, 11, 29),
    schedule='0 8 * * 1',  # Mondays 08:00 UTC: covers the previous Mon-Sun, CloudFront logs for Sunday are complete by then
    catchup=False,
    max_active_runs=1,
)

def run_dag():
    @task(execution_timeout=timedelta(minutes=45))
    def weekly_api_usage_report():
        import tempfile
        from src.misc.api_usage_report import run_weekly_report

        with tempfile.TemporaryDirectory() as tmp:
            # Posts to DISCORD_API_USAGE if set, otherwise DISCORD_ALERTS
            run_weekly_report(days=7, out_dir=tmp)

    weekly_api_usage_report()

run_dag()

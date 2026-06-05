from datetime import datetime, timedelta

from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook


LABEL_POOL_REATTEST_TABLE_ID = "tblU8WV0sxYUz6Kcp"


@dag(
    default_args={
        "owner": "mseidl",
        "retries": 2,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="oli_label_pool_reattest_reminder",
    description="Checks Label Pool Reattest in Airtable and reminds Discord if rows still need approval.",
    tags=["oli", "airtable", "daily"],
    start_date=datetime(2026, 6, 5),
    schedule="0 9 * * *",
    catchup=False,
)
def etl():
    @task()
    def remind_pending_label_pool_reattest():
        import os

        import pandas as pd
        from pyairtable import Api

        import src.misc.airtable_functions as at
        from src.misc.helper_functions import send_discord_message

        def clean_value(value, max_len=32):
            if isinstance(value, list):
                value = value[0] if len(value) > 0 else None

            if value is None:
                return "-"

            try:
                if pd.isna(value):
                    return "-"
            except (TypeError, ValueError):
                pass

            value = str(value).replace("\\x", "0x").strip()
            if value == "":
                return "-"

            if len(value) <= max_len:
                return value
            return f"{value[:max_len - 3]}..."

        def short_address(value):
            value = clean_value(value, max_len=80)
            if value == "-" or len(value) <= 18:
                return value
            return f"{value[:10]}...{value[-6:]}"

        airtable_api_key = os.getenv("AIRTABLE_API_KEY")
        airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
        api = Api(airtable_api_key)
        table = api.table(airtable_base_id, LABEL_POOL_REATTEST_TABLE_ID)

        df = at.read_pending_label_pool_reattest(api, airtable_base_id, table)
        if df.empty:
            print("No Label Pool Reattest rows pending approval.")
            return

        chain_counts = df["chain_id"].fillna("unknown").value_counts().head(8)
        chain_summary = ", ".join(
            f"{chain}: {count}" for chain, count in chain_counts.items()
        )

        sample = df.head(10)
        sample_lines = []
        for row in sample.itertuples(index=False):
            sample_lines.append(
                "- "
                f"{clean_value(row.chain_id, 24)} | "
                f"{short_address(row.address)} | "
                f"project={clean_value(row.owner_project)} | "
                f"category={clean_value(row.usage_category)} | "
                f"contract={clean_value(row.contract_name)} | "
                f"attester={short_address(row.attester)}"
            )

        more_count = len(df) - len(sample)
        more_line = f"\n...and {more_count} more." if more_count > 0 else ""
        airtable_url = f"https://airtable.com/{airtable_base_id}/{LABEL_POOL_REATTEST_TABLE_ID}"

        message = (
            "**Label Pool Reattest reminder**\n"
            f"{len(df)} entries still need approval in Airtable: {airtable_url}\n"
            f"By chain: {chain_summary}\n"
            "Sample rows:\n"
            f"{chr(10).join(sample_lines)}"
            f"{more_line}"
        )

        send_discord_message(message, os.getenv("DISCORD_CONTRACTS"))

    remind_pending_label_pool_reattest()


etl()

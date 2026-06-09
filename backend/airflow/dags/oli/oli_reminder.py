from datetime import datetime, timedelta

from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook


LABEL_POOL_REATTEST_TABLE_ID = "tblU8WV0sxYUz6Kcp"
OSS_DIRECTORY_REPOS = [
    {
        "full_name": "growthepAI/oss-directory",
        "label": "growthepAI fork",
        "frontend_author": "growthepAI",
    },
    {
        "full_name": "opensource-observer/oss-directory",
        "label": "OSO main repo",
        "metadata_path": "data/projects/",
    },
]


@dag(
    default_args={
        "owner": "mseidl",
        "retries": 2,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="oli_reminder",
    description="Checks Label Pool Reattest in Airtable and reminds Discord if rows still need approval as well as reminding about open PRs in the OSS Directory repos.",
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

    @task()
    def remind_open_oss_directory_prs():
        import os

        from github import Github

        from src.misc.helper_functions import send_discord_message

        def get_github_client():
            github_token = os.getenv("GITHUB_TOKEN")
            return Github(github_token) if github_token else Github()

        def is_metadata_pr(pr, metadata_path):
            if metadata_path is None:
                return False

            for changed_file in pr.get_files():
                if changed_file.filename.startswith(metadata_path):
                    return True

            return False

        def format_pr_line(pr, repo_config, relevant=None):
            author = pr.user.login if pr.user else "unknown"
            age_days = (datetime.utcnow() - pr.created_at.replace(tzinfo=None)).days

            tags = []
            if repo_config.get("frontend_author") == author:
                tags.append("frontend")
            if relevant is True:
                tags.append("project metadata")
            elif relevant is False:
                tags.append("other")
            if pr.draft:
                tags.append("draft")

            tag_text = f" [{' | '.join(tags)}]" if tags else ""
            return (
                f"- #{pr.number}{tag_text} {pr.title} "
                f"by @{author}, opened {age_days}d ago: {pr.html_url}"
            )

        def append_section(lines, title, prs):
            lines.append(title)
            if prs:
                lines.extend(prs)
            else:
                lines.append("- none")

        def send_chunked(message_lines, webhook_url, max_chars=1900):
            chunks = []
            current = ""

            for line in message_lines:
                next_line = f"{line}\n"
                if current and len(current) + len(next_line) > max_chars:
                    chunks.append(current.rstrip())
                    current = next_line
                else:
                    current += next_line

            if current:
                chunks.append(current.rstrip())

            for idx, chunk in enumerate(chunks, start=1):
                suffix = f"\n\n_part {idx}/{len(chunks)}_" if len(chunks) > 1 else ""
                send_discord_message(f"{chunk}{suffix}", webhook_url)

        github_client = get_github_client()
        message_lines = ["**OSS Directory open PR reminder**"]
        reminder_pr_count = 0

        for repo_config in OSS_DIRECTORY_REPOS:
            repo = github_client.get_repo(repo_config["full_name"])
            open_prs = list(
                repo.get_pulls(state="open", sort="created", direction="asc")
            )

            if repo_config.get("metadata_path"):
                relevant_lines = []

                for pr in open_prs:
                    relevant = is_metadata_pr(pr, repo_config["metadata_path"])
                    if relevant:
                        line = format_pr_line(pr, repo_config, relevant=True)
                        relevant_lines.append(line)

                reminder_pr_count += len(relevant_lines)
                message_lines.append("")
                message_lines.append(
                    f"**{repo_config['label']}** ({repo_config['full_name']}): "
                    f"{len(relevant_lines)} project metadata PRs"
                )

                append_section(
                    message_lines,
                    f"Project metadata PRs ({repo_config['metadata_path']}):",
                    relevant_lines,
                )
            else:
                reminder_pr_count += len(open_prs)
                message_lines.append("")
                message_lines.append(
                    f"**{repo_config['label']}** ({repo_config['full_name']}): "
                    f"{len(open_prs)} open PRs"
                )

                if open_prs:
                    message_lines.extend(
                        format_pr_line(pr, repo_config) for pr in open_prs
                    )
                else:
                    message_lines.append("- none")

        if reminder_pr_count == 0:
            print("No relevant OSS Directory PRs.")
            return

        send_chunked(message_lines, os.getenv("DISCORD_CONTRACTS"))

    remind_pending_label_pool_reattest()
    remind_open_oss_directory_prs()


etl()

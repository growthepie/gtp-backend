from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

DB_NAMES = ["fun", "oli", "web3"]
PREFIX = "db_backups"
RETENTION_DAYS = 10


@dag(
    default_args={
        "owner": "mseidl",
        "retries": 2,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="utility_db_dump",
    description="Daily full PostgreSQL backup to Hetzner Object Storage via pg_dump.",
    tags=["utility", "backup", "daily"],
    start_date=datetime(2026, 3, 12),
    schedule="00 20 * * *",
)
def backup_db_to_hetzner():
    def make_backup_task(db_name, trigger_rule):
        # One task per database so a failed upload only retries that database
        # (web3 is ~136 GB: dump ~2.5h + upload several hours).
        @task(task_id=f"backup_{db_name}", trigger_rule=trigger_rule)
        def run_backup():
            import os
            import subprocess
            import tempfile
            from datetime import datetime, timezone
            from src.misc.helper_functions import upload_file_to_s3

            db_user = "db_backup"
            db_passwd = os.getenv("DB_PASSWORD_BACKUP")
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT") or "5432"

            if not all([db_user, db_passwd, db_host]):
                raise ValueError(
                    "Missing required DB env vars: DB_PASSWORD_BACKUP, DB_HOST"
                )

            # Allow DB_HOST to include a port (e.g., "host:5432")
            if db_host and ":" in db_host and not db_host.startswith("["):
                host_part, port_part = db_host.rsplit(":", 1)
                if port_part.isdigit():
                    db_host = host_part
                    if not os.getenv("DB_PORT"):
                        db_port = port_part

            bucket_name = os.getenv("HETZNER_BUCKET_NAME")
            if not all([os.getenv("HETZNER_ACCESS_KEY_ID"), os.getenv("HETZNER_SECRET_ACCESS_KEY")]):
                raise ValueError(
                    "Missing required Hetzner env vars: HETZNER_ACCESS_KEY_ID, HETZNER_SECRET_ACCESS_KEY"
                )

            env = os.environ.copy()
            env["PGPASSWORD"] = db_passwd

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
            filename = f"{db_name}_{ts}.dump"
            blob_path = f"{PREFIX}/{db_name}/date={ts[:10]}/{filename}"

            print(f"Starting backup for database '{db_name}' to s3://{bucket_name}/{blob_path}...")

            with tempfile.TemporaryDirectory() as tmpdir:
                dump_path = os.path.join(tmpdir, filename)
                cmd = [
                    "pg_dump",
                    "--format=custom",
                    "--compress=5",
                    "--no-owner",
                    "--no-acl",
                    "--host",
                    db_host,
                    "--port",
                    str(db_port),
                    "--username",
                    db_user,
                    "--file",
                    dump_path,
                    db_name,
                ]

                try:
                    result = subprocess.run(
                        cmd,
                        check=True,
                        env=env,
                        capture_output=True,
                        text=True,
                    )
                    if result.stdout:
                        print(result.stdout)
                    if result.stderr:
                        print(result.stderr)
                except subprocess.CalledProcessError as exc:
                    if exc.stdout:
                        print(f"pg_dump stdout:\n{exc.stdout}")
                    if exc.stderr:
                        print(f"pg_dump stderr:\n{exc.stderr}")
                    raise

                size_gb = os.path.getsize(dump_path) / 1e9
                print(f"pg_dump finished: {size_gb:.1f} GB, uploading...")

                upload_file_to_s3(
                    bucket_name,
                    blob_path,
                    dump_path,
                    empty_cf_cache=False,
                    destination="hetzner",
                )

                print(f"Uploaded backup to s3://{bucket_name}/{blob_path}")

        return run_backup()

    def make_cleanup_task(db_name):
        # Runs only after this database's backup succeeded, so repeated upload
        # failures never age out the last good backups.
        @task(task_id=f"cleanup_{db_name}")
        def cleanup_old_backups():
            import boto3
            import os
            import re
            from datetime import datetime, timedelta, timezone

            bucket_name = os.getenv("HETZNER_BUCKET_NAME")
            s3 = boto3.client(
                "s3",
                endpoint_url="https://fsn1.your-objectstorage.com",
                aws_access_key_id=os.getenv("HETZNER_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("HETZNER_SECRET_ACCESS_KEY"),
            )

            cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
            date_re = re.compile(r"/date=(\d{4}-\d{2}-\d{2})/")
            delete_prefix = f"{PREFIX}/{db_name}/"
            continuation_token = None
            while True:
                list_kwargs = {"Bucket": bucket_name, "Prefix": delete_prefix}
                if continuation_token:
                    list_kwargs["ContinuationToken"] = continuation_token

                response = s3.list_objects_v2(**list_kwargs)
                for obj in response.get("Contents", []):
                    key = obj["Key"]
                    match = date_re.search(key)
                    if not match:
                        continue
                    try:
                        blob_date = datetime.strptime(match.group(1), "%Y-%m-%d").replace(
                            tzinfo=timezone.utc
                        )
                    except ValueError:
                        continue
                    if blob_date < cutoff:
                        print(f"Deleting old backup: s3://{bucket_name}/{key}")
                        s3.delete_object(Bucket=bucket_name, Key=key)

                if not response.get("IsTruncated"):
                    break
                continuation_token = response.get("NextContinuationToken")

        return cleanup_old_backups()

    # Backups run sequentially (avoid parallel pg_dumps / 136 GB of concurrent tmp files),
    # but each one runs even if the previous database's backup failed (all_done).
    prev_backup = None
    for db_name in DB_NAMES:
        backup = make_backup_task(db_name, "all_success" if prev_backup is None else "all_done")
        cleanup = make_cleanup_task(db_name)
        backup >> cleanup
        if prev_backup is not None:
            prev_backup >> backup
        prev_backup = backup


backup_db_to_hetzner()

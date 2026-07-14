from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook


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
    @task()
    def run_backup():
        import boto3
        import os
        import re
        import subprocess
        import tempfile
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
        prefix = "db_backups"
        retention_days = 10
        hetzner_access_key = os.getenv("HETZNER_ACCESS_KEY_ID")
        hetzner_secret_key = os.getenv("HETZNER_SECRET_ACCESS_KEY")

        if not all([hetzner_access_key, hetzner_secret_key]):
            raise ValueError(
                "Missing required Hetzner env vars: HETZNER_ACCESS_KEY_ID, HETZNER_SECRET_ACCESS_KEY"
            )

        env = os.environ.copy()
        env["PGPASSWORD"] = db_passwd

        db_names = ["fun", "oli", "web3"]
        s3 = boto3.client(
            "s3",
            endpoint_url="https://fsn1.your-objectstorage.com",
            aws_access_key_id=hetzner_access_key,
            aws_secret_access_key=hetzner_secret_key,
        )

        for name in db_names:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
            filename = f"{name}_{ts}.dump"
            blob_path = f"{prefix}/{name}/date={ts[:10]}/{filename}"
            
            print(f"Starting backup for database '{name}' to s3://{bucket_name}/{blob_path}...")

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
                    name,
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

                upload_file_to_s3(
                    bucket_name,
                    blob_path,
                    dump_path,
                    empty_cf_cache=False,
                    destination="hetzner",
                )

                print(f"Uploaded backup to s3://{bucket_name}/{blob_path}")

        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        date_re = re.compile(r"/date=(\d{4}-\d{2}-\d{2})/")
        for name in db_names:
            delete_prefix = f"{prefix}/{name}/"
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

    run_backup()


backup_db_to_hetzner()

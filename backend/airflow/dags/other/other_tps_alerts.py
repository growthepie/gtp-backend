from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='other_tps_alerts',
    description='Check against our Redis DB and alert via Discord/Telegram if new TPS all-time-highs are detected.',
    tags=['other', 'near-real-time'],
    start_date=datetime(2025,10,14),
    schedule='*/30 * * * *', # run every 30 minutes
    catchup=False,
    max_active_runs=1,
)

def run_dag():
    @task()
    def run_tps_global():      
        import os
        import time
        import json
        import signal
        import sys
        import redis
        from src.misc.helper_functions import  generate_screenshot, send_telegram_message, send_discord_message
        
        import time
        start_time = time.time()

        # Redis constants
        REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
        #REDIS_HOST = 'localhost' # for testing
        REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
        REDIS_DB = int(os.getenv("REDIS_DB", "0"))
        REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

        REDIS_ZSET_KEY_ATH_HISTORY = "global:tps:ath_history"
        REDIS_KEY_LAST_ALERTED_ATH_SCORE = "global:tps:ath_alert:last_score"
        CHECK_INTERVAL_SEC = 2
        COOLDOWN_AFTER_NEW_HIGH_SEC = 300
        MONITOR_RUNTIME_SEC = 29 * 60

        TG_BOT_TOKEN = os.getenv("GROWTHEPIE_BOT_TOKEN")
        TG_CHAT_ID = "@growthepie_alerts"

        # Graceful exit flag
        running = True
        def handle_exit(signum, frame):
            nonlocal running
            print("\n🛑 Received shutdown signal. Exiting gracefully...")
            running = False

        signal.signal(signal.SIGINT, handle_exit)
        signal.signal(signal.SIGTERM, handle_exit)

        def decode_payload(raw_val):
            try:
                if isinstance(raw_val, str):
                    return json.loads(raw_val)
                payload = json.loads(raw_val.decode("utf-8"))
            except Exception as e:
                print(f"⚠️ Failed to decode JSON: {e}")
                return None
            return payload

        def get_latest_entry(r, REDIS_ZSET_KEY):
            """Return latest zset entry as (payload_dict, score) or (None, None)."""
            latest = r.zrevrange(REDIS_ZSET_KEY, 0, 0, withscores=True)
            if not latest:
                return None, None
            raw_val, score = latest[0]
            payload = decode_payload(raw_val)
            if payload is None:
                return None, None
            return payload, score

        def get_latest_unalerted_entry(r, REDIS_ZSET_KEY, last_alerted_score):
            """Return latest ATH event with score greater than last alerted score."""
            if last_alerted_score is None:
                return None, None
            min_score = "-inf" if last_alerted_score == float("-inf") else f"({last_alerted_score}"
            entries = r.zrangebyscore(
                REDIS_ZSET_KEY,
                min_score,
                "+inf",
                withscores=True,
            )
            if not entries:
                return None, None
            raw_val, score = entries[-1]
            payload = decode_payload(raw_val)
            if payload is None:
                return None, None
            return payload, score

        def get_last_alerted_score(r):
            raw_score = r.get(REDIS_KEY_LAST_ALERTED_ATH_SCORE)
            if raw_score is None:
                return None
            if isinstance(raw_score, bytes):
                raw_score = raw_score.decode("utf-8")
            try:
                return float(raw_score)
            except (TypeError, ValueError):
                print(f"⚠️ Invalid last alerted score '{raw_score}', resetting alert baseline.")
                return None

        def set_last_alerted_score(r, score):
            r.set(REDIS_KEY_LAST_ALERTED_ATH_SCORE, str(score))

        def on_new_tps_high(new_tps, payload):
            """Placeholder for your real reaction logic (Telegram, Discord, etc.)."""
            ts = str(payload.get("timestamp") or payload.get("timestamp_ms"))
            ts = ts.split(".")[0]
            chain_breakdown = payload.get("chain_breakdown") or {}
            sorted_chain_breakdown = dict(
                sorted(chain_breakdown.items(), key=lambda item: item[1], reverse=True)[:5]
            )
            formatted_chain_breakdown = "\n".join(f"• {name.title()}: {value:.1f}" for name, value in sorted_chain_breakdown.items())

            url = "https://www.growthepie.com/ethereum-ecosystem/metrics?tps=true"
            filename = f"tps_global/{ts}_ecosystem.png"
            selector = "#content-panel > div > main > div > div.flex.flex-col.pt-\[15px\] > div.px-\[20px\].md\:pl-\[45px\].md\:pr-\[60px\].text-color-text-primary.z-\[1\] > div.grid.grid-cols-\[1fr\,1fr\,1fr\].gap-\[15px\].w-full.\@container > div.flex.flex-col.lg\:flex-row.gap-\[15px\].col-span-3.\@\[1040px\]\:col-span-2 > div:nth-child(1) > div > div"
            os.makedirs(os.path.dirname(f"generated_images/{filename}"), exist_ok=True)
            generate_screenshot(url, filename, width=1200, wait_for_timeout=3000, selector=selector, clip_height=180)

            message = [
                f"🥧 **New all-time high in Ecosystem TPS:** `{new_tps:.2f}`",
                f"*(at {ts} UTC)*\n",
                "**Top chains by TPS at time of ATH:**",
                formatted_chain_breakdown,
                f"[View on growthepie.com]({url})"
            ]
            message = "\n".join(message)

            #send_telegram_message(TG_BOT_TOKEN, TG_CHAT_ID, message, image_path=f"generated_images/{filename}")
            send_discord_message(message, os.getenv("GTP_AI_WEBHOOK_URL"), image_paths=f"generated_images/{filename}")
            
        print("🔌 Connecting to Redis...")
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=False
        )
        try:
            r.ping()
        except redis.exceptions.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            sys.exit(1)
        print(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

        # Get initial TPS
        payload, score = get_latest_entry(r, REDIS_ZSET_KEY_ATH_HISTORY)
        if not payload:
            print(f"ℹ️ No entries found in '{REDIS_ZSET_KEY_ATH_HISTORY}', waiting for data...")
            latest_ath = float("-inf")
        else:
            latest_ath = float(payload.get("tps", float("-inf")))
            print(f"🔧 Initial internal ATH set to {latest_ath:.2f}")

        last_alerted_score = get_last_alerted_score(r)
        if last_alerted_score is None:
            if score is not None:
                last_alerted_score = score
                set_last_alerted_score(r, last_alerted_score)
                print(f"🔧 Initial alert baseline set to Redis score {last_alerted_score}")
            else:
                last_alerted_score = float("-inf")

        while running:
            payload, score = get_latest_unalerted_entry(r, REDIS_ZSET_KEY_ATH_HISTORY, last_alerted_score)
            if payload:
                try:
                    current_tps = float(payload.get("tps", float("-inf")))
                except (TypeError, ValueError):
                    current_tps = float("-inf")

                on_new_tps_high(current_tps, payload)
                set_last_alerted_score(r, score)
                last_alerted_score = score
                latest_ath = current_tps
                time.sleep(COOLDOWN_AFTER_NEW_HIGH_SEC)
                continue
            else:
                payload, _ = get_latest_entry(r, REDIS_ZSET_KEY_ATH_HISTORY)
                if payload:
                    try:
                        latest_ath = float(payload.get("tps", float("-inf")))
                    except (TypeError, ValueError):
                        latest_ath = float("-inf")
                    print(f"ℹ️ No unalerted TPS high. Latest ATH: {latest_ath:.2f}, last alerted score: {last_alerted_score}")
            
            if start_time + MONITOR_RUNTIME_SEC < time.time():
                print("⏰ Approaching task timeout, exiting loop to allow for graceful restart.")
                break

            time.sleep(CHECK_INTERVAL_SEC)

        print("👋 Exiting monitor loop.")
        r.close()

    run_tps_global()
run_dag()

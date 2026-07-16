from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook
from pendulum import timezone

CET = timezone("Europe/Paris")

@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='other_highlights_bot',
    description='Send highlights from growthepie.com to Discord and Telegram',
    tags=['highlights',],
    start_date=CET.convert(datetime(2023, 9, 1, 8, 0)),
    schedule='30 8 * * *', ## CET TIMEZONE here instead of UTC
    catchup=False  # Ensures only future runs are scheduled, not backfilled
)

def highlights_bot():            
    @task()
    def run_highlights_tg():
        import os
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        from src.config import gtp_metrics_new
        from src.misc.helper_functions import highlights_prep, send_telegram_message, generate_fundamentals_chart_screenshot, chain_origin_key_to_url_slug
        from src.main_config import get_main_config

        db_connector = DbConnector()
        main_config = get_main_config()

        TG_BOT_TOKEN = os.getenv("GROWTHEPIE_BOT_TOKEN")
        TG_CHAT_ID = "@growthepie_alerts"

        run_dict = {
            'ethereum_ecosystem': 'Ethereum Ecosystem',
            'ethereum': 'Ethereum Mainnet',
        }
        
        for origin_key, name in run_dict.items():
            query_params = {
                "origin_key": origin_key,
                "days" : 2,
                "limit": 5
            }
            df = execute_jinja_query(db_connector, 'api/select_highlights.sql.j2', query_params, return_df=True)

            if not df.empty:
                highlights = highlights_prep(df, gtp_metrics_new)

                for highlight in highlights:
                    metric_key = highlight['metric_key']
                    metric_id = highlight['metric_id']
                    date = highlight['date']
                    highlight_type = highlight['type']
                    metric_conf = gtp_metrics_new['chains'][metric_id]
                    metric_fe = metric_conf['url_path'].split('/')[-1]
                    
                    message = (
                        f"🥧 **{highlight['metric_name']} {highlight['header']} for {name}: {highlight['value']}**\n\n"
                        f"_{highlight['text']}_\n"
                        f"{highlight['date']}\n\n"
                        f"[View on growthepie.com](https://www.growthepie.com/fundamentals/{metric_fe})"
                    )
                    
                    if highlight_type != 'growth_1':
                        ## Screenshot the metric chart card (the /embed/fundamentals/... pages
                        # were removed from the frontend). For a specific chain we reconcile the
                        # chart to show only that chain; the ecosystem aggregate keeps the default
                        # multi-chain view. The new page ignores the old query params, so drop them.
                        chain_slug = None if origin_key == 'ethereum_ecosystem' else chain_origin_key_to_url_slug(origin_key)
                        url = f"https://www.growthepie.com/fundamentals/{metric_fe}"
                        print(f"🌐 Chart URL: {url}")
                        filename = f"{date}_{metric_key}.png"
                        generate_fundamentals_chart_screenshot(url, filename, chain_slug=chain_slug, height=1000, width=1400, wait_for_timeout=4000)
                        #send_discord_message(message, os.getenv("GTP_AI_WEBHOOK_URL"), image_paths=f"generated_images/{filename}")
                        send_telegram_message(TG_BOT_TOKEN, TG_CHAT_ID, message, image_path=f"generated_images/{filename}")
                    else:
                        ## only 3x growth pct highlights to TG
                        if highlight['others']['growth_pct_growth'] > 3:
                            #send_discord_message(message, os.getenv("GTP_AI_WEBHOOK_URL"))
                            send_telegram_message(TG_BOT_TOKEN, TG_CHAT_ID, message)
                        
    @task()
    def run_highlights_discord():
        import os
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        from src.config import gtp_metrics_new
        from src.misc.helper_functions import highlights_prep, send_discord_message, generate_fundamentals_chart_screenshot, chain_origin_key_to_url_slug
        from src.main_config import get_main_config

        db_connector = DbConnector()
        main_config = get_main_config()

        for chain in main_config:
            origin_key = chain.origin_key
            name = chain.name
            
            query_params = {
                "origin_key": origin_key,
                "days" : 2,
                "limit": 5
            }
            df = execute_jinja_query(db_connector, 'api/select_highlights.sql.j2', query_params, return_df=True)

            if not df.empty:
                highlights = highlights_prep(df, gtp_metrics_new)

                for highlight in highlights:     
                    metric_key = highlight['metric_key']
                    metric_id = highlight['metric_id']
                    
                    if chain.api_in_main and chain.api_deployment_flag == 'PROD' and metric_id not in chain.api_exclude_metrics:         
                        date = highlight['date']
                        highlight_type = highlight['type']
                        metric_conf = gtp_metrics_new['chains'][metric_id]
                        metric_fe = metric_conf['url_path'].split('/')[-1]
                        
                        if highlight_type in ['ath_multiple', 'ath_regular', 'ath_multiple_since', 'ath_regular_since'] or highlight_type.startswith('lifetime_'):
                        
                            message = (
                                f"🥧 **{highlight['metric_name']} {highlight['header']} for {name}: {highlight['value']}**\n\n"
                                f"_{highlight['text']}_\n"
                                f"{highlight['date']}\n\n"
                                f"[View on growthepie.com](https://www.growthepie.com/fundamentals/{metric_fe})"
                            )
                            
                            if highlight_type != 'growth_1':
                                ## Screenshot the metric chart card (the /embed/fundamentals/...
                                # pages were removed from the frontend). Reconcile the chart to show
                                # only the chain that produced the highlight, then clip to the card.
                                # The new page ignores the old query params, so drop them.
                                chain_slug = chain_origin_key_to_url_slug(origin_key)
                                url = f"https://www.growthepie.com/fundamentals/{metric_fe}"
                                print(f"🌐 Chart URL: {url}")
                                filename = f"{date}_{metric_key}.png"
                                generate_fundamentals_chart_screenshot(url, filename, chain_slug=chain_slug, height=1000, width=1400, wait_for_timeout=4000)
                                send_discord_message(message, os.getenv("GTP_AI_WEBHOOK_URL"), image_paths=f"generated_images/{filename}")
                                #send_telegram_message(TG_BOT_TOKEN, TG_CHAT_ID, message, image_path=f"generated_images/{filename}")
                            else:
                                send_discord_message(message, os.getenv("GTP_AI_WEBHOOK_URL"))
                                #send_telegram_message(TG_BOT_TOKEN, TG_CHAT_ID, message)

    #run_analyst()
    run_highlights_tg()
    run_highlights_discord()
    
highlights_bot()
    
import os
import sys
import getpass
import asyncio
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
from pendulum import timezone

CET = timezone("Europe/Paris")

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='nader')
    },
    dag_id='other_gtp_analyst',
    description='Generate AI Insights with Social Media Automation',
    tags=['ai', 'milestones', 'metrics', 'social', 'twitter', 'charts'],
    start_date=CET.convert(datetime(2023, 9, 1, 8, 0)),
    schedule_interval='30 8 * * *', ## CET TIMEZONE here instead of UTC
    catchup=False  # Ensures only future runs are scheduled, not backfilled
)

def gtp_analyst():
    @task(execution_timeout=timedelta(minutes=60))
    def run_analyst():
        """
        Run social media automation pipeline with AI-generated tweets and chart images
        
        Required environment variables:
        - GTP_ANALYST_WEBHOOK_URL_LOCAL: Discord webhook URL for posting results
        - OPENAI_API_KEY: OpenAI API key for tweet generation
        """
        from dotenv import load_dotenv
        load_dotenv(override=True)  # Force reload environment variables
        
        from src.misc.social_media_integration import SocialMediaAutomation
        
        # Check for required API key
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("❌ OpenAI API key not found! Please add OPENAI_API_KEY to your environment variables.")
        
        # Initialize and run the social media automation pipeline
        automation = SocialMediaAutomation()
        print("🚀 Starting automated social media content generation from Airflow DAG...")
        
        # Run the async pipeline using asyncio
        result = asyncio.run(automation.run_automated_social_pipeline())
        
        if result:
            print(f"✅ Social media pipeline completed successfully!")
            print(f"Generated {len(result.get('tweets', []))} tweets")
            print(f"Generated {len(result.get('generated_images', []))} chart images")
            print(f"Processed {len(result.get('responses', []))} milestone responses")
        else:
            print("⚠️ Social media pipeline completed with no results or encountered errors")

    run_analyst()
gtp_analyst()
    
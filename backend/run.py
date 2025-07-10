import os
import pprint
import platform
import multiprocessing
import sys

# Import the class you want to debug
from src.misc.octant_v2 import OctantV2
from src.db_connector import DbConnector

# JSONCreation is no longer the primary class we're using here, but we can leave it
from src.api.json_creation import JSONCreation

from dotenv import load_dotenv
load_dotenv(".env.local")

db_user = os.getenv("DB_USERNAME")
db_passwd = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_DATABASE")

print(f"DB_USER: {db_user}")

# The DbConnector is needed by the OctantV2 class
# db_connector = DbConnector(db_user, db_passwd, db_host, db_name)
print("Initializing DbConnector for SQLite...")
db_connector = DbConnector(db_name='local_octant.db') # Use a local SQLite database for testing

if __name__ == '__main__':
    if platform.system() == 'Windows':
        multiprocessing.freeze_support()

    # --- Configuration for Local Octant Debugging ---
    s3_bucket = os.getenv("S3_BUCKET")  # Default to 'growthepie' if not set
    cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")  # Default to None if not set
    api_version = 'v1'

    print(f"Using S3 bucket: '{s3_bucket}'", f"and CloudFront distribution ID: '{cf_distribution_id}'")
    print(f"API Version: '{api_version}'")

    # sys.exit(0)  # Exit early for debugging purposes

    try:
        user = os.getlogin()
    except OSError:
        user = 'local_user'

    print(f"Running in local mode as user: '{user}'")

    # Instantiate the OctantV2 class, now passing the SQLite connector
    octant_runner = OctantV2(
        s3_bucket=s3_bucket,
        cf_distribution_id=cf_distribution_id,
        db_connector=db_connector,
        api_version=api_version,
        user=user
    )

    # --- Call the OctantV2 methods you want to test ---
    print("Starting Octant V2 script for local debugging with SQLite...")

    # NOTE: The first time you run this with a new SQLite DB, 
    # you MUST enable the table creation logic in octant_v2.py (see instructions below).

    print("STEP 1: Loading data for all epochs into the SQLite database...")
    octant_runner.run_load_octant_data_for_all_epochs()
    print("...Finished loading epoch data.")

    print("STEP 2: Creating all JSON output files locally from SQLite data...")
    octant_runner.run_create_all_octant_jsons()
    print("...Finished creating JSON files.")

    print("Octant V2 local debugging run complete.")



    # --- Original code from your run.py is commented out below ---
    # json_creator = JSONCreation(None, None, db_connector, api_version)
    # run_screenshots(s3_bucket, cf_distribution_id, api_version, user)
    # json_creator.create_labels_parquet('full')
    # ... and so on

# To run this file:
# python ./backend/run.py
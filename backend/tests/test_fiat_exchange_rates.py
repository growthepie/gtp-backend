import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, date, timedelta
import argparse
import pandas as pd
import os


def main():
    from src.db_connector import DbConnector
    from src.adapters.adapter_currency_conversion import AdapterCurrencyConversion
    from src.currency_config import get_supported_currencies

    parser = argparse.ArgumentParser(description="Test fiat exchange rates loading (matches DAG logic)")
    parser.add_argument("--force-backfill", action="store_true", help="Force backfill even if marker exists")
    parser.add_argument("--reset", action="store_true", help="Remove marker file to simulate first run")
    args = parser.parse_args()

    db_connector = DbConnector()
    currencies = get_supported_currencies()
    adapter_params = {
        'currencies': currencies,
        'force_refresh': False
    }
    ad = AdapterCurrencyConversion(adapter_params, db_connector)

    # Check marker file (same logic as DAG)
    marker_file = '/tmp/fiat_rates_initialized'
    
    if args.reset:
        if os.path.exists(marker_file):
            os.remove(marker_file)
            print("Marker file removed. Next run will be treated as first run.")
        return

    is_first_run = not os.path.exists(marker_file) or args.force_backfill

    if is_first_run:
        print("First run detected. Backfilling 365 days of historical fiat rates...")
        # Backfill 365 days
        end = date.today()
        start = end - timedelta(days=365)
        frames = []
        for offset in range(366):  # 365 days + today
            d = start + timedelta(days=offset)
            load_params = {
                'load_type': 'historical_rates',
                'date': d.strftime('%Y-%m-%d'),
                'currencies': currencies
            }
            df = ad.extract(load_params)
            if not df.empty:
                frames.append(df)
        
        if frames:
            df_all = pd.concat(frames)
            ad.load(df_all)
            print(f"Backfilled {len(df_all)} rows of historical fiat rates.")
        
        # Create marker file (unless force-backfill)
        if not args.force_backfill:
            with open(marker_file, 'w') as f:
                f.write(str(datetime.now()))
            print("First run complete. Future runs will load daily rates only.")
    else:
        print("Regular daily run. Loading current fiat rates...")
        # Regular daily load
        load_params = {
            'load_type': 'current_rates',
            'currencies': currencies
        }
        df = ad.extract(load_params)
        if not df.empty:
            ad.load(df)
            print(f"Loaded {len(df)} rows of current fiat rates.")
        else:
            print("No current fiat rates fetched.")


if __name__ == "__main__":
    main()
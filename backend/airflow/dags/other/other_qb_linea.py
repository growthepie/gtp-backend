from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
from airflow.sdk import dag, task
from src.adapters.adapter_logs import AdapterLogs
from src.db_connector import DbConnector
from src.misc.airflow_utils import alert_via_webhook
from web3 import Web3

RPC_URL = "https://linea.drpc.org"
CHAIN_NAME = "linea"
CHUNK_SIZE = 10_000
ORIGIN_KEY = "linea"

# address: https://lineascan.build/address/0xfd5fb23e06e46347d8724486cdb681507592e237
INVOICE_TOPIC = "0xc6da70dbb809f46821a66136527962f1e93ac500c61f1878c39417b2fa8c35a6"
BURN_TOPIC = "0x0e2419f2e998f267b7ebbe863f0c8144b9bd6741dafa3386eae42e2a460f0167"

INVOICE_LOOKBACK_DAYS = 3
BURN_LOOKBACK_DAYS = 3

def _build_logs_adapter() -> Tuple[AdapterLogs, Web3]:
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    return AdapterLogs(w3), w3


def _resolve_from_block(db_connector: DbConnector, lookback_days: int) -> int:
    target_date = datetime.utcnow().date() - timedelta(days=lookback_days)
    from_block = db_connector.get_first_block_of_the_day(CHAIN_NAME, target_date)
    if from_block is None:
        raise ValueError(f"Failed to resolve from_block for {CHAIN_NAME} with lookback {lookback_days} days")
    return from_block


def _format_invoice_logs(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    df = raw_df[raw_df["topics"].apply(lambda x: isinstance(x, (list, tuple)) and len(x) >= 4)].copy()
    if df.empty:
        return df

    df["date"] = df["topics"].apply(lambda x: int(x[2], 16))
    df["date"] = pd.to_datetime(df["date"], unit="s").dt.strftime("%Y-%m-%d")
    df["qb_amountPaid_eth"] = df["data"].apply(lambda x: int(x[:66], 16) / 10**18)
    df["qb_amountRequested_eth"] = df["data"].apply(lambda x: int(x[66:], 16) / 10**18)
    df["origin_key"] = ORIGIN_KEY

    melted = df[["origin_key", "date", "qb_amountPaid_eth", "qb_amountRequested_eth"]].melt(
        id_vars=["origin_key", "date"],
        value_vars=["qb_amountPaid_eth", "qb_amountRequested_eth"],
        var_name="metric_key",
        value_name="value",
    )

    return melted.set_index(["origin_key", "metric_key", "date"])


def _block_numbers_to_dates(block_numbers: pd.Series, w3: Web3) -> pd.Series:
    if block_numbers.empty:
        return block_numbers

    block_numbers_int = block_numbers.astype(int)
    unique_blocks = block_numbers_int.unique().tolist()
    timestamp_cache = {}
    for block in unique_blocks:
        block_data = w3.eth.get_block(block)
        timestamp_cache[block] = pd.to_datetime(block_data["timestamp"], unit="s").strftime("%Y-%m-%d")

    return block_numbers_int.map(timestamp_cache)


def _format_burn_logs(raw_df: pd.DataFrame, w3: Web3) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    df = raw_df.copy()
    df["qb_ethBurnt_eth"] = df["data"].apply(lambda x: int(x[:66], 16) / 10**18)
    df["qb_lineaTokensBridged_linea"] = df["data"].apply(lambda x: int(x[66:], 16) / 10**18)
    df["date"] = _block_numbers_to_dates(df["blockNumber"], w3)
    df["origin_key"] = ORIGIN_KEY

    grouped = (
        df[["origin_key", "date", "qb_ethBurnt_eth", "qb_lineaTokensBridged_linea"]]
        .groupby(["origin_key", "date"], as_index=False)
        .sum()
    )

    melted = grouped.melt(
        id_vars=["origin_key", "date"],
        value_vars=["qb_ethBurnt_eth", "qb_lineaTokensBridged_linea"],
        var_name="metric_key",
        value_name="value",
    )

    return melted.set_index(["origin_key", "metric_key", "date"])


@dag(
    default_args={
        "owner": "lorenz",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="other_qb_linea",
    description="Linea chain burns and invoices",
    tags=["other"],
    start_date=datetime(2025, 11, 11),
    schedule="11 2 * * *",  # Every day at 02:11
)
def run_dag():

    @task(task_id="process_invoice_logs")
    def process_invoice_logs():
        adapter, w3 = _build_logs_adapter()
        db_connector = DbConnector()
        from_block = _resolve_from_block(db_connector, INVOICE_LOOKBACK_DAYS)
        to_block = w3.eth.block_number

        extract_params = {
            "from_block": from_block,
            "to_block": to_block,
            "chunk_size": CHUNK_SIZE,
            "topics": [
                INVOICE_TOPIC,
                None,
                None,
                None,
            ],
        }

        logs = adapter.extract(extract_params)
        df = adapter.turn_logs_into_df(logs)
        if df.empty:
            print("No invoice logs found for the current window.")
            return

        formatted_df = _format_invoice_logs(df)
        if formatted_df.empty:
            print("Invoice logs could not be formatted; skipping upsert.")
            return

        db_connector.upsert_table("fact_kpis", formatted_df)
        print(f"{len(formatted_df)} invoice logs processed and upserted successfully.")

    @task(task_id="process_burn_bridge_logs")
    def process_burn_bridge_logs():
        adapter, w3 = _build_logs_adapter()
        db_connector = DbConnector()
        from_block = _resolve_from_block(db_connector, BURN_LOOKBACK_DAYS)
        to_block = w3.eth.block_number

        extract_params = {
            "from_block": from_block,
            "to_block": to_block,
            "chunk_size": CHUNK_SIZE,
            "topics": [BURN_TOPIC],
        }

        logs = adapter.extract(extract_params)
        df = adapter.turn_logs_into_df(logs)
        if df.empty:
            print("No burn/bridge logs found for the current window.")
            return

        formatted_df = _format_burn_logs(df, w3)
        if formatted_df.empty:
            print("Burn/bridge logs could not be formatted; skipping upsert.")
            return

        db_connector.upsert_table("fact_kpis", formatted_df)
        print(f"{len(formatted_df)} burn logs processed and upserted successfully.")

    @task(task_id="create_json_files")
    def create_json_files():
        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan, empty_cloudfront_cache
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        ### Load profit calculation data
        df_profit = execute_jinja_query(
            db_connector, 
            "api/quick_bites/linea_profit_calculation.sql.j2", 
            query_parameters={}, 
            return_df=True
        )
        
        if not df_profit.empty:
            # Convert date to unix timestamp (milliseconds)
            dt_profit = pd.to_datetime(df_profit['date'], errors="raise", utc=True)
            df_profit['unix_timestamp'] = (
                (dt_profit - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
            ).astype("int64")
            
            # Sort by date
            df_profit = df_profit.sort_values('date').reset_index(drop=True)
            
            # Get column names (handling potential lowercase conversion)
            cols = df_profit.columns.tolist()
            
            # Create data structure for profit calculation
            profit_dict = {
                "data": {
                    "daily": {
                        "types": ["unix"] + [col for col in cols if col not in ['date', 'unix_timestamp']],
                        "values": [
                            [row['unix_timestamp']] + [row[col] for col in cols if col not in ['date', 'unix_timestamp']]
                            for _, row in df_profit.iterrows()
                        ]
                    }
                }
            }
            
            profit_dict = fix_dict_nan(profit_dict, 'linea_profit_calculation', send_notification=False)
            upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/linea/profit_calculation', profit_dict, cf_distribution_id, invalidate=False)
            print("Profit calculation JSON uploaded successfully.")

        ### Load burn data
        df_burn = execute_jinja_query(
            db_connector,
            "api/quick_bites/linea_burn.sql.j2",
            query_parameters={},
            return_df=True
        )
        
        if not df_burn.empty:
            # Convert date to unix timestamp (milliseconds)
            dt_burn = pd.to_datetime(df_burn['date'], errors="raise", utc=True)
            df_burn['unix_timestamp'] = (
                (dt_burn - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
            ).astype("int64")
            
            # Sort by date
            df_burn = df_burn.sort_values('date').reset_index(drop=True)
            
            # Get column names
            cols = df_burn.columns.tolist()
            
            # Create data structure for burn data
            burn_dict = {
                "data": {
                    "daily": {
                        "types": ["unix"] + [col for col in cols if col not in ['date', 'unix_timestamp']],
                        "values": [
                            [row['unix_timestamp']] + [row[col] for col in cols if col not in ['date', 'unix_timestamp']]
                            for _, row in df_burn.iterrows()
                        ]
                    }
                }
            }
            
            burn_dict = fix_dict_nan(burn_dict, 'linea_burn', send_notification=False)
            upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/linea/burn', burn_dict, cf_distribution_id, invalidate=False)
            print("Burn data JSON uploaded successfully.")

        ### Load burn KPIs (totals)
        df_kpis = execute_jinja_query(
            db_connector,
            "api/quick_bites/linea_burn_kpis.sql.j2",
            query_parameters={},
            return_df=True
        )
        
        if not df_kpis.empty:
            # Assuming the KPIs query returns a single row with totals
            kpis_dict = {
                "data": df_kpis.iloc[0].to_dict()
            }
            
            kpis_dict = fix_dict_nan(kpis_dict, 'linea_burn_kpis', send_notification=True)
            upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/linea/kpis', kpis_dict, cf_distribution_id, invalidate=False)
            print("Burn KPIs JSON uploaded successfully.")

        ### Empty CloudFront cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/linea/*')
        print("CloudFront cache invalidated for Linea data.")

    @task(task_id="run_linea_burn_alerts")
    def run_linea_burn_alerts():
        """
        Discord alerts for the Linea token-burn tracker Quick Bite, on cumulative
        totals crossing fixed milestones:
          - total ETH burned: every 10 ETH
          - total LINEA tokens burned: every 10,000,000 LINEA
          - total revenue (gas fee income, USD, since 2025-09-11): every $1,000,000
          - total operating costs (USD, since 2025-09-11): every $1,000,000
          - total USD burned (USD value of ETH + LINEA burns combined): every $100,000
        Mirrors the QB's own KPI query but parameterized by date cutoff so we can
        compare today's totals to yesterday's totals without any state table.
        """
        import os
        import pandas as pd
        from src.misc.helper_functions import send_discord_message, generate_screenshot

        db_connector = DbConnector()
        qb_url = "https://www.growthepie.com/quick-bites/linea-burn"
        webhook_url = os.getenv("GTP_AI_WEBHOOK_URL")

        def _maybe_screenshot(label):
            try:
                fname = f"linea_burn_{label}_{pd.Timestamp.utcnow().strftime('%Y%m%d')}.png"
                generate_screenshot(qb_url, fname, width=1400, height=1200, wait_for_timeout=4000)
                return f"generated_images/{fname}"
            except Exception as e:
                print(f"⚠️ Screenshot failed for {label}: {e}")
                return None

        def _fmt_usd(v): return f"${v:,.0f}"
        def _fmt_int(v): return f"{int(v):,}"
        def _fmt_eth(v): return f"{v:,.2f} ETH"
        def _fmt_linea(v): return f"{v:,.0f} LINEA"

        def _kpi_totals(date_cutoff_sql):
            """Cumulative KPI totals for all dates strictly less than date_cutoff_sql.
            Mirrors api/quick_bites/linea_burn_kpis.sql.j2."""
            query = f"""
                WITH linea_tokens_eth AS (
                    SELECT
                        base."date",
                        base.lineaTokensBridged_linea,
                        base.ethBurnt_eth,
                        base.lineaTokensBridged_linea * linea_price.value AS lineaTokensBridged_usd,
                        base.ethBurnt_eth * eth_price.value AS ethBurnt_usd
                    FROM (
                        SELECT
                            "date",
                            origin_key,
                            MAX(CASE WHEN metric_key = 'qb_lineaTokensBridged_linea' THEN value END) AS lineaTokensBridged_linea,
                            MAX(CASE WHEN metric_key = 'qb_ethBurnt_eth' THEN value END) AS ethBurnt_eth
                        FROM public.fact_kpis
                        WHERE
                            (metric_key = 'qb_lineaTokensBridged_linea' OR metric_key = 'qb_ethBurnt_eth')
                            AND origin_key = 'linea'
                            AND "date" < {date_cutoff_sql}
                        GROUP BY "date", origin_key
                    ) base
                    LEFT JOIN (
                        SELECT "date", value FROM public.fact_kpis
                        WHERE metric_key = 'price_usd' AND origin_key = 'linea'
                    ) linea_price ON base.date = linea_price.date
                    LEFT JOIN (
                        SELECT "date", value FROM public.fact_kpis
                        WHERE metric_key = 'price_usd' AND origin_key = 'ethereum'
                    ) eth_price ON base.date = eth_price.date
                ),
                gas_operating_costs AS (
                    SELECT
                        base.date,
                        base.value AS gas_fee_income,
                        op.value AS operating_costs,
                        base.value * price.value AS gas_fee_income_usd,
                        op.value * price.value AS operating_costs_usd
                    FROM (
                        SELECT "date", value FROM public.fact_kpis
                        WHERE origin_key = 'linea'
                          AND metric_key = 'profit_eth'
                          AND "date" > '2025-09-10'
                          AND "date" < {date_cutoff_sql}
                    ) base
                    LEFT JOIN (
                        SELECT "date", value FROM public.fact_kpis
                        WHERE origin_key = 'linea' AND metric_key = 'qb_amountRequested_eth'
                    ) op ON base.date = op.date
                    LEFT JOIN (
                        SELECT "date", value FROM public.fact_kpis
                        WHERE metric_key = 'price_usd' AND origin_key = 'ethereum'
                    ) price ON base.date = price.date
                )
                SELECT
                    SUM(lineaTokensBridged_linea) AS totals_lineaTokensBridged_linea,
                    SUM(ethBurnt_eth) AS totals_ethBurnt_eth,
                    SUM(lineaTokensBridged_usd) AS totals_lineaTokensBridged_usd,
                    SUM(ethBurnt_usd) AS totals_ethBurnt_usd,
                    SUM(gas_fee_income_usd) AS totals_gas_fee_income_usd,
                    SUM(operating_costs_usd) AS totals_operating_costs_usd
                FROM linea_tokens_eth
                FULL OUTER JOIN gas_operating_costs ON linea_tokens_eth.date = gas_operating_costs.date
            """
            df = db_connector.execute_query(query, load_df=True)
            if df is None or df.empty:
                return {}
            row = df.iloc[0].to_dict()
            return {k: float(v) if pd.notna(v) else 0.0 for k, v in row.items()}

        today = _kpi_totals("CURRENT_DATE")
        prior = _kpi_totals("CURRENT_DATE - INTERVAL '1 day'")

        if not today or not prior:
            print("ℹ️ Linea KPI totals empty; skipping all milestone checks.")
            return

        # virtual metric: total USD burned = USD value of ETH burns + USD value of LINEA burns
        today['totals_burned_usd'] = today.get('totals_ethBurnt_usd', 0.0) + today.get('totals_lineaTokensBridged_usd', 0.0)
        prior['totals_burned_usd'] = prior.get('totals_ethBurnt_usd', 0.0) + prior.get('totals_lineaTokensBridged_usd', 0.0)

        checks = [
            # (key, threshold, label, fmt, headline)
            ('totals_ethBurnt_eth',                10,             "eth_burned_10",     _fmt_eth,   "Linea Burn — crossed `{m}` total ETH burned milestone"),
            ('totals_lineaTokensBridged_linea',    10_000_000,     "linea_burned_10m",  _fmt_linea, "Linea Burn — crossed `{m}` total LINEA burned milestone"),
            ('totals_gas_fee_income_usd',          1_000_000,      "revenue_1m",        _fmt_usd,   "Linea Burn — crossed `{m}` total revenue milestone (since 2025-09-11)"),
            ('totals_operating_costs_usd',         1_000_000,      "opcosts_1m",        _fmt_usd,   "Linea Burn — crossed `{m}` total operating costs milestone (since 2025-09-11)"),
            ('totals_burned_usd',                  100_000,        "usd_burned_100k",   _fmt_usd,   "Linea Burn — crossed `{m}` total USD burned milestone"),
        ]

        for key, threshold, label, fmt, headline_tpl in checks:
            t_now = float(today.get(key) or 0.0)
            t_prior = float(prior.get(key) or 0.0)
            n_now = int(t_now // threshold)
            n_before = int(t_prior // threshold)
            if n_now > n_before:
                milestone = n_now * threshold
                crossed = n_now - n_before
                extra = "" if crossed == 1 else f" (crossed {crossed} milestones in one day)"
                headline = headline_tpl.format(m=fmt(milestone))
                message = (
                    f"🥧 **{headline}**{extra}\n"
                    f"Cumulative total now `{fmt(t_now)}` (was `{fmt(t_prior)}` as of yesterday)\n"
                    f"[View on growthepie.com]({qb_url})"
                )
                send_discord_message(message, webhook_url, image_paths=_maybe_screenshot(label))
            else:
                print(f"ℹ️ {label}: {fmt(t_now)} below next milestone (n_now={n_now}, n_before={n_before}).")

    # Define task dependencies
    process_invoice_logs() >> process_burn_bridge_logs() >> create_json_files() >> run_linea_burn_alerts()


run_dag()

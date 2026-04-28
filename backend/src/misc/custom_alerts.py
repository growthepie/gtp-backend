import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

import pandas as pd

from src.db_connector import DbConnector
from src.misc.helper_functions import generate_screenshot, send_discord_message
from src.misc.jinja_helper import execute_jinja_query


def fmt_int(value: float) -> str:
    return f"{int(value):,}"


def fmt_usd(value: float) -> str:
    return f"${value:,.0f}"


def fmt_eth(value: float) -> str:
    return f"{value:,.2f} ETH"


def fmt_2dp(value: float) -> str:
    return f"{value:,.2f}"


@dataclass
class Alerter:
    url: str
    screenshot_prefix: str
    webhook_url: Optional[str] = None
    alerter_type: Optional[str] = None

    def __post_init__(self):
        if self.webhook_url is None:
            self.webhook_url = os.getenv("GTP_AI_WEBHOOK_URL")

    def _maybe_screenshot(self, label: str) -> Optional[str]:
        try:
            filename = f"{self.screenshot_prefix}_{label}_{pd.Timestamp.utcnow().strftime('%Y%m%d')}.png"
            generate_screenshot(self.url, filename, width=1400, height=1200, wait_for_timeout=4000)
            return f"generated_images/{filename}"
        except Exception as exc:
            print(f"Screenshot failed for {label}: {exc}")
            return None

    def send(self, message: str, label: Optional[str] = None):
        image_path = self._maybe_screenshot(label) if label else None
        send_discord_message(message, self.webhook_url, image_paths=image_path)


def prepare_series(df: pd.DataFrame, date_col: str = "date", value_col: str = "value") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[date_col, value_col])

    series = df[[date_col, value_col]].copy()
    series[date_col] = pd.to_datetime(series[date_col])
    series[value_col] = pd.to_numeric(series[value_col], errors="coerce")
    series = series.dropna(subset=[date_col, value_col]).sort_values(date_col).reset_index(drop=True)
    return series


def milestone_crossing(current_value: float, previous_value: float, threshold: float):
    current_bucket = int(current_value // threshold)
    previous_bucket = int(previous_value // threshold)
    if current_bucket <= previous_bucket:
        return None
    return current_bucket * threshold, current_bucket - previous_bucket


def milestone_suffix(crossed_count: int) -> str:
    return "" if crossed_count == 1 else f" (crossed {crossed_count} milestones in one day)"


def maybe_send_ath_alert(
    alerter: Alerter,
    df: pd.DataFrame,
    *,
    date_col: str,
    value_col: str,
    label: str,
    title: str,
    fmt: Callable[[float], str],
    when_text: Optional[Callable[[pd.Series], str]] = None,
) -> bool:
    series = prepare_series(df, date_col=date_col, value_col=value_col)
    if series.empty:
        print(f"{label}: no data; skipping.")
        return False

    latest_row = series.iloc[-1]
    latest_date = latest_row[date_col]
    latest_value = float(latest_row[value_col])
    prior = series[series[date_col] < latest_date]
    if prior.empty:
        print(f"{label}: only one period of data; skipping.")
        return False

    prior_max = float(prior[value_col].max())
    if latest_value <= prior_max:
        #print(f"{label}: latest {fmt(latest_value)} <= prior max {fmt(prior_max)}.")
        return False

    if prior_max > 0:
        delta_text = f"(prior max `{fmt(prior_max)}`, +{((latest_value - prior_max) / prior_max * 100):.1f}%)"
    else:
        delta_text = "(first recorded high)"

    period_text = when_text(latest_row) if when_text else f"on {latest_date.date()}"
    
    if alerter.alerter_type == 'applications':
        msg_link = f"[View on growthepie.com]({alerter.url}/{label})"
    else:
        msg_link = f"[View on growthepie.com]({alerter.url})"
    
    message = (
        f"🥧 **{title}**\n"
        f"`{fmt(latest_value)}` {period_text} {delta_text}\n"
        f"{msg_link}"
    )
    alerter.send(message, label=label)
    return True


def maybe_send_series_sum_milestone_alert(
    alerter: Alerter,
    df: pd.DataFrame,
    *,
    date_col: str,
    value_col: str,
    threshold: float,
    label: str,
    title: str,
    fmt: Callable[[float], str],
) -> bool:
    series = prepare_series(df, date_col=date_col, value_col=value_col)
    if series.empty:
        print(f"{label}: no data; skipping.")
        return False

    latest_row = series.iloc[-1]
    latest_date = latest_row[date_col]
    latest_value = float(latest_row[value_col])
    current_total = float(series[value_col].sum())
    previous_total = current_total - latest_value

    crossing = milestone_crossing(current_total, previous_total, threshold)
    if crossing is None:
        print(f"{label}: cumulative {fmt(current_total)} below next milestone.")
        return False

    milestone_value, crossed_count = crossing
    message = (
        f"🥧 **{title.format(m=fmt(milestone_value))}**{milestone_suffix(crossed_count)}\n"
        f"Cumulative total now `{fmt(current_total)}` "
        f"(was `{fmt(previous_total)}` before {latest_date.date()}'s `{fmt(latest_value)}`)\n"
        f"[View on growthepie.com]({alerter.url})"
    )
    alerter.send(message, label=label)
    return True


def maybe_send_running_total_milestone_alert(
    alerter: Alerter,
    df: pd.DataFrame,
    *,
    date_col: str,
    value_col: str,
    threshold: float,
    label: str,
    title: str,
    fmt: Callable[[float], str],
) -> bool:
    series = prepare_series(df, date_col=date_col, value_col=value_col)
    if series.empty:
        print(f"{label}: no data; skipping.")
        return False

    latest_row = series.iloc[-1]
    latest_date = latest_row[date_col]
    latest_value = float(latest_row[value_col])
    prior = series[series[date_col] < latest_date]
    previous_value = float(prior[value_col].iloc[-1]) if not prior.empty else 0.0

    crossing = milestone_crossing(latest_value, previous_value, threshold)
    if crossing is None:
        print(f"{label}: total {fmt(latest_value)} below next milestone.")
        return False

    milestone_value, crossed_count = crossing
    message = (
        f"🥧 **{title.format(m=fmt(milestone_value))}**{milestone_suffix(crossed_count)}\n"
        f"Total now `{fmt(latest_value)}` (was `{fmt(previous_value)}` before {latest_date.date()})\n"
        f"[View on growthepie.com]({alerter.url})"
    )
    alerter.send(message, label=label)
    return True


def run_totals_milestone_checks(
    alerter: Alerter,
    today: dict,
    prior: dict,
    checks: list[tuple[str, float, str, Callable[[float], str], str]],
):
    for key, threshold, label, fmt, title in checks:
        current_value = float(today.get(key) or 0.0)
        previous_value = float(prior.get(key) or 0.0)
        crossing = milestone_crossing(current_value, previous_value, threshold)
        if crossing is None:
            print(f"{label}: {fmt(current_value)} below next milestone.")
            continue

        milestone_value, crossed_count = crossing
        message = (
            f"🥧 **{title.format(m=fmt(milestone_value))}**{milestone_suffix(crossed_count)}\n"
            f"Cumulative total now `{fmt(current_value)}` (was `{fmt(previous_value)}` as of yesterday)\n"
            f"[View on growthepie.com]({alerter.url})"
        )
        alerter.send(message, label=label)


def load_fact_kpi_series(db_connector: DbConnector, origin_key: str, metric_key: str, days_back: int) -> pd.DataFrame:
    return execute_jinja_query(
        db_connector,
        "api/select_fact_kpis.sql.j2",
        query_parameters={"origin_key": origin_key, "metric_key": metric_key, "days": days_back},
        return_df=True,
    )


def _sql_string_list(values: list[str]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def load_app_metric_histories(
    db_connector: DbConnector,
    owner_projects: list[str],
    metric_key: str,
    origin_keys: list[str],
) -> pd.DataFrame:
    if not owner_projects or not origin_keys:
        return pd.DataFrame(columns=["owner_project", "date", "value"])

    owners_sql = _sql_string_list(owner_projects)
    origin_keys_sql = _sql_string_list(origin_keys)

    if metric_key == "daa":
        daily_raw_sql = f"""
            SELECT
                oli.owner_project,
                fact.date,
                COALESCE(hll_cardinality(hll_union_agg(fact.hll_addresses))::numeric, 0) AS value
            FROM public.fact_active_addresses_contract_hll fact
            JOIN public.vw_oli_label_pool_gold_pivoted_v2 oli USING (address, origin_key)
            WHERE oli.owner_project IN ({owners_sql})
              AND fact.origin_key IN ({origin_keys_sql})
              AND fact.date < CURRENT_DATE
            GROUP BY 1, 2
        """
    elif metric_key in {"txcount", "fees_paid_usd"}:
        daily_raw_sql = f"""
            SELECT
                fact.owner_project,
                fact.date,
                SUM(fact.{metric_key})::numeric AS value
            FROM public.vw_apps_contract_level_materialized fact
            WHERE fact.owner_project IN ({owners_sql})
              AND fact.origin_key IN ({origin_keys_sql})
              AND fact.date < CURRENT_DATE
            GROUP BY 1, 2
        """
    else:
        raise ValueError(f"Unsupported app ATH metric: {metric_key}")

    query = f"""
        WITH daily_raw AS (
            {daily_raw_sql}
        ),
        bounds AS (
            SELECT
                owner_project,
                MIN(date) AS min_date
            FROM daily_raw
            GROUP BY 1
        ),
        calendar AS (
            SELECT
                b.owner_project,
                gs::date AS date
            FROM bounds b
            CROSS JOIN LATERAL generate_series(
                b.min_date,
                CURRENT_DATE - INTERVAL '1 day',
                '1 day'::interval
            ) gs
        )
        SELECT
            c.owner_project,
            c.date,
            COALESCE(r.value, 0) AS value
        FROM calendar c
        LEFT JOIN daily_raw r
            ON r.owner_project = c.owner_project
           AND r.date = c.date
        ORDER BY 1, 2
    """
    df = db_connector.execute_query(query, load_df=True)
    if df is None or df.empty:
        return pd.DataFrame(columns=["owner_project", "date", "value"])

    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)
    return df


def run_app_ath_alerts():
    from src.main_config import get_main_config

    db_connector = DbConnector()
    alerter = Alerter(
        url="https://www.growthepie.com/applications/",
        screenshot_prefix="app_ath",
        alerter_type='applications'
    )
    app_metric_configs = {
        "txcount": {"metric_name": "Transaction Count", "fmt": fmt_int},
        "daa": {"metric_name": "Active Addresses", "fmt": fmt_int},
        "fees_paid_usd": {"metric_name": "Gas Fees Paid", "fmt": fmt_usd},
    }
    min_30d_txcount = 10_000
    min_history_days = 14

    main_config = get_main_config()
    app_origin_keys = [
        chain.origin_key
        for chain in main_config
        if chain.api_in_apps and chain.api_deployment_flag == "PROD"
    ]
    if not app_origin_keys:
        print("app_ath_alerts: no PROD app chains configured; skipping.")
        return

    apps_df = db_connector.get_active_projects(filtered_by_chains=app_origin_keys)
    if apps_df is None or apps_df.empty:
        print("app_ath_alerts: no active apps returned; skipping.")
        return

    apps_df = apps_df[apps_df["txcount"] >= min_30d_txcount].copy()
    if apps_df.empty:
        print(f"app_ath_alerts: no apps above {min_30d_txcount:,} tx in the last 30 days; skipping.")
        return

    apps_df["display_name"] = apps_df["display_name"].fillna(apps_df["name"])
    owner_projects = apps_df["name"].tolist()
    print(f"app_ath_alerts: checking {len(owner_projects)} apps across {len(app_origin_keys)} chains.")

    for metric_key, metric_conf in app_metric_configs.items():
        df_metric = load_app_metric_histories(db_connector, owner_projects, metric_key, app_origin_keys)
        if df_metric.empty:
            print(f"app_ath_alerts: no data returned for metric {metric_key}; skipping.")
            continue

        for app in apps_df.itertuples(index=False):
            df_app = df_metric[df_metric["owner_project"] == app.name]
            if df_app.empty:
                print(f"app_ath_alerts: no {metric_key} history for {app.name}; skipping.")
                continue

            if df_app["date"].nunique() < min_history_days:
                print(
                    f"app_ath_alerts: {app.name} has only {df_app['date'].nunique()} "
                    f"days of {metric_key} history; skipping."
                )
                continue

            maybe_send_ath_alert(
                alerter,
                df_app,
                date_col="date",
                value_col="value",
                label=f"{app.name}",
                title=f"New daily ATH - {app.display_name} {metric_conf['metric_name']} across tracked chains",
                fmt=metric_conf["fmt"],
            )


def run_pectra_type4_ath_alerts():
    db_connector = DbConnector()
    alerter = Alerter(
        url="https://www.growthepie.com/quick-bites/pectra-upgrade",
        screenshot_prefix="pectra_type4",
    )
    chains = ["ethereum", "optimism", "base", "unichain", "arbitrum"]
    chain_list_sql = ", ".join(f"'{chain}'" for chain in chains)

    query = f"""
        SELECT
            fk."date" AS day,
            fk.origin_key,
            fk.value
        FROM public.fact_kpis fk
        WHERE fk.metric_key = 'txcount_type4'
          AND fk.origin_key IN ({chain_list_sql})
          AND fk."date" >= DATE '2025-05-07'
          AND fk."date" < CURRENT_DATE
        ORDER BY fk."date" ASC
    """
    df = db_connector.execute_query(query, load_df=True)
    if df is None or df.empty:
        print("No type-4 data found; skipping ATH checks.")
        return

    df["day"] = pd.to_datetime(df["day"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)

    for origin_key, df_chain in df.groupby("origin_key"):
        maybe_send_ath_alert(
            alerter,
            df_chain,
            date_col="day",
            value_col="value",
            label=origin_key,
            title=f"New daily ATH - type-4 (EIP-7702) smart-wallet upgrade txs on `{origin_key.title()}`",
            fmt=fmt_int,
        )

    df_total = df.groupby("day", as_index=False)["value"].sum()
    series = prepare_series(df_total, date_col="day", value_col="value")
    if series.empty:
        return
    latest_row = series.iloc[-1]
    latest_day = latest_row["day"]
    latest_value = float(latest_row["value"])
    prior = series[series["day"] < latest_day]
    if prior.empty:
        print("combined: only one day of data; skipping.")
        return
    prior_max = float(prior["value"].max())
    if latest_value <= prior_max:
        print(f"combined: latest {fmt_int(latest_value)} <= prior max {fmt_int(prior_max)}.")
        return

    if prior_max > 0:
        delta_text = f"(prior daily max `{fmt_int(prior_max)}`, +{((latest_value - prior_max) / prior_max * 100):.1f}%)"
    else:
        delta_text = "(first recorded high)"
    chain_names = ", ".join(chain.title() for chain in chains)
    message = (
        "🥧 **New daily ATH - type-4 (EIP-7702) smart-wallet upgrade txs across all tracked chains**\n"
        f"`{fmt_int(latest_value)}` on {latest_day.date()} {delta_text}\n"
        f"Chains: {chain_names}\n"
        f"[View on growthepie.com]({alerter.url})"
    )
    alerter.send(message, label="combined")


def run_arbitrum_timeboost_alerts():
    db_connector = DbConnector()
    alerter = Alerter(
        url="https://www.growthepie.com/quick-bites/arbitrum-timeboost",
        screenshot_prefix="timeboost",
    )
    days_back = (datetime.now(timezone.utc) - datetime(2025, 4, 10, tzinfo=timezone.utc)).days

    df_usd = load_fact_kpi_series(db_connector, "arbitrum", "fees_paid_priority_usd", days_back)
    maybe_send_series_sum_milestone_alert(
        alerter,
        df_usd,
        date_col="date",
        value_col="value",
        threshold=1_000_000,
        label="usd_cum_1m",
        title="Arbitrum Timeboost - crossed `{m}` cumulative USD revenue milestone",
        fmt=fmt_usd,
    )

    df_eth = load_fact_kpi_series(db_connector, "arbitrum", "fees_paid_priority_eth", days_back)
    maybe_send_series_sum_milestone_alert(
        alerter,
        df_eth,
        date_col="date",
        value_col="value",
        threshold=1_000,
        label="eth_cum_1k",
        title="Arbitrum Timeboost - crossed `{m}` cumulative ETH revenue milestone",
        fmt=fmt_eth,
    )
    maybe_send_ath_alert(
        alerter,
        df_eth,
        date_col="date",
        value_col="value",
        label="daily_eth_ath",
        title="New daily ATH - Arbitrum Timeboost priority-fee revenue (ETH)",
        fmt=fmt_eth,
    )


def run_shopify_usdc_alerts():
    db_connector = DbConnector()
    alerter = Alerter(
        url="https://www.growthepie.com/quick-bites/base-commerce",
        screenshot_prefix="shopify_usdc",
    )
    days_back = (datetime.now(timezone.utc) - datetime(2025, 6, 1, tzinfo=timezone.utc)).days

    df_vol = load_fact_kpi_series(db_connector, "shopify_usdc", "gross_volume_usdc", days_back)
    maybe_send_series_sum_milestone_alert(
        alerter,
        df_vol,
        date_col="date",
        value_col="value",
        threshold=1_000_000,
        label="usd_cum_1m",
        title="Base Commerce - crossed `{m}` cumulative USDC volume settled milestone",
        fmt=fmt_usd,
    )
    maybe_send_ath_alert(
        alerter,
        df_vol,
        date_col="date",
        value_col="value",
        label="daily_usdc_ath",
        title="New daily ATH - Base Commerce USDC volume settled",
        fmt=fmt_usd,
    )

    df_customers = load_fact_kpi_series(db_connector, "shopify_usdc", "total_unique_payers", days_back)
    maybe_send_running_total_milestone_alert(
        alerter,
        df_customers,
        date_col="date",
        value_col="value",
        threshold=1_000,
        label="customers_1k",
        title="Base Commerce - crossed `{m}` total unique customers milestone",
        fmt=fmt_int,
    )

    df_returning = load_fact_kpi_series(db_connector, "shopify_usdc", "returning_payers", days_back)
    maybe_send_ath_alert(
        alerter,
        df_returning,
        date_col="date",
        value_col="value",
        label="daily_returning_ath",
        title="New daily ATH - Base Commerce returning customers",
        fmt=fmt_int,
    )

    today_utc = datetime.now(timezone.utc).date()
    returning_series = prepare_series(df_returning, date_col="date", value_col="value")
    if returning_series.empty:
        print("returning_payers: no data; skipping weekly and monthly ATH checks.")
        return

    if today_utc.weekday() == 0:
        weekly = returning_series.copy()
        weekly["week_start"] = weekly["date"].dt.to_period("W-SUN").dt.start_time
        weekly = weekly[weekly["week_start"] + pd.Timedelta(days=7) <= pd.Timestamp(today_utc)]
        weekly = weekly.groupby("week_start", as_index=False)["value"].sum().sort_values("week_start")
        maybe_send_ath_alert(
            alerter,
            weekly,
            date_col="week_start",
            value_col="value",
            label="weekly_returning_ath",
            title="New weekly ATH - Base Commerce returning customers",
            fmt=fmt_int,
            when_text=lambda row: f"for week of {row['week_start'].date()} - {(row['week_start'] + pd.Timedelta(days=6)).date()}",
        )
    else:
        print(f"weekly_returning_ath: skipped because {today_utc} is not Monday.")

    if today_utc.day == 1:
        monthly = returning_series.copy()
        monthly["month_start"] = monthly["date"].dt.to_period("M").dt.start_time
        monthly = monthly[monthly["month_start"] < pd.Timestamp(today_utc.replace(day=1))]
        monthly = monthly.groupby("month_start", as_index=False)["value"].sum().sort_values("month_start")
        maybe_send_ath_alert(
            alerter,
            monthly,
            date_col="month_start",
            value_col="value",
            label="monthly_returning_ath",
            title="New monthly ATH - Base Commerce returning customers",
            fmt=fmt_int,
            when_text=lambda row: f"for {row['month_start'].strftime('%B %Y')}",
        )
    else:
        print(f"monthly_returning_ath: skipped because {today_utc} is not the first of the month.")


def run_ethereum_scaling_alerts():
    db_connector = DbConnector()
    alerter = Alerter(
        url="https://www.growthepie.com/quick-bites/ethereum-scaling",
        screenshot_prefix="eth_scaling",
    )
    target_tps = 10_000
    step = 50

    def latest_month_tps(cutoff_sql: str):
        query = f"""
            SELECT
                date_trunc('month', date) AS month,
                (AVG(value) FILTER (WHERE metric_key = 'gas_limit'))
                    / NULLIF(AVG(value) FILTER (WHERE metric_key = 'block_time_seconds'), 0)
                    / 100000 / 2 AS tps
            FROM public.fact_kpis
            WHERE metric_key IN ('gas_limit', 'block_time_seconds')
              AND origin_key = 'ethereum'
              AND date < {cutoff_sql}
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 1
        """
        df = db_connector.execute_query(query, load_df=True)
        if df is None or df.empty or pd.isna(df.iloc[0]["tps"]):
            return None, None
        return pd.to_datetime(df.iloc[0]["month"]), float(df.iloc[0]["tps"])

    today_month, today_tps = latest_month_tps("CURRENT_DATE")
    prior_month, prior_tps = latest_month_tps("CURRENT_DATE - INTERVAL '1 day'")
    if today_tps is None or prior_tps is None or today_tps <= 0 or prior_tps <= 0:
        print(f"ethereum_scaling: insufficient TPS data (today={today_tps}, prior={prior_tps}).")
        return

    today_multiple = target_tps / today_tps
    prior_multiple = target_tps / prior_tps
    today_bucket = int(today_multiple // step)
    prior_bucket = int(prior_multiple // step)
    if today_multiple >= prior_multiple or today_bucket >= prior_bucket:
        print(
            f"ethereum_scaling: no downward {step}x crossing "
            f"(today={today_multiple:.1f}x, yesterday={prior_multiple:.1f}x)."
        )
        return

    threshold = (today_bucket + 1) * step
    crossed_count = prior_bucket - today_bucket
    month_note = ""
    if today_month is not None and prior_month is not None and today_month != prior_month:
        month_note = f" - new month rolled in ({prior_month.strftime('%b %Y')} -> {today_month.strftime('%b %Y')})"
    message = (
        f"🥧 **Ethereum Scaling - required multiplier to reach 10k TPS dropped below `{threshold}x`**"
        f"{milestone_suffix(crossed_count)}{month_note}\n"
        f"Now `{today_multiple:,.1f}x` (yesterday `{prior_multiple:,.1f}x`)\n"
        f"Latest monthly avg TPS `{today_tps:,.2f}` (prior view `{prior_tps:,.2f}`)\n"
        f"[View on growthepie.com]({alerter.url})"
    )
    alerter.send(message, label=f"mult_{threshold}x")


def run_eip8004_alerts():
    db_connector = DbConnector()
    alerter = Alerter(
        url="https://www.growthepie.com/quick-bites/eip8004",
        screenshot_prefix="eip8004",
    )

    def global_kpis(cutoff_sql: str) -> dict:
        query = f"""
            SELECT
                COUNT(*) FILTER (WHERE event = 'Registered') AS total_registrations,
                COUNT(DISTINCT agent_details->>'owner')
                    FILTER (WHERE event = 'Registered') AS unique_owners,
                (
                    SELECT COUNT(*) FROM (
                        SELECT origin_key, agent_id
                        FROM public.vw_eip8004_agents
                        WHERE event = 'NewFeedback'
                          AND date < {cutoff_sql}
                        GROUP BY 1, 2
                    ) sub
                ) AS services_with_reviewers
            FROM public.vw_eip8004_agents
            WHERE date < {cutoff_sql}
        """
        df = db_connector.execute_query(query, load_df=True)
        if df is None or df.empty:
            return {}
        row = df.iloc[0].to_dict()
        return {key: float(value) if pd.notna(value) else 0.0 for key, value in row.items()}

    today = global_kpis("CURRENT_DATE")
    prior = global_kpis("CURRENT_DATE - INTERVAL '1 day'")
    run_totals_milestone_checks(
        alerter,
        today,
        prior,
        [
            ("total_registrations", 50_000, "registrations_50k", fmt_int, "AI Agents - crossed `{m}` total registrations milestone"),
            ("unique_owners", 10_000, "owners_10k", fmt_int, "AI Agents - crossed `{m}` unique owners milestone"),
            ("services_with_reviewers", 10_000, "reviewed_10k", fmt_int, "AI Agents - crossed `{m}` services-with-reviewers milestone"),
        ],
    )

    daily_query = """
        SELECT
            date,
            event,
            COUNT(*) AS daily_count
        FROM public.vw_eip8004_agents
        WHERE event IN ('Registered', 'NewFeedback')
          AND date < CURRENT_DATE
        GROUP BY date, event
        ORDER BY date ASC
    """
    df_daily = db_connector.execute_query(daily_query, load_df=True)
    if df_daily is None or df_daily.empty:
        print("eip8004: no daily Registered/NewFeedback data; skipping daily ATH checks.")
    else:
        for event_name, friendly, label in [
            ("Registered", "registrations", "daily_registrations_ath"),
            ("NewFeedback", "reviews", "daily_reviews_ath"),
        ]:
            maybe_send_ath_alert(
                alerter,
                df_daily[df_daily["event"] == event_name],
                date_col="date",
                value_col="daily_count",
                label=label,
                title=f"New daily ATH - AI Agents {friendly}",
                fmt=fmt_int,
            )

    def per_chain_totals(cutoff_sql: str) -> dict:
        query = f"""
            SELECT
                origin_key,
                COUNT(*) AS total_registered
            FROM public.vw_eip8004_agents
            WHERE event = 'Registered'
              AND date < {cutoff_sql}
            GROUP BY origin_key
        """
        df = db_connector.execute_query(query, load_df=True)
        if df is None or df.empty:
            return {}
        return {row["origin_key"]: float(row["total_registered"]) for _, row in df.iterrows()}

    chain_today = per_chain_totals("CURRENT_DATE")
    chain_prior = per_chain_totals("CURRENT_DATE - INTERVAL '1 day'")
    for origin_key in sorted(chain_today):
        current_value = float(chain_today.get(origin_key) or 0.0)
        previous_value = float(chain_prior.get(origin_key) or 0.0)
        crossing = milestone_crossing(current_value, previous_value, 10_000)
        if crossing is None:
            print(f"chain_{origin_key}_10k: {fmt_int(current_value)} below next milestone.")
            continue

        milestone_value, crossed_count = crossing
        message = (
            f"🥧 **AI Agents - `{origin_key.title()}` crossed `{fmt_int(milestone_value)}` registered agents milestone**"
            f"{milestone_suffix(crossed_count)}\n"
            f"Total on {origin_key.title()} now `{fmt_int(current_value)}` "
            f"(was `{fmt_int(previous_value)}` as of yesterday)\n"
            f"[View on growthepie.com]({alerter.url})"
        )
        alerter.send(message, label=f"chain_{origin_key}_10k")


def run_fusaka_alerts():
    db_connector = DbConnector()
    alerter = Alerter(
        url="https://www.growthepie.com/quick-bites/fusaka",
        screenshot_prefix="fusaka",
    )
    dencun_block = 19_426_587

    daily_query = f"""
        SELECT
            block_date AS day,
            AVG(blob_gas_used::NUMERIC / 1024 / 128) AS avg_blob_count,
            AVG(target_blob_gas::NUMERIC / 1024 / 128) AS avg_target_blob_count,
            SUM(gas_used::NUMERIC) / NULLIF(COUNT(*), 0) AS avg_gas_used,
            SUM(gas_limit::NUMERIC) / NULLIF(COUNT(*), 0) AS avg_gas_limit
        FROM public.ethereum_blocks
        WHERE "number" >= {dencun_block}
          AND block_date < CURRENT_DATE
        GROUP BY block_date
        ORDER BY block_date ASC
    """
    df_daily = db_connector.execute_query(daily_query, load_df=True)
    if df_daily is None or df_daily.empty:
        print("fusaka: no daily blob/gas data since Dencun; skipping ATH checks.")
    else:
        for value_col, friendly, fmt, label in [
            ("avg_target_blob_count", "Ethereum L1 blob target (per-block)", fmt_2dp, "blob_target_ath"),
            ("avg_blob_count", "Ethereum L1 avg blob count (per-block)", fmt_2dp, "blob_count_ath"),
            ("avg_gas_limit", "Ethereum L1 gas limit (per-block avg)", fmt_int, "gas_limit_ath"),
            ("avg_gas_used", "Ethereum L1 gas used (per-block avg)", fmt_int, "gas_used_ath"),
        ]:
            maybe_send_ath_alert(
                alerter,
                df_daily,
                date_col="day",
                value_col=value_col,
                label=label,
                title=f"New daily ATH - {friendly}",
                fmt=fmt,
            )

    def cumulative_totals(cutoff_sql: str) -> dict:
        query = f"""
            SELECT
                FLOOR(SUM(blob_gas_used::NUMERIC) / 128 / 1024)::BIGINT AS total_blobs,
                SUM((blob_base_fee::NUMERIC * blob_gas_used::NUMERIC) / 1e18) AS total_blob_fees_eth
            FROM public.ethereum_blocks
            WHERE "number" >= {dencun_block}
              AND block_date < {cutoff_sql}
        """
        df = db_connector.execute_query(query, load_df=True)
        if df is None or df.empty:
            return {"total_blobs": 0.0, "total_blob_fees_eth": 0.0}
        row = df.iloc[0].to_dict()
        return {key: float(value) if pd.notna(value) else 0.0 for key, value in row.items()}

    run_totals_milestone_checks(
        alerter,
        cumulative_totals("CURRENT_DATE"),
        cumulative_totals("CURRENT_DATE - INTERVAL '1 day'"),
        [
            ("total_blobs", 5_000_000, "blobs_5m_since_dencun", fmt_int, "Fusaka - crossed `{m}` total blob count milestone (since Dencun)"),
            ("total_blob_fees_eth", 1_000, "blob_fees_1k_since_dencun", fmt_eth, "Fusaka - crossed `{m}` total blob fees paid milestone (since Dencun)"),
        ],
    )


def run_linea_burn_alerts():
    db_connector = DbConnector()
    alerter = Alerter(
        url="https://www.growthepie.com/quick-bites/linea-burn",
        screenshot_prefix="linea_burn",
    )

    def kpi_totals(cutoff_sql: str) -> dict:
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
                    WHERE (metric_key = 'qb_lineaTokensBridged_linea' OR metric_key = 'qb_ethBurnt_eth')
                      AND origin_key = 'linea'
                      AND "date" < {cutoff_sql}
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
                      AND "date" < {cutoff_sql}
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
        return {key: float(value) if pd.notna(value) else 0.0 for key, value in row.items()}

    today = kpi_totals("CURRENT_DATE")
    prior = kpi_totals("CURRENT_DATE - INTERVAL '1 day'")
    if not today or not prior:
        print("linea_burn: KPI totals empty; skipping.")
        return

    today["totals_burned_usd"] = today.get("totals_ethBurnt_usd", 0.0) + today.get("totals_lineaTokensBridged_usd", 0.0)
    prior["totals_burned_usd"] = prior.get("totals_ethBurnt_usd", 0.0) + prior.get("totals_lineaTokensBridged_usd", 0.0)

    run_totals_milestone_checks(
        alerter,
        today,
        prior,
        [
            ("totals_ethBurnt_eth", 10, "eth_burned_10", fmt_eth, "Linea Burn - crossed `{m}` total ETH burned milestone"),
            (
                "totals_lineaTokensBridged_linea",
                10_000_000,
                "linea_burned_10m",
                lambda value: f"{value:,.0f} LINEA",
                "Linea Burn - crossed `{m}` total LINEA burned milestone",
            ),
            ("totals_gas_fee_income_usd", 1_000_000, "revenue_1m", fmt_usd, "Linea Burn - crossed `{m}` total revenue milestone (since 2025-09-11)"),
            ("totals_operating_costs_usd", 1_000_000, "opcosts_1m", fmt_usd, "Linea Burn - crossed `{m}` total operating costs milestone (since 2025-09-11)"),
            ("totals_burned_usd", 100_000, "usd_burned_100k", fmt_usd, "Linea Burn - crossed `{m}` total USD burned milestone"),
        ],
    )


def run_robinhood_alerts():
    db_connector = DbConnector()
    alerter = Alerter(
        url="https://www.growthepie.com/quick-bites/robinhood-stock",
        screenshot_prefix="robinhood",
    )

    supply_query = """
        WITH date_range AS (
            SELECT generate_series(
                (SELECT MIN(date) FROM public.robinhood_daily),
                CURRENT_DATE - INTERVAL '1 day',
                '1 day'::interval
            )::date AS date
        ),
        all_stocks AS (
            SELECT contract_address
            FROM public.robinhood_stock_list
            WHERE active = TRUE
        ),
        grid AS (
            SELECT s.contract_address, d.date
            FROM all_stocks s CROSS JOIN date_range d
        ),
        daily_metrics AS (
            SELECT
                contract_address,
                date,
                SUM(CASE WHEN metric_key = 'total_minted' THEN value ELSE 0 END) -
                SUM(CASE WHEN metric_key = 'total_burned' THEN value ELSE 0 END) AS net_change
            FROM public.robinhood_daily
            WHERE metric_key IN ('total_minted', 'total_burned')
            GROUP BY contract_address, date
        ),
        joined AS (
            SELECT g.contract_address, g.date, COALESCE(dm.net_change, 0) AS net_change
            FROM grid g
            LEFT JOIN daily_metrics dm
                ON g.contract_address = dm.contract_address AND g.date = dm.date
        ),
        cum AS (
            SELECT
                contract_address,
                date,
                SUM(net_change) OVER (
                    PARTITION BY contract_address
                    ORDER BY date
                    ROWS UNBOUNDED PRECEDING
                ) AS total_supply
            FROM joined
        )
        SELECT date, SUM(total_supply) AS total_stocks_tokenized
        FROM cum
        GROUP BY date
        ORDER BY date
    """
    df_supply = db_connector.execute_query(supply_query, load_df=True)
    maybe_send_running_total_milestone_alert(
        alerter,
        df_supply,
        date_col="date",
        value_col="total_stocks_tokenized",
        threshold=1_000,
        label="supply_1k",
        title="Robinhood Stocks - crossed `{m}` total tokenized stocks milestone",
        fmt=fmt_int,
    )

    df_totals = execute_jinja_query(
        db_connector,
        "api/quick_bites/robinhood_totals_daily.sql.j2",
        query_parameters={},
        return_df=True,
    )
    if df_totals is None or df_totals.empty:
        print("robinhood: no totals data; skipping market value and 7d checks.")
        return

    maybe_send_running_total_milestone_alert(
        alerter,
        df_totals,
        date_col="date",
        value_col="total_market_value_sum",
        threshold=10_000_000,
        label="mv_10m",
        title="Robinhood Stocks - crossed `{m}` total market value milestone",
        fmt=fmt_usd,
    )

    totals_series = prepare_series(df_totals, date_col="date", value_col="total_market_value_sum")
    if len(totals_series) < 9:
        print("robinhood_pct7d: need at least 9 days of data.")
        return

    totals_series["pct_7d"] = totals_series["total_market_value_sum"].pct_change(periods=7) * 100
    latest_row = totals_series.iloc[-1]
    latest_pct = latest_row["pct_7d"]
    prior_pct = totals_series.iloc[-2]["pct_7d"]
    if pd.isna(latest_pct) or pd.isna(prior_pct):
        print(f"robinhood_pct7d: insufficient data (latest={latest_pct}, prior={prior_pct}).")
        return
    if prior_pct > 20.0 or latest_pct <= 20.0:
        print(f"robinhood_pct7d: latest {latest_pct:+.2f}%, prior {prior_pct:+.2f}% - no upward crossing.")
        return

    latest_day = latest_row["date"]
    latest_value = float(latest_row["total_market_value_sum"])
    message = (
        "🥧 **Robinhood Stocks - 7-day total market value change crossed above +20%**\n"
        f"Now `+{latest_pct:.2f}%` on {latest_day.date()} (yesterday `{prior_pct:+.2f}%`)\n"
        f"Total market value now `{fmt_usd(latest_value)}`\n"
        f"[View on growthepie.com]({alerter.url})"
    )
    alerter.send(message, label="pct7d_20")

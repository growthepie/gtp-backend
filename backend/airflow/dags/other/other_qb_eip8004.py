from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        "owner": "lorenz",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="other_qb_eip8004",
    description="Quick Bite on EIP-8004 + backfilling of raw tables for EIP-8004",
    tags=["other"],
    start_date=datetime(2026, 2, 13),
    schedule="41 1 * * *",  # Every day at 01:41
)
def run_dag():

    @task
    def get_eip8004_events():
        from src.adapters.adapter_eip8004 import EIP8004Adapter
        from src.db_connector import DbConnector
        
        db_connector = DbConnector()
        ad = EIP8004Adapter({}, db_connector)
        df = ad.extract({
            'chains': ['*'],
            'events': ['*']
        })
        df_uri = ad.extract_uri({
            'chains': ['*'],
            'df': df # alternatively: 'days_back': 3, scrapes metadata behind all URIs from the last 3 days
        })
        ad.load(df)
        ad.load_uri(df_uri)

    @task
    def create_jsons_for_quick_bite():
        import os
        import pandas as pd
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan, empty_cloudfront_cache

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        # json 1: table, top200 AI agents with metadata based on feedback count (gamable metric... no way around this)
        query = """
            WITH feedback AS (
                SELECT
                    origin_key,
                    agent_id,
                    COUNT(*) AS feedback_count_all,
                    COUNT(*) FILTER (
                        WHERE agent_details->>'rating' ~ '^[0-9]+\\.?[0-9]*$'
                        AND (agent_details->>'rating')::numeric BETWEEN 0 AND 100
                    ) AS feedback_count_valid,
                    ROUND(AVG((agent_details->>'rating')::numeric) FILTER (
                        WHERE agent_details->>'rating' ~ '^[0-9]+\\.?[0-9]*$'
                        AND (agent_details->>'rating')::numeric BETWEEN 0 AND 100
                    ), 2) AS avg_rating,
                    COUNT(DISTINCT agent_details->>'client') AS unique_clients
                FROM public.vw_eip8004_agents
                WHERE
                    event = 'NewFeedback'
                    AND agent_details ? 'rating'
                GROUP BY origin_key, agent_id
            ),
            uri_safe_services AS (
                SELECT DISTINCT ON (v.origin_key, v.agent_id)
                    v.origin_key,
                    v.agent_id,
                    CASE
                        WHEN v.agent_details->>'uri' = '' THEN NULL
                        WHEN v.agent_details->>'uri' LIKE 'data%'
                        OR v.agent_details->>'uri' LIKE 'ipfs%'
                        OR v.agent_details->>'uri' LIKE 'https://ipfs.io/ipfs/%'
                        OR v.agent_details->>'uri' LIKE '{%' THEN TRUE
                        WHEN v.agent_details->>'uri' LIKE 'http%' THEN FALSE
                    END AS safe_uri,
                    e.uri_json,
                    CASE WHEN jsonb_typeof(e.uri_json->'services') = 'array'
                        THEN e.uri_json->'services'
                        ELSE '[]'::jsonb
                    END AS services
                FROM public.vw_eip8004_agents v
                LEFT JOIN public.eip8004_uri e ON v.origin_key = e.origin_key AND v.agent_id = e.agent_id
                WHERE v."event" = 'Registered'
                ORDER BY v.origin_key, v.agent_id, v."date" DESC
            )
            SELECT
                v.origin_key,
                v.agent_id,
                u.uri_json->>'name' AS name,
                u.uri_json->>'image' AS image,
                u.uri_json->>'description' AS description,
                CASE
                    WHEN jsonb_typeof(u.uri_json->'x402Support') = 'boolean'
                        THEN (u.uri_json->>'x402Support')::boolean
                    WHEN jsonb_typeof(u.uri_json->'x402Support') = 'object'
                        THEN (u.uri_json->'x402Support'->>'enabled')::boolean
                    ELSE NULL
                END AS x402_support,
                (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                    WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) IN ('web', 'website', 'http') LIMIT 1) AS service_web_endpoint,
                (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                    WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) IN ('mcp', 'mcp server') LIMIT 1) AS service_mcp_endpoint,
                (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                    WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) IN ('a2a', 'a2a agent') LIMIT 1) AS service_a2a_endpoint,
                (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                    WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) = 'oasf' LIMIT 1) AS service_oasf_endpoint,
                (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                    WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) = 'ens' LIMIT 1) AS service_ens_endpoint,
                (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                    WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) = 'did' LIMIT 1) AS service_did_endpoint,
                (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                    WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) = 'email' LIMIT 1) AS service_email_endpoint,
                v.feedback_count_all,
                v.feedback_count_valid,
                v.avg_rating,
                v.unique_clients,
                u.safe_uri,
                u.uri_json
            FROM feedback v
            LEFT JOIN uri_safe_services u ON v.origin_key = u.origin_key AND v.agent_id = u.agent_id
            ORDER BY v.unique_clients DESC
            LIMIT 200;
        """
        df_top_agents = db_connector.execute_query(query, load_df=True)

        top_agents_columns = [
            "origin_key",
            "agent_id",
            "name",
            "image",
            "description",
            "x402_support",
            "service_web_endpoint",
            "service_mcp_endpoint",
            "service_a2a_endpoint",
            "service_oasf_endpoint",
            "service_ens_endpoint",
            "service_did_endpoint",
            "service_email_endpoint",
            "feedback_count_all",
            "feedback_count_valid",
            "avg_rating",
            "unique_clients",
            "safe_uri",
        ]
        top_agents_types = [
            "string",
            "number",
            "string",
            "string",
            "string",
            "boolean",
            "string",
            "string",
            "string",
            "string",
            "string",
            "string",
            "string",
            "number",
            "number",
            "number",
            "number",
            "boolean",
        ]
        top_agents_rows = [
            [row[col] for col in top_agents_columns]
            for _, row in df_top_agents.iterrows()
        ]
        top_agents_dict = {
            "data": {
                "top_agents": {
                    "columns": top_agents_columns,
                    "types": top_agents_types,
                    "rows": top_agents_rows,
                }
            }
        }
        top_agents_dict = fix_dict_nan(top_agents_dict, "eip8004_top_agents", send_notification=False)
        upload_json_to_cf_s3(
            s3_bucket,
            "v1/quick-bites/eip8004/top_agents",
            top_agents_dict,
            cf_distribution_id,
            invalidate=False,
        )

        # json 2 - chart, cumulative sum of total count 'Registered' and 'NewFeedback' events per day
        query = """
            SELECT
                date,
                event,
                COUNT(*) AS daily_count,
                (SUM(COUNT(*)) OVER (PARTITION BY event ORDER BY date))::integer AS cumulative_sum
            FROM public.vw_eip8004_agents
            WHERE event IN ('Registered', 'NewFeedback')
            GROUP BY date, event
            ORDER BY date, event;
        """
        df_cumulative = db_connector.execute_query(query, load_df=True)
        dt = pd.to_datetime(df_cumulative["date"], errors="raise", utc=True)
        df_cumulative["unix_timestamp"] = (
            (dt - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
        ).astype("int64")
        df_cumulative = df_cumulative.sort_values(["date", "event"]).reset_index(drop=True)

        # Pivot event series into chart columns (one column per event)
        df_cumulative_wide = (
            df_cumulative.pivot_table(
                index="unix_timestamp",
                columns="event",
                values="cumulative_sum",
                aggfunc="last",
            )
            .sort_index()
            .fillna(0)
        )
        df_daily_wide = (
            df_cumulative.pivot_table(
                index="unix_timestamp",
                columns="event",
                values="daily_count",
                aggfunc="last",
            )
            .sort_index()
            .fillna(0)
        )

        df_cumulative_wide.columns = [f"{col}_cumulative_sum" for col in df_cumulative_wide.columns]
        df_daily_wide.columns = [f"{col}_daily_count" for col in df_daily_wide.columns]

        df_cumulative_wide = pd.concat([df_cumulative_wide, df_daily_wide], axis=1).reset_index()
        event_columns = [col for col in df_cumulative_wide.columns if col != "unix_timestamp"]
        cumulative_dict = {
            "data": {
                "timeseries": {
                    "types": ["unix"] + event_columns,
                    "values": [
                        [int(row["unix_timestamp"])] + [int(row[col]) for col in event_columns]
                        for _, row in df_cumulative_wide.iterrows()
                    ],
                }
            }
        }
        cumulative_dict = fix_dict_nan(cumulative_dict, "eip8004_events_cumulative", send_notification=False)
        upload_json_to_cf_s3(
            s3_bucket,
            "v1/quick-bites/eip8004/events_cumulative",
            cumulative_dict,
            cf_distribution_id,
            invalidate=False,
        )

        # json 3 - table, breakdown by origin_key
        query = """
        SELECT 
            v.origin_key,
            MIN(v.date) FILTER (WHERE v.event = 'Registered') AS first_registered_date,
            COUNT(*) FILTER (WHERE v.event = 'Registered') AS total_registered,
            u.valid_registrations,
            COUNT(*) FILTER (WHERE v.event = 'NewFeedback') AS total_feedback,
            COUNT(DISTINCT v.agent_details->>'owner') FILTER (WHERE v.event = 'Registered') AS unique_owners,
            CASE 
                WHEN COUNT(DISTINCT v.agent_details->>'owner') FILTER (WHERE v.event = 'Registered') > 0 
                THEN (ROUND(
                    COUNT(*) FILTER (WHERE v.event = 'Registered')::numeric / 
                    COUNT(DISTINCT v.agent_details->>'owner') FILTER (WHERE v.event = 'Registered')::numeric, 
                    2
                ))::float
                ELSE NULL
            END AS agents_per_owner
        FROM public.vw_eip8004_agents v
        LEFT JOIN (
            SELECT origin_key, COUNT(*) AS valid_registrations
            FROM public.eip8004_uri
            WHERE uri_json != '{}'
            GROUP BY origin_key
        ) u ON v.origin_key = u.origin_key
        GROUP BY v.origin_key, u.valid_registrations
        ORDER BY total_registered DESC;
        """
        df_origin_breakdown = db_connector.execute_query(query, load_df=True)
        if "first_registered_date" in df_origin_breakdown.columns:
            df_origin_breakdown["first_registered_date"] = (
                pd.to_datetime(df_origin_breakdown["first_registered_date"], errors="coerce")
                .dt.strftime("%Y-%m-%d")
            )

        origin_breakdown_columns = [
            "origin_key",
            "first_registered_date",
            "total_registered",
            "valid_registrations",
            "total_feedback",
            "unique_owners",
            "agents_per_owner",
        ]
        origin_breakdown_types = [
            "string",
            "string",
            "number",
            "number",
            "number",
            "number",
            "number",
        ]
        origin_breakdown_rows = [
            [row[col] for col in origin_breakdown_columns]
            for _, row in df_origin_breakdown.iterrows()
        ]
        origin_breakdown_dict = {
            "data": {
                "origin_breakdown": {
                    "columns": origin_breakdown_columns,
                    "types": origin_breakdown_types,
                    "rows": origin_breakdown_rows,
                }
            }
        }
        origin_breakdown_dict = fix_dict_nan(origin_breakdown_dict, "eip8004_origin_breakdown", send_notification=False)
        upload_json_to_cf_s3(
            s3_bucket,
            "v1/quick-bites/eip8004/origin_breakdown",
            origin_breakdown_dict,
            cf_distribution_id,
            invalidate=False,
        )

        # json 3 chart, count of 'Registered' events per origin key over time
        query = """
            SELECT
                v.origin_key,
                s.name_short,
                s.colors->'dark'->>0 AS color,
                v.date,
                COUNT(*) AS registered_count
            FROM public.vw_eip8004_agents v
            LEFT JOIN public.sys_main_conf s ON v.origin_key = s.origin_key
            WHERE v.event = 'Registered'
            GROUP BY v.origin_key, s.name_short, s.colors->'dark'->>0, v.date
            ORDER BY v.date DESC, v.origin_key;
        """
        df_registered = db_connector.execute_query(query, load_df=True)
        dt_registered = pd.to_datetime(df_registered["date"], errors="raise", utc=True)
        df_registered["unix_timestamp"] = (
            (dt_registered - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
        ).astype("int64")
        df_registered = df_registered.sort_values(["date", "origin_key"]).reset_index(drop=True)

        # Pivot origin_key series into dynamic chart columns (supports new origin_keys automatically)
        df_registered_daily_wide = (
            df_registered.pivot_table(
                index="unix_timestamp",
                columns="origin_key",
                values="registered_count",
                aggfunc="last",
            )
            .sort_index()
            .fillna(0)
        )

        origin_keys = df_registered_daily_wide.columns.tolist()
        name_short_map = (
            df_registered[["origin_key", "name_short"]]
            .drop_duplicates(subset=["origin_key"])
            .set_index("origin_key")["name_short"]
            .to_dict()
        )
        color_map = (
            df_registered[["origin_key", "color"]]
            .drop_duplicates(subset=["origin_key"])
            .set_index("origin_key")["color"]
            .to_dict()
        )
        chain_names = [
            name_short_map.get(origin_key) if pd.notna(name_short_map.get(origin_key)) else origin_key
            for origin_key in origin_keys
        ]
        chain_colors = [
            color_map.get(origin_key) if pd.notna(color_map.get(origin_key)) else None
            for origin_key in origin_keys
        ]
        df_registered_daily_wide.columns = [f"{col}_registered_count" for col in origin_keys]
        df_registered_wide = df_registered_daily_wide.reset_index()
        registered_columns = [col for col in df_registered_wide.columns if col != "unix_timestamp"]
        registered_dict = {
            "data": {
                "names": chain_names,
                "colors": chain_colors,
                "timeseries": {
                    "types": ["unix"] + registered_columns,
                    "values": [
                        [int(row["unix_timestamp"])] + [int(row[col]) for col in registered_columns]
                        for _, row in df_registered_wide.iterrows()
                    ],
                }
            }
        }
        registered_dict = fix_dict_nan(registered_dict, "eip8004_registered_cumulative", send_notification=False)
        upload_json_to_cf_s3(
            s3_bucket,
            "v1/quick-bites/eip8004/registered_cumulative",
            registered_dict,
            cf_distribution_id,
            invalidate=False,
        )

        # json 4 kpis, total agents, unique owners, agents with feedback
        query = """
            SELECT 'total_agents' AS kpi, COUNT(*) AS value
            FROM public.vw_eip8004_agents
            WHERE event = 'Registered'

            UNION ALL

            SELECT 'unique_owners' AS kpi, COUNT(DISTINCT agent_details->>'owner') AS value
            FROM public.vw_eip8004_agents
            WHERE event = 'Registered'

            UNION ALL

            SELECT 'agents_with_feedback' AS kpi, COUNT(*) AS value
            FROM (
                SELECT origin_key, agent_id
                FROM public.vw_eip8004_agents
                WHERE event = 'NewFeedback'
                GROUP BY 1, 2
            ) sub;
        """
        df_kpis = db_connector.execute_query(query, load_df=True)
        kpi_values = {row["kpi"]: row["value"] for _, row in df_kpis.iterrows()}
        kpis_dict = {"data": kpi_values}
        kpis_dict = fix_dict_nan(kpis_dict, "eip8004_kpis", send_notification=True)
        upload_json_to_cf_s3(
            s3_bucket,
            "v1/quick-bites/eip8004/kpis",
            kpis_dict,
            cf_distribution_id,
            invalidate=False,
        )

        # json 4.1 pie chart, counting types of services mentioned in URIs (web, mcp, a2a, oasf, ens, did, email)
        query = """
            WITH uri_safe AS (
                SELECT
                    origin_key,
                    agent_id,
                    CASE WHEN jsonb_typeof(uri_json->'services') = 'array'
                        THEN uri_json->'services'
                        ELSE '[]'::jsonb
                    END AS services
                FROM public.eip8004_uri
            ),
            agents AS (
                SELECT
                    (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                        WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) IN ('web', 'website', 'http') LIMIT 1) AS service_web_endpoint,
                    (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                        WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) IN ('mcp', 'mcp server') LIMIT 1) AS service_mcp_endpoint,
                    (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                        WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) IN ('a2a', 'a2a agent') LIMIT 1) AS service_a2a_endpoint,
                    (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                        WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) = 'oasf' LIMIT 1) AS service_oasf_endpoint,
                    (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                        WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) = 'ens' LIMIT 1) AS service_ens_endpoint,
                    (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                        WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) = 'did' LIMIT 1) AS service_did_endpoint,
                    (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem
                        WHERE LOWER(COALESCE(elem->>'name', elem->>'type')) = 'email' LIMIT 1) AS service_email_endpoint
                FROM uri_safe u
            )
            SELECT
                COUNT(*) AS total_agents,
                COUNT(service_web_endpoint) AS web_count,
                COUNT(service_mcp_endpoint) AS mcp_count,
                COUNT(service_a2a_endpoint) AS a2a_count,
                COUNT(service_oasf_endpoint) AS oasf_count,
                COUNT(service_ens_endpoint) AS ens_count,
                COUNT(service_did_endpoint) AS did_count,
                COUNT(service_email_endpoint) AS email_count
            FROM agents;
        """
        df_service_counts = db_connector.execute_query(query, load_df=True)
        service_counts = df_service_counts.iloc[0].to_dict()
        service_order = ["web", "mcp", "a2a", "oasf", "ens", "did", "email"]
        service_count_rows = [
            [service, int(service_counts.get(f"{service}_count", 0) or 0)]
            for service in service_order
        ]
        service_counts_dict = {
            "data": {
                "service_counts": {
                    "columns": ["service", "count"],
                    "types": ["string", "number"],
                    "rows": service_count_rows,
                }
            }
        }
        service_counts_dict = fix_dict_nan(service_counts_dict, "eip8004_service_counts", send_notification=False)
        upload_json_to_cf_s3(
            s3_bucket,
            "v1/quick-bites/eip8004/service_counts",
            service_counts_dict,
            cf_distribution_id,
            invalidate=False,
        )

        # json 5 chart, percentage of agents with invalid URI metadata
        query = """
            SELECT 
                v.date,
                CASE WHEN u.uri_json = '{}' OR u.uri_json IS NULL THEN 'invalid_uri' ELSE 'valid_uri' END AS status,
                COUNT(*) AS value
            FROM public.vw_eip8004_agents v
            LEFT JOIN public.eip8004_uri u 
                ON v.origin_key = u.origin_key 
                AND v.agent_id = u.agent_id
            WHERE v.event = 'Registered'
            GROUP BY 1, 2
            ORDER BY 1;
        """
        df_invalid_uri = db_connector.execute_query(query, load_df=True)
        dt_invalid_uri = pd.to_datetime(df_invalid_uri["date"], errors="raise", utc=True)
        df_invalid_uri["unix_timestamp"] = (
            (dt_invalid_uri - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
        ).astype("int64")
        df_invalid_uri = df_invalid_uri.sort_values(["date", "status"]).reset_index(drop=True)
        df_invalid_uri_wide = (
            df_invalid_uri.pivot_table(
                index="unix_timestamp",
                columns="status",
                values="value",
                aggfunc="last",
            )
            .sort_index()
            .fillna(0)
            .reset_index()
        )
        status_columns = [col for col in df_invalid_uri_wide.columns if col != "unix_timestamp"]
        invalid_uri_dict = {
            "data": {
                "timeseries": {
                    "types": ["unix"] + status_columns,
                    "values": [
                        [int(row["unix_timestamp"])] + [int(row[col]) for col in status_columns]
                        for _, row in df_invalid_uri_wide.iterrows()
                    ],
                }
            }
        }
        invalid_uri_dict = fix_dict_nan(invalid_uri_dict, "eip8004_invalid_uri_daily", send_notification=False)
        upload_json_to_cf_s3(
            s3_bucket,
            "v1/quick-bites/eip8004/invalid_uri_daily",
            invalid_uri_dict,
            cf_distribution_id,
            invalidate=False,
        )

        # json 6 kpis, totals of agents valid vs invalid URIs
        query = """
            SELECT 
                CASE WHEN uri_json = '{}' THEN 'empty_uri' ELSE 'valid_uri' END AS status,
                COUNT(*) AS value,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)::float AS percentage
            FROM public.eip8004_uri
            GROUP BY 1;
        """
        df_uri_quality = db_connector.execute_query(query, load_df=True)
        uri_quality_dict = {
            "data": {
                "uri_quality": {
                    "columns": ["status", "value", "percentage"],
                    "types": ["string", "number", "number"],
                    "rows": [
                        [row["status"], row["value"], row["percentage"]]
                        for _, row in df_uri_quality.iterrows()
                    ],
                }
            }
        }
        uri_quality_dict = fix_dict_nan(uri_quality_dict, "eip8004_uri_quality", send_notification=False)
        upload_json_to_cf_s3(
            s3_bucket,
            "v1/quick-bites/eip8004/uri_quality",
            uri_quality_dict,
            cf_distribution_id,
            invalidate=False,
        )

        # Invalidate the whole eip8004 quick-bite folder once all uploads are done.
        empty_cloudfront_cache(cf_distribution_id, "/v1/quick-bites/eip8004/*")

    @task
    def run_eip8004_alerts():
        """
        Discord alerts for the EIP-8004 AI-agents Quick Bite:
          - total registrations crossing each 50,000 milestone
          - unique owners crossing each 10,000 milestone
          - services with reviewers (agents with NewFeedback) crossing each 10,000 milestone
          - new daily ATH for registrations (Registered events per day)
          - new daily ATH for reviews (NewFeedback events per day)
          - per-chain registrations crossing each 10,000 milestone
        Self-contained: no state table; cumulative checks compare today's view
        (date < CURRENT_DATE) to yesterday's view (date < CURRENT_DATE - 1).
        """
        import os
        import pandas as pd
        from src.misc.helper_functions import send_discord_message, generate_screenshot
        from src.db_connector import DbConnector

        db_connector = DbConnector()
        qb_url = "https://www.growthepie.com/quick-bites/eip8004"
        webhook_url = os.getenv("GTP_AI_WEBHOOK_URL")

        def _maybe_screenshot(label):
            try:
                fname = f"eip8004_{label}_{pd.Timestamp.utcnow().strftime('%Y%m%d')}.png"
                generate_screenshot(qb_url, fname, width=1400, height=1200, wait_for_timeout=4000)
                return f"generated_images/{fname}"
            except Exception as e:
                print(f"⚠️ Screenshot failed for {label}: {e}")
                return None

        def _fmt_int(v): return f"{int(v):,}"

        ## ---- global cumulative KPIs (today vs yesterday) ----
        def _global_kpis(date_cutoff_sql):
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
                              AND date < {date_cutoff_sql}
                            GROUP BY 1, 2
                        ) sub
                    ) AS services_with_reviewers
                FROM public.vw_eip8004_agents
                WHERE date < {date_cutoff_sql}
            """
            df = db_connector.execute_query(query, load_df=True)
            if df is None or df.empty:
                return {}
            row = df.iloc[0].to_dict()
            return {k: float(v) if pd.notna(v) else 0.0 for k, v in row.items()}

        today = _global_kpis("CURRENT_DATE")
        prior = _global_kpis("CURRENT_DATE - INTERVAL '1 day'")

        global_checks = [
            ('total_registrations',     50_000, "registrations_50k", "AI Agents — crossed `{m}` total registrations milestone"),
            ('unique_owners',           10_000, "owners_10k",        "AI Agents — crossed `{m}` unique owners milestone"),
            ('services_with_reviewers', 10_000, "reviewed_10k",      "AI Agents — crossed `{m}` services-with-reviewers milestone"),
        ]
        for key, threshold, label, headline_tpl in global_checks:
            t_now = float(today.get(key) or 0.0)
            t_prior = float(prior.get(key) or 0.0)
            n_now = int(t_now // threshold)
            n_before = int(t_prior // threshold)
            if n_now > n_before:
                milestone = n_now * threshold
                crossed = n_now - n_before
                extra = "" if crossed == 1 else f" (crossed {crossed} milestones in one day)"
                headline = headline_tpl.format(m=_fmt_int(milestone))
                message = (
                    f"🥧 **{headline}**{extra}\n"
                    f"Total now `{_fmt_int(t_now)}` (was `{_fmt_int(t_prior)}` as of yesterday)\n"
                    f"[View on growthepie.com]({qb_url})"
                )
                send_discord_message(message, webhook_url, image_paths=_maybe_screenshot(label))
            else:
                print(f"ℹ️ {label}: {_fmt_int(t_now)} below next milestone (n_now={n_now}, n_before={n_before}).")

        ## ---- daily ATH: Registered & NewFeedback ----
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
            print("No daily Registered/NewFeedback data; skipping daily ATH checks.")
        else:
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            df_daily['daily_count'] = pd.to_numeric(df_daily['daily_count'], errors='coerce').fillna(0)

            for evt, friendly, label in [
                ('Registered',  "registrations", "daily_registrations_ath"),
                ('NewFeedback', "reviews",       "daily_reviews_ath"),
            ]:
                series = df_daily[df_daily['event'] == evt].sort_values('date')
                if series.empty:
                    print(f"ℹ️ {label}: no data; skipping.")
                    continue
                latest_day = series['date'].iloc[-1]
                latest_value = float(series['daily_count'].iloc[-1])
                prior_s = series[series['date'] < latest_day]
                if prior_s.empty:
                    print(f"ℹ️ {label}: only one day of data — no prior max.")
                    continue
                prior_max = float(prior_s['daily_count'].max())
                if latest_value > prior_max:
                    if prior_max > 0:
                        delta_str = f"(prior daily max `{_fmt_int(prior_max)}`, +{((latest_value - prior_max) / prior_max * 100):.1f}%)"
                    else:
                        delta_str = "(first recorded high)"
                    message = (
                        f"🥧 **New daily ATH — AI Agents {friendly}**\n"
                        f"`{_fmt_int(latest_value)}` on {latest_day.date()} {delta_str}\n"
                        f"[View on growthepie.com]({qb_url})"
                    )
                    send_discord_message(message, webhook_url, image_paths=_maybe_screenshot(label))
                else:
                    print(f"ℹ️ {label}: latest {_fmt_int(latest_value)} ≤ prior max {_fmt_int(prior_max)}.")

        ## ---- per-chain registrations milestone (10k) ----
        def _per_chain_totals(date_cutoff_sql):
            query = f"""
                SELECT
                    origin_key,
                    COUNT(*) AS total_registered
                FROM public.vw_eip8004_agents
                WHERE event = 'Registered'
                  AND date < {date_cutoff_sql}
                GROUP BY origin_key
            """
            df = db_connector.execute_query(query, load_df=True)
            if df is None or df.empty:
                return {}
            return {row['origin_key']: float(row['total_registered']) for _, row in df.iterrows()}

        chain_today = _per_chain_totals("CURRENT_DATE")
        chain_prior = _per_chain_totals("CURRENT_DATE - INTERVAL '1 day'")
        threshold = 10_000

        for origin_key in sorted(chain_today.keys()):
            t_now = float(chain_today.get(origin_key) or 0.0)
            t_prior = float(chain_prior.get(origin_key) or 0.0)
            n_now = int(t_now // threshold)
            n_before = int(t_prior // threshold)
            if n_now > n_before:
                milestone = n_now * threshold
                crossed = n_now - n_before
                extra = "" if crossed == 1 else f" (crossed {crossed} milestones in one day)"
                message = (
                    f"🥧 **AI Agents — `{origin_key.title()}` crossed `{_fmt_int(milestone)}` registered agents milestone**{extra}\n"
                    f"Total on {origin_key.title()} now `{_fmt_int(t_now)}` (was `{_fmt_int(t_prior)}` as of yesterday)\n"
                    f"[View on growthepie.com]({qb_url})"
                )
                send_discord_message(message, webhook_url, image_paths=_maybe_screenshot(f"chain_{origin_key}_10k"))
            else:
                print(f"ℹ️ chain_{origin_key}: {_fmt_int(t_now)} below next 10k milestone.")

    get_eip8004_events_task = get_eip8004_events()
    create_jsons_for_quick_bite_task = create_jsons_for_quick_bite()
    eip8004_alerts_task = run_eip8004_alerts()

    get_eip8004_events_task >> create_jsons_for_quick_bite_task >> eip8004_alerts_task

run_dag()

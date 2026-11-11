-- Spikes found by the spike watch and what happened to each one.
-- Written by src/quick_bites/spike_watch (DAG: other_qb_spike_watch).
-- upsert_table() runs with create_table=False, so run this once manually before the first DAG run.
CREATE TABLE IF NOT EXISTS public.quick_bite_spike_events (
    origin_key          varchar     NOT NULL,
    metric_key          varchar     NOT NULL,
    "date"              date        NOT NULL,
    value               numeric,
    baseline_median     numeric,                -- median of the 28 days before "date"
    ratio               numeric,                -- value / baseline_median
    robust_z            numeric,                -- (value - median) / (1.4826 * MAD)
    status              varchar     NOT NULL,   -- detected | suppressed | over_daily_cap | investigating | not_newsworthy | dry_run | pr_opened | failed
    slug                varchar,                -- quick bite slug, once one was drafted
    pr_url              varchar,
    headline            varchar,                -- one-line finding from the investigation
    agent_cost_usd      numeric,
    error               varchar,
    detected_at         timestamptz,
    updated_at          timestamptz,
    PRIMARY KEY (origin_key, metric_key, "date")
);

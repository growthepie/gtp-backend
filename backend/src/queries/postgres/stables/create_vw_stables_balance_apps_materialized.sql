-- Stablecoin balance per application (OLI owner_project), chain and day, in USD and ETH.
-- Used by the app metric 'stables_balance' (app overview + app detail JSONs).
--
-- Rebuilt from fact_stables_balances with the current labels on every refresh, so label changes, newly mapped
-- stablecoins and backfills are reflected for all dates. Refreshed daily by metrics_stables_balances:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY public.vw_stables_balance_apps_materialized;
-- REFRESH must run as the view's owner: create it with the role Airflow uses.
--
-- Attribution: same cross-chain OLI labels as vw_apps_contract_level_materialized, so stablecoins line up with the
-- app's other metrics. Issuer-owned contracts and CEX wallets count as apps (decided 2026-10-21).
-- Prices: same logic as vw_fact_stables_balances_v1 (vw_fact_stables_mcap_v2 value_usd / value, USD fiat fallback 1).
-- Zero days: each (app, chain) has a row for every day from its first day with a balance to the chain's latest
-- measured day, so charts drop to 0 instead of gapping.

-- replaces the first version of this view (vw_stables_held_apps_materialized)
DROP MATERIALIZED VIEW IF EXISTS public.vw_stables_held_apps_materialized;
DROP MATERIALIZED VIEW IF EXISTS public.vw_stables_balance_apps_materialized;

CREATE MATERIALIZED VIEW public.vw_stables_balance_apps_materialized
TABLESPACE pg_default
AS WITH prices AS (
    SELECT DISTINCT ON ("date", origin_key, token_id)
        "date",
        origin_key,
        token_id,
        value_usd / NULLIF(value, 0) AS price_usd
    FROM public.vw_fact_stables_mcap_v2
    WHERE metric_key IN ('total_supply', 'track_on_l1')
      AND value > 0
    ORDER BY "date", origin_key, token_id, metric_key
),
held AS (
    SELECT
        b."date",
        b.origin_key,
        l.owner_project,
        SUM(b.balance * COALESCE(p.price_usd, CASE WHEN s.fiat = 'usd' THEN 1 END)) AS usd
    FROM public.fact_stables_balances b
    JOIN public.vw_oli_label_pool_gold_pivoted_cross_chain_v1 l
        ON l.address = b.address AND l.origin_key = b.origin_key AND l.owner_project IS NOT NULL
    LEFT JOIN public.sys_stables_v2 s ON s.token_id = b.token_id
    LEFT JOIN prices p ON p."date" = b."date" AND p.origin_key = b.origin_key AND p.token_id = b.token_id
    GROUP BY 1, 2, 3
),
latest AS (
    SELECT origin_key, MAX("date") AS "date"
    FROM public.fact_stables_balances
    GROUP BY 1
),
grid AS (
    SELECT p.owner_project, p.origin_key, d::date AS "date"
    FROM (SELECT owner_project, origin_key, MIN("date") AS first_date FROM held GROUP BY 1, 2) p
    JOIN latest l USING (origin_key)
    CROSS JOIN generate_series(p.first_date, l."date", interval '1 day') d
),
eth_price AS (
    SELECT "date", value AS price_usd
    FROM public.fact_kpis
    WHERE metric_key = 'price_usd' AND origin_key = 'ethereum' AND value > 0
)
SELECT
    g."date",
    g.origin_key,
    g.owner_project,
    COALESCE(h.usd, 0) AS stables_balance_usd,
    COALESCE(h.usd, 0) / e.price_usd AS stables_balance_eth
FROM grid g
LEFT JOIN held h USING (owner_project, origin_key, "date")
LEFT JOIN eth_price e USING ("date")
WITH DATA;

-- unique index: required for REFRESH ... CONCURRENTLY (readers such as the JSON DAG are not blocked during refresh)
CREATE UNIQUE INDEX idx_vw_stables_balance_apps_pk ON public.vw_stables_balance_apps_materialized USING btree (owner_project, origin_key, "date");
CREATE INDEX idx_vw_stables_balance_apps_date ON public.vw_stables_balance_apps_materialized USING btree ("date");

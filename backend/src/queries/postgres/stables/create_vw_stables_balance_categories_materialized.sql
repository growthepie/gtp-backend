-- Stablecoin balance per usage category (OLI usage_category = blockspace category_id), chain and day, in USD and ETH.
-- Used by the blockspace overview and category comparison JSONs (src/api/blockspace_json_creation.py).
--
-- Rebuilt from fact_stables_balances with the current labels on every refresh. Refreshed daily by metrics_stables_balances:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY public.vw_stables_balance_categories_materialized;
-- REFRESH must run as the view's owner: create it with the role Airflow uses.
--
-- Attribution: same cross-chain OLI labels as blockspace_fact_category_level, so categories line up with gas fees / txcount.
-- Tracked addresses without a usage_category count as 'unlabeled' (like unlabeled blockspace). Shares in the JSONs are
-- relative to the balance of all tracked addresses, not the chain's stablecoin supply.
-- Prices: same logic as vw_stables_balance_apps_materialized (vw_fact_stables_mcap_v2 value_usd / value, USD fiat fallback 1).
-- Zero days: each (category, chain) has a row for every day from its first day with a balance to the chain's latest
-- measured day, so charts drop to 0 instead of gapping.

DROP MATERIALIZED VIEW IF EXISTS public.vw_stables_balance_categories_materialized;

CREATE MATERIALIZED VIEW public.vw_stables_balance_categories_materialized
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
        COALESCE(l.usage_category, 'unlabeled') AS category_id,
        SUM(b.balance * COALESCE(p.price_usd, CASE WHEN s.fiat = 'usd' THEN 1 END)) AS usd
    FROM public.fact_stables_balances b
    LEFT JOIN public.vw_oli_label_pool_gold_pivoted_cross_chain_v1 l
        ON l.address = b.address AND l.origin_key = b.origin_key
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
    SELECT p.category_id, p.origin_key, d::date AS "date"
    FROM (SELECT category_id, origin_key, MIN("date") AS first_date FROM held GROUP BY 1, 2) p
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
    g.category_id,
    COALESCE(h.usd, 0) AS stables_balance_usd,
    COALESCE(h.usd, 0) / e.price_usd AS stables_balance_eth
FROM grid g
LEFT JOIN held h USING (category_id, origin_key, "date")
LEFT JOIN eth_price e USING ("date")
WITH DATA;

-- unique index: required for REFRESH ... CONCURRENTLY (readers such as the JSON DAG are not blocked during refresh)
CREATE UNIQUE INDEX idx_vw_stables_balance_categories_pk ON public.vw_stables_balance_categories_materialized USING btree (category_id, origin_key, "date");
CREATE INDEX idx_vw_stables_balance_categories_date ON public.vw_stables_balance_categories_materialized USING btree ("date");

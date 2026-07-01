DROP MATERIALIZED VIEW IF EXISTS public.vw_apps_contract_level_materialized;

CREATE MATERIALIZED VIEW public.vw_apps_contract_level_materialized
TABLESPACE pg_default
AS WITH labeled_facts AS (
    SELECT
        fact.date,
        fact.address,
        fact.origin_key,
        oli.contract_name,
        oli.owner_project,
        oli.usage_category,
        fact.txcount,
        fact.gas_fees_eth,
        fact.gas_fees_usd,
        fact.daa,
        fact.success_rate
    FROM public.blockspace_fact_contract_level fact
    JOIN public.vw_oli_label_pool_gold_pivoted_cross_chain_v1 oli
        ON fact.address = oli.address
        AND fact.origin_key = oli.origin_key
    WHERE oli.owner_project IS NOT NULL
)
SELECT
    labeled_facts.date,
    labeled_facts.address,
    labeled_facts.origin_key,
    labeled_facts.contract_name,
    labeled_facts.owner_project,
    cat.main_category_id AS main_category_key,
    labeled_facts.usage_category AS sub_category_key,
    sum(labeled_facts.txcount) AS txcount,
    sum(labeled_facts.gas_fees_eth) AS fees_paid_eth,
    sum(labeled_facts.gas_fees_usd) AS fees_paid_usd,
    sum(labeled_facts.daa) AS daa,
    sum(labeled_facts.success_rate) AS success_rate
FROM labeled_facts
LEFT JOIN public.oli_categories cat
    ON labeled_facts.usage_category = cat.category_id::text
GROUP BY
    labeled_facts.date,
    labeled_facts.address,
    labeled_facts.origin_key,
    labeled_facts.contract_name,
    labeled_facts.owner_project,
    cat.main_category_id,
    labeled_facts.usage_category
WITH DATA;

CREATE INDEX idx_date ON public.vw_apps_contract_level_materialized USING btree (date);
CREATE INDEX idx_origin_key ON public.vw_apps_contract_level_materialized USING btree (origin_key);
CREATE INDEX idx_owner_project ON public.vw_apps_contract_level_materialized USING btree (owner_project);

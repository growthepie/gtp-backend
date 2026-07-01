DROP MATERIALIZED VIEW IF EXISTS public.vw_oli_label_pool_gold_pivoted_cross_chain_v1;

CREATE MATERIALIZED VIEW public.vw_oli_label_pool_gold_pivoted_cross_chain_v1
TABLESPACE pg_default
AS WITH exact_labels AS (
    SELECT
        address,
        origin_key,
        caip2,
        contract_name,
        owner_project,
        usage_category,
        is_factory_contract,
        deployment_tx,
        deployer_address,
        deployment_date
    FROM public.vw_oli_label_pool_gold_pivoted_v2
),
cross_chain_labels AS (
    SELECT
        address,
        CASE
            WHEN count(DISTINCT contract_name) FILTER (WHERE contract_name IS NOT NULL) <= 1
            THEN min(contract_name) FILTER (WHERE contract_name IS NOT NULL)
        END AS contract_name,
        CASE
            WHEN count(DISTINCT owner_project) FILTER (WHERE owner_project IS NOT NULL) <= 1
            THEN min(owner_project) FILTER (WHERE owner_project IS NOT NULL)
        END AS owner_project,
        CASE
            WHEN count(DISTINCT usage_category) FILTER (WHERE usage_category IS NOT NULL) <= 1
            THEN min(usage_category) FILTER (WHERE usage_category IS NOT NULL)
        END AS usage_category,
        CASE
            WHEN count(DISTINCT is_factory_contract) FILTER (WHERE is_factory_contract IS NOT NULL) <= 1
            THEN bool_or(is_factory_contract) FILTER (WHERE is_factory_contract IS NOT NULL)
        END AS is_factory_contract,
        array_agg(DISTINCT origin_key ORDER BY origin_key) AS source_origin_keys
    FROM exact_labels
    GROUP BY address
),
address_origin_pairs AS (
    SELECT DISTINCT
        address,
        origin_key
    FROM public.blockspace_fact_contract_level

    UNION

    SELECT
        address,
        origin_key
    FROM exact_labels
)
SELECT
    pairs.address,
    pairs.origin_key,
    sys.caip2,
    coalesce(exact.contract_name, cross_chain.contract_name) AS contract_name,
    coalesce(exact.owner_project, cross_chain.owner_project) AS owner_project,
    coalesce(exact.usage_category, cross_chain.usage_category) AS usage_category,
    coalesce(exact.is_factory_contract, cross_chain.is_factory_contract) AS is_factory_contract,
    exact.deployment_tx,
    exact.deployer_address,
    exact.deployment_date,
    CASE
        WHEN exact.address IS NOT NULL THEN 'exact'
        ELSE 'cross_chain'
    END AS label_source,
    exact.address IS NOT NULL AS has_exact_label,
    CASE
        WHEN exact.address IS NOT NULL THEN ARRAY[pairs.origin_key]
        ELSE cross_chain.source_origin_keys
    END AS source_origin_keys
FROM address_origin_pairs pairs
LEFT JOIN exact_labels exact
    ON pairs.address = exact.address
    AND pairs.origin_key = exact.origin_key
LEFT JOIN cross_chain_labels cross_chain
    ON pairs.address = cross_chain.address
LEFT JOIN public.sys_main_conf sys
    ON pairs.origin_key = sys.origin_key
WHERE exact.address IS NOT NULL
    OR coalesce(
        cross_chain.contract_name,
        cross_chain.owner_project,
        cross_chain.usage_category,
        cross_chain.is_factory_contract::text
    ) IS NOT NULL
WITH DATA;

CREATE INDEX idx_vw_oli_cross_chain_address_origin
    ON public.vw_oli_label_pool_gold_pivoted_cross_chain_v1 USING btree (address, origin_key);
CREATE INDEX idx_vw_oli_cross_chain_origin_key
    ON public.vw_oli_label_pool_gold_pivoted_cross_chain_v1 USING btree (origin_key);
CREATE INDEX idx_vw_oli_cross_chain_owner_project
    ON public.vw_oli_label_pool_gold_pivoted_cross_chain_v1 USING btree (owner_project);
CREATE INDEX idx_vw_oli_cross_chain_usage_category
    ON public.vw_oli_label_pool_gold_pivoted_cross_chain_v1 USING btree (usage_category);

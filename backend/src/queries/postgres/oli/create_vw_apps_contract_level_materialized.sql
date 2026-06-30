    DROP MATERIALIZED VIEW IF EXISTS public.vw_apps_contract_level_materialized;

    CREATE MATERIALIZED VIEW public.vw_apps_contract_level_materialized
    TABLESPACE pg_default
    AS WITH exact_labels AS (
        SELECT
            address,
            origin_key,
            contract_name,
            owner_project,
            usage_category
        FROM public.vw_oli_label_pool_gold_pivoted_v2
        WHERE owner_project IS NOT NULL
    ),
    cross_chain_labels AS (
        SELECT
            address,
            min(contract_name) FILTER (WHERE contract_name IS NOT NULL) AS contract_name,
            min(owner_project) AS owner_project,
            min(usage_category) FILTER (WHERE usage_category IS NOT NULL) AS usage_category
        FROM exact_labels
        GROUP BY address
        HAVING count(DISTINCT owner_project) = 1
            AND count(DISTINCT usage_category) FILTER (WHERE usage_category IS NOT NULL) <= 1
    ),
    labeled_facts AS (
        SELECT
            fact.date,
            fact.address,
            fact.origin_key,
            coalesce(exact.contract_name, cross_chain.contract_name) AS contract_name,
            coalesce(exact.owner_project, cross_chain.owner_project) AS owner_project,
            coalesce(exact.usage_category, cross_chain.usage_category) AS usage_category,
            fact.txcount,
            fact.gas_fees_eth,
            fact.gas_fees_usd,
            fact.daa,
            fact.success_rate
        FROM public.blockspace_fact_contract_level fact
        LEFT JOIN exact_labels exact
            ON fact.address = exact.address
            AND fact.origin_key = exact.origin_key
        LEFT JOIN cross_chain_labels cross_chain
            ON fact.address = cross_chain.address
        WHERE coalesce(exact.owner_project, cross_chain.owner_project) IS NOT NULL
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

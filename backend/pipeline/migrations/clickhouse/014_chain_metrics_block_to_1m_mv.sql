-- Roll up block-level chain metrics to 1-minute granularity
CREATE MATERIALIZED VIEW IF NOT EXISTS gtp.chain_metrics_block_to_1m_mv
TO gtp.chain_metrics_1m
AS SELECT
    toStartOfMinute(timestamp) AS timestamp,
    origin_key,
    count() AS block_count,
    min(block_number) AS block_min,
    max(block_number) AS block_max,
    sum(txcount) AS txcount,
    sum(txcount_failed) AS txcount_failed,
    sum(txcount_contract_creation) AS txcount_contract_creation,
    sum(gas_used) AS gas_used,
    sum(gas_limit) AS gas_limit,
    sum(fee_native) AS fee_native,
    sum(fee_eth) AS fee_eth,
    sum(fee_usd) AS fee_usd,
    -- For medians, we take the average of block medians (approximation)
    avg(median_fee_native) AS median_fee_native,
    avg(median_fee_usd) AS median_fee_usd,
    avg(median_fee_eth) AS median_fee_eth
FROM gtp.chain_metrics_block
GROUP BY timestamp, origin_key

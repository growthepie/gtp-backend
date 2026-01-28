-- Roll up block-level contract metrics to 1-minute granularity
CREATE MATERIALIZED VIEW IF NOT EXISTS gtp.contract_metrics_block_to_1m_mv
TO gtp.contract_metrics_1m
AS SELECT
    toStartOfMinute(timestamp) AS timestamp,
    origin_key,
    contract_address,
    sum(txcount) AS txcount,
    sum(txcount_failed) AS txcount_failed,
    sum(gas_used) AS gas_used,
    sum(gas_limit) AS gas_limit,
    sum(fee_native) AS fee_native,
    sum(fee_eth) AS fee_eth,
    sum(fee_usd) AS fee_usd,
    avg(median_fee_native) AS median_fee_native,
    avg(median_fee_usd) AS median_fee_usd,
    avg(median_fee_eth) AS median_fee_eth
FROM gtp.contract_metrics_block
GROUP BY timestamp, origin_key, contract_address

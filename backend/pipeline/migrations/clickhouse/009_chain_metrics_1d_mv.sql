CREATE MATERIALIZED VIEW IF NOT EXISTS gtp.chain_metrics_1d_mv
TO gtp.chain_metrics_1d
AS SELECT
    toDate(hour_ts) AS day_ts,
    origin_key,
    sum(block_count) AS block_count,
    sum(txcount) AS txcount,
    sum(txcount_failed) AS txcount_failed,
    sum(txcount_contract_creation) AS txcount_contract_creation,
    sum(gas_used) AS gas_used,
    sum(gas_limit) AS gas_limit,
    sum(fee_native) AS fee_native,
    sum(fee_eth) AS fee_eth,
    sum(fee_usd) AS fee_usd
FROM gtp.chain_metrics_1h
GROUP BY day_ts, origin_key

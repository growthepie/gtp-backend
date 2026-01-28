CREATE MATERIALIZED VIEW IF NOT EXISTS gtp.contract_metrics_1h_mv
TO gtp.contract_metrics_1h
AS SELECT
    toStartOfHour(timestamp) AS hour_ts,
    origin_key,
    contract_address,
    sum(txcount) AS txcount,
    sum(txcount_failed) AS txcount_failed,
    sum(gas_used) AS gas_used,
    sum(gas_limit) AS gas_limit,
    sum(fee_native) AS fee_native,
    sum(fee_eth) AS fee_eth,
    sum(fee_usd) AS fee_usd
FROM gtp.contract_metrics_1m
GROUP BY hour_ts, origin_key, contract_address

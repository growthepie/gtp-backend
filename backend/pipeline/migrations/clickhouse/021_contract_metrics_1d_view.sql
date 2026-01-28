-- View that aggregates contract_metrics_1d for clean querying
CREATE OR REPLACE VIEW gtp.contract_metrics_1d_v AS
SELECT 
    day_ts,
    origin_key,
    contract_address,
    sum(txcount) as txcount,
    sum(txcount_failed) as txcount_failed,
    sum(gas_used) as gas_used,
    sum(gas_limit) as gas_limit,
    sum(fee_native) as fee_native,
    sum(fee_eth) as fee_eth,
    sum(fee_usd) as fee_usd
FROM gtp.contract_metrics_1d
GROUP BY day_ts, origin_key, contract_address

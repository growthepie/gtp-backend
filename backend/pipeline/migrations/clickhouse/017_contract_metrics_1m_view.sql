-- View that aggregates contract_metrics_1m for clean querying
CREATE OR REPLACE VIEW gtp.contract_metrics_1m_v AS
SELECT 
    timestamp,
    origin_key,
    contract_address,
    sum(txcount) as txcount,
    sum(txcount_failed) as txcount_failed,
    sum(gas_used) as gas_used,
    sum(gas_limit) as gas_limit,
    sum(fee_native) as fee_native,
    sum(fee_eth) as fee_eth,
    sum(fee_usd) as fee_usd,
    avg(median_fee_native) as median_fee_native,
    avg(median_fee_usd) as median_fee_usd,
    avg(median_fee_eth) as median_fee_eth
FROM gtp.contract_metrics_1m
GROUP BY timestamp, origin_key, contract_address

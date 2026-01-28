-- View that aggregates chain_metrics_1d for clean querying
CREATE OR REPLACE VIEW gtp.chain_metrics_1d_v AS
SELECT 
    day_ts,
    origin_key,
    sum(block_count) as block_count,
    sum(txcount) as txcount,
    sum(txcount_failed) as txcount_failed,
    sum(txcount_contract_creation) as txcount_contract_creation,
    sum(gas_used) as gas_used,
    sum(gas_limit) as gas_limit,
    sum(fee_native) as fee_native,
    sum(fee_eth) as fee_eth,
    sum(fee_usd) as fee_usd
FROM gtp.chain_metrics_1d
GROUP BY day_ts, origin_key

-- Contract metrics daily rollup
CREATE TABLE IF NOT EXISTS gtp.contract_metrics_1d (
    day_ts Date,
    origin_key LowCardinality(String),
    contract_address String,
    
    txcount UInt64,
    txcount_failed UInt64,
    gas_used UInt64,
    gas_limit UInt64,
    fee_native Float64,
    fee_eth Float64,
    fee_usd Float64
    
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day_ts)
ORDER BY (origin_key, contract_address, day_ts)
SETTINGS index_granularity = 8192

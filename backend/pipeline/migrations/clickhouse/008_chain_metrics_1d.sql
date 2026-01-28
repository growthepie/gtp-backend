-- Chain metrics daily rollup
CREATE TABLE IF NOT EXISTS gtp.chain_metrics_1d (
    day_ts Date,
    origin_key LowCardinality(String),
    
    block_count UInt64,
    txcount UInt64,
    txcount_failed UInt64,
    txcount_contract_creation UInt64,
    gas_used UInt64,
    gas_limit UInt64,
    fee_native Float64,
    fee_eth Float64,
    fee_usd Float64
    
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day_ts)
ORDER BY (origin_key, day_ts)
SETTINGS index_granularity = 8192

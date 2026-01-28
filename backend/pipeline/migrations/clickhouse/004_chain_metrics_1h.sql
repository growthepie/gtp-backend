-- Chain metrics hourly rollup
CREATE TABLE IF NOT EXISTS gtp.chain_metrics_1h (
    hour_ts DateTime,
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
PARTITION BY toYYYYMM(hour_ts)
ORDER BY (origin_key, hour_ts)
TTL hour_ts + INTERVAL 90 DAY
SETTINGS index_granularity = 8192

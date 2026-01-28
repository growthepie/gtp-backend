CREATE TABLE IF NOT EXISTS gtp.contract_metrics_1m (
    timestamp DateTime,
    origin_key LowCardinality(String),
    contract_address String,
    
    -- Transaction metrics
    txcount UInt64,
    txcount_failed UInt64,
    
    -- Gas metrics
    gas_used UInt64,
    gas_limit UInt64,
    
    -- Fee metrics
    fee_native Float64,
    fee_eth Float64,
    fee_usd Float64,
    
    -- Median fees (pre-computed per minute)
    median_fee_native Float64,
    median_fee_usd Float64,
    median_fee_eth Float64
    
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (origin_key, contract_address, timestamp)
TTL timestamp + INTERVAL 7 DAY
SETTINGS index_granularity = 8192

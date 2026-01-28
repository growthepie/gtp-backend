-- Block-level chain metrics (one row per block)
-- This is the source of truth, rolled up to 1m via MV

CREATE TABLE IF NOT EXISTS gtp.chain_metrics_block (
    timestamp DateTime,
    origin_key LowCardinality(String),
    block_number UInt64,
    
    -- Transaction metrics
    txcount UInt32,
    txcount_failed UInt32,
    txcount_contract_creation UInt32,
    
    -- Gas metrics
    gas_used UInt64,
    gas_limit UInt64,
    
    -- Fee metrics
    fee_native Float64,
    fee_eth Float64,
    fee_usd Float64,
    
    -- Median fees for this block
    median_fee_native Float64,
    median_fee_usd Float64,
    median_fee_eth Float64
    
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (origin_key, block_number)
TTL timestamp + INTERVAL 12 HOUR
SETTINGS index_granularity = 8192

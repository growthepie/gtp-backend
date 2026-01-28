-- Stores raw (timestamp, chain, contract, address) tuples
-- ClickHouse computes uniq() at query time
-- ReplacingMergeTree deduplicates on merge
-- contract_address = '' means chain-level (no specific contract)

CREATE TABLE IF NOT EXISTS gtp.active_addresses (
    timestamp DateTime,
    origin_key LowCardinality(String),
    contract_address String DEFAULT '',
    address String
    
) ENGINE = ReplacingMergeTree()
ORDER BY (origin_key, timestamp, address, contract_address)
TTL timestamp + INTERVAL 7 DAY
SETTINGS index_granularity = 8192

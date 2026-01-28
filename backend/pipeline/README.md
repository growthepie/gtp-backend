# Backend V2 - Blockchain Analytics Pipeline

High-throughput blockchain data collection and analytics pipeline. Designed to handle 30+ chains including high-TPS chains like MegaETH (50k+ txs/block).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Collector Manager                         │
│  - Watches chain head                                        │
│  - Manages block queue                                       │
│  - Spawns workers based on RPC capacity                     │
└─────────────────────────────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   ┌─────────┐   ┌─────────┐   ┌─────────┐
   │Worker 1 │   │Worker 2 │   │Worker 3 │
   │(RPC A)  │   │(RPC B)  │   │(RPC A)  │
   └────┬────┘   └────┬────┘   └────┬────┘
        │              │              │
        └──────────────┼──────────────┘
                       ▼
                 ┌──────────┐
                 │ClickHouse│
                 └──────────┘
                       │
            ┌──────────┼──────────┐
            ▼          ▼          ▼
      ┌─────────┐ ┌─────────┐ ┌─────────┐
      │ _block  │ │   _1m   │ │   _1h   │  (auto rollup via MVs)
      │ 12h TTL │ │  7d TTL │ │ 90d TTL │
      └─────────┘ └─────────┘ └─────────┘
```

## Quick Start

### 1. Start ClickHouse

```bash
docker-compose up -d
```

### 2. Run Migrations

```bash
./scripts/ch_run-migrations.sh
```

### 3. Start Price Service

```bash
export COINGECKO_API_KEY=your_key
python scripts/price_service.py
```

### 4. Start Collector

```bash
cd collectors
PYTHONPATH=src python -m collector.queue_main \
    --chain megaeth \
    --config configs/megaeth_rpcs.json
```

## RPC Configuration

Create a config file for each chain:

```json
{
  "rpcs": [
    {
      "url": "https://fast-rpc.example.com",
      "max_workers": 5,
      "priority": 1
    },
    {
      "url": "https://slow-rpc.example.com", 
      "max_workers": 1,
      "priority": 2
    }
  ]
}
```

- `max_workers`: How many concurrent workers can use this RPC
- `priority`: Lower = preferred (used first)

## ClickHouse Schema

### Tables

| Table | Granularity | TTL | Purpose |
|-------|-------------|-----|---------|
| `chain_metrics_block` | Per block | 12h | Raw block data |
| `chain_metrics_1m` | 1 minute | 7d | Minute rollup |
| `chain_metrics_1h` | 1 hour | 90d | Hourly rollup |
| `chain_metrics_1d` | 1 day | ∞ | Daily rollup |
| `contract_metrics_*` | Same | Same | Per-contract metrics |
| `active_addresses` | Per block | 12h | Address activity |

### Rollups

Materialized views automatically roll up data:

```
chain_metrics_block → chain_metrics_1m → chain_metrics_1h → chain_metrics_1d
```

### Querying

Use the `_v` views for clean aggregated data:

```sql
-- Minute data
SELECT * FROM gtp.chain_metrics_1m_v 
WHERE origin_key = 'megaeth' 
ORDER BY timestamp DESC LIMIT 10;

-- Hourly data
SELECT * FROM gtp.chain_metrics_1h_v 
WHERE origin_key = 'megaeth' 
ORDER BY hour_ts DESC LIMIT 24;

-- Top contracts today
SELECT contract_address, sum(txcount) as txs
FROM gtp.contract_metrics_1d_v
WHERE origin_key = 'megaeth'
GROUP BY contract_address
ORDER BY txs DESC LIMIT 20;
```

## Project Structure

```
pipeline/
├── docker-compose.yml          # ClickHouse
├── migrations/
│   └── clickhouse/             # Schema migrations
├── scripts/
│   ├── ch_run-migrations.sh    # Run migrations
│   └── price_service.py        # CoinGecko price fetcher
├── collectors/
│   ├── configs/                # RPC configs per chain
│   ├── data/                   # Queue state, prices
│   └── src/collector/
│       ├── queue/              # Queue-based collector
│       ├── models.py           # BlockFacts, TxFacts
│       ├── rpc.py              # RPC client
│       ├── processor.py        # Block processor
│       ├── prices.py           # Price reader
│       └── queue_main.py       # CLI entry point
└── README.md
```

## Key Features

- **Horizontal scaling**: Add more RPCs = more workers = faster collection
- **Fault tolerant**: Failed blocks return to queue for retry
- **State persistence**: Queue state saved on shutdown, restored on startup
- **RPC-aware**: Respects `max_workers` per RPC to avoid rate limits
- **High-TPS ready**: Handles 50k+ tx blocks (MegaETH, etc.)
- **Auto-rollup**: ClickHouse MVs aggregate data automatically

## CLI Options

```
python -m collector.queue_main --help

Options:
  --chain TEXT            Chain identifier (required)
  --config PATH           RPC config JSON file
  --rpc JSON              Inline RPC config (can repeat)
  --native-token TEXT     Native token symbol (default: ETH)
  --clickhouse-url TEXT   ClickHouse URL (default: http://localhost:8123)
  --clickhouse-db TEXT    Database name (default: gtp)
  --reorg-depth INT       Blocks behind head (default: 10)
  --start-block INT       Starting block (default: current head)
  --state-dir PATH        State file directory (default: data)
  --stats-interval FLOAT  Stats logging interval (default: 10)
```

## Monitoring

Stats are logged every 10 seconds:

```
2024-01-28 15:00:00 [info] Stats chain=megaeth pending=5 processing=3 done=1000 failed=0 blocks_per_sec=8.5 workers=8
```

- `pending`: Blocks waiting to be processed
- `processing`: Blocks currently being fetched
- `done`: Total blocks completed
- `failed`: Blocks that failed after max retries
- `blocks_per_sec`: Recent throughput

## Graceful Shutdown

Press `Ctrl+C` to shutdown. The collector will:
1. Stop accepting new blocks
2. Wait for in-progress blocks to complete
3. Save queue state to disk
4. Exit cleanly

On restart, it resumes from where it left off.
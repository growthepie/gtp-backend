# Collectors

Queue-based blockchain data collectors for high-throughput chains.

## Overview

The collector fetches blocks from RPC nodes and inserts metrics directly into ClickHouse. It uses an in-memory queue with multiple workers to maximize throughput while respecting RPC rate limits.

## Installation

```bash
cd collectors
pip install -r requirements.txt
```

## Usage

### Basic

```bash
PYTHONPATH=src python -m collector.queue_main \
    --chain megaeth \
    --config configs/megaeth_rpcs.json
```

### With Options

```bash
PYTHONPATH=src python -m collector.queue_main \
    --chain megaeth \
    --config configs/megaeth_rpcs.json \
    --clickhouse-url http://localhost:8123 \
    --reorg-depth 10 \
    --stats-interval 10
```

### Inline RPC Config

```bash
PYTHONPATH=src python -m collector.queue_main \
    --chain base \
    --rpc '{"url": "https://mainnet.base.org", "max_workers": 3}' \
    --rpc '{"url": "https://base.llamarpc.com", "max_workers": 1}'
```

## RPC Configuration

Create a JSON config file per chain in `configs/`:

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

### Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `url` | Yes | - | RPC endpoint URL |
| `max_workers` | No | 1 | Max concurrent workers using this RPC |
| `priority` | No | 1 | Lower = preferred (used first) |

### Guidelines

- **Good RPC**: `max_workers: 3-5`, `priority: 1`
- **Rate-limited RPC**: `max_workers: 1`, `priority: 2`
- **Fallback RPC**: `max_workers: 1`, `priority: 3`

Total workers = sum of all `max_workers` across RPCs.

## How It Works

```
┌──────────────────────────────────────────────────────┐
│                 Collector Manager                     │
│                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │ Head Watcher│  │ Block Queue │  │  RPC Pool   │  │
│  │ (1s poll)   │──│ (in-memory) │  │ (slots)     │  │
│  └─────────────┘  └──────┬──────┘  └──────┬──────┘  │
│                          │                │         │
└──────────────────────────┼────────────────┼─────────┘
                           │                │
              ┌────────────┼────────────────┼────────────┐
              │            │                │            │
              ▼            ▼                ▼            ▼
         ┌────────┐   ┌────────┐      ┌────────┐   ┌────────┐
         │Worker 1│   │Worker 2│ ...  │Worker N│   │Worker M│
         └───┬────┘   └───┬────┘      └───┬────┘   └───┬────┘
             │            │               │            │
             └────────────┴───────────────┴────────────┘
                                  │
                                  ▼
                            ┌──────────┐
                            │ClickHouse│
                            └──────────┘
```

1. **Head Watcher** polls chain head every 1 second
2. New blocks are added to the **Block Queue**
3. **Workers** claim blocks from queue, acquire RPC slots
4. Workers fetch block + receipts, process, insert to ClickHouse
5. On success, block is marked done; on failure, returned to queue

## State Persistence

Queue state is saved to `data/{chain}_queue_state.json` on shutdown:

```json
{
  "chain": "megaeth",
  "last_head": 7000000,
  "pending_blocks": [6999998, 6999999],
  "failed_blocks": [6999500],
  "done_count": 5000,
  "saved_at": 1706454000.0
}
```

On restart, the collector resumes from saved state.

## CLI Reference

```
Options:
  --chain TEXT            Chain identifier (required)
  --config PATH           Path to RPC config JSON file
  --rpc TEXT              Inline RPC config as JSON (repeatable)
  --native-token TEXT     Native token symbol [default: ETH]
  --clickhouse-url TEXT   ClickHouse HTTP URL [default: http://localhost:8123]
  --clickhouse-db TEXT    ClickHouse database [default: gtp]
  --reorg-depth INT       Blocks behind head to consider safe [default: 10]
  --start-block INT       Block to start from [default: current head]
  --state-dir PATH        Directory for state files [default: data]
  --stats-interval FLOAT  Seconds between stats logs [default: 10]
```

## Monitoring

Stats logged every `--stats-interval` seconds:

```
2024-01-28 15:00:00 [info] Stats chain=megaeth pending=5 processing=3 done=1000 failed=0 blocks_per_sec=8.5 workers=8
```

| Metric | Description |
|--------|-------------|
| `pending` | Blocks in queue waiting |
| `processing` | Blocks currently being fetched |
| `done` | Total blocks completed |
| `failed` | Blocks failed after max retries |
| `blocks_per_sec` | Throughput (last 60s) |
| `workers` | Active worker count |

## Data Collected

### Chain Metrics (per block)

- `block_number`, `timestamp`
- `txcount`, `txcount_failed`, `txcount_contract_creation`
- `gas_used`, `gas_limit`
- `fee_native`, `fee_eth`, `fee_usd`
- `median_fee_native`, `median_fee_eth`, `median_fee_usd`

### Contract Metrics (per contract per block)

- Same as chain metrics, grouped by `contract_address`

### Active Addresses

- Unique addresses (from/to) per block
- Used for `uniq()` queries at query time

## Price Service

Collectors read prices from `data/prices.json`. Run the price service separately:

```bash
export COINGECKO_API_KEY=your_key
python scripts/price_service.py
```

This updates prices every 10 minutes.

## Troubleshooting

### Blocks processing slowly

- Add more RPCs with higher `max_workers`
- Check RPC response times
- Reduce `--reorg-depth` if chain is stable

### High failure rate

- Check RPC health
- Increase timeouts in `rpc.py`
- Lower `max_workers` for problematic RPCs

### Queue keeps growing

- RPCs can't keep up with block production
- Add faster RPCs
- For very high-TPS chains, may need multiple collector instances

### State file issues

- Delete `data/{chain}_queue_state.json` to start fresh
- Or use `--start-block` to override
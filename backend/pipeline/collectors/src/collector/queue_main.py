#!/usr/bin/env python3
"""
Queue-based collector CLI.

Usage:
    python -m collector.queue_main --chain megaeth --config rpcs.json

RPC Config file (rpcs.json):
{
    "rpcs": [
        {"url": "https://rpc1.example.com", "max_workers": 3, "priority": 1},
        {"url": "https://rpc2.example.com", "max_workers": 1, "priority": 2}
    ]
}

Or inline:
    python -m collector.queue_main --chain megaeth \\
        --rpc '{"url": "https://rpc1.example.com", "max_workers": 3}' \\
        --rpc '{"url": "https://rpc2.example.com", "max_workers": 1}'
"""

import argparse
import asyncio
import json
from pathlib import Path

import structlog

from .queue import CollectorManager

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Queue-based blockchain collector for high-throughput chains"
    )
    
    parser.add_argument(
        "--chain",
        required=True,
        help="Chain identifier (e.g., megaeth, base)",
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to RPC config JSON file",
    )
    
    parser.add_argument(
        "--rpc",
        action="append",
        dest="rpcs",
        help="RPC config as JSON string (can be repeated)",
    )
    
    parser.add_argument(
        "--native-token",
        default="ETH",
        help="Native token symbol (default: ETH)",
    )
    
    parser.add_argument(
        "--clickhouse-url",
        default="http://localhost:8123",
        help="ClickHouse HTTP URL",
    )
    
    parser.add_argument(
        "--clickhouse-db",
        default="gtp",
        help="ClickHouse database name",
    )
    
    parser.add_argument(
        "--reorg-depth",
        type=int,
        default=10,
        help="Blocks behind head to consider safe (default: 10)",
    )
    
    parser.add_argument(
        "--start-block",
        type=int,
        help="Block to start from (default: current head)",
    )
    
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("data"),
        help="Directory for state files (default: data)",
    )
    
    parser.add_argument(
        "--stats-interval",
        type=float,
        default=10.0,
        help="Seconds between stats logging (default: 10)",
    )
    
    return parser.parse_args()


def load_rpc_configs(args) -> list[dict]:
    """Load RPC configs from file or CLI args."""
    configs = []
    
    # Load from file
    if args.config:
        with open(args.config) as f:
            data = json.load(f)
            configs.extend(data.get("rpcs", []))
    
    # Load from CLI
    if args.rpcs:
        for rpc_json in args.rpcs:
            configs.append(json.loads(rpc_json))
    
    if not configs:
        raise ValueError("No RPC configs provided. Use --config or --rpc")
    
    return configs


async def main():
    args = parse_args()
    
    # Load RPC configs
    rpc_configs = load_rpc_configs(args)
    
    total_workers = sum(c.get("max_workers", 1) for c in rpc_configs)
    logger.info(
        "Starting queue-based collector",
        chain=args.chain,
        rpc_count=len(rpc_configs),
        total_workers=total_workers,
    )
    
    # Create and run manager
    manager = CollectorManager(
        chain=args.chain,
        rpc_configs=rpc_configs,
        native_token=args.native_token,
        clickhouse_url=args.clickhouse_url,
        clickhouse_db=args.clickhouse_db,
        reorg_depth=args.reorg_depth,
        state_dir=args.state_dir,
        stats_interval=args.stats_interval,
    )
    
    await manager.run(start_block=args.start_block)


if __name__ == "__main__":
    asyncio.run(main())

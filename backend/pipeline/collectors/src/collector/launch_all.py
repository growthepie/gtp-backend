#!/usr/bin/env python3
"""
Launch collectors for all enabled chains.

Usage:
    python -m collector.launch_all --config configs/chains.json
    
    # Or specific chains only:
    python -m collector.launch_all --config configs/chains.json --chain megaeth --chain base
"""

import argparse
import asyncio
import json
import signal
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


def load_config(config_path: Path) -> dict:
    """Load chains configuration."""
    with open(config_path) as f:
        return json.load(f)


async def run_collector(chain: str, chain_config: dict, defaults: dict) -> None:
    """Run a single chain collector."""
    logger.info(f"Starting collector for {chain}")
    
    manager = CollectorManager(
        chain=chain,
        rpc_configs=chain_config["rpcs"],
        native_token=chain_config.get("native_token", "ETH"),
        clickhouse_url=defaults.get("clickhouse_url", "http://localhost:8123"),
        clickhouse_db=defaults.get("clickhouse_db", "gtp"),
        reorg_depth=chain_config.get("reorg_depth", 10),
        state_dir=Path(defaults.get("state_dir", "data")),
        stats_interval=defaults.get("stats_interval", 10),
    )
    
    try:
        await manager.run()
    except Exception as e:
        logger.error(f"Collector {chain} crashed", error=str(e))
        raise


async def main_async(config: dict, selected_chains: list[str] | None = None):
    """Run all enabled collectors."""
    chains = config["chains"]
    defaults = config.get("defaults", {})
    
    # Filter to enabled chains
    enabled_chains = {
        name: cfg for name, cfg in chains.items()
        if cfg.get("enabled", True)
    }
    
    # Further filter if specific chains requested
    if selected_chains:
        enabled_chains = {
            name: cfg for name, cfg in enabled_chains.items()
            if name in selected_chains
        }
    
    if not enabled_chains:
        logger.error("No chains to run!")
        return
    
    logger.info(
        "Launching collectors",
        chains=list(enabled_chains.keys()),
        count=len(enabled_chains),
    )
    
    # Create tasks for each chain
    tasks = [
        asyncio.create_task(run_collector(chain, cfg, defaults))
        for chain, cfg in enabled_chains.items()
    ]
    
    # Wait for all (or until one crashes)
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error("A collector crashed, shutting down all", error=str(e))
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser(description="Launch multi-chain collectors")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/chains.json"),
        help="Path to chains config file",
    )
    parser.add_argument(
        "--chain",
        action="append",
        dest="chains",
        help="Specific chain(s) to run (can be repeated)",
    )
    
    args = parser.parse_args()
    
    config = load_config(args.config)
    
    asyncio.run(main_async(config, args.chains))


if __name__ == "__main__":
    main()

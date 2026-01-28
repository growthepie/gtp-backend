"""
Collector Manager: Orchestrates queue, workers, and RPC pool.

This is the main entry point for the queue-based collector.
"""

import asyncio
import signal
from pathlib import Path
from typing import Optional
import structlog
import aiohttp

from .block_queue import BlockQueue
from .rpc_pool import RPCPool, RPCEndpoint
from .worker import Worker
from ..processor import BlockProcessor

logger = structlog.get_logger()


class CollectorManager:
    """
    Manages the collection process for a single chain.
    
    Features:
    - Watches chain head and adds blocks to queue
    - Spawns workers based on available RPC slots
    - Graceful shutdown with state persistence
    - Stats logging
    """
    
    def __init__(
        self,
        chain: str,
        rpc_configs: list[dict],
        native_token: str = "ETH",
        clickhouse_url: str = "http://localhost:8123",
        clickhouse_db: str = "gtp",
        reorg_depth: int = 10,
        state_dir: Path = Path("data"),
        stats_interval: float = 10.0,
    ):
        self.chain = chain
        self.native_token = native_token
        self.clickhouse_url = clickhouse_url
        self.clickhouse_db = clickhouse_db
        self.reorg_depth = reorg_depth
        self.stats_interval = stats_interval
        
        # Components
        self.queue = BlockQueue(
            chain=chain,
            state_file=state_dir / f"{chain}_queue_state.json",
        )
        self.rpc_pool = RPCPool.from_config(rpc_configs)
        self.processor = BlockProcessor(
            origin_key=chain,
            native_token=native_token,
        )
        
        # Workers
        self._workers: list[Worker] = []
        self._worker_tasks: list[asyncio.Task] = []
        
        # State
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._http_session: Optional[aiohttp.ClientSession] = None
        
        # Use first RPC for head watching
        self._head_rpc_url = rpc_configs[0]["url"]
    
    async def run(self, start_block: Optional[int] = None) -> None:
        """Main run loop."""
        self._running = True
        self._http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        )
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        try:
            # Initialize queue
            await self.queue.initialize(start_block)
            
            # If starting fresh (no state, no start_block), just set head position
            # The head watcher will add new blocks as they come
            stats = await self.queue.get_stats()
            if stats.pending == 0 and stats.failed == 0 and start_block is None:
                head = await self._get_chain_head()
                safe_head = head - self.reorg_depth
                await self.queue.set_head(safe_head)
                logger.info("Starting fresh from current head", head=safe_head)
            
            logger.info(
                "Collector manager starting",
                chain=self.chain,
                total_rpc_workers=self.rpc_pool.total_max_workers,
            )
            
            # Spawn initial workers
            await self._spawn_workers()
            
            # Run main loops concurrently
            await asyncio.gather(
                self._head_watcher_loop(),
                self._stats_logger_loop(),
                self._wait_for_shutdown(),
                return_exceptions=True,
            )
            
        except Exception as e:
            logger.error("Manager error", error=str(e))
        finally:
            await self._shutdown()
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self._signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            # Fall back to signal.signal
            signal.signal(signal.SIGINT, lambda s, f: self._signal_handler())
            signal.signal(signal.SIGTERM, lambda s, f: self._signal_handler())
    
    def _signal_handler(self) -> None:
        """Handle shutdown signal."""
        logger.info("Shutdown signal received")
        self._running = False
        self._shutdown_event.set()
    
    async def _wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()
    
    async def _shutdown(self) -> None:
        """Clean shutdown."""
        logger.info("Shutting down...")
        self._running = False
        
        # Stop all workers
        for worker in self._workers:
            worker.stop()
        
        # Wait for workers to finish (with timeout)
        if self._worker_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._worker_tasks, return_exceptions=True),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning("Workers didn't stop in time, cancelling")
                for task in self._worker_tasks:
                    task.cancel()
        
        # Save queue state
        await self.queue.save_state()
        
        # Close HTTP session
        if self._http_session:
            await self._http_session.close()
        
        logger.info("Shutdown complete")
    
    async def _spawn_workers(self) -> None:
        """Spawn workers based on RPC pool capacity."""
        num_workers = self.rpc_pool.total_max_workers
        
        for i in range(num_workers):
            worker_id = f"worker-{i+1}"
            
            worker = Worker(
                worker_id=worker_id,
                queue=self.queue,
                rpc_pool=self.rpc_pool,
                processor=self.processor,
                clickhouse_url=self.clickhouse_url,
                clickhouse_db=self.clickhouse_db,
            )
            
            self._workers.append(worker)
            task = asyncio.create_task(worker.start())
            self._worker_tasks.append(task)
            
            logger.info("Spawned worker", worker_id=worker_id)
    
    async def _head_watcher_loop(self) -> None:
        """Watch chain head and add new blocks to queue."""
        while self._running:
            try:
                head = await self._get_chain_head()
                safe_head = head - self.reorg_depth
                
                added = await self.queue.update_head(safe_head)
                if added > 0:
                    logger.debug("Added new blocks to queue", count=added, head=safe_head)
                    
            except Exception as e:
                logger.error("Error watching head", error=str(e))
            
            await asyncio.sleep(1.0)
    
    async def _stats_logger_loop(self) -> None:
        """Periodically log stats."""
        while self._running:
            await asyncio.sleep(self.stats_interval)
            
            try:
                queue_stats = await self.queue.get_stats()
                
                active_workers = len([w for w in self._workers if w._running])
                
                logger.info(
                    "Stats",
                    chain=self.chain,
                    pending=queue_stats.pending,
                    processing=queue_stats.processing,
                    done=queue_stats.done,
                    failed=queue_stats.failed,
                    blocks_per_sec=f"{queue_stats.blocks_per_second:.1f}",
                    workers=active_workers,
                )
                
            except Exception as e:
                logger.error("Error logging stats", error=str(e))
    
    async def _get_chain_head(self) -> int:
        """Get current chain head block number."""
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1,
        }
        
        async with self._http_session.post(self._head_rpc_url, json=payload) as resp:
            if resp.status != 200:
                raise Exception(f"RPC error: {await resp.text()}")
            data = await resp.json()
            if "error" in data:
                raise Exception(f"RPC error: {data['error']}")
            return int(data["result"], 16)
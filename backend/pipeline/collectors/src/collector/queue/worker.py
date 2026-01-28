"""
Worker: Fetches blocks from queue and inserts to ClickHouse.
"""

import asyncio
from typing import Optional
import structlog
import aiohttp

from .block_queue import BlockQueue
from .rpc_pool import RPCPool
from ..rpc import RPCClient
from ..processor import BlockProcessor

logger = structlog.get_logger()


class Worker:
    """
    Worker that fetches blocks and inserts directly to ClickHouse.
    
    Lifecycle:
    1. Acquire RPC slot from pool
    2. Claim block from queue
    3. Fetch block data
    4. Insert to ClickHouse
    5. Mark complete (or fail)
    6. Repeat
    """
    
    def __init__(
        self,
        worker_id: str,
        queue: BlockQueue,
        rpc_pool: RPCPool,
        processor: BlockProcessor,
        clickhouse_url: str = "http://localhost:8123",
        clickhouse_db: str = "gtp",
    ):
        self.worker_id = worker_id
        self.queue = queue
        self.rpc_pool = rpc_pool
        self.processor = processor
        self.clickhouse_url = clickhouse_url
        self.clickhouse_db = clickhouse_db
        
        self._running = False
        self._current_rpc: Optional[str] = None
        self._current_block: Optional[int] = None
        self._blocks_processed = 0
        self._http_session: Optional[aiohttp.ClientSession] = None
    
    async def start(self) -> None:
        """Start the worker loop."""
        self._running = True
        self._http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        
        logger.info("Worker starting", worker_id=self.worker_id)
        
        try:
            while self._running:
                try:
                    await self._process_one_block()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Worker error", worker_id=self.worker_id, error=str(e))
                    await asyncio.sleep(1.0)
        finally:
            await self._cleanup()
    
    async def _cleanup(self) -> None:
        """Clean up resources."""
        if self._http_session:
            await self._http_session.close()
        
        # Release any held resources
        if self._current_rpc:
            await self.rpc_pool.release(self._current_rpc, success=True)
        if self._current_block:
            await self.queue.release_block(self._current_block)
        
        logger.info(
            "Worker stopped",
            worker_id=self.worker_id,
            blocks_processed=self._blocks_processed,
        )
    
    def stop(self) -> None:
        """Signal worker to stop."""
        self._running = False
    
    async def _process_one_block(self) -> None:
        """Process a single block from the queue."""
        
        # 1. Claim a block first
        block_number = await self.queue.claim_block(self.worker_id)
        if block_number is None:
            # No blocks in queue, wait
            await asyncio.sleep(0.5)
            return
        
        self._current_block = block_number
        
        # 2. Acquire RPC slot
        rpc_url = await self.rpc_pool.acquire(self.worker_id)
        if rpc_url is None:
            # No RPC slots available, release block and wait
            await self.queue.release_block(block_number)
            self._current_block = None
            await asyncio.sleep(0.1)
            return
        
        self._current_rpc = rpc_url
        
        try:
            # 3. Fetch and process
            block_data = await self._fetch_block(rpc_url, block_number)
            
            # 4. Insert to ClickHouse
            await self._insert_to_clickhouse(block_data)
            
            # 5. Mark complete
            await self.queue.complete_block(block_number)
            self._blocks_processed += 1
            
            logger.debug(
                "Block processed",
                worker_id=self.worker_id,
                block_number=block_number,
                tx_count=block_data.tx_count,
            )
            
            await self.rpc_pool.release(rpc_url, success=True)
            
        except Exception as e:
            logger.warning(
                "Failed to process block",
                worker_id=self.worker_id,
                block_number=block_number,
                error=str(e),
            )
            await self.queue.fail_block(block_number, str(e))
            await self.rpc_pool.release(rpc_url, success=False)
            
        finally:
            self._current_rpc = None
            self._current_block = None
    
    async def _fetch_block(self, rpc_url: str, block_number: int):
        """Fetch block data from RPC."""
        async with RPCClient(
            endpoints=[rpc_url],
            max_concurrent=1,
            timeout=15,
        ) as rpc:
            block, receipts = await rpc.get_block_with_receipts(block_number)
            return self.processor.process(block, receipts)
    
    async def _insert_to_clickhouse(self, block_data) -> None:
        """Insert block data directly to ClickHouse."""
        
        # Chain metrics
        timestamp = block_data.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        # Compute aggregates
        fees_native = [tx.fee_native for tx in block_data.transactions]
        fees_usd = [tx.fee_usd for tx in block_data.transactions]
        fees_eth = [tx.fee_eth for tx in block_data.transactions]
        
        median_fee_native = median(fees_native) if fees_native else 0
        median_fee_usd = median(fees_usd) if fees_usd else 0
        median_fee_eth = median(fees_eth) if fees_eth else 0
        
        chain_values = (
            f"(toDateTime('{timestamp}', 'UTC'), '{block_data.origin_key}', {block_data.block_number}, "
            f"{block_data.tx_count}, {block_data.tx_count_failed}, {block_data.tx_count_contract_creation}, "
            f"{block_data.gas_used}, {block_data.gas_limit}, "
            f"{sum(fees_native)}, {sum(fees_eth)}, {sum(fees_usd)}, "
            f"{median_fee_native}, {median_fee_usd}, {median_fee_eth})"
        )
        
        chain_query = f"""
            INSERT INTO {self.clickhouse_db}.chain_metrics_block 
            (timestamp, origin_key, block_number, txcount, txcount_failed, txcount_contract_creation,
             gas_used, gas_limit, fee_native, fee_eth, fee_usd, 
             median_fee_native, median_fee_usd, median_fee_eth)
            VALUES {chain_values}
        """
        
        await self._execute_clickhouse(chain_query)
        
        # Contract metrics (batch insert)
        contract_data = {}
        for tx in block_data.transactions:
            if tx.target_contract:
                if tx.target_contract not in contract_data:
                    contract_data[tx.target_contract] = {
                        "txcount": 0,
                        "txcount_failed": 0,
                        "gas_used": 0,
                        "gas_limit": 0,
                        "fees_native": [],
                        "fees_eth": [],
                        "fees_usd": [],
                    }
                c = contract_data[tx.target_contract]
                c["txcount"] += 1
                if not tx.success:
                    c["txcount_failed"] += 1
                c["gas_used"] += tx.gas_used
                c["gas_limit"] += tx.gas_limit
                c["fees_native"].append(tx.fee_native)
                c["fees_eth"].append(tx.fee_eth)
                c["fees_usd"].append(tx.fee_usd)
        
        if contract_data:
            contract_values = []
            for contract_address, c in contract_data.items():
                contract_addr_escaped = contract_address.replace("'", "\\'")
                contract_values.append(
                    f"(toDateTime('{timestamp}', 'UTC'), '{block_data.origin_key}', {block_data.block_number}, "
                    f"'{contract_addr_escaped}', {c['txcount']}, {c['txcount_failed']}, "
                    f"{c['gas_used']}, {c['gas_limit']}, "
                    f"{sum(c['fees_native'])}, {sum(c['fees_eth'])}, {sum(c['fees_usd'])}, "
                    f"{median(c['fees_native'])}, {median(c['fees_usd'])}, {median(c['fees_eth'])})"
                )
            
            contract_query = f"""
                INSERT INTO {self.clickhouse_db}.contract_metrics_block
                (timestamp, origin_key, block_number, contract_address, txcount, txcount_failed,
                 gas_used, gas_limit, fee_native, fee_eth, fee_usd,
                 median_fee_native, median_fee_usd, median_fee_eth)
                VALUES {', '.join(contract_values)}
            """
            
            await self._execute_clickhouse(contract_query)
        
        # Active addresses
        addresses = set()
        for tx in block_data.transactions:
            addresses.add((tx.from_address, ""))
            if tx.to_address:
                addresses.add((tx.to_address, ""))
            if tx.target_contract:
                addresses.add((tx.from_address, tx.target_contract))
        
        if addresses:
            addr_values = [
                f"(toDateTime('{timestamp}', 'UTC'), '{block_data.origin_key}', '{contract}', '{addr}')"
                for addr, contract in addresses
            ]
            
            # Batch in chunks
            for i in range(0, len(addr_values), 5000):
                chunk = addr_values[i:i+5000]
                addr_query = f"""
                    INSERT INTO {self.clickhouse_db}.active_addresses
                    (timestamp, origin_key, contract_address, address)
                    VALUES {', '.join(chunk)}
                """
                await self._execute_clickhouse(addr_query)
    
    async def _execute_clickhouse(self, query: str) -> None:
        """Execute a ClickHouse query."""
        async with self._http_session.post(
            self.clickhouse_url,
            data=query,
            params={"user": "gtp", "password": "gtp_local"},
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                raise Exception(f"ClickHouse error: {error}")


def median(values: list[float]) -> float:
    """Compute median of a list."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n % 2 == 0:
        return (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2
    return sorted_vals[n//2]
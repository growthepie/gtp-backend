"""
RPC client for fetching blocks and receipts from EVM nodes.

Supports:
- Multiple RPC endpoints (round-robin)
- Parallel fetching with semaphore
- Automatic retries with backoff
"""

import asyncio
from typing import Any, Optional

import aiohttp
import structlog

logger = structlog.get_logger()


class RPCError(Exception):
    """RPC call failed after retries."""
    pass


class RPCClient:
    """
    Async JSON-RPC client for EVM nodes.
    
    Features:
    - Connection pooling via aiohttp
    - Round-robin across multiple endpoints
    - Retry with exponential backoff
    - Request ID tracking
    """
    
    def __init__(
        self,
        endpoints: list[str],
        max_concurrent: int = 10,
        timeout: int = 10,  # Reduced from 30s
        max_retries: int = 3,
    ):
        self.endpoints = endpoints
        self.max_concurrent = max_concurrent
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._request_id = 0
        self._endpoint_index = 0
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
    
    def _next_endpoint(self) -> str:
        """Round-robin endpoint selection."""
        endpoint = self.endpoints[self._endpoint_index]
        self._endpoint_index = (self._endpoint_index + 1) % len(self.endpoints)
        return endpoint
    
    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id
    
    async def _call(self, method: str, params: list[Any]) -> Any:
        """Make a single RPC call with retries."""
        
        last_error = None
        
        for attempt in range(self.max_retries):
            endpoint = self._next_endpoint()
            request_id = self._next_request_id()
            
            payload = {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": request_id,
            }
            
            try:
                async with self._semaphore:
                    async with self._session.post(endpoint, json=payload) as resp:
                        if resp.status != 200:
                            raise RPCError(f"HTTP {resp.status}: {await resp.text()}")
                        
                        data = await resp.json()
                        
                        if "error" in data:
                            raise RPCError(f"RPC error: {data['error']}")
                        
                        return data.get("result")
                        
            except (aiohttp.ClientError, asyncio.TimeoutError, RPCError) as e:
                last_error = e
                wait_time = 2 ** attempt  # 1, 2, 4 seconds
                logger.warning(
                    "RPC call failed, retrying",
                    method=method,
                    attempt=attempt + 1,
                    max_retries=self.max_retries,
                    wait_time=wait_time,
                    error=str(e),
                )
                await asyncio.sleep(wait_time)
        
        raise RPCError(f"RPC call failed after {self.max_retries} retries: {last_error}")
    
    async def get_block_number(self) -> int:
        """Get the latest block number."""
        result = await self._call("eth_blockNumber", [])
        return int(result, 16)
    
    async def get_block(self, block_number: int, full_transactions: bool = False) -> dict:
        """Get block by number.
        
        Args:
            block_number: Block number to fetch
            full_transactions: If True, include full tx objects. If False, just hashes.
                              For high-TPS chains, False is much faster.
        """
        hex_block = hex(block_number)
        result = await self._call("eth_getBlockByNumber", [hex_block, full_transactions])
        if result is None:
            raise RPCError(f"Block {block_number} not found")
        return result
    
    async def get_block_receipts(self, block_number: int) -> list[dict]:
        """
        Get all transaction receipts for a block.
        
        Uses eth_getBlockReceipts (supported by most modern nodes).
        Falls back to individual eth_getTransactionReceipt calls if needed.
        """
        hex_block = hex(block_number)
        
        try:
            result = await self._call("eth_getBlockReceipts", [hex_block])
            if result is not None:
                return result
        except RPCError as e:
            if "method not found" in str(e).lower():
                logger.warning("eth_getBlockReceipts not supported, falling back to individual calls")
            else:
                raise
        
        # Fallback: get block with tx hashes, then fetch receipts individually
        block = await self.get_block(block_number, full_transactions=False)
        tx_hashes = block.get("transactions", [])
        
        tasks = [self._call("eth_getTransactionReceipt", [tx_hash]) for tx_hash in tx_hashes]
        receipts = await asyncio.gather(*tasks)
        
        return [r for r in receipts if r is not None]
    
    async def get_block_with_receipts(self, block_number: int) -> tuple[dict, list[dict]]:
        """Get block header and all receipts in parallel.
        
        Only fetches block header (no full tx objects) since receipts contain
        most of what we need. This is much faster for high-TPS chains.
        """
        block_task = self.get_block(block_number, full_transactions=False)
        receipts_task = self.get_block_receipts(block_number)
        
        block, receipts = await asyncio.gather(block_task, receipts_task)
        return block, receipts
    
    async def get_block_with_receipts_full(self, block_number: int) -> tuple[dict, list[dict]]:
        """Get block with full transactions and all receipts.
        
        Use this when you need transaction input data (e.g., to detect contract calls).
        Slower than get_block_with_receipts for high-TPS chains.
        """
        block_task = self.get_block(block_number, full_transactions=True)
        receipts_task = self.get_block_receipts(block_number)
        
        block, receipts = await asyncio.gather(block_task, receipts_task)
        return block, receipts
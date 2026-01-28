"""
RPC Pool: Manages multiple RPC endpoints with worker limits.

Each RPC has a max_workers limit - the pool ensures we don't exceed it.
"""

import asyncio
from dataclasses import dataclass
from typing import Optional
import structlog

logger = structlog.get_logger()


@dataclass
class RPCEndpoint:
    url: str
    max_workers: int = 1
    priority: int = 1  # Lower = higher priority
    
    # Runtime state
    current_workers: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    
    @property
    def available_slots(self) -> int:
        return self.max_workers - self.current_workers
    
    @property
    def failure_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests


class RPCPool:
    """
    Manages a pool of RPC endpoints with worker limits.
    
    Features:
    - Respects max_workers per RPC
    - Priority-based selection
    - Tracks success/failure rates
    - Automatic slot release
    """
    
    def __init__(self, endpoints: list[RPCEndpoint]):
        self.endpoints = sorted(endpoints, key=lambda e: e.priority)
        self._lock = asyncio.Lock()
    
    @classmethod
    def from_config(cls, config: list[dict]) -> "RPCPool":
        """Create pool from config dicts."""
        endpoints = [
            RPCEndpoint(
                url=c["url"],
                max_workers=c.get("max_workers", 1),
                priority=c.get("priority", 1),
            )
            for c in config
        ]
        return cls(endpoints)
    
    async def acquire(self, worker_id: str) -> Optional[str]:
        """
        Acquire an RPC endpoint for a worker.
        
        Returns RPC URL or None if no slots available.
        """
        async with self._lock:
            for endpoint in self.endpoints:
                if endpoint.available_slots > 0:
                    endpoint.current_workers += 1
                    logger.debug(
                        "Worker acquired RPC",
                        worker_id=worker_id,
                        rpc_url=endpoint.url[:50],
                        current_workers=endpoint.current_workers,
                        max_workers=endpoint.max_workers,
                    )
                    return endpoint.url
            return None
    
    async def release(self, rpc_url: str, success: bool = True) -> None:
        """Release an RPC slot."""
        async with self._lock:
            for endpoint in self.endpoints:
                if endpoint.url == rpc_url:
                    endpoint.current_workers = max(0, endpoint.current_workers - 1)
                    endpoint.total_requests += 1
                    if not success:
                        endpoint.failed_requests += 1
                    return
    
    async def get_stats(self) -> list[dict]:
        """Get stats for all endpoints."""
        async with self._lock:
            return [
                {
                    "url": e.url[:50] + "..." if len(e.url) > 50 else e.url,
                    "max_workers": e.max_workers,
                    "current_workers": e.current_workers,
                    "available_slots": e.available_slots,
                    "total_requests": e.total_requests,
                    "failure_rate": f"{e.failure_rate:.1%}",
                }
                for e in self.endpoints
            ]
    
    @property
    def total_available_slots(self) -> int:
        """Total available worker slots across all RPCs."""
        return sum(e.available_slots for e in self.endpoints)
    
    @property 
    def total_max_workers(self) -> int:
        """Total max workers across all RPCs."""
        return sum(e.max_workers for e in self.endpoints)

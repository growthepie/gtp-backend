"""
Telemetry: Track RPC and worker metrics.

Provides visibility into:
- RPC health (requests, failures, latency)
- Worker throughput
- Queue depth over time
"""

import time
from dataclasses import dataclass, field
from typing import Optional
import structlog

logger = structlog.get_logger()


@dataclass
class RPCMetrics:
    """Metrics for a single RPC endpoint."""
    url: str
    requests_total: int = 0
    requests_failed: int = 0
    blocks_fetched: int = 0
    
    # Latency tracking (rolling window)
    latencies_ms: list[float] = field(default_factory=list)
    max_latency_samples: int = 100
    
    def record_request(self, success: bool, latency_ms: float, blocks: int = 0) -> None:
        self.requests_total += 1
        if not success:
            self.requests_failed += 1
        else:
            self.blocks_fetched += blocks
        
        self.latencies_ms.append(latency_ms)
        if len(self.latencies_ms) > self.max_latency_samples:
            self.latencies_ms.pop(0)
    
    @property
    def success_rate(self) -> float:
        if self.requests_total == 0:
            return 1.0
        return (self.requests_total - self.requests_failed) / self.requests_total
    
    @property
    def avg_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        return sum(self.latencies_ms) / len(self.latencies_ms)
    
    @property
    def p99_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_latencies = sorted(self.latencies_ms)
        idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
    
    def to_dict(self) -> dict:
        return {
            "url": self.url[:60] + "..." if len(self.url) > 60 else self.url,
            "requests_total": self.requests_total,
            "requests_failed": self.requests_failed,
            "blocks_fetched": self.blocks_fetched,
            "success_rate": f"{self.success_rate:.1%}",
            "avg_latency_ms": f"{self.avg_latency_ms:.0f}",
            "p99_latency_ms": f"{self.p99_latency_ms:.0f}",
        }


@dataclass 
class WorkerMetrics:
    """Metrics for a single worker."""
    worker_id: str
    blocks_processed: int = 0
    blocks_failed: int = 0
    total_processing_time_ms: float = 0
    
    def record_block(self, success: bool, processing_time_ms: float) -> None:
        if success:
            self.blocks_processed += 1
        else:
            self.blocks_failed += 1
        self.total_processing_time_ms += processing_time_ms
    
    @property
    def avg_processing_time_ms(self) -> float:
        total = self.blocks_processed + self.blocks_failed
        if total == 0:
            return 0.0
        return self.total_processing_time_ms / total
    
    def to_dict(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "blocks_processed": self.blocks_processed,
            "blocks_failed": self.blocks_failed,
            "avg_processing_time_ms": f"{self.avg_processing_time_ms:.0f}",
        }


class Telemetry:
    """
    Central telemetry collector.
    
    Usage:
        telemetry = Telemetry(chain="megaeth")
        telemetry.record_rpc_request(url, success=True, latency_ms=150, blocks=1)
        telemetry.record_worker_block(worker_id, success=True, processing_time_ms=500)
        telemetry.log_summary()
    """
    
    def __init__(self, chain: str):
        self.chain = chain
        self.start_time = time.time()
        self._rpc_metrics: dict[str, RPCMetrics] = {}
        self._worker_metrics: dict[str, WorkerMetrics] = {}
        
        # Overall counters
        self.total_blocks_processed = 0
        self.total_blocks_failed = 0
    
    def record_rpc_request(
        self, 
        url: str, 
        success: bool, 
        latency_ms: float,
        blocks: int = 0
    ) -> None:
        """Record an RPC request."""
        if url not in self._rpc_metrics:
            self._rpc_metrics[url] = RPCMetrics(url=url)
        self._rpc_metrics[url].record_request(success, latency_ms, blocks)
    
    def record_worker_block(
        self,
        worker_id: str,
        success: bool,
        processing_time_ms: float
    ) -> None:
        """Record a block processed by a worker."""
        if worker_id not in self._worker_metrics:
            self._worker_metrics[worker_id] = WorkerMetrics(worker_id=worker_id)
        self._worker_metrics[worker_id].record_block(success, processing_time_ms)
        
        if success:
            self.total_blocks_processed += 1
        else:
            self.total_blocks_failed += 1
    
    def get_rpc_summary(self) -> list[dict]:
        """Get summary of all RPC metrics."""
        return [m.to_dict() for m in self._rpc_metrics.values()]
    
    def get_worker_summary(self) -> list[dict]:
        """Get summary of all worker metrics."""
        return [m.to_dict() for m in self._worker_metrics.values()]
    
    def get_overall_stats(self) -> dict:
        """Get overall statistics."""
        uptime = time.time() - self.start_time
        total_blocks = self.total_blocks_processed + self.total_blocks_failed
        
        return {
            "chain": self.chain,
            "uptime_seconds": int(uptime),
            "total_blocks_processed": self.total_blocks_processed,
            "total_blocks_failed": self.total_blocks_failed,
            "overall_success_rate": f"{self.total_blocks_processed / max(total_blocks, 1):.1%}",
            "avg_blocks_per_second": f"{self.total_blocks_processed / max(uptime, 1):.2f}",
        }
    
    def log_summary(self) -> None:
        """Log a full telemetry summary."""
        overall = self.get_overall_stats()
        
        logger.info(
            "Telemetry Summary",
            **overall
        )
        
        # Log RPC stats
        for rpc in self.get_rpc_summary():
            logger.info("RPC Stats", **rpc)
    
    def log_compact(self) -> None:
        """Log a compact one-line summary."""
        overall = self.get_overall_stats()
        
        # Find worst performing RPC
        worst_rpc = None
        worst_rate = 1.0
        for url, metrics in self._rpc_metrics.items():
            if metrics.success_rate < worst_rate and metrics.requests_total > 10:
                worst_rate = metrics.success_rate
                worst_rpc = url[:30]
        
        logger.info(
            "Stats",
            chain=self.chain,
            blocks=self.total_blocks_processed,
            failed=self.total_blocks_failed,
            blocks_per_sec=overall["avg_blocks_per_second"],
            worst_rpc=worst_rpc,
            worst_rpc_success=f"{worst_rate:.0%}" if worst_rpc else None,
        )

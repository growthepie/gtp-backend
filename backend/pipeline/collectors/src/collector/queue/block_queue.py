"""
In-memory block queue with persistence.

Manages which blocks need to be fetched, tracks processing state,
and saves/restores state on shutdown/startup.
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional
import structlog

logger = structlog.get_logger()


class BlockStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


@dataclass
class BlockTask:
    block_number: int
    status: BlockStatus = BlockStatus.PENDING
    worker_id: Optional[str] = None
    claimed_at: Optional[float] = None
    attempts: int = 0
    last_error: Optional[str] = None


@dataclass
class QueueStats:
    pending: int = 0
    processing: int = 0
    done: int = 0
    failed: int = 0
    blocks_per_second: float = 0.0


class BlockQueue:
    """
    In-memory queue for block collection tasks.
    
    Features:
    - Thread-safe async operations
    - Automatic timeout of stale tasks
    - State persistence to file
    - Stats tracking
    """
    
    def __init__(
        self,
        chain: str,
        state_file: Optional[Path] = None,
        task_timeout: float = 60.0,  # Seconds before task is considered stale
        max_retries: int = 3,
    ):
        self.chain = chain
        self.state_file = state_file or Path(f"data/{chain}_queue_state.json")
        self.task_timeout = task_timeout
        self.max_retries = max_retries
        
        self._tasks: dict[int, BlockTask] = {}
        self._lock = asyncio.Lock()
        self._last_head: int = 0
        self._done_count: int = 0
        self._done_timestamps: list[float] = []  # For calculating blocks/sec
        
    async def initialize(self, start_block: Optional[int] = None) -> None:
        """Initialize queue, optionally loading from state file."""
        if self.state_file.exists():
            await self._load_state()
            logger.info(
                "Loaded queue state from file",
                chain=self.chain,
                pending=len([t for t in self._tasks.values() if t.status == BlockStatus.PENDING]),
                failed=len([t for t in self._tasks.values() if t.status == BlockStatus.FAILED]),
            )
        elif start_block is not None:
            self._last_head = start_block - 1
            logger.info("Initialized fresh queue", chain=self.chain, start_block=start_block)
        # If no state file and no start_block, _last_head stays None
        # and will be set by set_head()
    
    async def set_head(self, head_block: int) -> None:
        """Set the current head without adding blocks to queue.
        
        Use this to initialize the head position when starting fresh.
        """
        async with self._lock:
            self._last_head = head_block
            logger.info("Set queue head", head=head_block)
    
    async def update_head(self, head_block: int) -> int:
        """
        Update chain head and add new blocks to queue.
        
        Returns number of blocks added.
        """
        async with self._lock:
            # If _last_head is 0 (uninitialized), don't try to add millions of blocks
            if self._last_head == 0:
                self._last_head = head_block
                logger.info("Initialized head position", head=head_block)
                return 0
            
            if head_block <= self._last_head:
                return 0
            
            added = 0
            for block_num in range(self._last_head + 1, head_block + 1):
                if block_num not in self._tasks:
                    self._tasks[block_num] = BlockTask(block_number=block_num)
                    added += 1
            
            self._last_head = head_block
            return added
    
    async def claim_block(self, worker_id: str) -> Optional[int]:
        """
        Claim a pending block for processing.
        
        Returns block number or None if no blocks available.
        """
        async with self._lock:
            now = time.time()
            
            # First, check for timed-out tasks and reclaim them
            for task in self._tasks.values():
                if (task.status == BlockStatus.PROCESSING and 
                    task.claimed_at and 
                    now - task.claimed_at > self.task_timeout):
                    logger.warning(
                        "Reclaiming timed-out task",
                        block_number=task.block_number,
                        worker_id=task.worker_id,
                    )
                    task.status = BlockStatus.PENDING
                    task.worker_id = None
                    task.claimed_at = None
            
            # Find a pending task (prefer lower block numbers)
            pending = [
                t for t in self._tasks.values() 
                if t.status == BlockStatus.PENDING
            ]
            
            if not pending:
                return None
            
            # Sort by block number, take lowest
            pending.sort(key=lambda t: t.block_number)
            task = pending[0]
            
            task.status = BlockStatus.PROCESSING
            task.worker_id = worker_id
            task.claimed_at = now
            task.attempts += 1
            
            return task.block_number
    
    async def complete_block(self, block_number: int) -> None:
        """Mark a block as successfully processed."""
        async with self._lock:
            if block_number in self._tasks:
                # Remove completed task to save memory
                del self._tasks[block_number]
                self._done_count += 1
                self._done_timestamps.append(time.time())
                
                # Keep only last 60 seconds of timestamps
                cutoff = time.time() - 60
                self._done_timestamps = [t for t in self._done_timestamps if t > cutoff]
    
    async def fail_block(self, block_number: int, error: str) -> None:
        """Mark a block as failed."""
        async with self._lock:
            if block_number not in self._tasks:
                return
            
            task = self._tasks[block_number]
            task.last_error = error
            
            if task.attempts >= self.max_retries:
                task.status = BlockStatus.FAILED
                logger.error(
                    "Block failed permanently",
                    block_number=block_number,
                    attempts=task.attempts,
                    error=error,
                )
            else:
                # Return to pending for retry
                task.status = BlockStatus.PENDING
                task.worker_id = None
                task.claimed_at = None
                logger.warning(
                    "Block failed, will retry",
                    block_number=block_number,
                    attempts=task.attempts,
                    error=error,
                )
    
    async def release_block(self, block_number: int) -> None:
        """Release a block back to pending (e.g., worker shutting down)."""
        async with self._lock:
            if block_number in self._tasks:
                task = self._tasks[block_number]
                if task.status == BlockStatus.PROCESSING:
                    task.status = BlockStatus.PENDING
                    task.worker_id = None
                    task.claimed_at = None
    
    async def get_stats(self) -> QueueStats:
        """Get current queue statistics."""
        async with self._lock:
            stats = QueueStats()
            for task in self._tasks.values():
                if task.status == BlockStatus.PENDING:
                    stats.pending += 1
                elif task.status == BlockStatus.PROCESSING:
                    stats.processing += 1
                elif task.status == BlockStatus.FAILED:
                    stats.failed += 1
            
            stats.done = self._done_count
            
            # Calculate blocks per second from last 60 seconds
            if self._done_timestamps:
                cutoff = time.time() - 60
                recent = [t for t in self._done_timestamps if t > cutoff]
                if len(recent) >= 2:
                    duration = recent[-1] - recent[0]
                    if duration > 0:
                        stats.blocks_per_second = len(recent) / duration
            
            return stats
    
    async def save_state(self) -> None:
        """Save queue state to file."""
        async with self._lock:
            # Only save pending and failed blocks
            pending = [t.block_number for t in self._tasks.values() if t.status == BlockStatus.PENDING]
            failed = [t.block_number for t in self._tasks.values() if t.status == BlockStatus.FAILED]
            processing = [t.block_number for t in self._tasks.values() if t.status == BlockStatus.PROCESSING]
            
            state = {
                "chain": self.chain,
                "last_head": self._last_head,
                "pending_blocks": pending + processing,  # Treat processing as pending on restart
                "failed_blocks": failed,
                "done_count": self._done_count,
                "saved_at": time.time(),
            }
            
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)
            
            logger.info(
                "Saved queue state",
                chain=self.chain,
                pending=len(pending) + len(processing),
                failed=len(failed),
            )
    
    async def _load_state(self) -> None:
        """Load queue state from file."""
        with open(self.state_file) as f:
            state = json.load(f)
        
        self.chain = state["chain"]
        self._last_head = state["last_head"]
        self._done_count = state.get("done_count", 0)
        
        for block_num in state.get("pending_blocks", []):
            self._tasks[block_num] = BlockTask(block_number=block_num)
        
        for block_num in state.get("failed_blocks", []):
            self._tasks[block_num] = BlockTask(
                block_number=block_num,
                status=BlockStatus.FAILED,
            )
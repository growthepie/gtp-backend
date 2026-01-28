"""
BlockFacts: The core data structure passed from collectors to aggregator.

Design principles:
- One message per block (not per transaction)
- Contains only data needed for aggregation
- Serializable to JSON for Redpanda
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional

import orjson
from pydantic import BaseModel, Field


class TxFacts(BaseModel):
    """
    Minimal transaction data needed for metric aggregation.
    
    We intentionally exclude:
    - Full input data (not needed for current metrics)
    - Logs/events (may add later)
    - Internal calls (would require tracing)
    """
    from_address: str = Field(description="Transaction sender")
    to_address: Optional[str] = Field(description="Recipient. None for contract creation")
    contract_created: Optional[str] = Field(default=None, description="Address of created contract, if any")
    
    gas_used: int = Field(description="Actual gas consumed")
    gas_limit: int = Field(description="Gas limit set by sender")
    
    fee_native: float = Field(description="Total fee in native token")
    fee_eth: float = Field(description="Total fee in ETH")
    fee_usd: float = Field(description="Total fee in USD")
    
    success: bool = Field(description="True if tx succeeded (status = 1)")
    is_contract_creation: bool = Field(default=False, description="True if this tx created a contract")
    
    # For contract-level metrics: which contract was interacted with
    # This is to_address for normal txs, contract_created for creations
    target_contract: Optional[str] = Field(default=None, description="Contract address for metrics attribution")


class BlockFacts(BaseModel):
    """
    Aggregated facts about a single block.
    
    This is the message format published to Redpanda. The aggregator
    consumes these and rolls them up into minute buckets.
    """
    origin_key: str = Field(description="Chain identifier (e.g., 'ethereum', 'arbitrum')")
    block_number: int = Field(description="Block height")
    block_hash: str = Field(description="Block hash")
    timestamp: datetime = Field(description="Block timestamp")
    
    # Block-level data
    gas_limit: int = Field(description="Block gas limit")
    gas_used: int = Field(description="Total gas used in block")
    
    # Transaction facts
    transactions: list[TxFacts] = Field(default_factory=list)
    
    # Summary stats (for quick access without iterating txs)
    tx_count: int = Field(description="Total transactions in block")
    tx_count_failed: int = Field(description="Failed transactions")
    tx_count_contract_creation: int = Field(description="Contract creation transactions")
    
    def to_json(self) -> bytes:
        """Serialize for Redpanda. Uses compact JSON."""
        return orjson.dumps(self.model_dump(), default=str)
    
    @classmethod
    def from_json(cls, data: bytes) -> "BlockFacts":
        """Deserialize from Redpanda message."""
        return cls.model_validate(orjson.loads(data))


def has_meaningful_input(input_data: str) -> bool:
    """
    Check if transaction has meaningful input data (likely contract interaction).
    
    - "0x" or "" = simple transfer
    - "0x" + 8 chars (4 bytes) = function selector only (rare but valid)
    - "0x" + more = function call with params
    """
    if not input_data or input_data == "0x":
        return False
    # At least a 4-byte function selector
    return len(input_data) >= 10

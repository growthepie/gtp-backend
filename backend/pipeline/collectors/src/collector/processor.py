"""
Block processor: transforms raw RPC data into BlockFacts.

This is where we extract the metrics we care about from blocks and receipts.
"""

from datetime import datetime, timezone
from typing import Optional

import structlog

from .models import BlockFacts, TxFacts, has_meaningful_input
from .prices import PriceReader, get_price_reader

logger = structlog.get_logger()


class BlockProcessor:
    """
    Transforms raw block and receipt data into BlockFacts.
    
    Handles:
    - Fee calculation (native, ETH, USD)
    - Contract creation detection
    - Contract interaction filtering
    """
    
    def __init__(
        self,
        origin_key: str,
        native_token: str = "ETH",
        price_reader: Optional[PriceReader] = None,
    ):
        self.origin_key = origin_key
        self.native_token = native_token
        self.price_reader = price_reader or get_price_reader()
    
    def process(self, block: dict, receipts: list[dict], transactions: list[dict] = None) -> BlockFacts:
        """
        Process a block and its receipts into BlockFacts.
        
        Args:
            block: Raw block data from eth_getBlockByNumber
            receipts: Raw receipts from eth_getBlockReceipts
            transactions: Optional full transaction objects (for input data).
                         If not provided, all txs with a 'to' address are considered contract calls.
            
        Returns:
            BlockFacts ready for publishing
        """
        # Parse block header
        block_number = int(block["number"], 16)
        block_hash = block["hash"]
        timestamp = datetime.fromtimestamp(int(block["timestamp"], 16), tz=timezone.utc)
        block_gas_limit = int(block["gasLimit"], 16)
        block_gas_used = int(block["gasUsed"], 16)
        
        # Build transaction lookup by hash (if full txs provided)
        tx_by_hash = {}
        if transactions:
            for tx in transactions:
                if isinstance(tx, dict):
                    tx_by_hash[tx["hash"]] = tx
        
        # Process each receipt
        tx_facts_list: list[TxFacts] = []
        tx_count_failed = 0
        tx_count_contract_creation = 0
        
        for receipt in receipts:
            tx_hash = receipt.get("transactionHash")
            tx = tx_by_hash.get(tx_hash) if tx_by_hash else None
            
            tx_facts = self._process_transaction(tx, receipt)
            if tx_facts:
                tx_facts_list.append(tx_facts)
                
                if not tx_facts.success:
                    tx_count_failed += 1
                if tx_facts.is_contract_creation:
                    tx_count_contract_creation += 1
        
        return BlockFacts(
            origin_key=self.origin_key,
            block_number=block_number,
            block_hash=block_hash,
            timestamp=timestamp,
            gas_limit=block_gas_limit,
            gas_used=block_gas_used,
            transactions=tx_facts_list,
            tx_count=len(tx_facts_list),
            tx_count_failed=tx_count_failed,
            tx_count_contract_creation=tx_count_contract_creation,
        )
    
    def _process_transaction(self, tx: Optional[dict], receipt: dict) -> Optional[TxFacts]:
        """
        Process a single transaction and receipt into TxFacts.
        
        Args:
            tx: Full transaction object (optional, for input data)
            receipt: Transaction receipt (required)
        """
        # Extract basic fields from receipt
        from_address = receipt["from"].lower()
        to_address = receipt.get("to")
        if to_address:
            to_address = to_address.lower()
        
        # Check for contract creation
        contract_created = receipt.get("contractAddress")
        if contract_created:
            contract_created = contract_created.lower()
        
        is_contract_creation = contract_created is not None
        
        # Get input data from transaction if available
        input_data = tx.get("input", "0x") if tx else None
        
        # Determine target contract for metrics
        # - For contract creation: the new contract
        # - For contract calls: the to_address (if has meaningful input, or if we don't have input data)
        # - For EOA transfers: None (no contract attribution)
        if is_contract_creation:
            target_contract = contract_created
        elif to_address:
            if input_data is not None:
                # We have input data - check if it's a contract call
                if has_meaningful_input(input_data):
                    target_contract = to_address
                else:
                    target_contract = None
            else:
                # No input data available - assume contract call if to_address exists
                # This is a reasonable heuristic for high-TPS chains where we skip full tx fetch
                target_contract = to_address
        else:
            target_contract = None
        
        # Gas metrics
        gas_used = int(receipt["gasUsed"], 16)
        gas_limit = int(receipt.get("gas", receipt.get("gasUsed")), 16)  # Some receipts don't have gas limit
        
        # Fee calculation
        # effectiveGasPrice is post-EIP-1559, fallback to gasPrice
        gas_price = int(receipt.get("effectiveGasPrice", receipt.get("gasPrice", "0x0")), 16)
        fee_native = (gas_used * gas_price) / 1e18
        
        # Convert to ETH and USD using price reader
        fee_eth = self.price_reader.convert_to_eth(fee_native, self.native_token)
        fee_usd = self.price_reader.convert_to_usd(fee_native, self.native_token)
        
        # Transaction status (EIP-658)
        # status: 0x1 = success, 0x0 = failure
        # Pre-Byzantium txs don't have status field
        status = receipt.get("status")
        if status is not None:
            success = int(status, 16) == 1
        else:
            # Pre-Byzantium: assume success if we got here
            success = True
        
        return TxFacts(
            from_address=from_address,
            to_address=to_address,
            contract_created=contract_created,
            gas_used=gas_used,
            gas_limit=gas_limit,
            fee_native=fee_native,
            fee_eth=fee_eth,
            fee_usd=fee_usd,
            success=success,
            is_contract_creation=is_contract_creation,
            target_contract=target_contract,
        )
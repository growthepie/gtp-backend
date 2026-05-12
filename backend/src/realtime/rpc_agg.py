import json
import logging
import time
import asyncio
import aiohttp
import redis.asyncio as aioredis
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware
from src.db_connector import DbConnector
from src.realtime.rpc_config import rpc_config
from src.realtime.sse_app_run.history_utils import HistoryCompressor, encode_history_entry, iso_from_ms
from src.realtime.sse_app_run.redis_keys import RedisKeys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("rt_backend")

# Constants
## TODO: specific to prep script, move to config
GAS_NATIVE_TRANSFER = 21000  # Standard gas for a native ETH transfer
GAS_ERC20_TRANSFER = 65000  # Standard gas for an ERC20 transfer
GAS_SWAP = 350000  # Gas for a swap operation (e.g., Uniswap)

PRICE_UPDATE_INTERVAL = 1800  # 30 minutes in seconds
COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
ETH_COINGECKO_ID = "ethereum"

# Redis constants
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)  # For AUTH if enabled
REDIS_STREAM_MAXLEN = 500
RECEIPT_PARSE_LIMIT = 500  # Max number of receipts to parse per block for fee calculations; 0 = no limit


def _coerce_positive_int(value: Any, default: int = 1) -> int:
    """Parse ints from decimal/hex strings and clamp to >= 1."""
    try:
        if isinstance(value, str):
            value = value.strip()
            parsed = int(value, 16) if value.lower().startswith("0x") else int(value)
        else:
            parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


class BlockchainProcessor(ABC):
    """Abstract base class for blockchain processors."""
    
    @abstractmethod
    async def fetch_latest_block(self, client: Any, chain_name: str, calc_fees: bool) -> Optional[Dict[str, Any]]:
        """Fetch the latest block for this blockchain type."""
        pass
    
    @abstractmethod
    async def initialize_client(self, url: str) -> Any:
        """Initialize the client for this blockchain type."""
        pass
    
    @abstractmethod
    def supports_tx_costs(self) -> bool:
        """Whether this blockchain processor supports transaction cost calculations."""
        pass


class EVMProcessor(BlockchainProcessor):
    """Processor for EVM-based blockchains."""
    
    def __init__(self, backend: 'RtBackend'):
        self.backend = backend
    
    async def initialize_client(self, url: str) -> AsyncWeb3:
        """Initialize Web3 client for EVM chains."""
        w3 = AsyncWeb3(AsyncHTTPProvider(url, request_kwargs={"timeout": 5}))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return w3
    
    def supports_tx_costs(self) -> bool:
        return True
    
    async def fetch_latest_block(self, web3: AsyncWeb3, chain_name: str, calc_fees: bool) -> Optional[Dict[str, Any]]:
        """Fetch the latest block receipts and derive all info from them for EVM chains."""
        try:
            stack = rpc_config[chain_name].get("stack", None)
            gas_token = rpc_config[chain_name].get("gas_token", {})
            gas_token_coingecko_id = gas_token.get("coingecko_id", ETH_COINGECKO_ID)
            gas_token_decimals = gas_token.get("decimals", 18)
            is_custom_gas_token = gas_token_coingecko_id != ETH_COINGECKO_ID

            # Only fetch receipts if we need to calculate fees
            if calc_fees:
                # Get all transaction receipts for the latest block
                #logger.info(f"Fetching latest block receipts for {chain_name}")
                try:
                    receipts = await web3.eth.get_block_receipts('latest')
                except Exception as e:
                    logger.warning(f"{chain_name}: block receipts unavailable, skipping tx cost calculation: {str(e)}")
                    receipts = None
            else:
                receipts = None
            
            if not receipts or len(receipts) == 0:
                # Handle empty blocks - we need to make one minimal call for basic info
                block = await web3.eth.get_block('latest', full_transactions=False)
                subblock_count = 1
                if chain_name == 'megaeth':
                    subblock_count = _coerce_positive_int(block.get('miniBlockCount', 1))
                    
                return {
                    "number": hex(block.number),
                    "transactions": [tx.hex() if isinstance(tx, bytes) else tx for tx in block.transactions],
                    "timestamp": hex(block.timestamp),
                    "gasUsed": hex(block.gasUsed),
                    "gasLimit": hex(block.gasLimit),
                    "subblock_count": subblock_count,
                }
            
            # Extract all block info from receipts
            first_receipt = receipts[0]
            block_number = first_receipt.blockNumber
            block = await web3.eth.get_block(block_number, full_transactions=False)
            block_timestamp = int(block.timestamp)
            block_gas_used = int(block.gasUsed)
            
            subblock_count = 1
            if chain_name == 'megaeth':
                subblock_count = _coerce_positive_int(block.get('miniBlockCount', 1))
            else:
                block_timestamp = int(block.timestamp)
            
            if RECEIPT_PARSE_LIMIT > 0 and len(receipts) > RECEIPT_PARSE_LIMIT:
                logger.info(
                    f"{chain_name}: Capping receipt parsing at {RECEIPT_PARSE_LIMIT} "
                    f"out of {len(receipts)} receipts"
                )
                receipts = receipts[:RECEIPT_PARSE_LIMIT]
            
            # Analyze receipts and calculate costs in one pass (sampled if capped)
            total_gas_used_sample = 0
            
            native_transfers = []
            erc20_transfers = []
            swaps = []
            gas_prices = []
            all_tx = []
            
            for receipt in receipts:
                gas_used = receipt.gasUsed
                effective_gas_price = receipt.effectiveGasPrice
                total_gas_used_sample += gas_used
                
                if effective_gas_price > 0:
                    gas_prices.append(effective_gas_price)    
                        
                    if stack and stack in ["op_stack", "l1", "basic", "zk_stack"]:
                        # l1_fee may be hex string, convert to int if needed
                        l1_fee = receipt.l1Fee if hasattr(receipt, 'l1Fee') else 0
                        
                        if isinstance(l1_fee, str):
                            l1_fee = float(int(l1_fee, 16))
                        else:
                            l1_fee = float(l1_fee)
                            
                        cost_wei = (gas_used * effective_gas_price) + l1_fee
                        tx_data = {
                            'gas_used': gas_used,
                            'cost_raw': cost_wei
                        }
                    elif stack and stack in ["nitro"]:
                        cost_wei = (gas_used * effective_gas_price)
                        tx_data = {
                            'gas_used': gas_used,
                            'cost_raw': cost_wei
                        }
                        
                    else:
                        tx_data = {
                            'gas_used': gas_used,
                            'cost_raw': 0
                        }
                    
                    # Categorize based on gas usage patterns
                    if gas_used <= GAS_NATIVE_TRANSFER * 1.2:
                        native_transfers.append(tx_data)
                    elif gas_used >= GAS_ERC20_TRANSFER * 0.8 and gas_used <= GAS_ERC20_TRANSFER * 1.2:
                        erc20_transfers.append(tx_data)
                    elif gas_used >= GAS_SWAP * 0.8 and gas_used <= GAS_SWAP * 1.2:
                        swaps.append(tx_data)
                        
                    all_tx.append(tx_data)

            #logger.info(f"Processed {len(receipts)} receipts for block {block_number} on {chain_name}. swaps: {len(swaps)}, native transfers: {len(native_transfers)}, erc20 transfers: {len(erc20_transfers)}")
            
            # # Calculate average costs with fallbacks
            def calc_avg_cost(transfers):
                if transfers:
                    return sum(tx['cost_raw'] for tx in transfers) / len(transfers) / 10 ** gas_token_decimals
                return 0

            # Calculate median cost
            def calculate_median_cost(transfers):
                if not transfers:
                    return 0
                costs = sorted(tx['cost_raw'] for tx in transfers)
                if len(costs) % 2 == 1:
                    return costs[len(costs) // 2] / 10 ** gas_token_decimals
                return (costs[len(costs) // 2 - 1] + costs[len(costs) // 2]) / 2 / 10 ** gas_token_decimals

            avg_cost_native = calc_avg_cost(all_tx)
            
            avg_native_cost_native = calculate_median_cost(native_transfers)
            avg_erc20_cost_native = calculate_median_cost(erc20_transfers)
            avg_swap_cost_native = calculate_median_cost(swaps)

            median_cost_native = calculate_median_cost(all_tx)

            total_costs_native = sum(tx['cost_raw'] for tx in all_tx) / 10 ** gas_token_decimals
            cost_per_gas_native = total_costs_native / total_gas_used_sample if total_gas_used_sample > 0 else 0
            
            ## if one of the costs is 0, use the average cost of the others
            if avg_native_cost_native == 0 and cost_per_gas_native > 0:
                avg_native_cost_native = cost_per_gas_native * GAS_NATIVE_TRANSFER
                    
            if avg_erc20_cost_native == 0 and cost_per_gas_native > 0:
                avg_erc20_cost_native = cost_per_gas_native * GAS_ERC20_TRANSFER
                    
            if avg_swap_cost_native == 0 and cost_per_gas_native > 0:
                avg_swap_cost_native = cost_per_gas_native * GAS_SWAP

            # Get token prices and calculate USD + ETH-equivalent costs
            await self.backend.update_eth_price()
            eth_price = self.backend.eth_price_usd
            gas_token_price = self.backend.token_prices_usd.get(gas_token_coingecko_id, 0 if is_custom_gas_token else eth_price)

            def native_to_eth_equivalent(value):
                if gas_token_price > 0 and eth_price > 0:
                    return value * gas_token_price / eth_price
                return 0

            def native_to_usd(value):
                return value * gas_token_price if gas_token_price > 0 else 0

            avg_cost_eth = native_to_eth_equivalent(avg_cost_native)
            avg_native_cost_eth = native_to_eth_equivalent(avg_native_cost_native)
            avg_erc20_cost_eth = native_to_eth_equivalent(avg_erc20_cost_native)
            avg_swap_cost_eth = native_to_eth_equivalent(avg_swap_cost_native)
            median_cost_eth = native_to_eth_equivalent(median_cost_native)

            avg_cost_usd = native_to_usd(avg_cost_native)
            avg_native_cost_usd = native_to_usd(avg_native_cost_native)
            avg_erc20_cost_usd = native_to_usd(avg_erc20_cost_native)
            avg_swap_cost_usd = native_to_usd(avg_swap_cost_native)
            median_cost_usd = native_to_usd(median_cost_native)

            # Build block dictionary using receipt data and current timestamp
            block_dict = {
                "number": hex(block_number),
                "transactions": [tx.hex() if isinstance(tx, bytes) else tx for tx in block.transactions],
                "timestamp": hex(block_timestamp),
                "timestamp_ms": block_timestamp * 1000,
                "gasUsed": hex(block_gas_used),
                "gasLimit": None,  # Not available in receipts
                "tx_cost_avg": avg_cost_eth,
                "tx_cost_avg_usd": avg_cost_usd,
                "tx_cost_native": avg_native_cost_eth,
                "tx_cost_native_usd": avg_native_cost_usd,
                "tx_cost_erc20_transfer": avg_erc20_cost_eth,
                "tx_cost_erc20_transfer_usd": avg_erc20_cost_usd,
                "tx_cost_swap": avg_swap_cost_eth,
                "tx_cost_swap_usd": avg_swap_cost_usd,
                "tx_cost_median": median_cost_eth,
                "tx_cost_median_usd": median_cost_usd,
                "subblock_count": subblock_count,
            }

            self.backend.chain_data[chain_name].update({
                "tx_cost_avg": avg_cost_eth,
                "tx_cost_avg_usd": avg_cost_usd,
                "tx_cost_native": avg_native_cost_eth,
                "tx_cost_erc20_transfer": avg_erc20_cost_eth,
                "tx_cost_native_usd": avg_native_cost_usd,
                "tx_cost_erc20_transfer_usd": avg_erc20_cost_usd,
                "tx_cost_swap": avg_swap_cost_eth,
                "tx_cost_swap_usd": avg_swap_cost_usd,
                "tx_cost_median": median_cost_eth,
                "tx_cost_median_usd": median_cost_usd,
            })

            # logger.info(self.backend.chain_data[chain_name])

            # logger.info(block_dict)
            return block_dict
            
        except Exception as e:
            logger.error(f"Exception fetching EVM block receipts from {chain_name}: {str(e)}")
            self.backend.chain_data[chain_name]["errors"] += 1
            return None

class StarknetProcessor(BlockchainProcessor):
    """Processor for Starknet blockchain."""
    
    def __init__(self, backend: 'RtBackend'):
        self.backend = backend
    
    async def initialize_client(self, url: str) -> str:
        """Return the URL as the 'client' for Starknet (we'll use backend's HTTP session)."""
        return url
    
    def supports_tx_costs(self) -> bool:
        return False  # Starknet has different gas model, skip tx costs for now
    
    async def fetch_latest_block(self, url: str, chain_name: str, calc_fees: bool) -> Optional[Dict[str, Any]]:
        """Fetch the latest block using Starknet JSON-RPC."""
        try:
            if not self.backend.http_session:
                logger.error("HTTP session not initialized")
                return None
            
            # Starknet JSON-RPC call to get latest block number
            payload = {
                "jsonrpc": "2.0",
                "method": "starknet_blockNumber",
                "params": [],
                "id": 1
            }
            
            async with self.backend.http_session.post(url, json=payload, timeout=10) as response:
                if response.status != 200:
                    logger.error(f"Failed to get Starknet block number from {chain_name}: HTTP {response.status}")
                    return None
                    
                result = await response.json()
                if "error" in result:
                    logger.error(f"Starknet RPC error for {chain_name}: {result['error']}")
                    return None
                    
                block_number = result.get("result")
                if block_number is None:
                    logger.error(f"No block number returned for {chain_name}")
                    return None
                    
                # Convert hex string to int if needed
                if isinstance(block_number, str):
                    block_number = int(block_number, 16) if block_number.startswith('0x') else int(block_number)
            
            # Get block details with transactions
            payload = {
                "jsonrpc": "2.0", 
                "method": "starknet_getBlockWithTxs",
                "params": [{"block_number": block_number}],
                "id": 2
            }
            
            async with self.backend.http_session.post(url, json=payload, timeout=10) as response:
                if response.status != 200:
                    logger.error(f"Failed to get Starknet block details from {chain_name}: HTTP {response.status}")
                    return None
                    
                result = await response.json()
                if "error" in result:
                    logger.error(f"Starknet block fetch error for {chain_name}: {result['error']}")
                    return None
                    
                block_data = result.get("result")
                if not block_data:
                    logger.error(f"No block data returned for {chain_name}")
                    return None
                
                # Extract transaction list - handle different possible formats
                transactions = []
                if "transactions" in block_data:
                    transactions = block_data["transactions"]
                elif "transaction_receipts" in block_data:
                    transactions = block_data["transaction_receipts"]
                
                # Get timestamp - try different field names
                timestamp = block_data.get("timestamp", 0)
                if timestamp == 0:
                    timestamp = block_data.get("block_timestamp", 0)
                    
                # Convert to our standard format
                block_dict = {
                    "number": hex(block_number),
                    "hash": str(block_data.get("block_hash", "0x0")),
                    "transactions": transactions,
                    "timestamp": hex(timestamp) if timestamp else "0x0",
                    "gasUsed": "N/A",  # Starknet doesn't use traditional gas
                    "gasLimit": "N/A",
                }
                
                #logger.debug(f"Fetched Starknet block {block_number} with {len(transactions)} transactions")
                return block_dict
                
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching Starknet block from {chain_name}")
            self.backend.chain_data[chain_name]["errors"] += 1
            return None
        except Exception as e:
            logger.error(f"Exception fetching Starknet block from {chain_name}: {str(e)}")
            self.backend.chain_data[chain_name]["errors"] += 1
            return None

class LighterProcessor(BlockchainProcessor):
    """
    Processor for Lighter (Elliot) explorer.
    - Uses the backend API:
        GET <base>/blocks
    - Returns a block-like dict; no fee calcs; TPS is computed by your rolling logic.
    """

    def __init__(self, backend: 'RtBackend'):
        self.backend = backend

    async def initialize_client(self, url: str) -> str:
        # url should be the base API: e.g. "https://mainnet.zklighter.elliot.ai/api/v1"
        return url

    def supports_tx_costs(self) -> bool:
        return False

    ## TODO: once lighter supports per-block timestamps, we can improve TPS accuracy by using tps_override over the latest 20 blocks
    async def fetch_latest_block(self, base_url: str, chain_name: str, calc_fees: bool) -> Optional[Dict[str, Any]]:
        try:
            if not self.backend.http_session:
                logger.error("HTTP session not initialized")
                return None

            # Fetch latest 20 blocks by that index
            blocks_url = f"{base_url.rstrip('/')}/blocks"
            async with self.backend.http_session.get(blocks_url, timeout=10) as resp:
                body = await resp.text()
                if resp.status != 200:
                    logger.error(f"[{chain_name}] HTTP {resp.status} {blocks_url}. Body: {body[:512]}")
                    return None
                try:
                    data = await resp.json()
                except Exception as e:
                    logger.error(f"[{chain_name}] JSON parse error on blocks: {e}. Body: {body[:512]}")
                    return None

            blocks = data
            if not isinstance(blocks, list) or not blocks:
                logger.error(f"[{chain_name}] Blocks payload empty/wrong shape: {data}")
                return None

            # Parse (height, size, updated_at); API says sort=desc, but sort defensively
            rows = []
            for b in blocks:
                try:
                    h = int(b["block_height"])
                    sz = int(b.get("block_size", 0))
                    ts = None
                    updated_at = b.get("updated_at")
                    if updated_at:
                        try:
                            ts = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).timestamp()
                        except Exception:
                            ts = None
                    rows.append((h, sz, ts))
                except Exception:
                    continue
            if not rows:
                logger.error(f"[{chain_name}] All blocks malformed")
                return None

            rows.sort(key=lambda x: x[0])  # oldest -> newest
            latest_height, latest_size, latest_ts = rows[-1]
            if latest_ts is None:
                latest_ts = time.time()
            now_unix = int(latest_ts)

            # Cache some window metadata (informational)
            total_txs_window = sum(sz for _, sz, _ in rows)
            self.backend.chain_data[chain_name].update({
                "tps_window_blocks": len(rows),
                "tps_window_txs": total_txs_window,
                "latest_block_height": latest_height,
                "last_updated_unix": now_unix,
            })

            # Return a block-like dict for your generic pipeline.
            # We supply tx_count; your calculate_tps() already prefers it.
            now_ms = int(latest_ts * 1000)
            tps_override = None
            valid_rows = [(h, sz, ts) for h, sz, ts in rows if ts is not None]
            if len(valid_rows) >= 2:
                valid_rows.sort(key=lambda x: x[0])
                t0 = valid_rows[0][2]
                t1 = valid_rows[-1][2]
                span = t1 - t0
                if span > 0:
                    total_txs = sum(sz for _, sz, _ in valid_rows)
                    n = len(valid_rows)
                    effective_span = span * (n / (n - 1))
                    tps_override = total_txs / effective_span
            return {
                "number": hex(latest_height),
                "timestamp": hex(now_ms // 1000),
                "timestamp_ms": now_ms,            # <-- new high-res timestamp for TPS math
                "transactions": [],
                "tx_count": latest_size,
                "tps_override": tps_override,
                "gasUsed": "N/A",
                "gasLimit": "N/A",
            }

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching Lighter blocks for {chain_name}")
            self.backend.chain_data[chain_name]["errors"] += 1
            return None
        except Exception as e:
            logger.error(f"Exception fetching Lighter blocks for {chain_name}: {str(e)}")
            self.backend.chain_data[chain_name]["errors"] += 1
            return None


class RtBackend:
    """
    Real-time Backend for blockchain TPS monitoring.
    Handles connections to multiple blockchain types and calculates TPS.
    """

    def __init__(self):
        """Initialize RPC endpoints, clients, and Redis connection for each chain."""
        # init db
        self.db_connector = DbConnector()

        # RPC endpoints with their chain identifiers
        self.RPC_ENDPOINTS = self._initialize_rpc_endpoints()

        # Initialize blockchain processors
        self.processors = {
            "evm": EVMProcessor(self),
            "evm_custom_gas": EVMProcessor(self),
            "starknet": StarknetProcessor(self),
            "lighter": LighterProcessor(self), 
        }

        # Initialize clients for each endpoint (will be populated in initialize_async)
        self.blockchain_clients: Dict[str, Any] = {}

        # Initialize chain data storage
        self.chain_data: Dict[str, Dict[str, Any]] = {}
        self.chain_metrics: Dict[str, Dict[str, Any]] = {}
        self._initialize_chain_data()
        self.history_compressor = HistoryCompressor()
        
        # Initialize ETH price tracking
        self.eth_price_usd: float = 0.0
        self.last_eth_price_update: Optional[float] = None
        self.eth_price_lock = asyncio.Lock()
        self.price_coingecko_ids = sorted({
            ETH_COINGECKO_ID,
            *[
                config.get("gas_token", {}).get("coingecko_id")
                for config in self.RPC_ENDPOINTS.values()
                if config.get("gas_token", {}).get("coingecko_id")
            ],
        })
        self.token_prices_usd: Dict[str, float] = {}
        self.last_token_price_update: Dict[str, float] = {}
        self.token_price_lock = asyncio.Lock()
        
        # Initialize HTTP session for API calls
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Initialize Redis connection
        self.redis_client: Optional[aioredis.Redis] = None

    async def initialize_async(self):
        """Initialize components that require async operations."""
        self.http_session = aiohttp.ClientSession()
        
        # Initialize Redis connection
        try:
            # Configure Redis connection parameters
            redis_params = {
                "host": REDIS_HOST,
                "port": REDIS_PORT,
                "db": REDIS_DB,
                "decode_responses": True,
                "socket_keepalive": True,
                "socket_keepalive_options": {},
                "retry_on_timeout": True,
                "health_check_interval": 30,
            }
            
            # Add password if provided
            if REDIS_PASSWORD:
                redis_params["password"] = REDIS_PASSWORD
                
            # For GCP Memorystore, you might need connection pooling
            self.redis_client = aioredis.Redis(**redis_params)
            
            # Test the connection
            await self.redis_client.ping()
            logger.info(f"Connected to Redis successfully at {REDIS_HOST}:{REDIS_PORT}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {str(e)}")
            raise
        
        await self._load_chain_ath_metrics()

        # Initialize blockchain clients
        await self._initialize_blockchain_clients()
        
        # Get initial ETH price
        await self.update_eth_price()

    async def close(self):
        """Clean up resources."""
        if self.http_session:
            await self.http_session.close()
        
        if self.redis_client:
            await self.redis_client.close()
            
        # Note: Starknet clients are just URLs, no cleanup needed
        # Only close actual HTTP clients (for EVM processors, Web3 handles this)

    def _initialize_rpc_endpoints(self) -> Dict[str, Dict[str, Any]]:
        """
        Initialize and return the RPC endpoint configurations.
        
        Returns:
            Dictionary with RPC endpoint configurations.
        """
        
        for chain_name, config in rpc_config.items():
            url = self.db_connector.get_special_use_rpc(chain_name, check_realtime=True)

            if not url or url == "None" or url == "":
                logger.error(f"No RPC URL configured for {chain_name}, skipping initialization")
                continue
            rpc_config[chain_name]['url'] = url

        logger.info(f"Initialized a total of {len(rpc_config)} RPC endpoints")
        return rpc_config

    async def _initialize_blockchain_clients(self) -> None:
        """Initialize clients for all blockchain endpoints."""
        for chain_name, config in self.RPC_ENDPOINTS.items():
            try:
                processor = config["processors"]
                processor = self.processors.get(processor)
                
                if not processor:
                    logger.error(f"No processor found for processors: {processor} (chain: {chain_name})")
                    continue
                
                client = await processor.initialize_client(config["url"])
                self.blockchain_clients[chain_name] = client
                logger.info(f"Initialized {processor} client for {chain_name}")
                
            except Exception as e:
                logger.error(f"Failed to initialize client for {chain_name}: {str(e)}")
                # Continue with other chains even if one fails

    def _initialize_chain_data(self) -> None:
        """Initialize empty chain data structures for all endpoints."""
        for chain_name, config in self.RPC_ENDPOINTS.items():
            self.chain_data[chain_name] = {
                "display_name": config["name"],
                "last_block_number": None,
                "last_block_timestamp": None,
                "last_tx_count": 0,
                "tps": 0,
                "gas_used": 0,
                "blocks_processed": 0,
                "errors": 0,
                "last_updated": None,
                "block_history": [],
                "block_time": 0,  # Average block time in seconds
                "block_time_history": [],  # Store last 10 block times for averaging
            }
            self.chain_metrics[chain_name] = {
                "ath": 0.0,
                "ath_timestamp": "",
                "ath_timestamp_ms": 0,
                "ath_block_number": 0,
            }

    async def _load_chain_ath_metrics(self) -> None:
        if not self.redis_client:
            return
        chains = list(self.chain_data.keys())
        if not chains:
            return
        try:
            pipe = self.redis_client.pipeline()
            for chain_name in chains:
                pipe.hgetall(RedisKeys.chain_tps_ath(chain_name))
            results = await pipe.execute()
            for chain_name, data in zip(chains, results):
                metrics = self.chain_metrics.setdefault(chain_name, {
                    "ath": 0.0,
                    "ath_timestamp": "",
                    "ath_timestamp_ms": 0,
                    "ath_block_number": 0,
                })
                if data:
                    metrics["ath"] = float(data.get("value", metrics["ath"]))
                    metrics["ath_timestamp"] = data.get("timestamp", metrics["ath_timestamp"])
                    metrics["ath_timestamp_ms"] = int(data.get("timestamp_ms", metrics["ath_timestamp_ms"]) or 0)
                    metrics["ath_block_number"] = int(data.get("block_number", metrics["ath_block_number"]) or 0)
        except Exception as exc:
            logger.warning(f"Failed to load chain ATH metrics: {exc}")

    async def update_eth_price(self) -> None:
        """Update the ETH price from CoinGecko API."""
        await self.update_token_prices(self.price_coingecko_ids)
        self.eth_price_usd = self.token_prices_usd.get(ETH_COINGECKO_ID, self.eth_price_usd)
        self.last_eth_price_update = self.last_token_price_update.get(ETH_COINGECKO_ID, self.last_eth_price_update)

    async def update_token_price(self, coingecko_id: str) -> None:
        """Update a single token price from CoinGecko API."""
        await self.update_token_prices([coingecko_id])

    async def update_token_prices(self, coingecko_ids: List[str]) -> None:
        """Update token prices from CoinGecko API in one batched request."""
        current_time = time.time()
        coingecko_ids = sorted({coingecko_id for coingecko_id in coingecko_ids if coingecko_id})
        if not coingecko_ids:
            return
        
        async with self.token_price_lock:
            needs_update = any(
                self.last_token_price_update.get(coingecko_id) is None or
                current_time - self.last_token_price_update[coingecko_id] >= PRICE_UPDATE_INTERVAL
                for coingecko_id in coingecko_ids
            )
            if needs_update:
                
                logger.info(f"Updating CoinGecko prices for: {', '.join(coingecko_ids)}")
                for coingecko_id in coingecko_ids:
                    self.last_token_price_update[coingecko_id] = current_time
                
                try:
                    if not self.http_session:
                        self.http_session = aiohttp.ClientSession()

                    params = {"ids": ",".join(coingecko_ids), "vs_currencies": "usd"}
                    async with self.http_session.get(COINGECKO_SIMPLE_PRICE_URL, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            for coingecko_id in coingecko_ids:
                                if coingecko_id in data and 'usd' in data[coingecko_id]:
                                    price_usd = float(data[coingecko_id]['usd'])
                                    self.token_prices_usd[coingecko_id] = price_usd
                                    if coingecko_id == ETH_COINGECKO_ID:
                                        self.eth_price_usd = price_usd
                                        self.last_eth_price_update = current_time
                                    logger.info(f"Updated {coingecko_id} price: ${price_usd:.6f}")
                                else:
                                    logger.error(f"Missing {coingecko_id} price in CoinGecko response")
                        else:
                            logger.error(f"Failed to fetch CoinGecko prices: HTTP {response.status}")
                except Exception as e:
                    logger.error(f"Error updating CoinGecko prices: {str(e)}")

    async def publish_to_redis(self, chain_name: str, data: Dict[str, Any]) -> None:
        """Publish chain data to Redis stream."""
        if not self.redis_client:
            logger.error("Redis client not initialized")
            return
            
        try:
            stream_key = f"chain:{chain_name}"

            ## make sure all keys are strings
            data = {k: str(v) if v is not None else "N/A" for k, v in data.items()}

            await self.redis_client.xadd(
                stream_key,
                data,
                maxlen=REDIS_STREAM_MAXLEN,
                approximate=True
            )
            
        except Exception as e:
            logger.error(f"Failed to publish to Redis stream for {chain_name}: {str(e)}")

    async def fetch_latest_block(self, chain_name: str, calc_fees=False) -> Optional[Dict[str, Any]]:
        """Fetch the latest block for any blockchain type."""
        try:
            config = self.RPC_ENDPOINTS[chain_name]
            processor = config["processors"]
            
            processor = self.processors.get(processor)
            if not processor:
                logger.error(f"No processor found for {processor}")
                return None
            
            client = self.blockchain_clients.get(chain_name)
            if not client:
                logger.error(f"No client found for {chain_name}")
                return None
            
            return await processor.fetch_latest_block(client, chain_name, calc_fees)
            
        except Exception as e:
            logger.error(f"Exception fetching block from {chain_name}: {str(e)}")
            self.chain_data[chain_name]["errors"] += 1
            return None
        
    def calculate_block_time(self, chain_name: str, current_block_number: int, current_timestamp: float, subblock_count: int = 1) -> float:
        """
        Calculate average block time based on current and previous block timestamps.
        Accounts for missed blocks by dividing time difference by number of blocks elapsed.
        
        Args:
            chain_name: Name of the blockchain
            current_block_number: Block number of the current block
            current_timestamp: Timestamp of the current block
            
        Returns:
            Average block time in seconds (rounded to 2 decimals)
        """
        chain = self.chain_data[chain_name]
        
        # If we have a previous block, calculate the time difference
        if chain["last_block_number"] is not None and chain["last_block_timestamp"] is not None:
            time_diff = current_timestamp - chain["last_block_timestamp"]
            blocks_elapsed = current_block_number - chain["last_block_number"]
            
            # Only add valid block times (positive time, positive blocks, and reasonable)
            if blocks_elapsed > 0 and time_diff > 0 and time_diff < 3600:  # Ignore if > 1 hour (likely a gap)
                # Calculate per-block time by dividing total time by number of blocks
                block_time = time_diff / blocks_elapsed / subblock_count
                    
                chain["block_time_history"].append(block_time)
                
                # Keep only last 10 block times
                if len(chain["block_time_history"]) > 10:
                    chain["block_time_history"] = chain["block_time_history"][-10:]
        
        # Calculate average from history
        if chain["block_time_history"]:
            avg_block_time = sum(chain["block_time_history"]) / len(chain["block_time_history"])
            chain["block_time"] = round(avg_block_time, 3)
        else:
            chain["block_time"] = 0
            
        return chain["block_time"]

    def calculate_tps(self, chain_name: str, current_block: Dict[str, Any]) -> float:
        chain = self.chain_data[chain_name]
        history_len = self.RPC_ENDPOINTS.get(chain_name, {}).get("block_history_len", 3)

        current_block_number = int(current_block["number"], 16)

        # Prefer ms if present (so you don’t hit 1-second quantization)
        if "timestamp_ms" in current_block:
            current_timestamp = current_block["timestamp_ms"] / 1000.0  # float seconds
        else:
            current_timestamp = float(int(current_block["timestamp"], 16))

        tx_count = current_block.get("tx_count")
        if tx_count is None:
            tx_count = len(current_block.get("transactions", []))

        gas_used = int(current_block["gasUsed"], 16) if current_block["gasUsed"] != "N/A" else 0

        if chain["last_block_number"] is not None and current_block_number <= chain["last_block_number"]:
            return chain["tps"]

        tps_override = current_block.get("tps_override")
        if tps_override is not None:
            # --- override path ---
            block_time = self.calculate_block_time(chain_name, current_block_number, current_timestamp)
            chain["block_history"].append({
                "number": current_block_number,
                "timestamp": current_timestamp,  # float seconds
                "tx_count": tx_count,
                "gas_used": gas_used,
                "is_estimated": False
            })
            if len(chain["block_history"]) > history_len:
                chain["block_history"] = chain["block_history"][-history_len:]

            self._update_chain_data(chain_name, current_block_number, current_timestamp, tx_count, float(tps_override))
            asyncio.create_task(self._publish_chain_update(chain_name, current_block_number, tx_count, gas_used, float(tps_override), block_time))
            return float(tps_override)

        # --- normal path (no override) ---
        if chain_name == "megaeth":
            subblock_count = _coerce_positive_int(current_block.get("subblock_count", 1))
            ##print(f"megaeth subblock_count: {subblock_count}")
            block_time = self.calculate_block_time(chain_name, current_block_number, current_timestamp, subblock_count)
        else:
            block_time = self.calculate_block_time(chain_name, current_block_number, current_timestamp)

        if chain["last_block_number"] is not None:
            blocks_missed = current_block_number - chain["last_block_number"] - 1
            if blocks_missed > 0:
                self._add_estimated_blocks(chain, blocks_missed, current_block_number, current_timestamp, tx_count)
                if blocks_missed > 10:
                    logger.warning(f"{chain_name}: Missed {blocks_missed} blocks between {chain['last_block_number']} and {current_block_number}")

        chain["block_history"].append({
            "number": current_block_number,
            "timestamp": current_timestamp,  # float seconds
            "tx_count": tx_count,
            "gas_used": gas_used,
            "is_estimated": False
        })
        if len(chain["block_history"]) > history_len:
            chain["block_history"] = chain["block_history"][-history_len:]

        tps = self._calculate_tps_from_history(chain["block_history"])
        self._update_chain_data(chain_name, current_block_number, current_timestamp, tx_count, tps)
        asyncio.create_task(self._publish_chain_update(chain_name, current_block_number, tx_count, gas_used, tps, block_time))
        return tps

    async def _publish_chain_update(self, chain_name: str, block_number: int, tx_count: int, gas_used: int, tps: float, block_time: float) -> None:
        """Publish chain update to Redis stream."""
        chain_data = self.chain_data[chain_name]
        timestamp_ms = int(time.time() * 1000)
        
        publish_data = {
            "timestamp": str(timestamp_ms),
            "display_name": chain_data.get("display_name", chain_name),
            "block_number": block_number,
            "tps": round(tps, 1),
            "tx_count": tx_count,
            "block_time": block_time,
            "gas_used": gas_used,
            "tx_cost_avg": chain_data.get("tx_cost_avg", 0),
            "tx_cost_avg_usd": chain_data.get("tx_cost_avg_usd", 0),
            "tx_cost_median": chain_data.get("tx_cost_median", 0),
            "tx_cost_median_usd": chain_data.get("tx_cost_median_usd", 0),
            "tx_cost_native": chain_data.get("tx_cost_native", 0),
            "tx_cost_native_usd": chain_data.get("tx_cost_native_usd", 0),
            "tx_cost_erc20_transfer": chain_data.get("tx_cost_erc20_transfer", 0),
            "tx_cost_erc20_transfer_usd": chain_data.get("tx_cost_erc20_transfer_usd", 0),
            "tx_cost_swap": chain_data.get("tx_cost_swap", 0),
            "tx_cost_swap_usd": chain_data.get("tx_cost_swap_usd", 0),
            "blocks_processed": chain_data.get("blocks_processed", 0),
            "errors": chain_data.get("errors", 0)
        }
        
        await self.publish_to_redis(chain_name, publish_data)
        await self._record_chain_metrics(chain_name, block_number, float(tps), timestamp_ms)

    async def _record_chain_metrics(self, chain_name: str, block_number: int, tps: float, timestamp_ms: int) -> None:
        if not self.redis_client:
            return
        if tps <= 0:
            return
        if not hasattr(self, "chain_metrics"):
            self.chain_metrics = {}
        metrics = self.chain_metrics.setdefault(chain_name, {
            "ath": 0.0,
            "ath_timestamp": "",
            "ath_timestamp_ms": 0,
            "ath_block_number": 0,
        })
        timestamp_iso = iso_from_ms(timestamp_ms)
        is_new_ath = tps > metrics.get("ath", 0.0)
        pipe = self.redis_client.pipeline()

        if is_new_ath:
            metrics["ath"] = tps
            metrics["ath_timestamp"] = timestamp_iso
            metrics["ath_timestamp_ms"] = timestamp_ms
            metrics["ath_block_number"] = block_number
            pipe.hset(RedisKeys.chain_tps_ath(chain_name), mapping={
                "value": str(tps),
                "timestamp": timestamp_iso,
                "timestamp_ms": str(timestamp_ms),
                "block_number": str(block_number),
            })
            history_entry = json.dumps({
                "tps": tps,
                "timestamp": timestamp_iso,
                "timestamp_ms": timestamp_ms,
                "block_number": block_number,
                "is_ath": True,
            })
            pipe.zadd(RedisKeys.chain_ath_history(chain_name), {history_entry: timestamp_ms})
            logger.info(f"🚀 NEW CHAIN ATH for {chain_name}: {tps} TPS!")

        history_key = RedisKeys.chain_tps_history_24h(chain_name)
        compact_entry = encode_history_entry(tps, timestamp_ms, is_new_ath)
        redundant = self.history_compressor.register(history_key, tps, timestamp_ms, compact_entry)
        pipe.zadd(history_key, {compact_entry: timestamp_ms})
        if redundant:
            pipe.zrem(history_key, redundant)

        try:
            await pipe.execute()
        except Exception as exc:
            logger.error(f"Failed to persist metrics for {chain_name}: {exc}")
        
    def _add_estimated_blocks(self, chain: Dict[str, Any], blocks_missed: int, 
                             current_block_number: int, current_timestamp: float, current_tx_count: int) -> None:
        """Add estimated blocks for missed blocks to maintain TPS calculation accuracy."""
        if len(chain["block_history"]) == 0:
            return
            
        last_block = chain["block_history"][-1]
        last_timestamp = last_block["timestamp"]
        last_tx_count = last_block["tx_count"]
        
        time_diff = current_timestamp - last_timestamp  # both floats
        avg_block_time = time_diff / (blocks_missed + 1)    
        avg_tx_per_block = (last_tx_count + current_tx_count) / 2
        avg_gas_per_block = (last_block["gas_used"] + 0) / 2
        
        for i in range(1, blocks_missed + 1):
            estimated_block = {
                "number": last_block["number"] + i,
                "timestamp": last_timestamp + (avg_block_time * i),  # keep float, no int()
                "tx_count": int(avg_tx_per_block),
                "gas_used": int(avg_gas_per_block),
                "is_estimated": True
            }
            chain["block_history"].append(estimated_block)
            #logger.debug(f"{estimated_block['number']}: Estimated block with {estimated_block['tx_count']} tx")

    def _calculate_tps_from_history(self, block_history: List[Dict[str, Any]]) -> float:
        n = len(block_history)
        if n < 2:
            return 0.0

        t0 = float(block_history[0]["timestamp"])
        t1 = float(block_history[-1]["timestamp"])
        span = t1 - t0
        if span <= 0:
            return 0.0

        total_tx = sum(b["tx_count"] for b in block_history)
        # Bias correction: there are (n-1) observed intervals for n blocks
        effective_span = span * (n / (n - 1))
        return total_tx / effective_span
        
    def _update_chain_data(self, chain_name: str, block_number: int, timestamp: float, 
                          tx_count: int, tps: float) -> None:
        """Update chain data with new block information."""
        chain = self.chain_data[chain_name]
        chain["last_block_number"] = block_number
        chain["last_block_timestamp"] = timestamp
        chain["last_tx_count"] = tx_count
        chain["blocks_processed"] += 1
        chain["tps"] = tps
        chain["last_updated"] = time.time()

    async def get_chain_streams_info(self) -> Dict[str, Any]:
        """Get information about all Redis streams for debugging purposes."""
        if not self.redis_client:
            return {}
            
        streams_info = {}
        
        for chain_name, config in self.RPC_ENDPOINTS.items():
            stream_key = f"chain:{chain_name}"
            
            try:
                stream_len = await self.redis_client.xlen(stream_key)
                
                latest_entry = None
                if stream_len > 0:
                    entries = await self.redis_client.xrevrange(stream_key, count=1)
                    if entries:
                        latest_entry = entries[0]
                
                streams_info[chain_name] = {
                    "stream_key": stream_key,
                    "length": stream_len,
                    "latest_entry": latest_entry,
                }
                
            except Exception as e:
                logger.error(f"Error getting stream info for {chain_name}: {str(e)}")
                streams_info[chain_name] = {"error": str(e)}
                
        return streams_info

    async def process_chain(self, chain_name: str) -> None:
        """Process a single chain continuously."""
        logger.info(f"Starting processing for chain: {chain_name}")
        sleeper = self.RPC_ENDPOINTS[chain_name].get("sleeper", 3)
        calc_fees = rpc_config[chain_name].get("calc_fees", False)
        
        while True:
            try:
                #logger.info(f"Fetching latest block for {chain_name} (calc_fees={calc_fees})")
                block = await self.fetch_latest_block(chain_name, calc_fees)
                
                if block:
                    tps = self.calculate_tps(chain_name, block)
                    
                    current_block_number = int(block['number'], 16)
                    last_logged_block = self.chain_data[chain_name].get('last_logged_block', None)
                    
                    if tps > 0 and current_block_number != last_logged_block:
                        self.chain_data[chain_name]["last_logged_block"] = current_block_number
                        
                await asyncio.sleep(sleeper)
                
            except Exception as e:
                logger.error(f"Error processing {chain_name}: {str(e)}")
                self.chain_data[chain_name]["errors"] += 1
                await asyncio.sleep(5)


async def main():
    """Main function to run the real-time blockchain TPS monitor."""
    logger.info("Starting Real-time Multi-Blockchain TPS Monitor")
    
    backend = RtBackend()
    
    try:
        await backend.initialize_async()
        
        # Check which clients were successfully initialized
        successful_chains = list(backend.blockchain_clients.keys())
        failed_chains = [name for name in backend.RPC_ENDPOINTS.keys() if name not in successful_chains]
        
        if failed_chains:
            logger.warning(f"Failed to initialize clients for: {', '.join(failed_chains)}")
        
        # Create tasks only for successfully initialized chains
        tasks = []
        for chain_name in successful_chains:
            task = asyncio.create_task(
                backend.process_chain(chain_name),
                name=f"process_{chain_name}"
            )
            tasks.append(task)
        
        if not tasks:
            logger.error("No chains successfully initialized! Exiting.")
            return
        
        logger.info(f"Started monitoring {len(tasks)} chains: {', '.join(successful_chains)}")
        
        # Print initial stream info
        streams_info = await backend.get_chain_streams_info()
        chain_types = {}
        for chain_name, info in streams_info.items():
            if chain_name in successful_chains:  # Only show successfully initialized chains
                chain_type = info.get("chain_type", "unknown")
                if chain_type not in chain_types:
                    chain_types[chain_type] = []
                chain_types[chain_type].append(chain_name)
        
        logger.info("Successfully initialized chains by type:")
        for chain_type, chains in chain_types.items():
            logger.info(f"  {chain_type}: {len(chains)} chains - {', '.join(chains)}")
        
        # Run all tasks concurrently
        await asyncio.gather(*tasks)
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}")
    finally:
        await backend.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        exit(1)

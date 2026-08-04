import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Any, Set, Optional, Tuple

import redis.asyncio as aioredis
import aiohttp_cors
from aiohttp import web

from history_utils import (
    HistoryCompressor,
    decode_history_entry,
    encode_history_entry,
)
from redis_keys import RedisKeys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("redis_sse_server")

# def send_discord_message(message, webhook_url=None):
#     data = {"content": message}
#     if webhook_url is None:
#         webhook_url = os.getenv('DISCORD_COMMS')
#     try:
#         response = requests.post(webhook_url, json=data, timeout=10)
#         # Check the response status code
#         if response.status_code == 204:
#             print("Message sent successfully")
#         else:
#             print(f"Error sending message: {response.status_code} - {response.text}")
#     except requests.exceptions.RequestException as e:
#         print(f"Error sending Discord message: {e}")


@dataclass
class ServerConfig:
    """Server configuration settings."""
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    server_host: str = "0.0.0.0"
    server_port: int = 8080
    update_interval: int = 1
    heartbeat_interval: int = 30
    max_history_events: int = 20
    cleanup_interval_ms: int = 60000
    max_connections: int = 500
    max_data_age_seconds: int = 60

    @classmethod
    def from_env(cls) -> 'ServerConfig':
        """Create config from environment variables."""
        return cls(
            redis_host=os.getenv("REDIS_HOST", cls.redis_host),
            redis_port=int(os.getenv("REDIS_PORT", cls.redis_port)),
            redis_db=int(os.getenv("REDIS_DB", cls.redis_db)),
            redis_password=os.getenv("REDIS_PASSWORD"),
            server_port=int(os.getenv("SERVER_PORT", cls.server_port)),
            max_connections=int(os.getenv("MAX_CONNECTIONS", cls.max_connections)),
            max_data_age_seconds=int(os.getenv("MAX_DATA_AGE_SECONDS", cls.max_data_age_seconds))
        )

@dataclass
class TPSRecord:
    """TPS record data structure."""
    value: float
    timestamp: str
    timestamp_ms: int
    chain_breakdown: Dict[str, float] = field(default_factory=dict)
    total_chains: int = 0
    active_chains: int = 0
    is_ath: bool = False
    chain_name: Optional[str] = None

class RedisSSEServer:
    """SSE Server that reads blockchain data from Redis streams."""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.redis_client: Optional[aioredis.Redis] = None
        self.connected_clients: Set[web.StreamResponse] = set()
        self.latest_data: Dict[str, Any] = {}
        self.global_metrics: Dict[str, Any] = {}
        
        # Global TPS tracking
        self.tps_ath: float = 0.0
        self.tps_ath_timestamp: str = ""
        self.tps_ath_timestamp_ms: int = 0
        self.tps_24h_high: float = 0.0
        self.tps_24h_high_timestamp: str = ""
        self.tps_24h_high_timestamp_ms: int = 0
        
        # Per-chain TPS tracking
        self.chain_metrics: Dict[str, Dict[str, Any]] = {}
        
        self._chain_cache: Optional[List[str]] = None
        self._chain_cache_time: Optional[datetime] = None
        self._cache_ttl = 60
        self._max_data_age_ms = self.config.max_data_age_seconds * 1000
        
        self.chain_clients = {}  # Track clients per chain
        self.history_compressor = HistoryCompressor()

        # Liveness tracking for the background data_update_loop
        self.last_loop_tick_ms: Optional[int] = None

    def _safe_float(self, value, default=0.0):
        try: return float(value) if value is not None else default
        except (ValueError, TypeError): return default

    def _safe_int(self, value, default=0):
        try: return int(value) if value is not None else default
        except (ValueError, TypeError): return default

    def _parse_iso_to_ms(self, value: str) -> int:
        if not value:
            return 0
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            return 0

    def _is_fresh_timestamp(self, timestamp_ms: int, now_ms: Optional[int] = None) -> bool:
        """Check whether a timestamp is within the freshness window."""
        if not timestamp_ms or timestamp_ms < 0:
            return False
        if now_ms is None:
            now_ms = int(datetime.now().timestamp() * 1000)
        age = now_ms - timestamp_ms
        return 0 <= age <= self._max_data_age_ms

    def _filter_fresh_chain_data(self, chain_data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Return only chains with data newer than the freshness window."""
        now_ms = int(datetime.now().timestamp() * 1000)
        fresh_data = {}
        stale_chains = []
        for chain_name, data in chain_data.items():
            ts_ms = self._safe_int(data.get("timestamp", 0))
            if self._is_fresh_timestamp(ts_ms, now_ms):
                fresh_data[chain_name] = data
            else:
                stale_chains.append(chain_name)
        return fresh_data, stale_chains

    async def _confirm_global_ath(self, candidate_value: float) -> bool:
        if candidate_value <= self.tps_ath:
            return False
        try:
            value, ts, ts_ms = await self.redis_client.hmget(
                RedisKeys.GLOBAL_TPS_ATH, "value", "timestamp", "timestamp_ms"
            )
        except Exception as e:
            logger.warning(f"Could not verify global ATH against Redis, skipping update: {e}")
            return False
        stored_value = self._safe_float(value, self.tps_ath)
        if stored_value > self.tps_ath:
            self.tps_ath = stored_value
            self.tps_ath_timestamp = ts or self.tps_ath_timestamp
            self.tps_ath_timestamp_ms = self._safe_int(ts_ms, self.tps_ath_timestamp_ms)
        return candidate_value > self.tps_ath

    async def _refresh_chain_ath_cache(self, chain_names: List[str]):
        if not chain_names:
            return
        try:
            if not self.redis_client:
                return
            pipe = self.redis_client.pipeline()
            for chain_name in chain_names:
                pipe.hgetall(RedisKeys.chain_tps_ath(chain_name))
            results = await pipe.execute()
            for chain_name, data in zip(chain_names, results):
                metrics = self.chain_metrics.setdefault(chain_name, {
                    "ath": 0, "ath_timestamp": "", "ath_timestamp_ms": 0,
                    "24h_high": 0, "24h_high_timestamp": "", "24h_high_timestamp_ms": 0
                })
                if data:
                    metrics["ath"] = self._safe_float(data.get("value", metrics["ath"]))
                    metrics["ath_timestamp"] = data.get("timestamp", metrics.get("ath_timestamp", ""))
                    metrics["ath_timestamp_ms"] = self._safe_int(
                        data.get("timestamp_ms", metrics.get("ath_timestamp_ms", 0))
                    )
        except Exception as e:
            logger.warning(f"Failed to refresh chain ATH cache: {e}")
        
    async def initialize(self):
        """Initialize Redis connection and load existing TPS records."""
        try:
            redis_params = {
                "host": self.config.redis_host,
                "port": self.config.redis_port,
                "db": self.config.redis_db,
                "decode_responses": True, "socket_keepalive": True,
                "retry_on_timeout": True, "health_check_interval": 30,
                "socket_timeout": 10, "socket_connect_timeout": 10,
            }
            if self.config.redis_password:
                redis_params["password"] = self.config.redis_password
                
            self.redis_client = aioredis.Redis(**redis_params)
            await self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {self.config.redis_host}:{self.config.redis_port}")
            
            await self._load_global_tps_records()
            await self._load_all_chain_metrics()
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {str(e)}")
            raise
    
    async def _load_global_tps_records(self):
        """Load existing GLOBAL TPS ATH and 24hr high from Redis."""
        try:
            pipe = self.redis_client.pipeline()
            pipe.hgetall(RedisKeys.GLOBAL_TPS_ATH)
            pipe.hgetall(RedisKeys.GLOBAL_TPS_24H)
            results = await pipe.execute()
            
            ath_data, h24_data = results
            
            if ath_data:
                self.tps_ath = self._safe_float(ath_data.get("value", 0))
                self.tps_ath_timestamp = ath_data.get("timestamp", "")
                self.tps_ath_timestamp_ms = self._safe_int(ath_data.get("timestamp_ms", 0))
                logger.info(f"📈 Loaded Global TPS ATH: {self.tps_ath}")
            
            if h24_data:
                self.tps_24h_high = self._safe_float(h24_data.get("value", 0))
                self.tps_24h_high_timestamp = h24_data.get("timestamp", "")
                self.tps_24h_high_timestamp_ms = self._safe_int(h24_data.get("timestamp_ms", 0))
                logger.info(f"📊 Loaded Global 24hr TPS High: {self.tps_24h_high}")
            
            await self._cleanup_24h_history()
            
        except Exception as e:
            logger.error(f"Error loading global TPS records: {str(e)}")

    async def _refresh_global_metrics_cache(self) -> None:
        """Refresh in-memory global ATH/24h values from Redis (supports manual corrections)."""
        if not self.redis_client:
            return
        try:
            value, ts, ts_ms = await self.redis_client.hmget(
                RedisKeys.GLOBAL_TPS_ATH, "value", "timestamp", "timestamp_ms"
            )
            self.tps_ath = self._safe_float(value, 0.0)
            self.tps_ath_timestamp = ts or ""
            self.tps_ath_timestamp_ms = self._safe_int(ts_ms, 0)

            value, ts, ts_ms = await self.redis_client.hmget(
                RedisKeys.GLOBAL_TPS_24H, "value", "timestamp", "timestamp_ms"
            )
            self.tps_24h_high = self._safe_float(value, 0.0)
            self.tps_24h_high_timestamp = ts or ""
            self.tps_24h_high_timestamp_ms = self._safe_int(ts_ms, 0)
        except Exception as e:
            logger.warning(f"Failed to refresh global metrics cache: {e}")

    async def _load_all_chain_metrics(self):
        """Load ATH and 24h high for all chains from Redis concurrently."""
        logger.info("Loading historical metrics for all chains...")
        chains = await self._get_all_chains()
        
        async def fetch_metrics(chain_name):
            try:
                pipe = self.redis_client.pipeline()
                pipe.hgetall(RedisKeys.chain_tps_ath(chain_name))
                pipe.hgetall(RedisKeys.chain_tps_24h(chain_name))
                ath_data, h24_data = await pipe.execute()
                
                self.chain_metrics[chain_name] = {
                    "ath": self._safe_float(ath_data.get("value", 0)) if ath_data else 0,
                    "ath_timestamp": ath_data.get("timestamp", "") if ath_data else "",
                    "ath_timestamp_ms": self._safe_int(ath_data.get("timestamp_ms", 0)) if ath_data else 0,
                    "24h_high": self._safe_float(h24_data.get("value", 0)) if h24_data else 0,
                    "24h_high_timestamp": h24_data.get("timestamp", "") if h24_data else "",
                    "24h_high_timestamp_ms": self._safe_int(h24_data.get("timestamp_ms", 0)) if h24_data else 0
                }
            except Exception as e:
                logger.warning(f"Could not load metrics for chain {chain_name}: {e}")

        tasks = [fetch_metrics(chain) for chain in chains]
        await asyncio.gather(*tasks)
        logger.info(f"✅ Loaded metrics for {len(self.chain_metrics)} chains.")
    
    async def _cleanup_24h_history(self):
        """Remove TPS history entries older than 24 hours for global and all chains."""
        try:
            now_ms = int(datetime.now().timestamp() * 1000)
            cutoff_time = now_ms - (24 * 60 * 60 * 1000)
            high_cutoff = self.tps_24h_high_timestamp_ms - 1 if self.tps_24h_high_timestamp_ms else 0
            global_cutoff = max(cutoff_time, high_cutoff)
            
            pipe = self.redis_client.pipeline()
            # Global cleanup
            pipe.zremrangebyscore(RedisKeys.TPS_HISTORY_24H, 0, global_cutoff)
            
            # Per-chain cleanup
            chains = await self._get_all_chains()
            for chain_name in chains:
                metrics = self.chain_metrics.get(chain_name, {})
                chain_high_ts = self._safe_int(metrics.get("24h_high_timestamp_ms", 0))
                chain_cutoff = max(cutoff_time, chain_high_ts - 1) if chain_high_ts else cutoff_time
                pipe.zremrangebyscore(RedisKeys.chain_tps_history_24h(chain_name), 0, chain_cutoff)

            results = await pipe.execute()
            removed_global = results[0]
            if removed_global > 0:
                logger.debug(f"🧹 Cleaned up {removed_global} old GLOBAL TPS history entries")

        except Exception as e:
            logger.error(f"Error cleaning up 24hr history: {str(e)}")

    async def _periodic_maintenance(self):
        """Combined maintenance: refresh 24h high and cleanup old data."""
        try:
            now_ms = int(datetime.now().timestamp() * 1000)
            min_ts = now_ms - 24 * 60 * 60 * 1000
            
            # Refresh Global 24h High
            entries = await self.redis_client.zrangebyscore(
                RedisKeys.TPS_HISTORY_24H, min_ts, now_ms
            )
            max_tps = 0
            max_entry = None
            for raw in entries:
                try:
                    data = decode_history_entry(raw)
                    if not data:
                        continue
                    tps = self._safe_float(data.get("tps", 0))
                    if tps > max_tps:
                        max_tps = tps
                        max_entry = data
                except Exception: continue

            if max_entry:
                await self.redis_client.hset(RedisKeys.GLOBAL_TPS_24H, mapping={
                    "value": str(max_tps), "timestamp": max_entry["timestamp"],
                    "timestamp_ms": max_entry["timestamp_ms"]
                })
                self.tps_24h_high = max_tps
                self.tps_24h_high_timestamp = max_entry["timestamp"]
                self.tps_24h_high_timestamp_ms = self._safe_int(max_entry.get("timestamp_ms", 0))

            # Cleanup
            await self._cleanup_24h_history()
            
            logger.info("♻️ Periodic maintenance complete (24h high refresh & history cleanup).")

        except Exception as e:
            logger.error(f"Error in periodic maintenance: {str(e)}")
    
    def _extract_chain_breakdown(self, chain_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract TPS values for each active chain."""
        breakdown = {}
        for chain_name, data in chain_data.items():
            if isinstance(data, dict) and "tps" in data:
                tps_value = self._safe_float(data.get("tps", 0))
                if tps_value > 0:
                    breakdown[chain_name] = tps_value
        return breakdown
    
    async def _store_global_tps_record(self, record: TPSRecord):
        """Store a GLOBAL TPS record in Redis."""
        try:
            history_entry = {
                "tps": record.value, "timestamp": record.timestamp,
                "timestamp_ms": record.timestamp_ms,
                "chain_breakdown": record.chain_breakdown,
                "total_chains": record.total_chains, "active_chains": record.active_chains,
                "is_ath": record.is_ath
            }
            pipe = self.redis_client.pipeline()
            if record.is_ath:
                entry_json = json.dumps(history_entry)
                pipe.zadd(RedisKeys.ATH_HISTORY, {entry_json: record.timestamp_ms})

            compact_entry = encode_history_entry(record.value, record.timestamp_ms, record.is_ath)
            redundant = self.history_compressor.register(
                RedisKeys.TPS_HISTORY_24H, record.value, record.timestamp_ms, compact_entry
            )
            pipe.zadd(RedisKeys.TPS_HISTORY_24H, {compact_entry: record.timestamp_ms})
            if redundant:
                pipe.zrem(RedisKeys.TPS_HISTORY_24H, redundant)
            await pipe.execute()
        except Exception as e:
            logger.error(f"Error storing global TPS record: {str(e)}")
            
    async def _update_all_chain_records(self, chain_data: Dict[str, Any]):
        """Update 24h high records for each individual chain."""
        now = datetime.now()
        timestamp = now.isoformat()
        timestamp_ms = int(now.timestamp() * 1000)

        await self._refresh_chain_ath_cache(list(chain_data.keys()))

        for chain_name, data in chain_data.items():
            current_tps = self._safe_float(data.get("tps", 0))
            if current_tps <= 0:
                continue

            metrics = self.chain_metrics.setdefault(chain_name, {
                "ath": 0, "ath_timestamp": "", "ath_timestamp_ms": 0,
                "24h_high": 0, "24h_high_timestamp": "", "24h_high_timestamp_ms": 0
            })
            
            # Check if 24h high is expired or a new high
            is_new_24h_high = False
            last_24h_ts_ms = self._safe_int(metrics.get("24h_high_timestamp_ms", 0), 0)
            if last_24h_ts_ms <= 0:
                last_24h_ts_ms = self._parse_iso_to_ms(metrics.get("24h_high_timestamp", ""))

            if last_24h_ts_ms > 0:
                if timestamp_ms - last_24h_ts_ms > 24 * 60 * 60 * 1000:
                    is_new_24h_high = True  # Expired, so this is the new high
                    metrics["24h_high"] = 0
                    metrics["24h_high_timestamp"] = ""
                    metrics["24h_high_timestamp_ms"] = 0
            else:  # No previous valid 24h high timestamp
                is_new_24h_high = True

            if not is_new_24h_high and current_tps > metrics.get("24h_high", 0):
                is_new_24h_high = True
            
            pipe = self.redis_client.pipeline()

            if is_new_24h_high:
                metrics["24h_high"] = current_tps
                metrics["24h_high_timestamp"] = timestamp
                metrics["24h_high_timestamp_ms"] = timestamp_ms
                pipe.hset(RedisKeys.chain_tps_24h(chain_name), mapping={
                    "value": str(current_tps), "timestamp": timestamp, "timestamp_ms": str(timestamp_ms)
                })

            if pipe:
                await pipe.execute()

    # In RedisSSEServer class

    async def _update_global_tps_records(self, current_tps: float, timestamp: str, chain_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update GLOBAL TPS ATH and 24hr high records, and return the new values."""
        try:
            current_timestamp_ms = int(datetime.now().timestamp() * 1000)
            chain_breakdown = self._extract_chain_breakdown(chain_data)
            
            # Start with current values
            new_ath = self.tps_ath
            new_ath_timestamp = self.tps_ath_timestamp
            new_24h_high = self.tps_24h_high
            new_24h_high_timestamp = self.tps_24h_high_timestamp

            is_new_ath = await self._confirm_global_ath(current_tps)
            pipe = self.redis_client.pipeline()

            if is_new_ath:
                old_ath = self.tps_ath
                self.tps_ath = current_tps
                self.tps_ath_timestamp = timestamp
                self.tps_ath_timestamp_ms = current_timestamp_ms
                new_ath, new_ath_timestamp = self.tps_ath, self.tps_ath_timestamp # Capture new values

                pipe.hset(RedisKeys.GLOBAL_TPS_ATH, mapping={
                    "value": str(current_tps), "timestamp": timestamp, "timestamp_ms": str(current_timestamp_ms)
                })
                logger.info(f"🚀 NEW GLOBAL TPS ALL-TIME HIGH: {current_tps} TPS! (Previous: {old_ath})")
                ath_message = f"""🚀 NEW GLOBAL TPS ALL-TIME HIGH: {current_tps:.2f} TPS! (Previous: {old_ath:.2f})
                \nTimestamp: {timestamp}\nActive Chains: {len(chain_breakdown)}\nChain Breakdown: {json.dumps(chain_breakdown, indent=2)}"""
                #send_discord_message(ath_message)

            if current_tps > self.tps_24h_high:
                self.tps_24h_high = current_tps
                self.tps_24h_high_timestamp = timestamp
                self.tps_24h_high_timestamp_ms = current_timestamp_ms
                new_24h_high, new_24h_high_timestamp = self.tps_24h_high, self.tps_24h_high_timestamp # Capture new values

                pipe.hset(RedisKeys.GLOBAL_TPS_24H, mapping={
                    "value": str(current_tps), "timestamp": timestamp, "timestamp_ms": str(current_timestamp_ms)
                })
                if not is_new_ath: # Avoid redundant logging if it's already an ATH
                    logger.info(f"📊 NEW GLOBAL 24HR TPS HIGH: {current_tps} TPS!")
            
            if pipe: await pipe.execute()

            record = TPSRecord(
                value=current_tps, timestamp=timestamp, timestamp_ms=current_timestamp_ms,
                chain_breakdown=chain_breakdown, total_chains=len(chain_data),
                active_chains=len(chain_breakdown), is_ath=is_new_ath
            )
            await self._store_global_tps_record(record)
            
            # Return a dictionary with the definitive values for this run
            return {
                "ath": new_ath,
                "ath_timestamp": new_ath_timestamp,
                "24h_high": new_24h_high,
                "24h_high_timestamp": new_24h_high_timestamp,
            }

        except Exception as e:
            logger.error(f"Error updating global TPS records: {str(e)}")
            # Return current state on error
            return {
                "ath": self.tps_ath,
                "ath_timestamp": self.tps_ath_timestamp,
                "24h_high": self.tps_24h_high,
                "24h_high_timestamp": self.tps_24h_high_timestamp,
            }

    # In RedisSSEServer class

    async def _calculate_global_metrics(self, chain_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate global metrics from chain data."""
        try:
            await self._refresh_global_metrics_cache()
            chain_data, stale_chains = self._filter_fresh_chain_data(chain_data)
            if stale_chains:
                logger.debug(f"Excluded stale chain data from global metrics: {', '.join(stale_chains)}")

            await self._update_all_chain_records(chain_data)

            tps_values = [self._safe_float(data.get("tps", 0)) for data in chain_data.values() if isinstance(data, dict)]
            total_tps = sum(tps_values)
            
            current_timestamp = datetime.now().isoformat()
            
            # Get the definitive record values for this calculation cycle
            updated_records = {
                "ath": self.tps_ath, "ath_timestamp": self.tps_ath_timestamp,
                "24h_high": self.tps_24h_high, "24h_high_timestamp": self.tps_24h_high_timestamp
            }
            if total_tps > 0:
                updated_records = await self._update_global_tps_records(total_tps, current_timestamp, chain_data)
            
            highest_tps = max(tps_values, default=0)
            active_chains = sum(1 for tps in tps_values if tps > 0)
            eth_data = chain_data.get("ethereum", {})
            ethereum_tx_cost_usd = self._safe_float(eth_data.get("tx_cost_erc20_transfer_usd"))
            ethereum_tx_cost_eth = self._safe_float(eth_data.get("tx_cost_erc20_transfer"))
            l2_metrics = self._calculate_l2_metrics(chain_data)
            
            return {
                "total_tps": round(total_tps, 1), "highest_tps": round(highest_tps, 1),
                "highest_l2_cost_usd": l2_metrics["highest_cost_usd"],
                "total_chains": len(chain_data), "active_chains": active_chains,
                "ethereum_tx_cost_usd": ethereum_tx_cost_usd, "ethereum_tx_cost_eth": ethereum_tx_cost_eth,
                "layer2s_tx_cost_usd": l2_metrics["avg_cost_usd"], "layer2s_tx_cost_eth": l2_metrics["avg_cost_eth"],
                # Use the explicit values returned from the update function
                "total_tps_ath": round(updated_records["ath"], 1),
                "total_tps_ath_timestamp": updated_records["ath_timestamp"],
                "total_tps_24h_high": round(updated_records["24h_high"], 1),
                "total_tps_24h_high_timestamp": updated_records["24h_high_timestamp"],
                "last_updated": current_timestamp
            }
        except Exception as e:
            logger.error(f"Error calculating global metrics: {str(e)}")
            return {"error": str(e)}
    
    async def periodic_maintenance_loop(self):
        """Combined maintenance loop: refresh 24h high and cleanup old data every 5 minutes."""
        logger.info("Starting periodic maintenance loop (every 5 minutes)")
        while True:
            try:
                await self._periodic_maintenance()
            except Exception as e:
                logger.error(f"Error in maintenance loop: {e}")
            await asyncio.sleep(300)
   
    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
    
    async def _get_all_chains(self) -> List[str]:
        """Get all chain names from Redis with caching."""
        now = datetime.now()

        if (self._chain_cache and self._chain_cache_time and
            (now - self._chain_cache_time).total_seconds() < self._cache_ttl):
            return self._chain_cache

        try:
            chains = set()
            cursor = 0
            pattern = "chain:*"
            while True:
                cursor, keys = await self.redis_client.scan(cursor=cursor, match=pattern, count=1000)
                for key in keys:
                    if ":tps:" in key:
                        continue  # Filter out metric keys
                    parts = key.split(':', 2)
                    if len(parts) >= 2:
                        chains.add(parts[1])
                if cursor == 0:
                    break

            self._chain_cache = sorted(list(chains))
            self._chain_cache_time = now
            return self._chain_cache

        except Exception as e:
            logger.error(f"Error getting chain keys: {str(e)}")
            return self._chain_cache or []
    
    async def _get_latest_chain_data(self, chain_name: str) -> Dict[str, Any]:
        """Get latest data for a specific chain from Redis stream."""
        try:
            stream_key = RedisKeys.chain_stream(chain_name)
            entries = await self.redis_client.xrevrange(stream_key, count=1)
            
            if not entries:
                return {"chain_name": chain_name, "display_name": chain_name, "tps": 0, "error": "No data available"}
            
            _entry_id, fields = entries[0]
            timestamp = self._safe_int(fields.get("timestamp", 0))
            
            return {
                "chain_name": chain_name, "display_name": fields.get("display_name", chain_name),
                "block_time": self._safe_float(fields.get("block_time", 0)),
                "tps": self._safe_float(fields.get("tps", 0)), "timestamp": timestamp,
                "tx_cost_erc20_transfer": self._safe_float(fields.get("tx_cost_erc20_transfer", 0)),
                "tx_cost_erc20_transfer_usd": self._safe_float(fields.get("tx_cost_erc20_transfer_usd", 0)),
                "tx_cost_swap": self._safe_float(fields.get("tx_cost_swap", 0)),
                "tx_cost_swap_usd": self._safe_float(fields.get("tx_cost_swap_usd", 0)),
                "tx_cost_avg": self._safe_float(fields.get("tx_cost_avg", 0)),
                "tx_cost_avg_usd": self._safe_float(fields.get("tx_cost_avg_usd", 0)),
                "tx_cost_median": self._safe_float(fields.get("tx_cost_median", 0)),
                "tx_cost_median_usd": self._safe_float(fields.get("tx_cost_median_usd", 0)),
                "last_updated": datetime.fromtimestamp(timestamp / 1000).isoformat() if timestamp else ""
            }
        except Exception as e:
            logger.error(f"Error getting data for {chain_name}: {str(e)}")
            return {"chain_name": chain_name, "display_name": chain_name, "tps": 0, "error": str(e)}

    async def _get_all_chain_data(self) -> Dict[str, Any]:
        """Get latest data for all chains concurrently."""
        chains = await self._get_all_chains()
        tasks = [self._get_latest_chain_data(chain) for chain in chains]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        chain_data = {}
        for chain_name, result in zip(chains, results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching data for {chain_name}: {result}")
                chain_data[chain_name] = {"chain_name": chain_name, "tps": 0, "error": str(result)}
            else:
                chain_data[chain_name] = result
        return chain_data
    
    def _calculate_l2_metrics(self, chain_data: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """Calculate Layer 2 transaction cost metrics."""
        l2_data = []
        for name, data in chain_data.items():
            if name != "ethereum" and isinstance(data, dict):
                tps = self._safe_float(data.get("tps", 0))
                cost_usd = self._safe_float(data.get("tx_cost_erc20_transfer_usd", 0))
                cost_eth = self._safe_float(data.get("tx_cost_erc20_transfer", 0))
                if tps > 0 and cost_usd > 0:
                    l2_data.append((cost_usd, cost_eth, tps))
        
        if not l2_data: return {"avg_cost_usd": None, "avg_cost_eth": None, "highest_cost_usd": None}
        
        total_weighted_usd = sum(cost_usd * tps for cost_usd, _, tps in l2_data)
        total_weighted_eth = sum(cost_eth * tps for _, cost_eth, tps in l2_data)
        total_tps = sum(tps for _, _, tps in l2_data)
        
        return {
            "avg_cost_usd": total_weighted_usd / total_tps if total_tps > 0 else None,
            "avg_cost_eth": total_weighted_eth / total_tps if total_tps > 0 else None,
            "highest_cost_usd": max(cost_usd for cost_usd, _, _ in l2_data)
        }

    async def _update_data(self):
        """Fetch latest data from Redis and update internal state."""
        try:
            chain_data = await self._get_all_chain_data()
            fresh_data, stale_chains = self._filter_fresh_chain_data(chain_data)
            if stale_chains:
                logger.debug(f"Ignoring stale chain data (> {self.config.max_data_age_seconds}s): {', '.join(stale_chains)}")

            global_metrics = await self._calculate_global_metrics(fresh_data)
            self.latest_data = fresh_data
            self.global_metrics = global_metrics
            logger.debug(f"Updated data for {len(fresh_data)} fresh chains, total TPS: {global_metrics.get('total_tps', 0)}")
        except Exception as e:
            logger.error(f"Error updating data: {str(e)}")
    
    async def _broadcast_to_clients(self):
        """Broadcast updated data to all connected SSE clients."""
        if not self.connected_clients: return
        try:
            message = json.dumps({
                "type": "update", "data": self.latest_data,
                "global_metrics": self.global_metrics,
                "timestamp": datetime.now().isoformat()
            })
            sse_message = f"data: {message}\n\n"
            disconnected_clients = set()
            for client in self.connected_clients:
                try: await client.write(sse_message.encode('utf-8'))
                except (ConnectionResetError, ConnectionAbortedError, OSError):
                    disconnected_clients.add(client)
                except Exception as e:
                    logger.warning(f"Error sending to client: {str(e)}")
                    disconnected_clients.add(client)
            if disconnected_clients:
                self.connected_clients.difference_update(disconnected_clients)
                logger.info(f"Removed {len(disconnected_clients)} disconnected clients. {len(self.connected_clients)} remaining.")
        except Exception as e:
            logger.error(f"Error broadcasting to clients: {str(e)}")
    
    async def data_update_loop(self):
        """Background task that continuously updates data and broadcasts to clients."""
        logger.info(f"Starting data update loop with {self.config.update_interval}s interval")
        while True:
            try:
                # Marks that the loop is still alive and cycling, independent of
                # whether this iteration succeeds or raises. If a call below hangs
                # (e.g. a stalled Redis connection with no socket timeout), this
                # timestamp stops advancing and /health can surface it.
                self.last_loop_tick_ms = int(datetime.now().timestamp() * 1000)

                await self._update_data()
                await self._broadcast_to_clients()  # Global broadcast
                
                # Broadcast to chain-specific clients
                for chain_name in self.latest_data.keys():
                    if chain_name in self.chain_clients:
                        enhanced_data = await self._get_chain_sse_data(chain_name)
                        if enhanced_data is None:
                            continue
                        await self._broadcast_to_chain_clients(chain_name, enhanced_data)
                
                await asyncio.sleep(self.config.update_interval)
            except Exception as e:
                logger.error(f"Error in data update loop: {str(e)}")
                await asyncio.sleep(5)
                
    ## chain sse methods
    async def chain_sse_handler(self, request):
        """Handle chain-specific SSE connection requests."""
        chain_name = request.match_info.get('chain_name')
        if not chain_name:
            return web.Response(status=400, text="Chain name not provided")
        
        # Check if chain exists
        if chain_name not in await self._get_all_chains():
            return web.Response(status=404, text=f"Chain '{chain_name}' not found")
        
        # Count total connections (global + chain)
        total_connections = len(self.connected_clients) + sum(len(clients) for clients in self.chain_clients.values())
        if total_connections >= self.config.max_connections:
            return web.Response(status=503, text="Too many connections")
        
        response = web.StreamResponse()
        response.headers.update({
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        })
        
        client_ip = request.remote
        logger.info(f"New chain SSE client connected from {client_ip} for chain: {chain_name}")
        
        try:
            await response.prepare(request)
            
            # Track chain-specific client ONLY (don't add to global clients)
            if chain_name not in self.chain_clients:
                self.chain_clients[chain_name] = set()
            self.chain_clients[chain_name].add(response)
            
            # Send initial chain data
            initial_data = await self._get_chain_sse_data(chain_name)
            initial_payload = {
                "type": "initial",
                "chain_name": chain_name,
                "data": initial_data,
                "timestamp": datetime.now().isoformat()
            }
            if initial_data is None:
                initial_payload["error"] = f"No fresh data available (>{self.config.max_data_age_seconds}s old)"

            initial_message = json.dumps(initial_payload)
            await response.write(f"data: {initial_message}\n\n".encode('utf-8'))
            
            # Keep connection alive with heartbeats
            while True:
                await asyncio.sleep(self.config.heartbeat_interval)
                try:
                    await response.write(b": heartbeat\n\n")
                except Exception:
                    break
                    
        except (ConnectionResetError, ConnectionAbortedError):
            logger.info(f"Chain SSE client {client_ip} disconnected from chain: {chain_name}")
        finally:
            # Clean up chain-specific client tracking only
            if chain_name in self.chain_clients:
                self.chain_clients[chain_name].discard(response)
                if not self.chain_clients[chain_name]:
                    del self.chain_clients[chain_name]
        
        return response

    async def _get_chain_sse_data(self, chain_name: str):
        """Get comprehensive chain data for SSE streaming, skipping stale entries."""
        try:
            # Get current chain data
            current_data = await self._get_latest_chain_data(chain_name)
            timestamp_ms = self._safe_int(current_data.get("timestamp", 0))
            if not self._is_fresh_timestamp(timestamp_ms):
                logger.info(f"Skipping stale data for chain {chain_name} (>{self.config.max_data_age_seconds}s old)")
                return None
            
            # Get chain metrics (ATH, 24h high)
            metrics = self.chain_metrics.get(chain_name, {})
            
            # Combine all data
            sse_data = {
                **current_data,
                "ath": metrics.get("ath", 0),
                "ath_timestamp": metrics.get("ath_timestamp", ""),
                "24h_high": metrics.get("24h_high", 0),
                "24h_high_timestamp": metrics.get("24h_high_timestamp", ""),
                "is_active": current_data.get("tps", 0) > 0
            }
            
            return sse_data
            
        except Exception as e:
            logger.error(f"Error getting chain SSE data for {chain_name}: {str(e)}")
            return {
                "chain_name": chain_name,
                "error": str(e),
                "tps": 0,
                "ath": 0,
                "24h_high": 0
            }

    async def _broadcast_to_chain_clients(self, chain_name: str, data):
        """Broadcast updates to clients subscribed to a specific chain."""
        if data is None:
            return
        if chain_name not in self.chain_clients or not self.chain_clients[chain_name]:
            return
            
        try:
            message = json.dumps({
                "type": "update",
                "chain_name": chain_name,
                "data": data,
                "timestamp": datetime.now().isoformat()
            })
            
            sse_message = f"data: {message}\n\n"
            disconnected_clients = set()
            
            for client in self.chain_clients[chain_name]:
                try:
                    await client.write(sse_message.encode('utf-8'))
                except (ConnectionResetError, ConnectionAbortedError, OSError):
                    disconnected_clients.add(client)
                except Exception as e:
                    logger.warning(f"Error sending to chain client: {str(e)}")
                    disconnected_clients.add(client)
            
            # Clean up disconnected clients
            if disconnected_clients:
                self.chain_clients[chain_name].difference_update(disconnected_clients)
                for client in disconnected_clients:
                    self.connected_clients.discard(client)
                
                logger.info(f"Removed {len(disconnected_clients)} disconnected clients from chain {chain_name}")
                
        except Exception as e:
            logger.error(f"Error broadcasting to chain {chain_name} clients: {str(e)}")
    
    async def sse_handler(self, request):
        """Handle SSE connection requests."""
        if len(self.connected_clients) >= self.config.max_connections:
            return web.Response(status=503, text="Too many connections")
        
        response = web.StreamResponse()
        response.headers.update({
            'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache',
            'Connection': 'keep-alive', 'X-Accel-Buffering': 'no'
        })
        client_ip = request.remote
        logger.info(f"New SSE client connected from {client_ip}")
        try:
            await response.prepare(request)
            self.connected_clients.add(response)
            initial_message = json.dumps({
                "type": "initial", "data": self.latest_data,
                "global_metrics": self.global_metrics, "timestamp": datetime.now().isoformat()
            })
            await response.write(f"data: {initial_message}\n\n".encode('utf-8'))
            while True:
                await asyncio.sleep(self.config.heartbeat_interval)
                try: await response.write(b": heartbeat\n\n")
                except Exception: break
        except (ConnectionResetError, ConnectionAbortedError):
            logger.info(f"Client {client_ip} disconnected")
        finally:
            self.connected_clients.discard(response)
        return response
    
    async def health_handler(self, request):
        """Health check endpoint."""
        chain_connections = sum(len(clients) for clients in self.chain_clients.values())
        total_connections = len(self.connected_clients) + chain_connections

        # A stalled Redis call (or any other silent hang) stops last_loop_tick_ms
        # from advancing without ever raising, so connection counts and cached
        # metrics below can look "healthy" even while no new data is flowing.
        now_ms = int(datetime.now().timestamp() * 1000)
        loop_stale_threshold_ms = max(self.config.update_interval * 5, 15) * 1000
        data_loop_age_ms = (now_ms - self.last_loop_tick_ms) if self.last_loop_tick_ms else None
        data_loop_healthy = data_loop_age_ms is not None and data_loop_age_ms <= loop_stale_threshold_ms

        status = "healthy" if data_loop_healthy else "degraded"
        body = {
            "status": status,
            "data_loop_healthy": data_loop_healthy,
            "data_loop_last_tick_ms_ago": data_loop_age_ms,
            "global_clients": len(self.connected_clients),
            "chain_clients": chain_connections,
            "total_connections": total_connections,
            "max_connections": self.config.max_connections,
            "total_chains": len(self.latest_data),
            "active_chains": self.global_metrics.get("active_chains", 0),
            "total_tps": self.global_metrics.get("total_tps", 0),
            "total_tps_ath": self.global_metrics.get("total_tps_ath", 0),
            "total_tps_24h_high": self.global_metrics.get("total_tps_24h_high", 0),
            "redis_connected": self.redis_client is not None,
            "chain_cache_active": self._chain_cache is not None,
            "timestamp": datetime.now().isoformat()
        }
        return web.json_response(body, status=200 if data_loop_healthy else 503)
    
    async def api_data_handler(self, request):
        """REST API endpoint to get current global data."""
        return web.json_response({
            "data": self.latest_data, "global_metrics": self.global_metrics,
            "timestamp": datetime.now().isoformat()
        })
    
    async def history_handler(self, request):
        """REST API endpoint to get global TPS history."""
        try:
            limit = int(request.query.get('limit', 50))
            hours = int(request.query.get('hours', 24))
            now_ms = int(datetime.now().timestamp() * 1000)
            min_ts = now_ms - (hours * 60 * 60 * 1000)
            
            raw_entries = await self.redis_client.zrevrangebyscore(
                RedisKeys.TPS_HISTORY_24H, now_ms, min_ts, start=0, num=limit
            )
            history = [decode_history_entry(raw_entry) for raw_entry in raw_entries]
            history = [entry for entry in history if entry]
            
            avg_tps = max_tps = min_tps = 0
            if history:
                tps_values = [h['tps'] for h in history]
                if tps_values:
                    avg_tps = sum(tps_values) / len(tps_values)
                    max_tps = max(tps_values)
                    min_tps = min(tps_values)

            return web.json_response({
                "history": history,
                "summary": {
                    "total_events": len(history), "time_range_hours": hours,
                    "avg_tps": round(avg_tps, 2), "max_tps": round(max_tps, 2),
                    "min_tps": round(min_tps, 2), "current_ath": round(self.tps_ath, 1),
                    "current_24h_high": round(self.tps_24h_high, 1),
                }, "timestamp": datetime.now().isoformat()
            })
        except ValueError as e:
            return web.json_response({"error": "Invalid query parameters", "message": str(e)}, status=400)
        except Exception as e:
            logger.error(f"Error in global history handler: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def chain_data_handler(self, request):
        """REST API endpoint to get current data for a single chain."""
        chain_name = request.match_info.get('chain_name')
        if not chain_name:
            return web.json_response({"error": "Chain name not provided"}, status=400)
            
        latest_data = self.latest_data.get(chain_name)
        metrics = self.chain_metrics.get(chain_name)

        if not latest_data:
            return web.json_response({"error": f"Data for chain '{chain_name}' not found"}, status=404)

        return web.json_response({
            "data": latest_data,
            "metrics": metrics,
            "timestamp": datetime.now().isoformat()
        })

    async def chain_history_handler(self, request):
        """REST API endpoint to get TPS history for a single chain."""
        chain_name = request.match_info.get('chain_name')
        if not chain_name:
            return web.json_response({"error": "Chain name not provided"}, status=400)
            
        try:
            limit = int(request.query.get('limit', 50))
            hours = int(request.query.get('hours', 24))
            
            now_ms = int(datetime.now().timestamp() * 1000)
            min_ts = now_ms - (hours * 60 * 60 * 1000)
            
            key = RedisKeys.chain_tps_history_24h(chain_name)
            raw_entries = await self.redis_client.zrevrangebyscore(
                key, now_ms, min_ts, start=0, num=limit
            )
            history = [decode_history_entry(entry) for entry in raw_entries]
            history = [entry for entry in history if entry]
            return web.json_response({"history": history, "timestamp": datetime.now().isoformat()})

        except ValueError as e:
            return web.json_response({"error": "Invalid query parameters", "message": str(e)}, status=400)
        except Exception as e:
            logger.error(f"Error in chain_history_handler for {chain_name}: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

async def create_app(server: RedisSSEServer):
    """Create and configure the aiohttp application."""
    app = web.Application()
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True, expose_headers="*", allow_headers="*", allow_methods="*"
        )
    })
    
    routes = [
        web.get("/events", server.sse_handler),  # Global SSE
        web.get("/events/chain/{chain_name}", server.chain_sse_handler),  # Chain SSE
        web.get("/health", server.health_handler),
        web.get("/api/data", server.api_data_handler),
        web.get("/api/history", server.history_handler),
        web.get("/api/chain/{chain_name}", server.chain_data_handler),
        web.get("/api/chain/{chain_name}/history", server.chain_history_handler),
    ]

    for route in routes:
        cors.add(app.router.add_route(route.method, route.path, route.handler))
    
    return app

async def main():
    """Main function to start the SSE server."""
    config = ServerConfig.from_env()
    server = RedisSSEServer(config)
    
    try:
        await server.initialize()
        app = await create_app(server)
        
        update_task = asyncio.create_task(server.data_update_loop())
        maintenance_task = asyncio.create_task(server.periodic_maintenance_loop())
        
        logger.info(f"🚀 Starting SSE server on {config.server_host}:{config.server_port}")
        logger.info(f"📡 Global SSE endpoint: /events")
        logger.info(f"📡 Chain SSE endpoint: /events/chain/{{chain_name}}")
        logger.info(f"🏥 Health check: /health")
        logger.info(f"📈 Global API: /api/data")
        logger.info(f"📊 Global History: /api/history")
        logger.info(f"⛓️ Chain Data: /api/chain/{{chain_name}}")
        logger.info(f"📜 Chain History: /api/chain/{{chain_name}}/history")
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, config.server_host, config.server_port)
        await site.start()

        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("🛑 Received shutdown signal")
        finally:
            logger.info("🧹 Cleaning up...")
            update_task.cancel()
            maintenance_task.cancel()
            await server.close()
            await runner.cleanup()
            logger.info("✅ Shutdown complete")
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Server shutdown requested by user")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        exit(1)

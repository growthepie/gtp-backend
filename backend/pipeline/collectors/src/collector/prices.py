"""
Price reader: Reads token prices from prices.json file.

Used by collectors to get USD conversion rates.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import structlog

logger = structlog.get_logger()

# Default path relative to project root
DEFAULT_PRICES_PATH = Path(__file__).parent.parent.parent / "data" / "prices.json"


class PriceReader:
    """
    Reads token prices from a JSON file with caching.
    
    Features:
    - Caches prices in memory
    - Only re-reads file if modified
    - Warns if prices are stale
    - Falls back to last known prices on error
    """
    
    def __init__(
        self,
        prices_path: Path = DEFAULT_PRICES_PATH,
        stale_threshold_seconds: int = 1200,  # 20 minutes
    ):
        self.prices_path = prices_path
        self.stale_threshold = stale_threshold_seconds
        
        self._prices: dict[str, float] = {}
        self._updated_at: Optional[datetime] = None
        self._file_mtime: float = 0
    
    def _maybe_reload(self) -> None:
        """Reload prices from file if it has been modified."""
        
        try:
            if not self.prices_path.exists():
                logger.warning("Prices file not found", path=str(self.prices_path))
                return
            
            # Check if file was modified
            current_mtime = self.prices_path.stat().st_mtime
            if current_mtime <= self._file_mtime:
                return  # No change
            
            # Read and parse
            with open(self.prices_path) as f:
                data = json.load(f)
            
            self._prices = data.get("prices", {})
            
            updated_str = data.get("updated_at")
            if updated_str:
                self._updated_at = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
            
            self._file_mtime = current_mtime
            
            logger.debug(
                "Reloaded prices",
                count=len(self._prices),
                updated_at=updated_str,
            )
            
        except Exception as e:
            logger.error("Failed to read prices file", error=str(e))
    
    def get_price(self, symbol: str) -> Optional[float]:
        """Get USD price for a token symbol (e.g., 'ETH')."""
        self._maybe_reload()
        return self._prices.get(symbol)
    
    def get_eth_price(self) -> float:
        """Get ETH/USD price, with fallback."""
        price = self.get_price("ETH")
        if price is None:
            logger.warning("ETH price not available, using fallback")
            return 3000.0  # Fallback
        return price
    
    def get_all_prices(self) -> dict[str, float]:
        """Get all available prices."""
        self._maybe_reload()
        return self._prices.copy()
    
    def is_stale(self) -> bool:
        """Check if prices are older than threshold."""
        if self._updated_at is None:
            return True
        
        age = (datetime.now(timezone.utc) - self._updated_at).total_seconds()
        return age > self.stale_threshold
    
    def convert_to_usd(self, amount: float, symbol: str) -> float:
        """Convert an amount in native token to USD."""
        price = self.get_price(symbol)
        if price is None:
            logger.warning("No price for symbol, returning 0", symbol=symbol)
            return 0.0
        return amount * price
    
    def convert_to_eth(self, amount: float, symbol: str) -> float:
        """Convert an amount in native token to ETH."""
        if symbol == "ETH":
            return amount
        
        token_price = self.get_price(symbol)
        eth_price = self.get_eth_price()
        
        if token_price is None:
            logger.warning("No price for symbol, returning 0", symbol=symbol)
            return 0.0
        
        # amount in token * token_price_usd / eth_price_usd = amount in ETH
        return (amount * token_price) / eth_price


# Global instance for convenience
_default_reader: Optional[PriceReader] = None


def get_price_reader() -> PriceReader:
    """Get or create the default price reader instance."""
    global _default_reader
    if _default_reader is None:
        _default_reader = PriceReader()
    return _default_reader

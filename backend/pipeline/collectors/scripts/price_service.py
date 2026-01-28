"""
Price Service: Fetches token prices from CoinGecko and writes to prices.json

Run as a standalone service:
    python scripts/price_service.py

Or with custom interval:
    python scripts/price_service.py --interval 600
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import structlog
import dotenv

dotenv.load_dotenv()

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Token mappings: our symbol -> CoinGecko ID
TOKEN_IDS = {
    "ETH": "ethereum",
    "MATIC": "matic-network",
    "BNB": "binancecoin",
    "AVAX": "avalanche-2",
    "FTM": "fantom",
    "OP": "optimism",
    "ARB": "arbitrum",
    "METIS": "metis-token",
    "CELO": "celo",
    "GLMR": "moonbeam",
    "KAVA": "kava",
    "MNT": "mantle",
    "IMX": "immutable-x",
}

# Default output path
DEFAULT_OUTPUT_PATH = Path(__file__).parent.parent / "data" / "prices.json"


class PriceService:
    def __init__(
        self,
        api_key: str,
        output_path: Path,
        tokens: list[str] | None = None,
    ):
        self.api_key = api_key
        self.output_path = output_path
        self.tokens = tokens or list(TOKEN_IDS.keys())
        self.base_url = "https://pro-api.coingecko.com/api/v3"
        self.last_prices: dict[str, float] = {}
        
        # Ensure output directory exists
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
    
    def fetch_prices(self) -> dict[str, float]:
        """Fetch current USD prices from CoinGecko."""
        
        # Build comma-separated list of CoinGecko IDs
        ids = [TOKEN_IDS[t] for t in self.tokens if t in TOKEN_IDS]
        ids_param = ",".join(ids)
        
        url = f"{self.base_url}/simple/price"
        params = {
            "ids": ids_param,
            "vs_currencies": "usd",
        }
        headers = {
            "x-cg-pro-api-key": self.api_key,
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Map back to our token symbols
            prices = {}
            for symbol, cg_id in TOKEN_IDS.items():
                if cg_id in data and "usd" in data[cg_id]:
                    prices[symbol] = data[cg_id]["usd"]
            
            logger.info("Fetched prices", count=len(prices))
            return prices
            
        except requests.RequestException as e:
            logger.error("Failed to fetch prices", error=str(e))
            return {}
    
    def write_prices(self, prices: dict[str, float]) -> None:
        """Write prices to JSON file."""
        
        if not prices:
            logger.warning("No prices to write, keeping previous file")
            return
        
        # Update cache
        self.last_prices.update(prices)
        
        output = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "prices": self.last_prices,
        }
        
        # Write atomically (write to temp, then rename)
        temp_path = self.output_path.with_suffix(".tmp")
        with open(temp_path, "w") as f:
            json.dump(output, f, indent=2)
        temp_path.rename(self.output_path)
        
        logger.info("Wrote prices", path=str(self.output_path), count=len(self.last_prices))
    
    def run_once(self) -> None:
        """Fetch and write prices once."""
        prices = self.fetch_prices()
        self.write_prices(prices)
    
    def run_loop(self, interval_seconds: int = 600) -> None:
        """Run continuously, fetching prices at interval."""
        
        logger.info("Starting price service", interval=interval_seconds)
        
        while True:
            try:
                self.run_once()
            except Exception as e:
                logger.error("Error in price loop", error=str(e))
            
            time.sleep(interval_seconds)


def main():
    parser = argparse.ArgumentParser(description="Fetch token prices from CoinGecko")
    parser.add_argument(
        "--interval",
        type=int,
        default=600,
        help="Fetch interval in seconds (default: 600 = 10 min)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output JSON file path",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (don't loop)",
    )
    args = parser.parse_args()
    
    # Get API key from environment
    api_key = os.environ.get("COINGECKO_API")
    if not api_key:
        logger.error("COINGECKO_API environment variable not set")
        return
    
    service = PriceService(api_key=api_key, output_path=args.output)
    
    if args.once:
        service.run_once()
    else:
        service.run_loop(interval_seconds=args.interval)


if __name__ == "__main__":
    main()

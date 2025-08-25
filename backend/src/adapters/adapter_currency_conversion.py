"""
Currency Conversion Adapter for Non-USD Stablecoins

This adapter fetches live exchange rates from multiple APIs and provides
currency conversion functionality for non-USD stablecoins.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Tuple, List

from src.adapters.abstract_adapters import AbstractAdapter
from src.currency_config import (
    get_supported_currencies,
    validate_exchange_rate,
    calculate_forex_rate_from_coingecko,
    calculate_forex_rate_from_alternative,
    get_primary_exchange_rate_url,
    get_backup_exchange_rate_url,
    RATE_FETCH_CONFIG
)
from src.misc.helper_functions import api_get_call, print_init, print_load, print_extract

class AdapterCurrencyConversion(AbstractAdapter):
    """
    Adapter for fetching and managing exchange rates for currency conversion.
    
    This adapter supports:
    1. Real-time exchange rate fetching from multiple APIs
    2. Rate validation and error handling
    3. Database storage for historical tracking
    4. Caching to minimize API calls
    5. Fallback mechanisms for API failures
    
    adapter_params can include:
        currencies: list - Specific currencies to fetch (default: all supported)
        cache_duration: int - Cache duration in seconds (default: 3600)
        force_refresh: bool - Force refresh rates regardless of cache (default: False)
    """
    
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Currency Conversion", adapter_params, db_connector)
        
        # Configuration
        self.currencies = adapter_params.get('currencies', get_supported_currencies())
        self.cache_duration = adapter_params.get('cache_duration', RATE_FETCH_CONFIG['cache_duration'])
        self.force_refresh = adapter_params.get('force_refresh', False)
        
        # Remove 'usd' from currencies list since USD/USD = 1.0
        if 'usd' in self.currencies:
            self.currencies.remove('usd')
            
        # Rate cache
        self.rate_cache = {}
        self.last_fetch_time = {}
        
        print_init(self.name, self.adapter_params)
        
    def extract(self, load_params: dict) -> pd.DataFrame:
        """
        Extract exchange rates from APIs.
        
        load_params can include:
            load_type: str - 'current_rates' or 'historical_rates'
            date: str - Specific date for historical rates (YYYY-MM-DD)
            currencies: list - Override currencies to fetch
        """
        load_type = load_params.get('load_type', 'current_rates')
        target_date = load_params.get('date')
        currencies = load_params.get('currencies', self.currencies)
        
        if load_type == 'current_rates':
            df = self._fetch_current_rates(currencies)
        elif load_type == 'historical_rates':
            df = self._fetch_historical_rates(currencies, target_date)
        else:
            raise ValueError(f"Unsupported load_type: {load_type}")
            
        print_extract(self.name, load_params, df.shape)
        return df
    
    def load(self, df: pd.DataFrame):
        """
        Load exchange rates to database.
        """
        if df.empty:
            print("No exchange rate data to load")
            return
            
        # Store in existing fact_kpis table
        table_name = 'fact_kpis'
        
        try:
            # Upsert to database
            self.db_connector.upsert_table(table_name, df)
            print_load(self.name, df.shape[0], table_name)
            
        except Exception as e:
            print(f"Failed to load exchange rates to database: {e}")
            raise
    
    def _fetch_current_rates(self, currencies: List[str]) -> pd.DataFrame:
        """
        Fetch current exchange rates for specified currencies.
        """
        rates_data = []
        current_time = datetime.now()
        
        for currency in currencies:
            if currency == 'usd':
                continue  # Skip USD
                
            # Check cache first
            if not self.force_refresh and self._is_rate_cached(currency):
                cached_rate = self.rate_cache[currency]
                
                rates_data.append({
                    'date': current_time.date(),
                    'metric_key': 'price_usd',
                    'origin_key': f'fiat_{currency}',
                    'value': cached_rate['rate']
                })
                continue
            
            # Fetch fresh rate
            rate, source = self._fetch_single_rate(currency, 'usd')
            
            if rate is not None:
                # Cache the rate
                self.rate_cache[currency] = {
                    'rate': rate,
                    'source': source,
                    'fetched_at': current_time
                }
                self.last_fetch_time[currency] = current_time
                
                rates_data.append({
                    'date': current_time.date(),
                    'metric_key': 'price_usd',
                    'origin_key': f'fiat_{currency}',
                    'value': rate
                })
                
                print(f"Fetched {currency.upper()}/USD rate: {rate:.6f} from {source}")
            else:
                print(f"Failed to fetch rate for {currency.upper()}")
        
        df = pd.DataFrame(rates_data)
        
        # Set proper index for fact_kpis table structure
        if not df.empty:
            df = df.set_index(['metric_key', 'origin_key', 'date'])
            
        return df
    
    def _fetch_single_rate(self, base_currency: str, target_currency: str) -> Tuple[Optional[float], str]:
        """
        Fetch exchange rate for a single currency pair with fallback.
        
        Returns:
            Tuple[Optional[float], str]: (rate, source) or (None, '') if failed
        """
        # Try primary API (CoinGecko)
        rate = self._fetch_from_coingecko(base_currency, target_currency)
        if rate is not None:
            return rate, 'coingecko'
            
        # Fallback to alternative API
        print(f"CoinGecko failed for {base_currency}/{target_currency}, trying backup API")
        rate = self._fetch_from_alternative_api(base_currency, target_currency)
        if rate is not None:
            return rate, 'exchangerate_api'
            
        print(f"All APIs failed for {base_currency}/{target_currency}")
        return None, ''
    
    def _fetch_from_coingecko(self, base_currency: str, target_currency: str) -> Optional[float]:
        """
        Fetch exchange rate from CoinGecko API.
        """
        try:
            url = get_primary_exchange_rate_url()
            
            response_data = api_get_call(
                url, 
                sleeper=1, 
                retries=RATE_FETCH_CONFIG['max_retries']
            )
            
            if not response_data:
                return None
                
            rate = calculate_forex_rate_from_coingecko(base_currency, target_currency, response_data)
            
            if rate is not None and validate_exchange_rate(base_currency, rate):
                return rate
            else:
                return None
                
        except Exception as e:
            print(f"CoinGecko API error for {base_currency}/{target_currency}: {e}")
            return None
    
    def _fetch_from_alternative_api(self, base_currency: str, target_currency: str) -> Optional[float]:
        """
        Fetch exchange rate from alternative API.
        """
        try:
            url = get_backup_exchange_rate_url()
            
            response_data = api_get_call(
                url,
                sleeper=1,
                retries=RATE_FETCH_CONFIG['max_retries']
            )
            
            if not response_data:
                return None
                
            rate = calculate_forex_rate_from_alternative(base_currency, target_currency, response_data)
            
            if rate is not None and validate_exchange_rate(base_currency, rate):
                return rate
            else:
                return None
                
        except Exception as e:
            print(f"Alternative API error for {base_currency}/{target_currency}: {e}")
            return None
    
    def _is_rate_cached(self, currency: str) -> bool:
        """
        Check if rate is cached and still valid.
        """
        if currency not in self.rate_cache or currency not in self.last_fetch_time:
            return False
            
        time_since_fetch = datetime.now() - self.last_fetch_time[currency]
        return time_since_fetch.total_seconds() < self.cache_duration
    
    def get_exchange_rate(self, base_currency: str, target_currency: str = 'usd') -> Optional[float]:
        """
        Get exchange rate for currency pair (public method for other adapters).
        
        Args:
            base_currency (str): Base currency code (e.g., 'eur', 'brl')
            target_currency (str): Target currency code (default: 'usd')
            
        Returns:
            Optional[float]: Exchange rate or None if not available
        """
        if base_currency == target_currency:
            return 1.0
            
        # Check cache first
        if self._is_rate_cached(base_currency):
            return self.rate_cache[base_currency]['rate']
            
        # Fetch fresh rate
        rate, source = self._fetch_single_rate(base_currency, target_currency)
        
        if rate is not None:
            # Update cache
            self.rate_cache[base_currency] = {
                'rate': rate,
                'source': source,
                'fetched_at': datetime.now()
            }
            self.last_fetch_time[base_currency] = datetime.now()
            
        return rate
    
    def get_cached_rates(self) -> Dict[str, Dict]:
        """
        Get all cached exchange rates.
        
        Returns:
            Dict[str, Dict]: Cached rates with metadata
        """
        return self.rate_cache.copy()
    
    def clear_cache(self):
        """
        Clear the exchange rate cache.
        """
        self.rate_cache.clear()
        self.last_fetch_time.clear()
        print("Exchange rate cache cleared")
    
    def get_cache_status(self) -> Dict[str, Dict]:
        """
        Get cache status for all currencies.
        
        Returns:
            Dict[str, Dict]: Cache status information
        """
        status = {}
        current_time = datetime.now()
        
        for currency in self.currencies:
            if currency in self.rate_cache:
                time_since_fetch = current_time - self.last_fetch_time.get(currency, current_time)
                is_fresh = time_since_fetch.total_seconds() < self.cache_duration
                
                status[currency] = {
                    'cached': True,
                    'rate': self.rate_cache[currency]['rate'],
                    'source': self.rate_cache[currency]['source'],
                    'age_seconds': time_since_fetch.total_seconds(),
                    'is_fresh': is_fresh,
                    'fetched_at': self.last_fetch_time[currency].isoformat()
                }
            else:
                status[currency] = {
                    'cached': False,
                    'rate': None,
                    'source': None,
                    'age_seconds': None,
                    'is_fresh': False,
                    'fetched_at': None
                }
                
        return status

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
    calculate_forex_rate_from_coingecko,
    calculate_forex_rate_from_alternative,
    get_primary_exchange_rate_url,
    get_backup_exchange_rate_url,
    get_backup_historical_exchange_rate_url,
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
    """
    
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Currency Conversion", adapter_params, db_connector)
        
        # Configuration
        self.currencies = adapter_params.get('currencies', get_supported_currencies())
        self.force_refresh = adapter_params.get('force_refresh', False)
        
        # Remove 'usd' from currencies list since USD/USD = 1.0
        if 'usd' in self.currencies:
            self.currencies.remove('usd')
            
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
            if not target_date:
                raise ValueError("historical_rates requires 'date' in YYYY-MM-DD format")
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
            
            # Fetch fresh rate
            rate, source = self._fetch_single_rate(currency, 'usd')
            
            if rate is not None:
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
            return rate
                
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
            return rate
                
        except Exception as e:
            print(f"Alternative API error for {base_currency}/{target_currency}: {e}")
            return None

    def _fetch_historical_rates(self, currencies: List[str], target_date: str) -> pd.DataFrame:
        """
        Fetch historical exchange rates for a specific date using backup historical API.

        The backup provider returns USD-based rates. For base->USD, we invert the value.
        """
        try:
            url = get_backup_historical_exchange_rate_url(target_date)
            response_data = api_get_call(
                url,
                sleeper=1,
                retries=RATE_FETCH_CONFIG['max_retries']
            )
        except Exception as e:
            print(f"Historical API error for date {target_date}: {e}")
            response_data = None

        rates_data: List[Dict] = []

        for currency in currencies:
            if currency == 'usd':
                continue
            rate_value: Optional[float] = None
            if response_data:
                try:
                    rates = response_data.get('rates', {}) or {}
                    # normalize keys to upper
                    rates_upper = {str(k).upper(): v for k, v in rates.items()}
                    api_base = str(response_data.get('base', 'EUR')).upper()

                    base_rate = rates_upper.get(currency.upper())

                    if api_base == 'USD':
                        # USD-based: EUR/USD = 1 / rates['EUR']
                        if base_rate:
                            rate_value = 1.0 / base_rate
                    else:
                        # EUR-based (or other): base/USD = (EUR->USD) / (EUR->base)
                        usd_rate = rates_upper.get('USD')
                        if usd_rate and base_rate:
                            rate_value = usd_rate / base_rate
                except Exception:
                    rate_value = None

            if rate_value is not None:
                rates_data.append({
                    'date': pd.to_datetime(target_date).date(),
                    'metric_key': 'price_usd',
                    'origin_key': f'fiat_{currency}',
                    'value': rate_value
                })
            else:
                print(f"No historical rate for {currency.upper()} on {target_date} (base={response_data.get('base') if response_data else 'N/A'})")

        df = pd.DataFrame(rates_data)
        if not df.empty:
            df = df.set_index(['metric_key', 'origin_key', 'date'])
        return df
    

    def get_exchange_rates_dataframe(self, base_currency: str, target_currency: str = 'usd', days: int = 30) -> pd.DataFrame:
        """
        Get exchange rates as a date-indexed DataFrame for historical analysis.
        
        Args:
            base_currency (str): Base currency code (e.g., 'eur', 'brl')
            target_currency (str): Target currency code (default: 'usd')
            days (int): Number of days of historical data to retrieve (default: 30)
            
        Returns:
            pd.DataFrame: Date-indexed DataFrame with exchange rates
                         Columns: ['exchange_rate']
                         Index: date
        """
        if base_currency == target_currency:
            # Return DataFrame with rate of 1.0 for all requested days
            from datetime import timedelta
            dates = pd.date_range(
                start=datetime.now().date() - timedelta(days=days-1),
                end=datetime.now().date(),
                freq='D'
            )
            return pd.DataFrame({
                'exchange_rate': 1.0
            }, index=dates)
        
        origin_key = f"fiat_{base_currency.lower()}"
        
        try:
            # Get historical rates from database
            df = self.db_connector.get_data_from_table(
                'fact_kpis',
                filters={
                    'metric_key': 'price_usd',
                    'origin_key': origin_key,
                },
                days=days
            )
            
            if not df.empty:
                # Reset index to work with the dataframe
                df_reset = df.reset_index()
                
                # Ensure we have the required columns
                if 'date' in df_reset.columns and 'value' in df_reset.columns:
                    # Rename value column to exchange_rate for clarity
                    df_rates = df_reset[['date', 'value']].copy()
                    df_rates.rename(columns={'value': 'exchange_rate'}, inplace=True)
                    
                    # Convert date column to datetime if it isn't already
                    df_rates['date'] = pd.to_datetime(df_rates['date'])
                    
                    # Sort by date and remove duplicates (keep latest if multiple per day)
                    df_rates = df_rates.sort_values('date').drop_duplicates('date', keep='last')
                    
                    # Set date as index
                    df_rates = df_rates.set_index('date')
                    
                    print(f"Retrieved {len(df_rates)} exchange rate records for {base_currency.upper()}/{target_currency.upper()}")
                    return df_rates
                    
        except Exception as e:
            print(f"Error retrieving exchange rates for {base_currency}: {e}")
        
        # If no data found, return empty DataFrame with correct structure
        print(f"Warning: No exchange rate data found for {base_currency.upper()}/{target_currency.upper()}")
        return pd.DataFrame(columns=['exchange_rate']).rename_axis('date')
    

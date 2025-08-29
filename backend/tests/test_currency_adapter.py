#!/usr/bin/env python3
"""
Test script for Currency Conversion Adapter
"""

import sys
import os
sys.path.append('/Users/nader/Documents/GitHub/gtp/backend')

from src.adapters.adapter_currency_conversion import AdapterCurrencyConversion
from src.db_connector import DbConnector
import pandas as pd

def test_currency_adapter():
    """Test the currency conversion adapter"""
    
    print("🧪 Testing Currency Conversion Adapter\n")
    
    # Mock database connector (we'll just test the API functionality)
    class MockDbConnector:
        def upsert_table(self, table_name, df):
            print(f"   📊 Mock DB: Would upsert {len(df)} rows to {table_name}")
    
    db_connector = MockDbConnector()
    
    # Test adapter initialization
    print("1️⃣ Testing adapter initialization...")
    adapter_params = {
        'currencies': ['eur', 'brl'],
        'force_refresh': True
    }
    
    adapter = AdapterCurrencyConversion(adapter_params, db_connector)
    print("   ✅ Adapter initialized successfully")
    
    # Test current rates extraction
    print("\n2️⃣ Testing current rates extraction...")
    load_params = {
        'load_type': 'current_rates',
        'currencies': ['eur', 'brl']
    }
    
    try:
        df = adapter.extract(load_params)
        print(f"   ✅ Extracted {len(df)} exchange rates")
        
        if not df.empty:
            print("   📊 Rate details (from extraction):")
            preview = df.reset_index().head(3)
            for _, row in preview.iterrows():
                origin = row['origin_key']
                val = row['value']
                print(f"      {origin}/price_usd: {val:.6f}")
        
    except Exception as e:
        print(f"   ❌ Extraction failed: {e}")
        return False
    
    # Test individual rate fetching
    print("\n3️⃣ Testing individual rate fetching...")
    try:
        eur_rate = adapter.get_exchange_rate('eur', 'usd')
        brl_rate = adapter.get_exchange_rate('brl', 'usd')
        
        print(f"   EUR/USD: {eur_rate:.6f}" if eur_rate else "   EUR/USD: Failed")
        print(f"   BRL/USD: {brl_rate:.6f}" if brl_rate else "   BRL/USD: Failed")
        
    except Exception as e:
        print(f"   ❌ Individual rate fetching failed: {e}")
        return False
    
    # Cache functionality removed in favor of DB-backed lookup; skipping
    
    # Test data loading (mock)
    print("\n5️⃣ Testing data loading...")
    try:
        if not df.empty:
            adapter.load(df)
            print("   ✅ Data loading test completed")
        else:
            print("   ⚠️  No data to load (extraction failed)")
            
    except Exception as e:
        print(f"   ❌ Data loading failed: {e}")
        return False
    
    print("\n🎉 Currency Conversion Adapter test completed successfully!")
    return True

if __name__ == "__main__":
    success = test_currency_adapter()
    sys.exit(0 if success else 1)
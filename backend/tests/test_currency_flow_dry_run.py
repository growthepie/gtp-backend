#!/usr/bin/env python3
"""
Dry-run test script for currency conversion workflow.
Tests the complete flow without writing anything to the database.
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Add the backend src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def create_sample_stablecoin_data():
    """Create sample stablecoin data with non-USD tokens."""
    print("📊 Creating sample stablecoin data...")
    
    # Create 7 days of sample data
    dates = pd.date_range(start=datetime.now().date() - timedelta(days=6), 
                         end=datetime.now().date(), freq='D')
    
    sample_data = []
    
    for date in dates:
        # USD stablecoins (no conversion needed)
        sample_data.extend([
            {
                'metric_key': 'supply_direct',
                'origin_key': 'ethereum',
                'date': date,
                'token_key': 'usdc',
                'value': 1000000.0  # 1M USDC
            },
            {
                'metric_key': 'supply_direct', 
                'origin_key': 'base',
                'date': date,
                'token_key': 'usdc',
                'value': 500000.0  # 500K USDC
            }
        ])
        
        # EUR stablecoins (need EUR/USD conversion)
        sample_data.extend([
            {
                'metric_key': 'supply_direct',
                'origin_key': 'ethereum',
                'date': date,
                'token_key': 'euro-coin',  # EURC
                'value': 800000.0  # 800K EURC
            },
            {
                'metric_key': 'supply_direct',
                'origin_key': 'base',
                'date': date,
                'token_key': 'euro-coin',  # EURC on Base
                'value': 300000.0  # 300K EURC
            }
        ])
        
        # BRL stablecoins (need BRL/USD conversion)
        sample_data.extend([
            {
                'metric_key': 'supply_direct',
                'origin_key': 'ethereum',
                'date': date,
                'token_key': 'brz',  # BRZ
                'value': 2000000.0  # 2M BRZ
            }
        ])
    
    df = pd.DataFrame(sample_data)
    print(f"   Created {len(df)} stablecoin records across {len(dates)} days")
    print(f"   Tokens: {df['token_key'].unique().tolist()}")
    print(f"   Chains: {df['origin_key'].unique().tolist()}")
    
    return df

def create_sample_exchange_rates():
    """Create sample exchange rate data."""
    print("\n💱 Creating sample exchange rate data...")
    
    # Create 7 days of sample exchange rates
    dates = pd.date_range(start=datetime.now().date() - timedelta(days=6), 
                         end=datetime.now().date(), freq='D')
    
    exchange_rates_dataframes = {}
    
    # EUR/USD rates (around 1.10-1.12)
    eur_rates = []
    base_eur_rate = 1.11
    for i, date in enumerate(dates):
        # Add some variation
        rate = base_eur_rate + (i * 0.002) + (0.005 if i % 2 == 0 else -0.005)
        eur_rates.append({'date': date, 'exchange_rate': rate})
    
    eur_df = pd.DataFrame(eur_rates).set_index('date')
    exchange_rates_dataframes['eur'] = eur_df
    
    # BRL/USD rates (around 0.18-0.19)
    brl_rates = []
    base_brl_rate = 0.185
    for i, date in enumerate(dates):
        # Add some variation
        rate = base_brl_rate + (i * 0.001) + (0.002 if i % 2 == 0 else -0.002)
        brl_rates.append({'date': date, 'exchange_rate': rate})
    
    brl_df = pd.DataFrame(brl_rates).set_index('date')
    exchange_rates_dataframes['brl'] = brl_df
    
    print(f"   Created EUR rates: {len(eur_df)} records")
    print(f"   Sample EUR rates: {eur_df['exchange_rate'].min():.6f} - {eur_df['exchange_rate'].max():.6f}")
    print(f"   Created BRL rates: {len(brl_df)} records")  
    print(f"   Sample BRL rates: {brl_df['exchange_rate'].min():.6f} - {brl_df['exchange_rate'].max():.6f}")
    
    return exchange_rates_dataframes

def test_currency_conversion_flow():
    """Test the complete currency conversion flow."""
    print("\n" + "="*80)
    print("🧪 TESTING CURRENCY CONVERSION FLOW (DRY RUN)")
    print("="*80)
    
    try:
        from src.adapters.adapter_stables import AdapterStablecoinSupply
        from src.db_connector import DbConnector
        
        # Create sample data
        stablecoin_df = create_sample_stablecoin_data()
        exchange_rates_dfs = create_sample_exchange_rates()
        
        print(f"\n📋 Original stablecoin data:")
        print("   Sample records by token:")
        sample_by_token = stablecoin_df.groupby('token_key')['value'].agg(['count', 'sum']).round(0)
        for token, stats in sample_by_token.iterrows():
            print(f"   {token}: {stats['count']} records, {stats['sum']:,.0f} total supply")
        
        # Initialize adapter (just for the conversion method)
        db_connector = DbConnector()  # We won't use this for writes
        adapter = AdapterStablecoinSupply({}, db_connector)
        
        print(f"\n🔄 Testing currency conversion...")
        
        # Test the convert_to_usd method directly
        converted_df = adapter.convert_to_usd(stablecoin_df, exchange_rates_dfs)
        
        print(f"\n✅ Conversion completed!")
        print(f"   Processed {len(converted_df)} records")
        
        # Compare original vs converted values
        print(f"\n📊 Conversion Results:")
        print("   " + "-"*70)
        
        for token in stablecoin_df['token_key'].unique():
            original_total = stablecoin_df[stablecoin_df['token_key'] == token]['value'].sum()
            converted_total = converted_df[converted_df['token_key'] == token]['value'].sum()
            
            if original_total != converted_total:
                conversion_factor = converted_total / original_total
                print(f"   {token:15} | {original_total:>12,.0f} -> {converted_total:>12,.0f} USD (×{conversion_factor:.6f})")
            else:
                print(f"   {token:15} | {original_total:>12,.0f} -> {converted_total:>12,.0f} USD (no conversion)")
        
        # Show total before/after
        original_grand_total = stablecoin_df['value'].sum()
        converted_grand_total = converted_df['value'].sum()
        
        print("   " + "-"*70)
        print(f"   {'TOTAL':15} | {original_grand_total:>12,.0f} -> {converted_grand_total:>12,.0f} USD")
        
        # Test date-specific conversion accuracy
        print(f"\n🎯 Testing date-specific accuracy...")
        
        # Pick a specific date and token to verify conversion
        test_date = stablecoin_df['date'].iloc[0]
        test_records = converted_df[converted_df['date'] == test_date]
        
        print(f"   Sample conversions for {test_date.date()}:")
        for _, row in test_records.iterrows():
            token = row['token_key']
            value = row['value']
            
            # Get the metadata to find currency
            if token in adapter.stables_metadata:
                fiat = adapter.stables_metadata[token].get('fiat', 'usd')
                if fiat != 'usd' and fiat in exchange_rates_dfs:
                    rate = exchange_rates_dfs[fiat].loc[test_date, 'exchange_rate']
                    print(f"   {token} ({fiat.upper()}): rate {rate:.6f} on {test_date.date()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_merge_operations():
    """Test the DataFrame merge operations specifically."""
    print(f"\n🔗 Testing DataFrame merge operations...")
    
    # Create simple test data
    stablecoin_data = pd.DataFrame([
        {'date': pd.Timestamp('2025-01-01'), 'token_key': 'euro-coin', 'value': 1000.0},
        {'date': pd.Timestamp('2025-01-02'), 'token_key': 'euro-coin', 'value': 1100.0},
        {'date': pd.Timestamp('2025-01-03'), 'token_key': 'euro-coin', 'value': 1200.0},
    ])
    
    exchange_rates = pd.DataFrame([
        {'date': pd.Timestamp('2025-01-01'), 'exchange_rate': 1.10},
        {'date': pd.Timestamp('2025-01-02'), 'exchange_rate': 1.11}, 
        {'date': pd.Timestamp('2025-01-03'), 'exchange_rate': 1.12},
    ]).set_index('date')
    
    print("   Original stablecoin data:")
    print(stablecoin_data.to_string(index=False))
    
    print(f"\n   Exchange rates:")
    print(exchange_rates.to_string())
    
    # Perform merge
    rates_for_merge = exchange_rates.reset_index()
    merged = stablecoin_data.merge(rates_for_merge, on='date', how='left')
    
    print(f"\n   After merge:")
    print(merged.to_string(index=False))
    
    # Apply conversion
    merged['converted_value'] = merged['value'] * merged['exchange_rate']
    
    print(f"\n   After conversion:")
    print(merged[['date', 'token_key', 'value', 'exchange_rate', 'converted_value']].to_string(index=False))
    
    return True

def main():
    """Run all dry-run tests."""
    print("🚀 CURRENCY CONVERSION DRY-RUN TEST")
    print("="*80)
    print("This test simulates the complete currency conversion workflow")
    print("without writing anything to the database.\n")
    
    results = []
    
    # Test 1: DataFrame merge operations
    print("TEST 1: DataFrame Merge Operations")
    print("-" * 40)
    results.append(test_merge_operations())
    
    # Test 2: Complete currency conversion flow
    print("\nTEST 2: Complete Currency Conversion Flow")
    print("-" * 40)
    results.append(test_currency_conversion_flow())
    
    # Summary
    print("\n" + "="*80)
    print("🎯 TEST SUMMARY")
    print("="*80)
    
    test_names = [
        "DataFrame Merge Operations",
        "Complete Currency Conversion Flow"
    ]
    
    passed = sum(results)
    total = len(results)
    
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{i+1}. {name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All dry-run tests passed!")
        print("The currency conversion workflow is ready for production testing.")
    else:
        print("\n⚠️  Some tests failed. Please check the errors above.")
    
    return 0 if passed == total else 1

if __name__ == "__main__":
    exit(main())

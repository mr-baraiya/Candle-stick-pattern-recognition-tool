#!/usr/bin/env python3
"""
Test script to verify fetch_data.py functionality
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from fetch_data import fetch_data, get_available_csv_files
import pandas as pd

def test_timeframes():
    """Test different timeframes with a sample CSV file"""
    csv_files = get_available_csv_files()
    if not csv_files:
        print("❌ No CSV files found")
        return
    
    # Use the first available CSV file
    test_file = csv_files[0]
    print(f"🧪 Testing with file: {test_file}")
    
    # Test date range - use a recent date range
    start_date = "2025-01-01"
    end_date = "2025-01-02"
    
    timeframes = ['1min', '5min', '15min', '30min', '1hour']
    
    for timeframe in timeframes:
        print(f"\n🔍 Testing timeframe: {timeframe}")
        print("-" * 50)
        
        try:
            data = fetch_data(test_file, start_date, end_date, timeframe)
            
            if data.empty:
                print(f"⚠️  No data returned for {timeframe}")
            else:
                print(f"✅ Success! Rows: {len(data)}")
                print(f"📊 Columns: {list(data.columns)}")
                if len(data) > 0:
                    print(f"📅 Date range: {data['datetime'].min()} to {data['datetime'].max()}")
                    print(f"💰 Sample prices: Open={data['open'].iloc[0]:.2f}, Close={data['close'].iloc[-1]:.2f}")
                
        except Exception as e:
            print(f"❌ Error with {timeframe}: {str(e)}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Starting fetch_data.py tests...")
    test_timeframes()
    print("\n✅ Tests completed!")

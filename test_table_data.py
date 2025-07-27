#!/usr/bin/env python3
"""
Test script to verify table data functionality and preprocessing
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from fetch_data import fetch_data, get_available_csv_files
from preprocess_data import preprocess_data
import pandas as pd

def test_table_data_preprocessing():
    """Test that table data is properly processed and formatted"""
    csv_files = get_available_csv_files()
    if not csv_files:
        print("❌ No CSV files found")
        return False
    
    # Use the first available CSV file
    test_file = csv_files[0]
    print(f"🧪 Testing table data with file: {test_file}")
    
    # Test with a small date range
    start_date = "2025-01-01"
    end_date = "2025-01-02"
    timeframe = "1min"
    
    try:
        # Fetch raw data
        print("\n📊 Fetching raw data...")
        raw_data = fetch_data(test_file, start_date, end_date, timeframe)
        
        if raw_data.empty:
            print("❌ No raw data returned")
            return False
        
        print(f"✅ Raw data fetched: {len(raw_data)} rows")
        print(f"📋 Raw columns: {list(raw_data.columns)}")
        
        # Test preprocessing
        print("\n🔄 Testing preprocessing...")
        processed_data = preprocess_data(raw_data.copy())
        
        if processed_data.empty:
            print("❌ No processed data returned")
            return False
        
        print(f"✅ Processed data: {len(processed_data)} rows")
        print(f"📋 Processed columns: {list(processed_data.columns)}")
        
        # Verify required columns exist
        required_columns = ['Open', 'High', 'Low', 'Close']
        missing_columns = [col for col in required_columns if col not in processed_data.columns]
        
        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            return False
        
        print("✅ All required OHLC columns present")
        
        # Test data types
        print("\n🔍 Testing data types...")
        for col in required_columns:
            if not pd.api.types.is_numeric_dtype(processed_data[col]):
                print(f"❌ Column {col} is not numeric")
                return False
        
        print("✅ All OHLC columns are numeric")
        
        # Test table data format (like what would be shown in the UI)
        print("\n📋 Testing table data format...")
        table_data = processed_data.head(10).to_dict('records')
        
        if not table_data:
            print("❌ No table data generated")
            return False
        
        print(f"✅ Table data generated: {len(table_data)} records")
        
        # Test first record structure
        first_record = table_data[0]
        for col in required_columns:
            if col not in first_record:
                print(f"❌ Column {col} missing from table record")
                return False
        
        print("✅ Table record structure valid")
        
        # Test data ranges are reasonable
        print("\n💰 Testing price data ranges...")
        for col in required_columns:
            values = processed_data[col]
            if values.min() <= 0:
                print(f"❌ Column {col} has non-positive values")
                return False
            if values.max() / values.min() > 10:  # Reasonable price range check
                print(f"⚠️  Column {col} has very wide price range (may be normal)")
        
        print("✅ Price data ranges appear reasonable")
        
        # Test datetime handling
        if 'datetime' in processed_data.columns:
            datetime_col = processed_data['datetime']
            if not pd.api.types.is_datetime64_any_dtype(datetime_col):
                print("❌ Datetime column is not datetime type")
                return False
            print("✅ Datetime column properly formatted")
        
        print(f"\n🎯 Sample processed record:")
        sample = processed_data.iloc[0]
        for col in ['datetime', 'Open', 'High', 'Low', 'Close', 'Volume']:
            if col in sample:
                print(f"   {col}: {sample[col]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during table data test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_data_consistency():
    """Test that data remains consistent between different timeframes"""
    csv_files = get_available_csv_files()
    if not csv_files:
        print("❌ No CSV files found")
        return False
    
    test_file = csv_files[0]
    start_date = "2025-01-01"
    end_date = "2025-01-01"  # Single day
    
    print(f"\n🔍 Testing data consistency across timeframes for {test_file}")
    
    timeframes = ['1min', '5min', '15min']
    data_sets = {}
    
    try:
        for tf in timeframes:
            data = fetch_data(test_file, start_date, end_date, tf)
            if not data.empty:
                data = preprocess_data(data)
                data_sets[tf] = data
        
        if len(data_sets) < 2:
            print("⚠️  Not enough timeframes available for consistency test")
            return True
        
        # Test that aggregated timeframes make sense
        if '1min' in data_sets and '5min' in data_sets:
            min1_data = data_sets['1min']
            min5_data = data_sets['5min']
            
            # Should have fewer 5min candles than 1min candles
            if len(min5_data) >= len(min1_data):
                print(f"❌ 5min timeframe has {len(min5_data)} rows but 1min has {len(min1_data)} rows")
                return False
            
            print(f"✅ Timeframe aggregation working: 1min({len(min1_data)}) -> 5min({len(min5_data)})")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during consistency test: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 Starting table data tests...")
    
    success = True
    
    print("\n" + "="*60)
    print("TEST 1: Table Data Preprocessing")
    print("="*60)
    success &= test_table_data_preprocessing()
    
    print("\n" + "="*60)
    print("TEST 2: Data Consistency")
    print("="*60)
    success &= test_data_consistency()
    
    print("\n" + "="*60)
    if success:
        print("✅ All table data tests passed!")
    else:
        print("❌ Some table data tests failed!")
    print("="*60)
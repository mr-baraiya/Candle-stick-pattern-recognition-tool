#!/usr/bin/env python3
"""
System verification script to test the complete candlestick pattern recognition system
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from fetch_data import fetch_data, get_available_csv_files, get_available_timeframes
from preprocess_data import preprocess_data
from patterns import (
    is_hammer, is_doji, is_rising_window, 
    is_evening_star, is_three_white_soldiers, 
    get_available_patterns
)
import pandas as pd

def test_pattern_detection():
    """Test that all pattern detection functions work correctly"""
    csv_files = get_available_csv_files()
    if not csv_files:
        print("❌ No CSV files found")
        return False
    
    test_file = csv_files[0]
    print(f"🧪 Testing pattern detection with file: {test_file}")
    
    # Get some data to test with
    start_date = "2025-01-01"
    end_date = "2025-01-02"
    timeframe = "1min"
    
    try:
        data = fetch_data(test_file, start_date, end_date, timeframe)
        if data.empty:
            print("❌ No data available for pattern testing")
            return False
        
        data = preprocess_data(data)
        if len(data) < 3:
            print("❌ Not enough data for pattern testing")
            return False
        
        print(f"✅ Loaded {len(data)} rows for pattern testing")
        
        # Test single-candle patterns
        print("\n📊 Testing single-candle patterns...")
        
        # Test Hammer pattern
        hammer_results = []
        for i in range(min(10, len(data))):
            try:
                result = is_hammer(data.iloc[i])
                hammer_results.append(result)
            except Exception as e:
                print(f"❌ Hammer pattern failed on row {i}: {e}")
                return False
        print(f"✅ Hammer pattern tested on {len(hammer_results)} rows")
        
        # Test Doji pattern
        doji_results = []
        for i in range(min(10, len(data))):
            try:
                result = is_doji(data.iloc[i])
                doji_results.append(result)
            except Exception as e:
                print(f"❌ Doji pattern failed on row {i}: {e}")
                return False
        print(f"✅ Doji pattern tested on {len(doji_results)} rows")
        
        # Test two-candle patterns
        print("\n📊 Testing two-candle patterns...")
        
        # Test Rising Window pattern
        if len(data) >= 2:
            rising_window_results = []
            for i in range(1, min(10, len(data))):
                try:
                    result = is_rising_window(data.iloc[i], data.iloc[i-1])
                    rising_window_results.append(result)
                except Exception as e:
                    print(f"❌ Rising Window pattern failed on row {i}: {e}")
                    return False
            print(f"✅ Rising Window pattern tested on {len(rising_window_results)} rows")
        
        # Test three-candle patterns
        print("\n📊 Testing three-candle patterns...")
        
        if len(data) >= 3:
            # Test Evening Star pattern
            evening_star_results = []
            for i in range(2, min(10, len(data))):
                try:
                    result = is_evening_star(data.iloc[i-2], data.iloc[i-1], data.iloc[i])
                    evening_star_results.append(result)
                except Exception as e:
                    print(f"❌ Evening Star pattern failed on row {i}: {e}")
                    return False
            print(f"✅ Evening Star pattern tested on {len(evening_star_results)} rows")
            
            # Test Three White Soldiers pattern
            three_soldiers_results = []
            for i in range(2, min(10, len(data))):
                try:
                    result = is_three_white_soldiers(data.iloc[i-2], data.iloc[i-1], data.iloc[i])
                    three_soldiers_results.append(result)
                except Exception as e:
                    print(f"❌ Three White Soldiers pattern failed on row {i}: {e}")
                    return False
            print(f"✅ Three White Soldiers pattern tested on {len(three_soldiers_results)} rows")
        
        # Report pattern detection statistics
        print(f"\n📈 Pattern Detection Summary:")
        total_patterns = sum([
            sum(hammer_results),
            sum(doji_results),
            sum(rising_window_results) if len(data) >= 2 else 0,
            sum(evening_star_results) if len(data) >= 3 else 0,
            sum(three_soldiers_results) if len(data) >= 3 else 0
        ])
        print(f"   Total patterns detected: {total_patterns}")
        print(f"   Hammer patterns: {sum(hammer_results)}")
        print(f"   Doji patterns: {sum(doji_results)}")
        if len(data) >= 2:
            print(f"   Rising Window patterns: {sum(rising_window_results)}")
        if len(data) >= 3:
            print(f"   Evening Star patterns: {sum(evening_star_results)}")
            print(f"   Three White Soldiers patterns: {sum(three_soldiers_results)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Pattern detection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_system_integration():
    """Test that all system components work together"""
    print("\n🔧 Testing system integration...")
    
    try:
        # Test available files
        csv_files = get_available_csv_files()
        if not csv_files:
            print("❌ No CSV files available")
            return False
        print(f"✅ Found {len(csv_files)} CSV files")
        
        # Test available timeframes
        timeframes = get_available_timeframes()
        if not timeframes:
            print("❌ No timeframes available")
            return False
        print(f"✅ Found {len(timeframes)} timeframes")
        
        # Test available patterns
        patterns = get_available_patterns()
        if not patterns:
            print("❌ No patterns available")
            return False
        print(f"✅ Found {len(patterns)} pattern types")
        
        # Test data pipeline for different timeframes
        test_file = csv_files[0]
        start_date = "2025-01-01"
        end_date = "2025-01-01"
        
        for timeframe_code, timeframe_name in timeframes[:3]:  # Test first 3 timeframes
            try:
                data = fetch_data(test_file, start_date, end_date, timeframe_code)
                if not data.empty:
                    processed = preprocess_data(data)
                    if not processed.empty:
                        print(f"✅ Data pipeline works for {timeframe_name}")
                    else:
                        print(f"⚠️  No processed data for {timeframe_name}")
                else:
                    print(f"⚠️  No raw data for {timeframe_name}")
            except Exception as e:
                print(f"❌ Data pipeline failed for {timeframe_name}: {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ System integration test failed: {e}")
        return False

def test_data_quality():
    """Test data quality and integrity"""
    print("\n🔍 Testing data quality...")
    
    csv_files = get_available_csv_files()
    if not csv_files:
        print("❌ No CSV files available")
        return False
    
    try:
        for csv_file in csv_files[:2]:  # Test first 2 files
            print(f"\n   Testing {csv_file}...")
            
            # Test basic data loading
            data = fetch_data(csv_file, "2025-01-01", "2025-01-01", "1min")
            if data.empty:
                print(f"⚠️  No data available for {csv_file}")
                continue
            
            # Test preprocessing
            processed = preprocess_data(data)
            if processed.empty:
                print(f"❌ Preprocessing failed for {csv_file}")
                return False
            
            # Check data integrity
            required_cols = ['Open', 'High', 'Low', 'Close']
            for col in required_cols:
                if col not in processed.columns:
                    print(f"❌ Missing column {col} in {csv_file}")
                    return False
                
                # Check for reasonable values
                values = processed[col]
                if values.isnull().any():
                    print(f"❌ Null values found in {col} for {csv_file}")
                    return False
                
                if (values <= 0).any():
                    print(f"❌ Non-positive values found in {col} for {csv_file}")
                    return False
            
            # Check High >= Low for all rows
            if not (processed['High'] >= processed['Low']).all():
                print(f"❌ High < Low violation found in {csv_file}")
                return False
            
            # Check High >= Open and High >= Close
            if not (processed['High'] >= processed['Open']).all():
                print(f"❌ High < Open violation found in {csv_file}")
                return False
            
            if not (processed['High'] >= processed['Close']).all():
                print(f"❌ High < Close violation found in {csv_file}")
                return False
            
            # Check Low <= Open and Low <= Close
            if not (processed['Low'] <= processed['Open']).all():
                print(f"❌ Low > Open violation found in {csv_file}")
                return False
            
            if not (processed['Low'] <= processed['Close']).all():
                print(f"❌ Low > Close violation found in {csv_file}")
                return False
            
            print(f"✅ Data quality checks passed for {csv_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data quality test failed: {e}")
        return False

def run_comprehensive_verification():
    """Run comprehensive system verification"""
    print("🚀 Starting comprehensive system verification...")
    print("="*70)
    
    all_tests_passed = True
    
    # Test 1: Pattern Detection
    print("\nTEST 1: Pattern Detection Functions")
    print("-" * 40)
    test1_result = test_pattern_detection()
    all_tests_passed &= test1_result
    
    # Test 2: System Integration
    print("\nTEST 2: System Integration")
    print("-" * 40)
    test2_result = test_system_integration()
    all_tests_passed &= test2_result
    
    # Test 3: Data Quality
    print("\nTEST 3: Data Quality and Integrity")
    print("-" * 40)
    test3_result = test_data_quality()
    all_tests_passed &= test3_result
    
    # Summary
    print("\n" + "="*70)
    print("VERIFICATION SUMMARY")
    print("="*70)
    print(f"Pattern Detection:     {'✅ PASSED' if test1_result else '❌ FAILED'}")
    print(f"System Integration:    {'✅ PASSED' if test2_result else '❌ FAILED'}")
    print(f"Data Quality:          {'✅ PASSED' if test3_result else '❌ FAILED'}")
    print("-" * 70)
    print(f"Overall Result:        {'✅ ALL TESTS PASSED' if all_tests_passed else '❌ SOME TESTS FAILED'}")
    print("="*70)
    
    return all_tests_passed

if __name__ == "__main__":
    success = run_comprehensive_verification()
    sys.exit(0 if success else 1)
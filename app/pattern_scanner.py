import pandas as pd
from datetime import datetime
from app.fetch_data import get_available_csv_files, get_available_timeframes, fetch_data
from app.preprocess_data import preprocess_data
from app.patterns import (
    is_hammer,
    is_doji,
    is_rising_window,
    is_evening_star,
    is_three_white_soldiers
)

def scan_all_patterns(start_date, end_date):
    """
    Scan all CSV files and timeframes for patterns
    Returns a list of dictionaries with pattern information
    """
    pattern_results = []
    csv_files = get_available_csv_files()
    timeframes = get_available_timeframes()
    
    for csv_file in csv_files:
        # Extract company name from filename
        company_name = csv_file.replace('.csv', '').replace('-', ' ').title()
        
        for timeframe_code, timeframe_display in timeframes:
            try:
                # Fetch and preprocess data
                df = fetch_data(csv_file, start_date, end_date, timeframe_code)
                if df.empty:
                    continue
                    
                df = preprocess_data(df)
                if df.empty:
                    continue
                
                # Apply pattern detection
                df = detect_patterns(df)
                
                # Extract pattern occurrences
                patterns_found = extract_pattern_occurrences(df, company_name, timeframe_display, csv_file)
                pattern_results.extend(patterns_found)
                
            except Exception as e:
                print(f"Error processing {csv_file} with {timeframe_display}: {e}")
                continue
    
    return pattern_results

def detect_patterns(df):
    """
    Apply all pattern detection functions to the dataframe
    """
    # Single candle patterns
    df['Hammer'] = df.apply(is_hammer, axis=1)
    df['Doji'] = df.apply(is_doji, axis=1)

    # Two-candle patterns
    df['RisingWindow'] = df.apply(
        lambda row: is_rising_window(row, df.iloc[df.index.get_loc(row.name) - 1]) if row.name > 0 else False,
        axis=1
    )

    # Three-candle patterns
    df['EveningStar'] = df.apply(
        lambda row: is_evening_star(
            df.iloc[df.index.get_loc(row.name) - 2],
            df.iloc[df.index.get_loc(row.name) - 1],
            row
        ) if row.name > 1 else False,
        axis=1
    )

    df['ThreeWhiteSoldiers'] = df.apply(
        lambda row: is_three_white_soldiers(
            df.iloc[df.index.get_loc(row.name) - 2],
            df.iloc[df.index.get_loc(row.name) - 1],
            row
        ) if row.name > 1 else False,
        axis=1
    )
    
    return df

def extract_pattern_occurrences(df, company_name, timeframe_display, csv_file):
    """
    Extract all pattern occurrences from the dataframe
    """
    pattern_list = []
    pattern_columns = ['Hammer', 'Doji', 'RisingWindow', 'EveningStar', 'ThreeWhiteSoldiers']
    pattern_names = {
        'Hammer': 'Hammer',
        'Doji': 'Doji', 
        'RisingWindow': 'Rising Window',
        'EveningStar': 'Evening Star',
        'ThreeWhiteSoldiers': 'Three White Soldiers'
    }
    
    for pattern_col in pattern_columns:
        if pattern_col in df.columns:
            pattern_df = df[df[pattern_col] == True]
            
            for _, row in pattern_df.iterrows():
                pattern_list.append({
                    'Script': company_name,
                    'Pattern': pattern_names[pattern_col],
                    'Timeframe': timeframe_display,
                    'Pattern_Create_Time': row['Date'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['Date']) else '',
                    'CSV_File': csv_file  # Add original CSV filename
                })
    
    return pattern_list

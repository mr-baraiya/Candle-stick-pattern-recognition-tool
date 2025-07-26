import pandas as pd

def preprocess_data(df):
    """
    Preprocess CSV data for candlestick pattern analysis
    """
    if df.empty:
        return df
    
    # Check if required columns exist (handle both lowercase and uppercase)
    required_columns_lower = ['open', 'high', 'low', 'close']
    required_columns_upper = ['Open', 'High', 'Low', 'Close']
    
    # Check which format we have
    has_lower = all(col in df.columns for col in required_columns_lower)
    has_upper = all(col in df.columns for col in required_columns_upper)
    
    if not has_lower and not has_upper:
        missing_columns = [col for col in required_columns_lower if col not in df.columns]
        raise KeyError(f"Missing columns in DataFrame: {missing_columns}")
    
    # Rename columns to match expected format (capitalize first letter) if needed
    if has_lower:
        df = df.rename(columns={
            'open': 'Open',
            'high': 'High', 
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        })
    
    # Handle datetime column - could be 'datetime' or need to be created from 'date' + 'time'
    if 'datetime' in df.columns:
        df['Date'] = df['datetime']
    elif 'Date' not in df.columns and 'date' in df.columns:
        if 'time' in df.columns:
            df['Date'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        else:
            df['Date'] = pd.to_datetime(df['date'])
    
    # Convert price columns to numeric and remove any rows with NaN values
    price_columns = ['Open', 'High', 'Low', 'Close']
    for col in price_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Drop rows with missing price data
    df.dropna(subset=price_columns, inplace=True)
    
    # Sort by date/time
    if 'Date' in df.columns:
        df = df.sort_values('Date').reset_index(drop=True)
    
    return df

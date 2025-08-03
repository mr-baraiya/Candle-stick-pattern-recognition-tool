import pandas as pd
import os
from dotenv import load_dotenv
from .redis_cache import redis_cache
import hashlib

# Load environment variables
load_dotenv()

# Data directory
DATA_DIR = os.path.join(os.path.dirname(__file__), 'Data')

def _get_file_hash(file_path):
    """Get hash of file modification time and size for cache validation"""
    try:
        stat = os.stat(file_path)
        hash_input = f"{stat.st_mtime}_{stat.st_size}"
        return hashlib.md5(hash_input.encode()).hexdigest()
    except Exception as e:
        print(f"❌ Error getting file hash: {str(e)}")
        return None

def fetch_data(csv_file, start_date, end_date, timeframe='1min'):
    """
    Fetch data with Redis-only caching:
    ✅ Redis cache for full timeframe data
    ✅ Redis cache for date range data  
    ✅ Direct CSV loading if Redis unavailable
    """
    print(f"🔍 fetch_data called: {csv_file}, {start_date} to {end_date}, {timeframe}")
    
    range_key = f"{csv_file}_{timeframe}_{start_date}_{end_date}"

    # ✅ Check Redis cache for exact date range match first
    if redis_cache.connected:
        cached_range_data = redis_cache.get_date_range_data(csv_file, timeframe, start_date, end_date)
        if cached_range_data is not None:
            print(f"⚡ Redis range cache hit for {range_key}")
            return cached_range_data

    # ✅ Load full timeframe data from Redis or CSV
    key = f"{csv_file}_{timeframe}"
    data = None
    
    # Check Redis cache for full data
    if redis_cache.connected:
        data = redis_cache.get_full_data(csv_file, timeframe)
        if data is not None:
            print(f"⚡ Redis full data cache hit for {key}")
    
    # Load from CSV if not in Redis
    if data is None:
        data = _load_from_csv(csv_file, timeframe)
        if data.empty:
            print(f"❌ No data loaded for {csv_file}")
            return pd.DataFrame()
        
        # Store in Redis cache
        if redis_cache.connected:
            redis_cache.set_full_data(csv_file, timeframe, data)

    # ✅ Slice data for requested range
    start_datetime = pd.to_datetime(start_date)
    end_datetime = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    if data.empty:
        print(f"❌ Data is empty for {csv_file}")
        return pd.DataFrame()

    # Ensure datetime is the index
    if 'datetime' in data.columns:
        data = data.set_index('datetime')
    elif data.index.name != 'datetime':
        print(f"❌ No datetime index found in data for {csv_file}")
        return pd.DataFrame()

    print(f"📅 Slicing data from {start_datetime} to {end_datetime}")
    print(f"📊 Available data range: {data.index.min()} to {data.index.max()}")
    
    # Filter data using boolean indexing
    filtered = data[(data.index >= start_datetime) & (data.index <= end_datetime)].copy()
    filtered.reset_index(inplace=True)
    
    print(f"✅ Filtered data: {len(filtered)} rows")

    # ✅ Cache this range in Redis
    if redis_cache.connected:
        redis_cache.set_date_range_data(csv_file, timeframe, start_date, end_date, filtered)
    
    return filtered


def _load_from_csv(csv_file, timeframe):
    """
    Load data directly from CSV and process for timeframe
    """
    csv_path = os.path.join(DATA_DIR, csv_file)
    
    print(f"🔄 Loading {csv_file} for {timeframe}")
    
    # Check if CSV file exists
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return pd.DataFrame()
    
    # Calculate current file hash for Redis cache validation
    current_hash = _get_file_hash(csv_path)
    
    # Check if Redis cache is invalid (file changed)
    if redis_cache.connected:
        cached_hash = redis_cache.get_file_hash(csv_file)
        if cached_hash and cached_hash != current_hash:
            print(f"🗑️ Redis cache invalid (file changed), clearing...")
            redis_cache.invalidate_file(csv_file)
    
    # Load and process data from CSV
    print(f"📂 Loading fresh data from CSV: {csv_path}")
    try:
        data = pd.read_csv(csv_path)
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return pd.DataFrame()
    
    if data.empty:
        print(f"❌ CSV file is empty: {csv_path}")
        return pd.DataFrame()
    
    # Check if required columns exist
    if 'date' not in data.columns or 'time' not in data.columns:
        print(f"❌ Required columns (date, time) not found in CSV. Available columns: {list(data.columns)}")
        return pd.DataFrame()
    
    # Process data
    data['datetime'] = pd.to_datetime(data['date'] + ' ' + data['time'])
    data = data.set_index('datetime')
    print(f"✅ Created datetime index. Date range: {data.index.min()} to {data.index.max()}")

    if timeframe != '1min':
        data = aggregate_timeframe(data, timeframe)
        print(f"📈 Aggregated to {timeframe}: {len(data)} rows")
    
    # Store file hash in Redis for future validation
    if redis_cache.connected:
        redis_cache.set_file_hash(csv_file, current_hash)
        print(f"☁️ Cached data to Redis: {csv_file}_{timeframe}")
    
    return data


def aggregate_timeframe(data, timeframe):
    """Aggregate data into different timeframes."""
    timeframe_map = {
        '1min': '1T',
        '5min': '5T',
        '15min': '15T',
        '30min': '30T',
        '1hour': '1H'
    }
    
    if timeframe not in timeframe_map:
        print(f"❌ Invalid timeframe: {timeframe}")
        return data
    
    print(f"🔄 Aggregating data to {timeframe} using rule '{timeframe_map[timeframe]}'")
    
    # Ensure data has the required columns
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        print(f"❌ Missing columns for aggregation: {missing_columns}")
        return data
    
    try:
        resampled = data.resample(timeframe_map[timeframe]).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        print(f"✅ Aggregated from {len(data)} to {len(resampled)} rows")
        return resampled
        
    except Exception as e:
        print(f"❌ Error during aggregation: {str(e)}")
        return data


def get_available_csv_files():
    """Return all available CSV files."""
    data_dir = os.path.join(os.path.dirname(__file__), 'Data')
    if os.path.exists(data_dir):
        csv_files = [file for file in os.listdir(data_dir) if file.endswith('.csv')]
        print(f"📁 Found {len(csv_files)} CSV files: {csv_files}")
        return csv_files
    else:
        print(f"❌ Data directory not found: {data_dir}")
        return []


def get_available_timeframes():
    return [
        ('1min', '1 Minute'),
        ('5min', '5 Minutes'),
        ('15min', '15 Minutes'),
        ('30min', '30 Minutes'),
        ('1hour', '1 Hour')
    ]

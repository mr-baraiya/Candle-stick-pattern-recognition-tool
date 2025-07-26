import pandas as pd
import os
import pickle

# Cache directories
CACHE_DIR = os.path.join(os.path.dirname(__file__), 'cache')
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# In-memory caches
_memory_cache = {}   # full timeframe data
_date_cache = {}     # filtered ranges

def fetch_data(csv_file, start_date, end_date, timeframe='1min'):
    """
    Fetch data with:
    ✅ Disk cache for full timeframe
    ✅ Memory cache for full timeframe
    ✅ Date range cache with sub-range optimization
    """
    print(f"🔍 fetch_data called: {csv_file}, {start_date} to {end_date}, {timeframe}")
    
    key = f"{csv_file}_{timeframe}"
    range_key = f"{key}_{start_date}_{end_date}"

    # ✅ Exact match in cache
    if range_key in _date_cache:
        print(f"⚡ Using exact date-range cache for {range_key}")
        return _date_cache[range_key]

    # ✅ Check if a parent range exists (bigger range already cached)
    parent_key = None
    for cached_range in _date_cache.keys():
        if cached_range.startswith(key):
            parts = cached_range.split('_')
            if len(parts) >= 4:
                start, end = parts[-2], parts[-1]
                if start <= start_date and end >= end_date:
                    parent_key = cached_range
                    break

    if parent_key:
        print(f"⚡ Using parent cache {parent_key} and slicing for new range")
        parent_data = _date_cache[parent_key]
        filtered = parent_data[
            (parent_data['datetime'] >= pd.to_datetime(start_date)) &
            (parent_data['datetime'] <= pd.to_datetime(end_date))
        ].copy()
        _date_cache[range_key] = filtered
        return filtered

    # ✅ Load full timeframe data from memory or disk
    if key in _memory_cache:
        print(f"⚡ Using in-memory cache for {key}")
        data = _memory_cache[key]
    else:
        data = _load_from_disk_or_csv(csv_file, timeframe)
        if data.empty:
            print(f"❌ No data loaded for {csv_file}")
            return pd.DataFrame()
        _memory_cache[key] = data

    # ✅ Slice data for requested range
    start_datetime = pd.to_datetime(start_date)
    end_datetime = pd.to_datetime(end_date)

    if data.empty:
        print(f"❌ Data is empty for {csv_file}")
        return pd.DataFrame()

    if data.index.name != 'datetime':
        data = data.set_index('datetime')

    print(f"📅 Slicing data from {start_datetime} to {end_datetime}")
    filtered = data.loc[start_datetime:end_datetime].copy()
    filtered.reset_index(inplace=True)
    
    print(f"✅ Filtered data: {len(filtered)} rows")

    # ✅ Cache this range for next time
    _date_cache[range_key] = filtered
    return filtered


def _load_from_disk_or_csv(csv_file, timeframe):
    """Load full data from disk cache or CSV."""
    data_dir = os.path.join(os.path.dirname(__file__), 'Data')
    csv_path = os.path.join(data_dir, csv_file)

    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return pd.DataFrame()

    cache_meta = os.path.join(CACHE_DIR, f"{csv_file}.meta")
    cache_file = os.path.join(CACHE_DIR, f"{csv_file}_{timeframe}.pkl")
    current_time = str(os.path.getmtime(csv_path))

    # ✅ If disk cache is valid, load from it
    if os.path.exists(cache_file) and os.path.exists(cache_meta):
        with open(cache_meta, 'r') as meta:
            cached_time = meta.read().strip()
        if cached_time == current_time:
            print(f"✅ Loaded {csv_file} [{timeframe}] from disk cache")
            with open(cache_file, 'rb') as f:
                return pickle.load(f)

    # ✅ Otherwise, load CSV and cache it
    return _load_and_cache(csv_path, cache_meta, timeframe, cache_file)


def _load_and_cache(csv_path, cache_meta, timeframe, cache_file):
    """Load CSV, aggregate if needed, and cache to disk."""
    print(f"🔄 Loading {os.path.basename(csv_path)} for {timeframe} and caching...")
    
    try:
        df = pd.read_csv(csv_path)
        print(f"📊 Loaded CSV with {len(df)} rows and columns: {list(df.columns)}")
        
        # Check if required columns exist
        if 'date' not in df.columns or 'time' not in df.columns:
            print(f"❌ Required columns (date, time) not found in CSV. Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        df = df.set_index('datetime')
        print(f"✅ Created datetime index. Date range: {df.index.min()} to {df.index.max()}")

        if timeframe != '1min':
            df = aggregate_timeframe(df, timeframe)
            print(f"📈 Aggregated to {timeframe}: {len(df)} rows")

        with open(cache_file, 'wb') as f:
            pickle.dump(df, f)
        with open(cache_meta, 'w') as meta:
            meta.write(str(os.path.getmtime(csv_path)))
        
        print(f"💾 Cached data to {cache_file}")
        return df
        
    except Exception as e:
        print(f"❌ Error loading CSV {csv_path}: {str(e)}")
        return pd.DataFrame()


def aggregate_timeframe(data, timeframe):
    """Aggregate data into different timeframes."""
    timeframe_map = {
        '1min': '1min',
        '5min': '5min',
        '15min': '15min',
        '30min': '30min',
        '1hour': '1h'
    }
    if timeframe not in timeframe_map:
        return data
    resampled = data.resample(timeframe_map[timeframe]).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    return resampled


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

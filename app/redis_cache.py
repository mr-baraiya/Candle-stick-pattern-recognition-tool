import os
import redis
import json
import pickle
import pandas as pd
from datetime import datetime, timedelta

class RedisCache:
    """Redis cache implementation for candlestick data"""
    
    def __init__(self):
        # Load Redis credentials from environment variables only
        self.redis_url = os.getenv('UPSTASH_REDIS_REST_URL')
        self.redis_token = os.getenv('UPSTASH_REDIS_REST_TOKEN')
        
        # Only proceed if both credentials are provided
        if not self.redis_url or not self.redis_token:
            print("⚠️ Redis credentials not found in environment variables")
            self.connected = False
            self.redis_client = None
            return
        
        try:
            # Initialize Redis connection with Upstash
            self.redis_client = redis.Redis(
                host=self.redis_url.replace('https://', '').replace('http://', ''),
                port=6379,
                password=self.redis_token,
                ssl=True,
                ssl_cert_reqs=None,
                decode_responses=False  # Keep as bytes for pickle data
            )
            
            # Test connection
            self.redis_client.ping()
            self.connected = True
            print("✅ Redis cache connected successfully")
            
        except Exception as e:
            print(f"❌ Redis connection failed: {str(e)}")
            self.connected = False
            self.redis_client = None
    
    def _get_key(self, prefix, csv_file, timeframe, start_date=None, end_date=None):
        """Generate cache key"""
        if start_date and end_date:
            return f"{prefix}:{csv_file}:{timeframe}:{start_date}:{end_date}"
        return f"{prefix}:{csv_file}:{timeframe}"
    
    def get_full_data(self, csv_file, timeframe):
        """Get full timeframe data from Redis cache"""
        if not self.connected:
            return None
            
        try:
            key = self._get_key("full_data", csv_file, timeframe)
            cached_data = self.redis_client.get(key)
            
            if cached_data:
                # Deserialize pandas DataFrame
                df = pickle.loads(cached_data)
                print(f"⚡ Redis cache hit for {key}")
                return df
            else:
                print(f"❌ Redis cache miss for {key}")
                return None
                
        except Exception as e:
            print(f"❌ Redis get error: {str(e)}")
            return None
    
    def set_full_data(self, csv_file, timeframe, dataframe, expire_hours=24):
        """Store full timeframe data in Redis cache"""
        if not self.connected:
            return False
            
        try:
            key = self._get_key("full_data", csv_file, timeframe)
            
            # Serialize pandas DataFrame
            serialized_data = pickle.dumps(dataframe)
            
            # Set with expiration
            success = self.redis_client.setex(
                key, 
                timedelta(hours=expire_hours), 
                serialized_data
            )
            
            if success:
                print(f"💾 Cached to Redis: {key} (expires in {expire_hours}h)")
                return True
            else:
                print(f"❌ Failed to cache to Redis: {key}")
                return False
                
        except Exception as e:
            print(f"❌ Redis set error: {str(e)}")
            return False
    
    def get_date_range_data(self, csv_file, timeframe, start_date, end_date):
        """Get date range filtered data from Redis cache"""
        if not self.connected:
            return None
            
        try:
            key = self._get_key("range_data", csv_file, timeframe, start_date, end_date)
            cached_data = self.redis_client.get(key)
            
            if cached_data:
                df = pickle.loads(cached_data)
                print(f"⚡ Redis range cache hit for {key}")
                return df
            else:
                print(f"❌ Redis range cache miss for {key}")
                return None
                
        except Exception as e:
            print(f"❌ Redis get range error: {str(e)}")
            return None
    
    def set_date_range_data(self, csv_file, timeframe, start_date, end_date, dataframe, expire_hours=6):
        """Store date range filtered data in Redis cache"""
        if not self.connected:
            return False
            
        try:
            key = self._get_key("range_data", csv_file, timeframe, start_date, end_date)
            
            # Serialize pandas DataFrame
            serialized_data = pickle.dumps(dataframe)
            
            # Set with shorter expiration for range data
            success = self.redis_client.setex(
                key, 
                timedelta(hours=expire_hours), 
                serialized_data
            )
            
            if success:
                print(f"💾 Cached range to Redis: {key} (expires in {expire_hours}h)")
                return True
            else:
                print(f"❌ Failed to cache range to Redis: {key}")
                return False
                
        except Exception as e:
            print(f"❌ Redis set range error: {str(e)}")
            return False
    
    def get_csv_file_hash(self, csv_file):
        """Get CSV file modification hash from Redis"""
        if not self.connected:
            return None
            
        try:
            key = f"file_hash:{csv_file}"
            cached_hash = self.redis_client.get(key)
            
            if cached_hash:
                return cached_hash.decode('utf-8')
            return None
                
        except Exception as e:
            print(f"❌ Redis get hash error: {str(e)}")
            return None
    
    def set_csv_file_hash(self, csv_file, file_hash):
        """Store CSV file modification hash in Redis"""
        if not self.connected:
            return False
            
        try:
            key = f"file_hash:{csv_file}"
            success = self.redis_client.setex(
                key, 
                timedelta(days=7),  # Keep file hashes for a week
                file_hash
            )
            
            if success:
                print(f"💾 Cached file hash to Redis: {key}")
                return True
            else:
                print(f"❌ Failed to cache file hash to Redis: {key}")
                return False
                
        except Exception as e:
            print(f"❌ Redis set hash error: {str(e)}")
            return False
    
    def invalidate_file_cache(self, csv_file):
        """Invalidate all cache entries for a specific CSV file"""
        if not self.connected:
            return False
            
        try:
            # Find all keys related to this CSV file
            pattern = f"*:{csv_file}:*"
            keys = self.redis_client.keys(pattern)
            
            if keys:
                deleted = self.redis_client.delete(*keys)
                print(f"🗑️ Invalidated {deleted} Redis cache entries for {csv_file}")
                return True
            else:
                print(f"ℹ️ No Redis cache entries found for {csv_file}")
                return True
                
        except Exception as e:
            print(f"❌ Redis invalidate error: {str(e)}")
            return False
    
    def get_cache_stats(self):
        """Get Redis cache statistics"""
        if not self.connected:
            return {"connected": False, "error": "Not connected to Redis"}
            
        try:
            info = self.redis_client.info()
            stats = {
                "connected": True,
                "total_keys": self.redis_client.dbsize(),
                "memory_used": info.get('used_memory_human', 'N/A'),
                "memory_peak": info.get('used_memory_peak_human', 'N/A'),
                "redis_version": info.get('redis_version', 'N/A')
            }
            return stats
            
        except Exception as e:
            return {"connected": False, "error": str(e)}

# Global Redis cache instance
redis_cache = RedisCache()

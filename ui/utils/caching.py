"""
Data caching utilities for improved performance.
Provides multiple caching strategies and automatic cache invalidation.
"""

import time
import hashlib
import json
import pickle
from pathlib import Path
from typing import Any, Dict, Optional, Callable, Union, List
from functools import wraps
from datetime import datetime, timedelta

from config.constants import DataConfig, ErrorMessages


class CacheEntry:
    """Represents a single cache entry with metadata."""
    
    def __init__(self, data: Any, ttl: Optional[float] = None):
        self.data = data
        self.created_at = time.time()
        self.ttl = ttl or DataConfig.DB_TIMEOUT_SECONDS
        self.access_count = 0
        self.last_accessed = self.created_at
    
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        return time.time() - self.created_at > self.ttl
    
    def access(self) -> Any:
        """Access the cached data and update metadata."""
        self.access_count += 1
        self.last_accessed = time.time()
        return self.data
    
    def get_age(self) -> float:
        """Get age of cache entry in seconds."""
        return time.time() - self.created_at


class MemoryCache:
    """In-memory cache with TTL and LRU eviction."""
    
    def __init__(self, max_size: int = 1000, default_ttl: float = 300):
        self.cache: Dict[str, CacheEntry] = {}
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'size': 0
        }
    
    def _generate_key(self, key: str, params: Dict[str, Any] = None) -> str:
        """Generate cache key with optional parameters."""
        if params:
            param_str = json.dumps(params, sort_keys=True)
            key = f"{key}:{hashlib.md5(param_str.encode()).hexdigest()}"
        return key
    
    def get(self, key: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Get item from cache."""
        cache_key = self._generate_key(key, params)
        
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            
            if entry.is_expired():
                del self.cache[cache_key]
                self.stats['misses'] += 1
                return None
            
            self.stats['hits'] += 1
            return entry.access()
        
        self.stats['misses'] += 1
        return None
    
    def set(self, key: str, data: Any, ttl: Optional[float] = None, params: Dict[str, Any] = None):
        """Set item in cache."""
        cache_key = self._generate_key(key, params)
        
        # Evict expired entries
        self._evict_expired()
        
        # Evict LRU entries if at capacity
        if len(self.cache) >= self.max_size:
            self._evict_lru()
        
        # Store new entry
        entry_ttl = ttl or self.default_ttl
        self.cache[cache_key] = CacheEntry(data, entry_ttl)
        self.stats['size'] = len(self.cache)
    
    def invalidate(self, key: str = None, pattern: str = None):
        """Invalidate cache entries."""
        if key:
            self.cache.pop(key, None)
        elif pattern:
            keys_to_remove = [k for k in self.cache.keys() if pattern in k]
            for k in keys_to_remove:
                del self.cache[k]
        else:
            self.cache.clear()
        
        self.stats['size'] = len(self.cache)
    
    def _evict_expired(self):
        """Remove expired entries."""
        expired_keys = [k for k, v in self.cache.items() if v.is_expired()]
        for key in expired_keys:
            del self.cache[key]
            self.stats['evictions'] += 1
    
    def _evict_lru(self):
        """Remove least recently used entry."""
        if not self.cache:
            return
        
        lru_key = min(self.cache.keys(), key=lambda k: self.cache[k].last_accessed)
        del self.cache[lru_key]
        self.stats['evictions'] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        hit_rate = self.stats['hits'] / (self.stats['hits'] + self.stats['misses']) if (self.stats['hits'] + self.stats['misses']) > 0 else 0
        
        return {
            **self.stats,
            'hit_rate': hit_rate,
            'max_size': self.max_size,
            'default_ttl': self.default_ttl
        }


class PersistentCache:
    """File-based persistent cache."""
    
    def __init__(self, cache_dir: Path = Path(".cache"), max_size_mb: float = 100):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True)
        self.max_size_mb = max_size_mb
        self.index_file = cache_dir / "cache_index.json"
        self.index = self._load_index()
    
    def _load_index(self) -> Dict[str, Dict[str, Any]]:
        """Load cache index from disk."""
        if self.index_file.exists():
            try:
                with open(self.index_file, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}
    
    def _save_index(self):
        """Save cache index to disk."""
        try:
            with open(self.index_file, 'w') as f:
                json.dump(self.index, f)
        except Exception:
            pass
    
    def _get_cache_file(self, key: str) -> Path:
        """Get cache file path for key."""
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.cache"
    
    def get(self, key: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Get item from persistent cache."""
        cache_key = f"{key}:{json.dumps(params, sort_keys=True)}" if params else key
        
        if cache_key not in self.index:
            return None
        
        entry_info = self.index[cache_key]
        
        # Check if expired
        if time.time() - entry_info['created_at'] > entry_info['ttl']:
            self.invalidate(cache_key)
            return None
        
        # Load data from file
        cache_file = self._get_cache_file(cache_key)
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                
                # Update access info
                entry_info['access_count'] += 1
                entry_info['last_accessed'] = time.time()
                self._save_index()
                
                return data
            except Exception:
                self.invalidate(cache_key)
        
        return None
    
    def set(self, key: str, data: Any, ttl: float = 3600, params: Dict[str, Any] = None):
        """Set item in persistent cache."""
        cache_key = f"{key}:{json.dumps(params, sort_keys=True)}" if params else key
        
        # Clean up old/expired entries
        self._cleanup()
        
        # Save data to file
        cache_file = self._get_cache_file(cache_key)
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
            
            # Update index
            self.index[cache_key] = {
                'created_at': time.time(),
                'ttl': ttl,
                'access_count': 0,
                'last_accessed': time.time(),
                'file_size': cache_file.stat().st_size
            }
            
            self._save_index()
        except Exception:
            pass
    
    def invalidate(self, key: str = None):
        """Invalidate cache entries."""
        if key:
            if key in self.index:
                cache_file = self._get_cache_file(key)
                cache_file.unlink(missing_ok=True)
                del self.index[key]
                self._save_index()
        else:
            # Clear all cache
            for cache_file in self.cache_dir.glob("*.cache"):
                cache_file.unlink(missing_ok=True)
            self.index.clear()
            self._save_index()
    
    def _cleanup(self):
        """Clean up expired entries and enforce size limits."""
        current_time = time.time()
        
        # Remove expired entries
        expired_keys = []
        for key, info in self.index.items():
            if current_time - info['created_at'] > info['ttl']:
                expired_keys.append(key)
        
        for key in expired_keys:
            self.invalidate(key)
        
        # Check total size
        total_size = sum(info.get('file_size', 0) for info in self.index.values())
        max_size_bytes = self.max_size_mb * 1024 * 1024
        
        if total_size > max_size_bytes:
            # Remove least recently used entries
            sorted_entries = sorted(
                self.index.items(),
                key=lambda x: x[1]['last_accessed']
            )
            
            for key, _ in sorted_entries:
                self.invalidate(key)
                total_size = sum(info.get('file_size', 0) for info in self.index.values())
                if total_size <= max_size_bytes * 0.8:  # Leave some headroom
                    break


class SmartCache:
    """Intelligent cache that uses both memory and persistent storage."""
    
    def __init__(
        self,
        memory_cache_size: int = 500,
        memory_ttl: float = 300,
        persistent_ttl: float = 3600,
        cache_dir: Path = Path(".cache")
    ):
        self.memory_cache = MemoryCache(memory_cache_size, memory_ttl)
        self.persistent_cache = PersistentCache(cache_dir)
        self.persistent_ttl = persistent_ttl
    
    def get(self, key: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Get item from cache (memory first, then persistent)."""
        # Try memory cache first
        data = self.memory_cache.get(key, params)
        if data is not None:
            return data
        
        # Try persistent cache
        data = self.persistent_cache.get(key, params)
        if data is not None:
            # Store in memory cache for faster access
            self.memory_cache.set(key, data, params=params)
            return data
        
        return None
    
    def set(self, key: str, data: Any, ttl: Optional[float] = None, params: Dict[str, Any] = None):
        """Set item in both caches."""
        memory_ttl = ttl or self.memory_cache.default_ttl
        persistent_ttl = ttl or self.persistent_ttl
        
        self.memory_cache.set(key, data, memory_ttl, params)
        self.persistent_cache.set(key, data, persistent_ttl, params)
    
    def invalidate(self, key: str = None, pattern: str = None):
        """Invalidate cache entries in both caches."""
        self.memory_cache.invalidate(key, pattern)
        if key:
            self.persistent_cache.invalidate(key)
        else:
            self.persistent_cache.invalidate()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get combined cache statistics."""
        return {
            'memory': self.memory_cache.get_stats(),
            'persistent': {
                'entries': len(self.persistent_cache.index),
                'total_size_mb': sum(info.get('file_size', 0) for info in self.persistent_cache.index.values()) / (1024 * 1024)
            }
        }


# Global cache instance
smart_cache = SmartCache()


def cached(
    key: str = None,
    ttl: float = 300,
    cache_instance: Union[MemoryCache, SmartCache] = None,
    use_params: bool = True
):
    """
    Decorator for caching function results.
    
    Args:
        key: Cache key (uses function name if not provided)
        ttl: Time to live in seconds
        cache_instance: Cache instance to use
        use_params: Whether to include function parameters in cache key
    """
    def decorator(func: Callable) -> Callable:
        cache = cache_instance or smart_cache
        cache_key = key or func.__name__
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate parameters dict for cache key
            params = None
            if use_params and (args or kwargs):
                params = {
                    'args': args,
                    'kwargs': kwargs
                }
            
            # Try to get from cache
            result = cache.get(cache_key, params)
            if result is not None:
                return result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl, params)
            
            return result
        
        # Add cache management methods to function
        wrapper.invalidate_cache = lambda: cache.invalidate(cache_key)
        wrapper.cache_stats = lambda: cache.get_stats()
        
        return wrapper
    return decorator


class QueryCache:
    """Specialized cache for database queries."""
    
    def __init__(self, cache_instance: SmartCache = None):
        self.cache = cache_instance or smart_cache
        self.query_stats = {
            'total_queries': 0,
            'cached_queries': 0,
            'cache_time_saved': 0.0
        }
    
    def cached_query(self, query: str, params: List[Any] = None, ttl: float = 600):
        """Cache database query results."""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key from query and parameters
                query_key = f"query:{hashlib.md5(query.encode()).hexdigest()}"
                cache_params = {'params': params} if params else None
                
                self.query_stats['total_queries'] += 1
                
                start_time = time.time()
                
                # Try cache first
                result = self.cache.get(query_key, cache_params)
                if result is not None:
                    self.query_stats['cached_queries'] += 1
                    self.query_stats['cache_time_saved'] += time.time() - start_time
                    return result
                
                # Execute query
                result = func(*args, **kwargs)
                
                # Cache result
                self.cache.set(query_key, result, ttl, cache_params)
                
                return result
            
            return wrapper
        return decorator
    
    def get_query_stats(self) -> Dict[str, Any]:
        """Get query cache statistics."""
        cache_hit_rate = (
            self.query_stats['cached_queries'] / self.query_stats['total_queries']
            if self.query_stats['total_queries'] > 0 else 0
        )
        
        return {
            **self.query_stats,
            'cache_hit_rate': cache_hit_rate,
            'avg_time_saved': (
                self.query_stats['cache_time_saved'] / self.query_stats['cached_queries']
                if self.query_stats['cached_queries'] > 0 else 0
            )
        }


# Global query cache instance
query_cache = QueryCache()


def invalidate_related_cache(patterns: List[str]):
    """Invalidate cache entries matching patterns."""
    for pattern in patterns:
        smart_cache.invalidate(pattern=pattern)


def warm_cache(data_loaders: Dict[str, Callable], background: bool = True):
    """Warm up cache with frequently accessed data."""
    async def _warm_cache():
        for key, loader in data_loaders.items():
            try:
                if asyncio.iscoroutinefunction(loader):
                    data = await loader()
                else:
                    data = loader()
                smart_cache.set(key, data)
            except Exception:
                pass  # Ignore errors during cache warming
    
    if background:
        import asyncio
        asyncio.create_task(_warm_cache())
    else:
        import asyncio
        asyncio.run(_warm_cache())
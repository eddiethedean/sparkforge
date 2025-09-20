"""
Caching strategies for SparkForge performance optimization.

This module provides various caching strategies including:
- Function result caching
- DataFrame caching
- Configuration caching
- Validation result caching
- Pipeline step result caching
"""

import hashlib
import pickle
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union


@dataclass
class CacheEntry:
    """Cache entry data structure."""

    key: str
    value: Any
    created_at: datetime
    last_accessed: datetime
    access_count: int
    size_bytes: int
    ttl_seconds: Optional[int] = None


@dataclass
class CacheStats:
    """Cache statistics data structure."""

    total_entries: int
    total_size_bytes: int
    hit_count: int
    miss_count: int
    eviction_count: int
    hit_rate: float
    memory_usage_mb: float


class MemoryCache:
    """In-memory cache with TTL and LRU eviction."""

    def __init__(self, max_size_mb: int = 100, default_ttl: Optional[int] = None):
        self.max_size_mb = max_size_mb
        self.default_ttl = default_ttl
        self.cache: Dict[str, CacheEntry] = {}
        self.access_order: List[str] = []
        self.lock = threading.RLock()

        # Statistics
        self.hit_count = 0
        self.miss_count = 0
        self.eviction_count = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self.lock:
            if key not in self.cache:
                self.miss_count += 1
                return None

            entry = self.cache[key]

            # Check TTL
            if entry.ttl_seconds and self._is_expired(entry):
                del self.cache[key]
                self.access_order.remove(key)
                self.miss_count += 1
                return None

            # Update access info
            entry.last_accessed = datetime.now()
            entry.access_count += 1

            # Move to end of access order (most recently used)
            self.access_order.remove(key)
            self.access_order.append(key)

            self.hit_count += 1
            return entry.value

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Set value in cache."""
        with self.lock:
            # Calculate size
            size_bytes = self._estimate_size(value)

            # Remove existing entry if it exists
            if key in self.cache:
                self._remove_entry(key)

            # Create new entry
            ttl = ttl_seconds or self.default_ttl
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.now(),
                last_accessed=datetime.now(),
                access_count=1,
                size_bytes=size_bytes,
                ttl_seconds=ttl,
            )

            # Add to cache
            self.cache[key] = entry
            self.access_order.append(key)

            # Check if we need to evict
            self._evict_if_needed()

    def delete(self, key: str) -> bool:
        """Delete entry from cache."""
        with self.lock:
            if key in self.cache:
                self._remove_entry(key)
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
            self.access_order.clear()

    def _remove_entry(self, key: str) -> None:
        """Remove entry from cache."""
        if key in self.cache:
            del self.cache[key]
            if key in self.access_order:
                self.access_order.remove(key)

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if entry is expired."""
        if not entry.ttl_seconds:
            return False

        elapsed = (datetime.now() - entry.created_at).total_seconds()
        return elapsed > entry.ttl_seconds

    def _estimate_size(self, obj: Any) -> int:
        """Estimate size of object in bytes."""
        try:
            return len(pickle.dumps(obj))
        except Exception:
            # Fallback estimation
            return len(str(obj).encode("utf-8"))

    def _evict_if_needed(self) -> None:
        """Evict entries if cache is too large."""
        current_size_mb = (
            sum(entry.size_bytes for entry in self.cache.values()) / 1024 / 1024
        )

        while current_size_mb > self.max_size_mb and self.access_order:
            # Remove least recently used entry
            oldest_key = self.access_order[0]
            self._remove_entry(oldest_key)
            self.eviction_count += 1

            # Recalculate size
            current_size_mb = (
                sum(entry.size_bytes for entry in self.cache.values()) / 1024 / 1024
            )

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        with self.lock:
            total_size_bytes = sum(entry.size_bytes for entry in self.cache.values())
            total_requests = self.hit_count + self.miss_count
            hit_rate = (
                (self.hit_count / total_requests * 100) if total_requests > 0 else 0
            )

            return CacheStats(
                total_entries=len(self.cache),
                total_size_bytes=total_size_bytes,
                hit_count=self.hit_count,
                miss_count=self.miss_count,
                eviction_count=self.eviction_count,
                hit_rate=hit_rate,
                memory_usage_mb=total_size_bytes / 1024 / 1024,
            )

    def cleanup_expired(self) -> int:
        """Remove expired entries and return count."""
        with self.lock:
            expired_keys = [
                key for key, entry in self.cache.items() if self._is_expired(entry)
            ]

            for key in expired_keys:
                self._remove_entry(key)

            return len(expired_keys)


class PersistentCache:
    """File-based persistent cache."""

    def __init__(self, cache_dir: Union[str, Path], max_file_size_mb: int = 50):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_file_size_mb = max_file_size_mb
        self.lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        """Get value from persistent cache."""
        cache_file = self._get_cache_file(key)

        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "rb") as f:
                entry = pickle.load(f)

            # Check if expired
            if entry.ttl_seconds and self._is_expired(entry):
                cache_file.unlink()
                return None

            return entry.value

        except Exception:
            # Remove corrupted cache file
            if cache_file.exists():
                cache_file.unlink()
            return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Set value in persistent cache."""
        cache_file = self._get_cache_file(key)

        try:
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.now(),
                last_accessed=datetime.now(),
                access_count=1,
                size_bytes=self._estimate_size(value),
                ttl_seconds=ttl_seconds,
            )

            # Check file size limit
            estimated_size = self._estimate_size(entry) / 1024 / 1024
            if estimated_size > self.max_file_size_mb:
                return  # Skip caching large objects

            with open(cache_file, "wb") as f:
                pickle.dump(entry, f)

        except Exception:
            # Skip caching if there's an error
            pass

    def delete(self, key: str) -> bool:
        """Delete entry from persistent cache."""
        cache_file = self._get_cache_file(key)

        if cache_file.exists():
            cache_file.unlink()
            return True
        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        for cache_file in self.cache_dir.glob("*.cache"):
            cache_file.unlink()

    def _get_cache_file(self, key: str) -> Path:
        """Get cache file path for key."""
        # Create hash of key to avoid filesystem issues
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.cache"

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if entry is expired."""
        if not entry.ttl_seconds:
            return False

        elapsed = (datetime.now() - entry.created_at).total_seconds()
        return elapsed > entry.ttl_seconds

    def _estimate_size(self, obj: Any) -> int:
        """Estimate size of object in bytes."""
        try:
            return len(pickle.dumps(obj))
        except Exception:
            return len(str(obj).encode("utf-8"))


class HybridCache:
    """Hybrid cache combining memory and persistent storage."""

    def __init__(self, memory_cache: MemoryCache, persistent_cache: PersistentCache):
        self.memory_cache = memory_cache
        self.persistent_cache = persistent_cache
        self.lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        """Get value from hybrid cache (memory first, then persistent)."""
        # Try memory cache first
        value = self.memory_cache.get(key)
        if value is not None:
            return value

        # Try persistent cache
        value = self.persistent_cache.get(key)
        if value is not None:
            # Promote to memory cache
            self.memory_cache.set(key, value)
            return value

        return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Set value in both caches."""
        # Set in memory cache
        self.memory_cache.set(key, value, ttl_seconds)

        # Set in persistent cache (with longer TTL if specified)
        persistent_ttl = ttl_seconds * 2 if ttl_seconds else None
        self.persistent_cache.set(key, value, persistent_ttl)

    def delete(self, key: str) -> bool:
        """Delete from both caches."""
        memory_deleted = self.memory_cache.delete(key)
        persistent_deleted = self.persistent_cache.delete(key)
        return memory_deleted or persistent_deleted

    def clear(self) -> None:
        """Clear both caches."""
        self.memory_cache.clear()
        self.persistent_cache.clear()


# Global cache instances
_memory_cache = MemoryCache(max_size_mb=100, default_ttl=3600)  # 1 hour default
_persistent_cache = PersistentCache(cache_dir=".cache/sparkforge", max_file_size_mb=50)
_hybrid_cache = HybridCache(_memory_cache, _persistent_cache)


def cache_result(
    ttl_seconds: Optional[int] = None, cache_type: str = "memory"
) -> Callable:
    """Decorator to cache function results."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key
            cache_key = _create_cache_key(func.__name__, args, kwargs)

            # Get appropriate cache
            cache = _get_cache(cache_type)

            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                return result

            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl_seconds)

            return result

        return wrapper

    return decorator


def cache_dataframe(ttl_seconds: int = 3600, cache_type: str = "hybrid") -> Callable:
    """Decorator specifically for caching DataFrame operations."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key including DataFrame schema
            cache_key = _create_dataframe_cache_key(func.__name__, args, kwargs)

            # Get appropriate cache
            cache = _get_cache(cache_type)

            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                return result

            # Execute function and cache result
            result = func(*args, **kwargs)

            # Cache result (DataFrames are cached by reference in Spark)
            cache.set(cache_key, result, ttl_seconds)

            return result

        return wrapper

    return decorator


def cache_validation_result(ttl_seconds: int = 1800) -> Callable:
    """Decorator for caching validation results."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = _create_cache_key(f"validation_{func.__name__}", args, kwargs)

            result = _memory_cache.get(cache_key)
            if result is not None:
                return result

            result = func(*args, **kwargs)
            _memory_cache.set(cache_key, result, ttl_seconds)

            return result

        return wrapper

    return decorator


def cache_pipeline_step(ttl_seconds: int = 7200) -> Callable:
    """Decorator for caching pipeline step results."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = _create_cache_key(f"pipeline_{func.__name__}", args, kwargs)

            result = _hybrid_cache.get(cache_key)
            if result is not None:
                return result

            result = func(*args, **kwargs)
            _hybrid_cache.set(cache_key, result, ttl_seconds)

            return result

        return wrapper

    return decorator


def _create_cache_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """Create cache key from function name and arguments."""
    # Create hash of arguments
    args_str = str(args) + str(sorted(kwargs.items()))
    args_hash = hashlib.md5(args_str.encode()).hexdigest()

    return f"{func_name}:{args_hash}"


def _create_dataframe_cache_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """Create cache key for DataFrame operations including schema info."""
    key_parts = [func_name]

    for arg in args:
        if hasattr(arg, "schema") and hasattr(arg, "count"):
            # DataFrame - include schema and row count
            schema_info = str(arg.schema)
            row_count = arg.count()
            key_parts.append(
                f"df:{hashlib.md5(schema_info.encode()).hexdigest()}:{row_count}"
            )
        else:
            key_parts.append(str(arg))

    for key, value in sorted(kwargs.items()):
        key_parts.append(f"{key}:{value}")

    key_str = ":".join(key_parts)
    return hashlib.md5(key_str.encode()).hexdigest()


def _get_cache(cache_type: str):
    """Get cache instance by type."""
    if cache_type == "memory":
        return _memory_cache
    elif cache_type == "persistent":
        return _persistent_cache
    elif cache_type == "hybrid":
        return _hybrid_cache
    else:
        raise ValueError(f"Unknown cache type: {cache_type}")


class CacheManager:
    """Centralized cache management."""

    def __init__(self):
        self.caches = {
            "memory": _memory_cache,
            "persistent": _persistent_cache,
            "hybrid": _hybrid_cache,
        }

    def get_stats(self) -> Dict[str, CacheStats]:
        """Get statistics for all caches."""
        stats = {}
        for name, cache in self.caches.items():
            if hasattr(cache, "get_stats"):
                stats[name] = cache.get_stats()
            elif hasattr(cache, "memory_cache"):
                stats[name] = cache.memory_cache.get_stats()
        return stats

    def clear_all(self) -> None:
        """Clear all caches."""
        for cache in self.caches.values():
            cache.clear()

    def cleanup_expired(self) -> Dict[str, int]:
        """Clean up expired entries in all caches."""
        results = {}
        for name, cache in self.caches.items():
            if hasattr(cache, "cleanup_expired"):
                results[name] = cache.cleanup_expired()
            elif hasattr(cache, "memory_cache"):
                results[name] = cache.memory_cache.cleanup_expired()
        return results

    def optimize_memory(self) -> None:
        """Optimize memory usage by cleaning up and evicting."""
        # Clean up expired entries
        self.cleanup_expired()

        # Force eviction in memory cache
        if hasattr(_memory_cache, "_evict_if_needed"):
            _memory_cache._evict_if_needed()


# Global cache manager
cache_manager = CacheManager()


# Example usage and testing
def example_cached_function(x: int, y: int) -> int:
    """Example function with caching."""
    time.sleep(0.1)  # Simulate work
    return x + y


@cache_result(ttl_seconds=300)  # Cache for 5 minutes
def cached_calculation(data: List[int]) -> int:
    """Example cached calculation."""
    time.sleep(0.2)  # Simulate work
    return sum(data)


@cache_dataframe(ttl_seconds=1800)  # Cache for 30 minutes
def cached_dataframe_operation(df, operation: str):
    """Example cached DataFrame operation."""
    time.sleep(0.3)  # Simulate work
    if operation == "count":
        return df.count()
    elif operation == "collect":
        return df.collect()
    else:
        return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SparkForge Caching Strategies")
    parser.add_argument("--test", action="store_true", help="Run cache tests")
    parser.add_argument("--stats", action="store_true", help="Show cache statistics")
    parser.add_argument("--clear", action="store_true", help="Clear all caches")

    args = parser.parse_args()

    if args.test:
        # Run cache tests
        print("Running cache tests...")

        # Test memory cache
        start_time = time.time()
        result1 = cached_calculation([1, 2, 3, 4, 5])
        first_call_time = time.time() - start_time

        start_time = time.time()
        result2 = cached_calculation([1, 2, 3, 4, 5])  # Should be cached
        second_call_time = time.time() - start_time

        print(f"First call: {first_call_time:.3f}s, result: {result1}")
        print(f"Second call: {second_call_time:.3f}s, result: {result2}")
        print(f"Cache hit speedup: {first_call_time / second_call_time:.1f}x")

    if args.stats:
        # Show cache statistics
        stats = cache_manager.get_stats()
        for cache_name, cache_stats in stats.items():
            print(f"\n{cache_name.upper()} Cache Statistics:")
            print(f"  Entries: {cache_stats.total_entries}")
            print(f"  Hit Rate: {cache_stats.hit_rate:.1f}%")
            print(f"  Memory Usage: {cache_stats.memory_usage_mb:.2f} MB")
            print(f"  Evictions: {cache_stats.eviction_count}")

    if args.clear:
        # Clear all caches
        cache_manager.clear_all()
        print("All caches cleared")

    if not any([args.test, args.stats, args.clear]):
        print("Use --test, --stats, or --clear to run cache operations")

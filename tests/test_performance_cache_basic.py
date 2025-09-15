#!/usr/bin/env python3
"""
Basic tests for the performance cache module.

This module provides essential test coverage for the performance caching features
without the complexity that caused issues before.

Key Features Tested:
- CacheConfig creation
- PerformanceCache basic functionality
- Cache operations (put, get, invalidate)
- Memory management
"""

import unittest
from unittest.mock import Mock
import time
from datetime import datetime, timedelta

from sparkforge.performance_cache import (
    PerformanceCache, CacheConfig, CacheEntry, CacheStrategy,
    ValidationCache, cached_validation, cached_dataframe,
    get_performance_cache, clear_performance_cache, get_cache_stats
)


class TestCacheConfig(unittest.TestCase):
    """Test CacheConfig dataclass."""
    
    def test_cache_config_defaults(self):
        """Test default cache configuration."""
        config = CacheConfig()
        
        self.assertEqual(config.max_cache_size, 100)
        self.assertEqual(config.max_memory_mb, 1024)
        self.assertEqual(config.default_ttl_seconds, 3600)
        self.assertTrue(config.enable_validation_cache)
        self.assertTrue(config.enable_dataframe_cache)
        self.assertTrue(config.enable_rule_cache)
        self.assertEqual(config.cleanup_interval_seconds, 300)
        self.assertTrue(config.enable_compression)
        self.assertFalse(config.enable_persistence)
        self.assertEqual(config.cache_directory, "/tmp/sparkforge_cache")
    
    def test_cache_config_custom(self):
        """Test custom cache configuration."""
        config = CacheConfig(
            max_cache_size=50,
            max_memory_mb=512,
            default_ttl_seconds=1800,
            enable_validation_cache=False
        )
        
        self.assertEqual(config.max_cache_size, 50)
        self.assertEqual(config.max_memory_mb, 512)
        self.assertEqual(config.default_ttl_seconds, 1800)
        self.assertFalse(config.enable_validation_cache)


class TestCacheEntry(unittest.TestCase):
    """Test CacheEntry dataclass."""
    
    def test_cache_entry_creation(self):
        """Test creating cache entry."""
        now = datetime.utcnow()
        entry = CacheEntry(
            key="test_key",
            value="test_value",
            created_at=now,
            last_accessed=now,
            size_bytes=100,
            ttl_seconds=3600,
            strategy=CacheStrategy.LRU
        )
        
        self.assertEqual(entry.key, "test_key")
        self.assertEqual(entry.value, "test_value")
        self.assertEqual(entry.size_bytes, 100)
        self.assertEqual(entry.ttl_seconds, 3600)
        self.assertEqual(entry.strategy, CacheStrategy.LRU)
        self.assertFalse(entry.is_expired)
    
    def test_cache_entry_expiration(self):
        """Test cache entry expiration."""
        # Create expired entry
        expired_time = datetime.utcnow() - timedelta(seconds=3700)
        entry = CacheEntry(
            key="test_key",
            value="test_value",
            created_at=expired_time,
            last_accessed=expired_time,
            size_bytes=100,
            ttl_seconds=3600,
            strategy=CacheStrategy.TTL
        )
        
        self.assertTrue(entry.is_expired)
        
        # Create non-expired entry
        recent_time = datetime.utcnow() - timedelta(seconds=1800)
        entry = CacheEntry(
            key="test_key",
            value="test_value",
            created_at=recent_time,
            last_accessed=recent_time,
            size_bytes=100,
            ttl_seconds=3600,
            strategy=CacheStrategy.TTL
        )
        
        self.assertFalse(entry.is_expired)


class TestPerformanceCache(unittest.TestCase):
    """Test PerformanceCache class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = CacheConfig(max_cache_size=10, max_memory_mb=1)
        self.cache = PerformanceCache(self.config)
    
    def tearDown(self):
        """Clean up after tests."""
        # Clear cache instead of shutdown
        self.cache.clear()
    
    def test_cache_initialization(self):
        """Test cache initialization."""
        self.assertEqual(self.cache.config, self.config)
        self.assertEqual(len(self.cache.cache), 0)
        self.assertEqual(self.cache.stats["hits"], 0)
        self.assertEqual(self.cache.stats["misses"], 0)
        self.assertEqual(self.cache.stats["evictions"], 0)
        self.assertEqual(self.cache.stats["total_size_bytes"], 0)
    
    def test_put_and_get(self):
        """Test basic put and get operations."""
        # Test putting and getting a value
        self.cache.put("key1", "value1")
        result = self.cache.get("key1")
        self.assertEqual(result, "value1")
        
        # Test cache hit
        self.assertEqual(self.cache.stats["hits"], 1)
        self.assertEqual(self.cache.stats["misses"], 0)
    
    def test_get_nonexistent_key(self):
        """Test getting non-existent key."""
        result = self.cache.get("nonexistent")
        self.assertIsNone(result)
        self.assertEqual(self.cache.stats["misses"], 1)
    
    def test_put_with_ttl(self):
        """Test putting value with TTL."""
        self.cache.put("key1", "value1", ttl_seconds=1)
        
        # Should be available immediately
        result = self.cache.get("key1")
        self.assertEqual(result, "value1")
        
        # Wait for expiration
        time.sleep(1.1)
        
        # Should be expired
        result = self.cache.get("key1")
        self.assertIsNone(result)
    
    def test_invalidate(self):
        """Test cache invalidation."""
        self.cache.put("key1", "value1")
        
        # Should be available
        result = self.cache.get("key1")
        self.assertEqual(result, "value1")
        
        # Invalidate
        success = self.cache.invalidate("key1")
        self.assertTrue(success)
        
        # Should not be available
        result = self.cache.get("key1")
        self.assertIsNone(result)
        
        # Test invalidating non-existent key
        success = self.cache.invalidate("nonexistent")
        self.assertFalse(success)
    
    def test_clear(self):
        """Test clearing all cache entries."""
        # Add some entries
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        
        self.assertEqual(len(self.cache.cache), 2)
        
        # Clear cache
        self.cache.clear()
        
        self.assertEqual(len(self.cache.cache), 0)
        self.assertEqual(self.cache.stats["total_size_bytes"], 0)
    
    def test_size_limit_eviction(self):
        """Test eviction when size limit is reached."""
        # Fill cache beyond size limit
        for i in range(15):  # More than max_cache_size (10)
            self.cache.put(f"key_{i}", f"value_{i}")
        
        # Should have evicted some entries
        self.assertLessEqual(len(self.cache.cache), 10)
        self.assertGreater(self.cache.stats["evictions"], 0)
    
    def test_get_stats(self):
        """Test getting cache statistics."""
        # Add some entries
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        
        # Access some entries
        self.cache.get("key1")
        self.cache.get("nonexistent")
        
        stats = self.cache.get_stats()
        
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 1)
        self.assertEqual(stats["hit_rate"], 0.5)
        self.assertEqual(stats["entry_count"], 2)
        self.assertGreater(stats["total_size_bytes"], 0)


class TestValidationCache(unittest.TestCase):
    """Test ValidationCache class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.cache = PerformanceCache()
        self.validation_cache = ValidationCache(self.cache)
    
    def tearDown(self):
        """Clean up after tests."""
        self.cache.clear()
    
    def test_validation_cache_initialization(self):
        """Test validation cache initialization."""
        self.assertEqual(self.validation_cache.cache, self.cache)
    
    def test_put_and_get_validation_result(self):
        """Test putting and getting validation results."""
        df_hash = "test_df_hash"
        rules_hash = "test_rules_hash"
        result = {"valid": True, "rate": 95.0}
        
        # Put validation result
        self.validation_cache.put_validation_result(df_hash, rules_hash, result)
        
        # Get validation result
        retrieved_result = self.validation_cache.get_validation_result(df_hash, rules_hash)
        self.assertEqual(retrieved_result, result)
    
    def test_get_nonexistent_validation_result(self):
        """Test getting non-existent validation result."""
        result = self.validation_cache.get_validation_result("nonexistent", "nonexistent")
        self.assertIsNone(result)
    
    def test_dataframe_hash_generation(self):
        """Test DataFrame hash generation."""
        # Create mock DataFrame
        df = Mock()
        df.schema = Mock()
        df.schema.__str__ = Mock(return_value="test_schema")
        df.limit = Mock(return_value=df)
        df.collect = Mock(return_value=[{"id": 1, "name": "test"}])
        
        # Generate hash
        df_hash = self.validation_cache.get_dataframe_hash(df)
        
        self.assertIsInstance(df_hash, str)
        self.assertEqual(len(df_hash), 32)  # MD5 hash length
    
    def test_rules_hash_generation(self):
        """Test rules hash generation."""
        rules = {
            "id": ["not_null"],
            "name": ["not_null", "not_empty"]
        }
        
        rules_hash = self.validation_cache.get_rules_hash(rules)
        
        self.assertIsInstance(rules_hash, str)
        self.assertEqual(len(rules_hash), 32)  # MD5 hash length
        
        # Same rules should produce same hash
        rules_hash2 = self.validation_cache.get_rules_hash(rules)
        self.assertEqual(rules_hash, rules_hash2)
        
        # Different rules should produce different hash
        different_rules = {"id": ["not_null"]}
        different_hash = self.validation_cache.get_rules_hash(different_rules)
        self.assertNotEqual(rules_hash, different_hash)


class TestConvenienceFunctions(unittest.TestCase):
    """Test convenience functions."""
    
    def test_get_performance_cache(self):
        """Test getting global performance cache."""
        cache1 = get_performance_cache()
        cache2 = get_performance_cache()
        
        # Should return the same instance
        self.assertIs(cache1, cache2)
    
    def test_clear_performance_cache(self):
        """Test clearing performance cache."""
        cache = get_performance_cache()
        
        # Add some data
        cache.put("key1", "value1")
        self.assertEqual(cache.get("key1"), "value1")
        
        # Clear cache
        clear_performance_cache()
        
        # Should be empty
        self.assertIsNone(cache.get("key1"))
    
    def test_get_cache_stats(self):
        """Test getting cache statistics."""
        cache = get_performance_cache()
        
        # Add some data
        cache.put("key1", "value1")
        cache.get("key1")
        cache.get("nonexistent")
        
        stats = get_cache_stats()
        
        self.assertIn("hits", stats)
        self.assertIn("misses", stats)
        self.assertIn("hit_rate", stats)
        self.assertIn("entry_count", stats)


if __name__ == "__main__":
    unittest.main()

"""
Mock Spark implementation for testing SparkForge without Spark dependencies.

This module provides a complete mock implementation of Apache Spark that can
replace the real Spark instance for testing SparkForge without requiring
Apache Spark installation.

Key Features:
- Zero Spark dependencies
- Full API compatibility with PySpark
- In-memory storage using SQLite
- Complete DataFrame operations support
- F.col expression system
- Catalog operations (databases, tables)
- Delta Lake simulation

Usage:
    from mock_spark import MockSparkSession, MockDataFrame
    from mock_spark import functions as F
    
    # Create mock Spark session
    spark = MockSparkSession()
    
    # Create DataFrame
    data = [("user1", "click", 100), ("user2", "purchase", 200)]
    df = spark.createDataFrame(data, ["user_id", "action", "value"])
    
    # Use DataFrame operations
    result = df.filter(F.col("value") > 50).groupBy("action").count()
    
    # Use catalog operations
    spark.catalog.createDatabase("analytics")
    df.write.saveAsTable("analytics.events")
"""

from .session import MockSparkSession
from .dataframe import MockDataFrame
from .functions import *
from .types import *
from .storage import MockStorageManager

__version__ = "0.1.0"
__all__ = [
    "MockSparkSession",
    "MockDataFrame", 
    "MockStorageManager",
]

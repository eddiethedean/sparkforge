#!/usr/bin/env python3
"""
Demonstration script for mock-spark limitations.

This script tests and demonstrates known limitations when using mock-spark
instead of real PySpark. Each test case shows the limitation, error message,
and workaround.

Run with: python scripts/demo_mock_spark_limitations.py
"""

import os
import sys
import traceback

# Set mock-spark as engine
os.environ["SPARKFORGE_ENGINE"] = "mock"

# Add project root and src to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_dir = os.path.join(project_root, "src")
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from mock_spark import SparkSession  # noqa: E402
from mock_spark import functions as F  # noqa: E402
from mock_spark.spark_types import (  # noqa: E402
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def test_limitation(name: str, description: str, test_func):
    """Run a limitation test and report results."""
    print(f"\n{'─' * 80}")
    print(f"LIMITATION: {name}")
    print(f"Description: {description}")
    print(f"{'─' * 80}")

    try:
        result = test_func()
        if result:
            print("✅ Test passed (limitation may be fixed or workaround works)")
        else:
            print("⚠️  Test completed but limitation exists")
    except Exception as e:
        print("❌ Limitation confirmed:")
        print(f"   Error: {type(e).__name__}: {str(e)}")
        print("   Traceback:")
        traceback.print_exc()
        return False
    return True


def limitation_001_tuple_data_column_names():
    """MOCK-001: Tuple data with StructType loses column names."""
    print("\nTesting: createDataFrame with tuple data and StructType schema")

    spark = SparkSession.builder.appName("test").getOrCreate()

    # Create data as tuples
    data = [
        ("user1", "click", 100),
        ("user2", "view", 200),
        ("user3", "purchase", 300),
    ]

    # Create StructType schema
    schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )

    # Try to create DataFrame
    print("Creating DataFrame with tuple data and StructType...")
    df = spark.createDataFrame(data, schema)

    print(f"DataFrame created. Columns: {df.columns}")

    # Try to filter using column name
    print("Attempting to filter using column name 'user_id'...")
    try:
        result = df.filter(F.col("user_id") == "user1")
        count = result.count()
        print(f"✅ Filter succeeded! Count: {count}")
        return True
    except Exception as e:
        print(f"❌ Filter failed: {e}")
        if "column_0" in str(e) or "column not found" in str(e).lower():
            print("\n   LIMITATION CONFIRMED: Column names lost!")
            print("   Expected: ['user_id', 'action', 'value']")
            print(f"   Actual: {df.columns}")
        return False


def limitation_002_filter_column_name_loss():
    """MOCK-004: Column names lost during filter operations."""
    print("\nTesting: Column name preservation after filter operations")

    spark = SparkSession.builder.appName("test").getOrCreate()

    # Create DataFrame with dict data (should work)
    data = [
        {"user_id": "user1", "action": "click", "value": 100},
        {"user_id": "user2", "action": "view", "value": 200},
        {"user_id": "user3", "action": "purchase", "value": 300},
    ]

    schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)
    print(f"Original DataFrame columns: {df.columns}")

    # Filter using column name
    print("Filtering with F.col('user_id')...")
    filtered = df.filter(F.col("user_id") == "user1")

    print(f"Filtered DataFrame columns: {filtered.columns}")

    # Try to use the filtered DataFrame
    print("Attempting to count filtered DataFrame...")
    try:
        count = filtered.count()
        print(f"✅ Count succeeded: {count}")

        # Try another operation
        print("Attempting to select column 'action' from filtered DataFrame...")
        try:
            selected = filtered.select("action")
            selected_count = selected.count()
            print(f"✅ Select succeeded: {selected_count}")
            return True
        except Exception as e:
            if "column_0" in str(e) or "column not found" in str(e).lower():
                print("❌ LIMITATION CONFIRMED: Column names lost after filter!")
                print(f"   Error: {e}")
                return False
            raise
    except Exception as e:
        print(f"❌ Count failed: {e}")
        if "column_0" in str(e) or "column not found" in str(e).lower():
            print("\n   LIMITATION CONFIRMED: Column names lost after filter!")
        return False


def limitation_003_parallel_execution():
    """MOCK-006: DuckDB thread isolation in parallel execution."""
    print("\nTesting: Schema visibility in parallel execution")

    spark = SparkSession.builder.appName("test").getOrCreate()

    # Create a schema in main thread
    schema_name = "test_schema"
    print(f"Creating schema '{schema_name}' in main thread...")

    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print("✅ Schema created in main thread")
    except Exception as e:
        print(f"⚠️  Schema creation: {e}")

    # Try to verify schema exists
    try:
        result = spark.sql(f"SHOW SCHEMAS LIKE '{schema_name}'").collect()
        print(f"✅ Schema visible in main thread: {len(result)} results")
    except Exception as e:
        print(f"⚠️  Schema verification: {e}")

    # Note: Full parallel execution test requires ThreadPoolExecutor
    # This is a simplified test
    print("\n⚠️  Full parallel execution test requires ThreadPoolExecutor")
    print("   This limitation manifests when:")
    print("   1. Multiple threads execute pipeline steps")
    print("   2. Each thread has its own DuckDB connection")
    print("   3. Schemas created in one thread are not visible to others")

    return True


def limitation_004_datetime_parsing():
    """MOCK-013: Datetime parsing with to_timestamp/to_date."""
    print("\nTesting: Datetime parsing functions")

    spark = SparkSession.builder.appName("test").getOrCreate()

    # Create DataFrame with string timestamps
    data = [
        {"id": 1, "timestamp_str": "2024-01-01 10:00:00"},
        {"id": 2, "timestamp_str": "2024-01-01 11:00:00"},
        {"id": 3, "timestamp_str": "2024-01-01 12:00:00"},
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("timestamp_str", StringType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)
    print(f"Created DataFrame with {df.count()} rows")

    # Try to convert string to timestamp
    print("Attempting to convert string to timestamp using F.to_timestamp...")
    try:
        result = df.withColumn(
            "timestamp", F.to_timestamp("timestamp_str", "yyyy-MM-dd HH:mm:ss")
        )
        result_count = result.count()
        print(f"✅ to_timestamp succeeded: {result_count} rows")
        return True
    except Exception as e:
        print("❌ LIMITATION CONFIRMED: Datetime parsing failed!")
        print(f"   Error: {type(e).__name__}: {str(e)}")
        if "SchemaError" in str(type(e)) or "ComputeError" in str(type(e)):
            print(
                "   Root cause: Polars backend doesn't handle string-to-timestamp conversion"
            )
        return False


def limitation_005_regexp_replace():
    """MOCK-012: regexp_replace SQL syntax incompatibility."""
    print("\nTesting: regexp_replace function")

    spark = SparkSession.builder.appName("test").getOrCreate()

    # Create DataFrame with timestamps that need cleaning
    data = [
        {"id": 1, "timestamp": "2024-01-01T10:00:00.123456"},
        {"id": 2, "timestamp": "2024-01-01T11:00:00.789012"},
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)
    print(f"Created DataFrame with {df.count()} rows")

    # Try to use regexp_replace to strip microseconds
    print("Attempting to use regexp_replace to strip microseconds...")
    try:
        result = df.withColumn(
            "cleaned_timestamp", F.regexp_replace("timestamp", r"\.\d+", "")
        )
        result_count = result.count()
        print(f"✅ regexp_replace succeeded: {result_count} rows")
        return True
    except Exception as e:
        print("❌ LIMITATION CONFIRMED: regexp_replace failed!")
        print(f"   Error: {type(e).__name__}: {str(e)}")
        if "Parser Error" in str(e) or "syntax error" in str(e).lower():
            print(
                "   Root cause: DuckDB SQL parser doesn't support PySpark's regexp_replace syntax"
            )
        return False


def limitation_006_analysis_exception():
    """MOCK-017: AnalysisException constructor signature."""
    print("\nTesting: AnalysisException constructor")

    from mock_spark.errors import AnalysisException

    # Try PySpark-style (optional stackTrace)
    print(
        "Attempting to create AnalysisException without stackTrace (PySpark style)..."
    )
    try:
        exc = AnalysisException("Table not found")
        print(f"✅ AnalysisException created: {exc}")
        return True
    except TypeError as e:
        print("❌ LIMITATION CONFIRMED: stackTrace parameter required!")
        print(f"   Error: {e}")
        print("\n   Trying with stackTrace=None...")
        try:
            exc = AnalysisException("Table not found", None)
            print(f"✅ AnalysisException created with stackTrace=None: {exc}")
            return False  # Limitation exists but workaround works
        except Exception as e2:
            print(f"❌ Even with stackTrace=None failed: {e2}")
            return False


def limitation_007_createDataFrame_schema_position():
    """MOCK-002: createDataFrame schema argument position."""
    print("\nTesting: createDataFrame schema argument handling")

    spark = SparkSession.builder.appName("test").getOrCreate()

    data = [{"id": 1, "name": "test"}]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )

    # Try positional schema (PySpark 3.5+ style)
    print("Attempting createDataFrame with positional schema...")
    try:
        df1 = spark.createDataFrame(data, schema)
        print(f"✅ Positional schema works: {df1.count()} rows")
        pos_works = True
    except Exception as e:
        print(f"⚠️  Positional schema failed: {e}")
        pos_works = False

    # Try keyword schema (mock-spark style)
    print("Attempting createDataFrame with keyword schema...")
    try:
        df2 = spark.createDataFrame(data, schema=schema)
        print(f"✅ Keyword schema works: {df2.count()} rows")
        kw_works = True
    except Exception as e:
        print(f"⚠️  Keyword schema failed: {e}")
        kw_works = False

    if pos_works and kw_works:
        print("✅ Both calling patterns work - limitation may be fixed")
        return True
    elif not pos_works and kw_works:
        print("⚠️  Only keyword schema works - limitation exists")
        return False
    else:
        print("❌ Both patterns failed - different issue")
        return False


def main():
    """Run all limitation tests."""
    print_section("Mock-Spark Limitations Demonstration")
    print("This script tests known limitations in mock-spark.")
    print("Each test demonstrates a specific limitation with real code.\n")

    limitations = [
        (
            "MOCK-001",
            "Tuple data with StructType loses column names",
            limitation_001_tuple_data_column_names,
        ),
        (
            "MOCK-004",
            "Column names lost during filter operations",
            limitation_002_filter_column_name_loss,
        ),
        (
            "MOCK-006",
            "DuckDB thread isolation in parallel execution",
            limitation_003_parallel_execution,
        ),
        (
            "MOCK-013",
            "Datetime parsing with to_timestamp/to_date",
            limitation_004_datetime_parsing,
        ),
        (
            "MOCK-012",
            "regexp_replace SQL syntax incompatibility",
            limitation_005_regexp_replace,
        ),
        (
            "MOCK-017",
            "AnalysisException constructor signature",
            limitation_006_analysis_exception,
        ),
        (
            "MOCK-002",
            "createDataFrame schema argument position",
            limitation_007_createDataFrame_schema_position,
        ),
    ]

    results = {}
    for lim_id, description, test_func in limitations:
        try:
            result = test_limitation(lim_id, description, test_func)
            results[lim_id] = result
        except Exception as e:
            print(f"❌ Test {lim_id} crashed: {e}")
            traceback.print_exc()
            results[lim_id] = False

    # Summary
    print_section("Summary")
    print("Limitation Test Results:\n")
    for lim_id, passed in results.items():
        status = "✅ PASSED/WORKAROUND WORKS" if passed else "❌ LIMITATION CONFIRMED"
        print(f"  {lim_id}: {status}")

    confirmed = sum(1 for v in results.values() if not v)
    total = len(results)

    print(f"\nConfirmed Limitations: {confirmed}/{total}")
    print(f"Fixed/Workaround Available: {total - confirmed}/{total}")


if __name__ == "__main__":
    main()

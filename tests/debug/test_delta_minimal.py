"""
Minimal reproduction test for Delta Lake configuration issue.

This test isolates the Delta Lake configuration problem by:
1. Creating a session with Delta config
2. Writing a simple Delta table
3. Verifying it works

This helps identify if the issue is in session creation, Delta operations,
or test environment setup.
"""

import os
import pytest
from pyspark.sql import SparkSession
from tests.conftest import _log_session_configs


@pytest.mark.real_spark_only
def test_delta_minimal_write(mock_spark_session):
    """
    Minimal test: Create session and write Delta table.
    
    This is the simplest possible Delta operation to isolate the issue.
    """
    import os
    print(f"🔍 test_delta_minimal_write: Test starting")
    print(f"🔍 test_delta_minimal_write: PID={os.getpid()}")
    print(f"🔍 test_delta_minimal_write: Session ID (Python)={id(mock_spark_session)}")
    
    # Verify session configs at test start
    _log_session_configs(mock_spark_session, "test_delta_minimal_write (test start)")
    
    # Create simple DataFrame
    print(f"🔍 test_delta_minimal_write: Creating test DataFrame...")
    test_data = [(1, "test1"), (2, "test2"), (3, "test3")]
    df = mock_spark_session.createDataFrame(test_data, ["id", "name"])
    
    # Verify configs before Delta write
    print(f"🔍 test_delta_minimal_write: Verifying configs before Delta write...")
    _log_session_configs(mock_spark_session, "test_delta_minimal_write (before write)")
    
    # Write to Delta table
    table_name = "test_schema.delta_minimal_test"
    print(f"🔍 test_delta_minimal_write: Writing to Delta table {table_name}...")
    
    try:
        # Create schema if needed
        mock_spark_session.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        
        # Drop table first to avoid truncate issues with Delta tables
        # Delta tables don't support truncate in batch mode, so we drop and recreate
        mock_spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
        
        # Write DataFrame to Delta table using append mode (table is dropped, so this creates it)
        df.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"✅ test_delta_minimal_write: Delta write succeeded")
        
        # Verify we can read it back
        result_df = mock_spark_session.table(table_name)
        count = result_df.count()
        print(f"✅ test_delta_minimal_write: Read back {count} rows")
        assert count == 3, f"Expected 3 rows, got {count}"
        
        # Cleanup
        mock_spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✅ test_delta_minimal_write: Test completed successfully")
        
    except Exception as e:
        import traceback
        print(f"❌ test_delta_minimal_write: Delta write failed!")
        print(f"❌ test_delta_minimal_write: Error type: {type(e).__name__}")
        print(f"❌ test_delta_minimal_write: Error message: {e}")
        _log_session_configs(mock_spark_session, "test_delta_minimal_write (ERROR CONTEXT)")
        print(f"❌ test_delta_minimal_write: Stack trace:")
        traceback.print_exc()
        raise


@pytest.mark.real_spark_only
def test_delta_minimal_direct_session():
    """
    Test creating session directly (outside fixture) and writing Delta table.
    
    This helps identify if the issue is fixture-related or session creation related.
    """
    import os
    from tests.conftest import _create_real_spark_session
    
    print(f"🔍 test_delta_minimal_direct_session: Test starting")
    print(f"🔍 test_delta_minimal_direct_session: PID={os.getpid()}")
    
    # Create session directly
    print(f"🔍 test_delta_minimal_direct_session: Creating session directly...")
    spark = _create_real_spark_session()
    
    try:
        print(f"🔍 test_delta_minimal_direct_session: Session ID (Python)={id(spark)}")
        _log_session_configs(spark, "test_delta_minimal_direct_session (after creation)")
        
        # Create simple DataFrame
        test_data = [(1, "test1"), (2, "test2")]
        df = spark.createDataFrame(test_data, ["id", "name"])
        
        # Write to Delta table
        table_name = "test_schema.delta_direct_test"
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        
        # Drop table first to avoid truncate issues with Delta tables
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        
        print(f"🔍 test_delta_minimal_direct_session: Writing to Delta table...")
        # Use append mode (table is dropped, so this creates it)
        df.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"✅ test_delta_minimal_direct_session: Delta write succeeded")
        
        # Verify
        result_df = spark.table(table_name)
        count = result_df.count()
        assert count == 2, f"Expected 2 rows, got {count}"
        
        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✅ test_delta_minimal_direct_session: Test completed successfully")
        
    finally:
        spark.stop()


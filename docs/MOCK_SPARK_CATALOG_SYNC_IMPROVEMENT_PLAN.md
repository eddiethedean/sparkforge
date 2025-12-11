# Mock-Spark Catalog Synchronization Improvement Plan

**Document Version:** 1.0  
**Date:** December 2024  
**Target Audience:** Mock-Spark Developers  
**Priority:** High  
**Issue:** Tables written via `saveAsTable()` are not immediately accessible via `spark.table()`

---

## Executive Summary

Mock-spark currently has a critical limitation where tables written using `saveAsTable()` are not immediately registered in the catalog, causing "Table or view not found" errors when attempting to read the table immediately after writing. This breaks compatibility with PySpark, where catalog registration is synchronous and atomic with the write operation.

**Impact:** This prevents mock-spark from being a drop-in replacement for PySpark in data pipelines that need to read tables immediately after writing them, which is a common pattern in ETL workflows.

---

## Problem Description

### Current Behavior

**PySpark (Expected Behavior):**
```python
df.write.mode("overwrite").saveAsTable("schema.table")
table = spark.table("schema.table")  # ✅ Works immediately, no delay
assert table.count() == df.count()    # ✅ Data is immediately available
```

**Mock-Spark (Current Broken Behavior):**
```python
df.write.mode("overwrite").saveAsTable("schema.table")
table = spark.table("schema.table")  # ❌ Raises: Table or view not found
# Requires retry with delays or manual catalog refresh
```

### Real-World Impact

In the PipelineBuilder framework, this issue manifests in multiple scenarios:

1. **Schema Evolution Detection** ([src/pipeline_builder/execution.py:607](src/pipeline_builder/execution.py:607)):
   - After writing a table, the code immediately tries to read it to check for schema differences
   - This fails in mock-spark, breaking schema evolution logic

2. **Context Population** ([src/pipeline_builder/execution.py:1145](src/pipeline_builder/execution.py:1145)):
   - After a Silver/Gold step writes a table, it's added to the execution context for downstream steps
   - The table must be readable immediately, but mock-spark's delay causes failures

3. **Test Failures** ([tests/system/test_schema_evolution_without_override.py:472-491](tests/system/test_schema_evolution_without_override.py:472-491)):
   - Tests require immediate table access after writing
   - Currently require retry logic with delays, which is a workaround, not a solution

---

## Root Cause Analysis

### Technical Root Cause

The issue stems from mock-spark's catalog registration being asynchronous or delayed, rather than synchronous and atomic with the write operation. The catalog is a metadata store that tracks which tables exist and their schemas. In PySpark, this registration happens atomically during `saveAsTable()`, but in mock-spark, there appears to be a delay or the registration happens in a separate step.

### Code Flow Analysis

**Expected Flow (PySpark):**
```
saveAsTable() called
  ↓
Write DataFrame to storage
  ↓
Register table in catalog (synchronous, atomic)
  ↓
Return (table is now immediately queryable)
```

**Current Flow (Mock-Spark):**
```
saveAsTable() called
  ↓
Write DataFrame to storage
  ↓
Return (table written, but...)
  ↓
[DELAY - catalog registration happens later or asynchronously]
  ↓
Table becomes queryable (after delay)
```

### Where the Fix Needs to Happen

Based on typical mock-spark architecture, the fix needs to be in:

1. **`mock_spark/dataframe.py`** or **`mock_spark/writer.py`**: The `DataFrameWriter.saveAsTable()` method
2. **`mock_spark/storage.py`**: The storage layer that manages table persistence
3. **`mock_spark/catalog.py`** or **`mock_spark/session.py`**: The catalog management system

---

## Detailed Implementation Requirements

### Requirement 1: Synchronous Catalog Registration

**File:** `mock_spark/writer.py` or `mock_spark/dataframe.py`

**Current Implementation (Hypothetical - needs verification):**
```python
class DataFrameWriter:
    def saveAsTable(self, name):
        """Save DataFrame as table."""
        # Write to storage
        self._storage.write_table(name, self._df, self._mode)
        # Catalog registration may happen asynchronously or be delayed
        return
```

**Required Implementation:**
```python
class DataFrameWriter:
    def saveAsTable(self, name):
        """
        Save DataFrame as table with immediate catalog registration.
        
        This method must ensure the table is immediately available
        in the catalog after the write operation completes.
        
        Args:
            name: Fully qualified table name (schema.table) or just table name
        """
        # Parse table name
        if "." in name:
            schema_name, table_name = name.split(".", 1)
        else:
            schema_name = "default"
            table_name = name
        
        # Ensure schema exists in catalog
        if not self._spark.catalog._schema_exists(schema_name):
            self._spark.catalog.createDatabase(schema_name, ignoreIfExists=True)
        
        # Write DataFrame to storage (synchronous)
        self._storage.write_table(schema_name, table_name, self._df, self._mode)
        
        # CRITICAL: Register table in catalog IMMEDIATELY and SYNCHRONOUSLY
        # This must happen before returning from saveAsTable()
        self._spark.catalog._register_table(
            schema_name, 
            table_name, 
            self._df.schema,
            self._storage.get_table_path(schema_name, table_name)
        )
        
        # Verify table is immediately accessible (for debugging/validation)
        if not self._spark.catalog.tableExists(f"{schema_name}.{table_name}"):
            raise RuntimeError(
                f"Catalog registration failed: table '{schema_name}.{table_name}' "
                f"not found in catalog immediately after saveAsTable()"
            )
```

### Requirement 2: Catalog Registration Method

**File:** `mock_spark/catalog.py` or `mock_spark/session.py`

**Required Implementation:**
```python
class Catalog:
    def _register_table(
        self, 
        schema: str, 
        table: str, 
        schema_obj: StructType,
        table_path: str
    ):
        """
        Register table in catalog immediately.
        
        This is a synchronous operation that must complete before
        saveAsTable() returns. The table must be immediately queryable
        via spark.table() after this method completes.
        
        Args:
            schema: Schema/database name
            table: Table name
            schema_obj: DataFrame schema (StructType)
            table_path: Path where table data is stored
        """
        # Ensure schema exists in catalog
        if schema not in self._catalog:
            self._catalog[schema] = {}
        
        # Register table with full metadata
        self._catalog[schema][table] = {
            "schema": schema_obj,
            "path": table_path,
            "mode": "managed",  # or "external" if applicable
            "created_at": datetime.now(),
            "last_modified": datetime.now(),
        }
        
        # Flush catalog to ensure it's immediately queryable
        # This should be a no-op if catalog is in-memory, but ensures
        # any caching or indexing is updated
        self._flush_catalog_cache()
        
        # Verify registration succeeded
        assert schema in self._catalog, f"Schema '{schema}' not in catalog"
        assert table in self._catalog[schema], f"Table '{table}' not in catalog"
```

### Requirement 3: Update table() Method for Immediate Access

**File:** `mock_spark/session.py`

**Required Implementation:**
```python
def table(self, table_name: str) -> DataFrame:
    """
    Get table as DataFrame.
    
    This method must be able to immediately access tables that were
    just written via saveAsTable(), without any delays or retries.
    
    Args:
        table_name: Fully qualified table name (schema.table) or just table name
    
    Returns:
        DataFrame with table data
    
    Raises:
        AnalysisException: If table is not found in catalog
    """
    # Parse table name
    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema = self.catalog.currentDatabase()
        table = table_name
    
    # Check catalog first - this should be the primary path
    # and should work immediately after saveAsTable()
    if not self.catalog.tableExists(f"{schema}.{table}"):
        # If not in catalog, check if it exists in storage but wasn't registered
        # This is a fallback for edge cases, but shouldn't be the primary path
        if self._storage._table_exists_in_storage(schema, table):
            # Auto-register it (lazy registration for backward compatibility)
            df = self._storage._read_table_from_storage(schema, table)
            self.catalog._register_table(schema, table, df.schema, 
                                        self._storage.get_table_path(schema, table))
            return df
        else:
            raise AnalysisException(f"Table or view not found: {table_name}")
    
    # Read table from storage using catalog metadata
    table_metadata = self.catalog._catalog[schema][table]
    return self._storage.read_table(
        schema, 
        table, 
        table_metadata["path"],
        table_metadata["schema"]
    )
```

### Requirement 4: Atomic Write + Registration

The write operation and catalog registration must be atomic. If the write fails, the catalog should not be updated. If the catalog registration fails, the write should be rolled back (if possible) or the error should be raised.

**Implementation Pattern:**
```python
def saveAsTable(self, name):
    """Atomic write + catalog registration."""
    try:
        # Step 1: Write to storage
        self._storage.write_table(...)
        
        # Step 2: Register in catalog (must succeed)
        self._spark.catalog._register_table(...)
        
        # Both operations succeeded - table is now available
    except Exception as e:
        # If registration fails, we should ideally rollback the write
        # At minimum, raise the error so the operation is not partially complete
        raise RuntimeError(f"Failed to save table '{name}': {e}") from e
```

---

## Test Cases

### Test Case 1: Immediate Table Access

```python
def test_immediate_table_access_after_save():
    """Test that table is immediately accessible after saveAsTable()."""
    from mock_spark import SparkSession
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    # Create test DataFrame
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    
    # Write table
    df.write.mode("overwrite").saveAsTable("test_schema.test_table")
    
    # CRITICAL: Should work immediately, no delay, no retry needed
    table = spark.table("test_schema.test_table")
    
    # Verify table is accessible and has correct data
    assert table is not None
    assert table.count() == 2
    assert "id" in table.columns
    assert "name" in table.columns
    
    # Verify data integrity
    rows = table.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"
```

### Test Case 2: Schema Evolution Workflow

```python
def test_schema_evolution_immediate_access():
    """Test schema evolution workflow with immediate table access."""
    from mock_spark import SparkSession
    from mock_spark.sql import functions as F
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    # Initial table
    df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    df1.write.mode("overwrite").saveAsTable("test_schema.events")
    
    # Immediately read to check schema (this is what PipelineBuilder does)
    existing_table = spark.table("test_schema.events")  # Must work immediately
    existing_schema = existing_table.schema
    
    # Add new column
    df2 = spark.createDataFrame([(2, "Bob", 25)], ["id", "name", "age"])
    
    # Merge schemas (simulating PipelineBuilder's schema evolution)
    # This requires immediate access to the existing table
    for field in existing_schema.fields:
        if field.name not in df2.columns:
            df2 = df2.withColumn(field.name, F.lit(None))
    
    # Write merged schema
    df2.write.mode("overwrite").saveAsTable("test_schema.events")
    
    # Immediately verify new schema
    final_table = spark.table("test_schema.events")  # Must work immediately
    assert "id" in final_table.columns
    assert "name" in final_table.columns
    assert "age" in final_table.columns
```

### Test Case 3: Parallel Write + Read

```python
def test_parallel_write_read():
    """Test that multiple threads can write and immediately read tables."""
    import threading
    from mock_spark import SparkSession
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    results = []
    errors = []
    
    def write_and_read(table_num):
        try:
            df = spark.createDataFrame([(table_num, f"data_{table_num}")], 
                                      ["id", "value"])
            df.write.mode("overwrite").saveAsTable(f"test_schema.table_{table_num}")
            
            # Immediately read (no delay)
            table = spark.table(f"test_schema.table_{table_num}")
            results.append((table_num, table.count()))
        except Exception as e:
            errors.append((table_num, str(e)))
    
    # Run 10 parallel write+read operations
    threads = [threading.Thread(target=write_and_read, args=(i,)) 
               for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    # All should succeed without errors
    assert len(errors) == 0, f"Errors occurred: {errors}"
    assert len(results) == 10
    assert all(count == 1 for _, count in results)
```

### Test Case 4: Append Mode Immediate Access

```python
def test_append_mode_immediate_access():
    """Test that append mode also provides immediate catalog access."""
    from mock_spark import SparkSession
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    # Initial write
    df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    df1.write.mode("overwrite").saveAsTable("test_schema.users")
    
    # Verify initial state
    table1 = spark.table("test_schema.users")  # Must work immediately
    assert table1.count() == 1
    
    # Append new data
    df2 = spark.createDataFrame([(2, "Bob")], ["id", "name"])
    df2.write.mode("append").saveAsTable("test_schema.users")
    
    # Immediately verify appended data
    table2 = spark.table("test_schema.users")  # Must work immediately
    assert table2.count() == 2
```

---

## Expected Behavior After Fix

### Success Criteria

1. **Immediate Availability**: Tables written via `saveAsTable()` are immediately accessible via `spark.table()` with zero delay
2. **No Retry Logic Needed**: Applications should not need retry logic or delays when reading tables after writing
3. **Atomic Operation**: Write and catalog registration happen atomically - either both succeed or both fail
4. **Thread Safety**: Multiple threads can write and read tables concurrently without catalog sync issues
5. **All Write Modes**: Works for `overwrite`, `append`, `error`, and `ignore` modes

### Performance Considerations

- Catalog registration should be fast (ideally O(1) for in-memory catalog)
- No significant performance penalty compared to current implementation
- The synchronous registration should not block other operations unnecessarily

---

## Migration and Backward Compatibility

### Breaking Changes

None expected. This is a bug fix that makes mock-spark more compatible with PySpark.

### Deprecation Notes

If mock-spark currently has any workarounds or async catalog registration mechanisms, those should be removed in favor of synchronous registration.

---

## References

### Related Code in PipelineBuilder

- **Schema Evolution Logic**: [src/pipeline_builder/execution.py:605-610](src/pipeline_builder/execution.py:605-610)
- **Context Population**: [src/pipeline_builder/execution.py:1136-1179](src/pipeline_builder/execution.py:1136-1179)
- **Test Retry Logic**: [tests/system/test_schema_evolution_without_override.py:472-491](tests/system/test_schema_evolution_without_override.py:472-491)

### PySpark Reference Behavior

In PySpark, `saveAsTable()` performs the following operations synchronously:

1. Write DataFrame to storage (Parquet, Delta, etc.)
2. Register table in Hive Metastore (or in-memory catalog for local mode)
3. Return (table is immediately queryable)

The catalog registration is part of the same transaction/operation as the write, ensuring atomicity.

---

## Implementation Priority

**Priority: HIGH**

This issue blocks full PySpark compatibility and requires workarounds in consuming applications. It should be addressed in the next mock-spark release.

---

## Questions for Mock-Spark Developers

1. Is the catalog currently in-memory or persisted? (This affects implementation details)
2. Are there any performance reasons for the current async/delayed registration?
3. Is there a specific catalog implementation (Hive Metastore, in-memory, etc.) that needs to be supported?
4. Are there any existing tests for catalog synchronization that are currently skipped or marked as expected failures?

---

## Conclusion

Fixing the catalog synchronization issue is critical for mock-spark to be a true drop-in replacement for PySpark. The fix requires making catalog registration synchronous and atomic with the `saveAsTable()` operation, ensuring tables are immediately queryable after writing.

This improvement will eliminate the need for retry logic in consuming applications and enable more complex data pipeline workflows that depend on immediate table access.


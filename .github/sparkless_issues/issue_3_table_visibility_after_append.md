# Table data not visible after append write in sparkless

## Summary

In sparkless 3.17.1, after writing data to a table using `append` mode, the data is not immediately visible when querying the table using `spark.table()`. The write operation reports success and the correct number of rows written, but subsequent queries return 0 rows.

## Version

- **sparkless**: 3.17.1
- **Python**: 3.9.18

## Steps to Reproduce

### High-Level Example

```python
from sparkless import SparkSession

spark = SparkSession('test')

# Create schema
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

# Create LogWriter (or any table write with append mode)
from pipeline_builder.writer import LogWriter

# Create LogWriter
log_writer = LogWriter(
    spark, schema="test_schema", table_name="pipeline_logs"
)

# Create some log rows (simplified example)
log_rows = [
    {"run_id": "test_run", "step_name": "step1", "status": "completed"},
    {"run_id": "test_run", "step_name": "step2", "status": "completed"},
]

# Write log rows (uses append mode internally)
log_result = log_writer.write_log_rows(log_rows, run_id="test_run")

# Verify write succeeded
assert log_result["success"] is True
assert log_result["rows_written"] == 2  # ✅ This assertion passes

# Try to read back the data
log_df = spark.table("test_schema.pipeline_logs")
count = log_df.count()

# ❌ This fails: count is 0, but we wrote 2 rows
assert count == 2, f"Expected 2 rows, got {count}"
```

### Internal Code Flow (Where the Bug Occurs)

The issue occurs in the internal write flow. Here's the exact code path that fails:

**1. LogWriter.write_log_rows()** (src/pipeline_builder/writer/core.py:536-604)
```python
def write_log_rows(
    self,
    log_rows: list[LogRow],
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    # ...
    # Create table if not exists
    self.storage_manager.create_table_if_not_exists(self.schema)
    
    # Write to storage - THIS CALLS write_batch()
    write_result = self.storage_manager.write_batch(
        log_rows, self.config.write_mode  # WriteMode.APPEND
    )
    
    # Returns success with rows_written count
    return {
        "success": True,
        "rows_written": write_result.get("rows_written", 0),  # ✅ Returns 2
        # ...
    }
```

**2. StorageManager.write_batch()** (src/pipeline_builder/writer/storage.py:553-576)
```python
def write_batch(
    self, log_rows: list[LogRow], write_mode: WriteMode = WriteMode.APPEND
) -> WriteResult:
    # Convert log rows to DataFrame
    df = self._create_dataframe_from_log_rows(log_rows)
    
    # Write DataFrame - THIS CALLS write_dataframe()
    return self.write_dataframe(df, write_mode)  # WriteMode.APPEND
```

**3. StorageManager.write_dataframe()** (src/pipeline_builder/writer/storage.py:414-520)
```python
def write_dataframe(
    self,
    df: DataFrame,
    write_mode: WriteMode = WriteMode.APPEND,
    partition_columns: Optional[list[str]] = None,
) -> WriteResult:
    # Prepare DataFrame for writing
    df_prepared = self._prepare_dataframe_for_write(df)
    
    # Check if Delta Lake is available
    delta_configured = _is_delta_lake_available(self.spark)
    
    if delta_configured:
        # Delta mode
        writer = (
            df_prepared.write.format("delta")
            .mode(write_mode.value)  # "append"
            .option("mergeSchema", "true")
        )
    else:
        # Fallback to parquet format when Delta Lake is not configured
        writer = df_prepared.write.format("parquet").mode(write_mode.value)  # "append"
    
    # Execute write operation - THIS IS WHERE THE WRITE HAPPENS
    writer.saveAsTable(self.table_fqn)  # ✅ Write succeeds, no exception
    
    # Returns success result
    return {
        "table_name": self.table_fqn,
        "write_mode": write_mode.value,
        "rows_written": len(log_rows),  # ✅ Returns 2
        "success": True,
        # ...
    }
```

**4. The Problem: Reading Back the Data**

After the write completes successfully, reading the table returns 0 rows:

```python
# Immediately after write_log_rows() completes:
log_df = spark.table("test_schema.pipeline_logs")  # ✅ No error, table exists
count = log_df.count()  # ❌ Returns 0, but we just wrote 2 rows!

# The table exists (no "Table not found" error)
# But it appears empty even though write reported success
```

**5. What Works in PySpark**

The exact same code flow works correctly in PySpark:

```python
# PySpark - same code, different result
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test').master('local[1]').getOrCreate()
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

# Same write operation
df.write.mode("append").saveAsTable("test_schema.pipeline_logs")

# Same read operation - THIS WORKS
result_df = spark.table("test_schema.pipeline_logs")
count = result_df.count()  # ✅ Returns 2 (correct!)
```

## Expected Behavior

After writing data to a table with `append` mode, the data should be immediately visible when querying the table using `spark.table()`, matching PySpark's behavior.

**PySpark equivalent (works correctly):**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test').master('local[1]').getOrCreate()
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

# Create table with initial data
data1 = [{"id": 1, "name": "test1"}]
df1 = spark.createDataFrame(data1, "id int, name string")
df1.write.mode("overwrite").saveAsTable("test_schema.test_table")

# Append more data
data2 = [{"id": 2, "name": "test2"}]
df2 = spark.createDataFrame(data2, "id int, name string")
df2.write.mode("append").saveAsTable("test_schema.test_table")

# Read back - this works correctly in PySpark
result_df = spark.table("test_schema.test_table")
count = result_df.count()
print(f"Count: {count}")  # Output: 2 ✅
```

## Actual Behavior

The write operation reports success:
```
INFO - Successfully wrote 6 rows to test_schema.pipeline_logs
```

But when querying the table immediately after:
```python
log_df = spark.table("test_schema.pipeline_logs")
count = log_df.count()  # Returns 0, not 6
```

The table exists (no "Table not found" error), but it appears empty even though data was just written to it.

## Additional Observations

1. **Write operation reports success**: The `write_log_rows()` method returns `{"success": True, "rows_written": 6}`
2. **Table exists**: `spark.table()` does not raise a "Table not found" error
3. **Table appears empty**: `count()` returns 0 even though rows were written
4. **Works in PySpark**: The same code works correctly in PySpark mode
5. **Simple append test works**: A minimal test case with `df.write.mode("append").saveAsTable()` works correctly
6. **Issue is specific to certain write patterns**: The issue appears when using the LogWriter's `write_batch()` method, which may use a different write path

## Impact

This bug affects any code that:
- Writes data to tables using append mode and immediately queries it
- Uses LogWriter to persist pipeline execution logs
- Relies on immediate data visibility after writes

## Workaround

No simple workaround exists. The data appears to be written but not immediately visible. Potential workarounds might include:
- Adding a delay before querying (not reliable)
- Using a different write method (if available)
- Refreshing the table catalog (if sparkless supports it)

## Related

This issue was discovered when testing pipeline logging functionality. The logs are written successfully but cannot be read back immediately after writing.

## Test Results

- **sparkless 3.17.1**: ❌ Fails - data not visible after append write
- **PySpark**: ✅ Works - data immediately visible after append write


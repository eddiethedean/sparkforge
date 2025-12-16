# Table data not visible after append write via LogWriter (parquet format tables)

## Summary

In sparkless 3.17.2, table visibility after append writes is **partially fixed** - simple DataFrame writes work correctly, but writes through LogWriter's storage manager (which creates tables using parquet format) still fail. Data is written successfully but not immediately visible when querying with `spark.table()`.

## Version

- **sparkless**: 3.17.2
- **Python**: 3.9.18

## Status

- ✅ **Simple append operations**: Fixed in 3.17.2
- ✅ **Minimal parquet format reproduction**: Works in 3.17.2
- ❌ **LogWriter in test/integration context**: Still broken (context-specific issue)

## Steps to Reproduce

### Working Case (Simple DataFrame Write)

```python
from sparkless import SparkSession

spark = SparkSession('test')
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

# Create and write data - THIS WORKS
data = [{"id": 1, "name": "test1"}]
df = spark.createDataFrame(data, "id int, name string")
df.write.mode("overwrite").saveAsTable("test_schema.test_table")

# Append data - THIS WORKS
data2 = [{"id": 2, "name": "test2"}]
df2 = spark.createDataFrame(data2, "id int, name string")
df2.write.mode("append").saveAsTable("test_schema.test_table")

# Read back - THIS WORKS in 3.17.2
result = spark.table("test_schema.test_table")
count = result.count()  # ✅ Returns 2 (correct!)
```

### Failing Case (LogWriter with Parquet Format)

```python
from sparkless import SparkSession
from pipeline_builder.writer import LogWriter

spark = SparkSession('test')
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

# Create LogWriter
log_writer = LogWriter(
    spark, schema="test_schema", table_name="pipeline_logs"
)

# Create log rows (simplified)
log_rows = [
    {"run_id": "test_run", "step_name": "step1", "status": "completed"},
    {"run_id": "test_run", "step_name": "step2", "status": "completed"},
]

# Write log rows - THIS REPORTS SUCCESS
log_result = log_writer.write_log_rows(log_rows, run_id="test_run")
# Returns: {"success": True, "rows_written": 2}

# Read back - THIS FAILS
log_df = spark.table("test_schema.pipeline_logs")
count = log_df.count()  # ❌ Returns 0 (should be 2!)
```

## Root Cause Analysis

The difference between the working and failing cases lies in **how the table is created**:

### Working Case (Simple DataFrame)
- Uses default table format (likely managed table)
- Direct `saveAsTable()` call
- Table is immediately queryable after write

### Failing Case (LogWriter)
- **Creates table with explicit parquet format** when Delta Lake is not configured
- Uses a two-step process: create empty table first, then append
- Table creation happens in `create_table_if_not_exists()` with parquet format
- Subsequent append writes use parquet format explicitly

## Internal Code Flow (Where It Fails)

### 1. LogWriter Initialization and Table Creation

**File**: `src/pipeline_builder/writer/storage.py`

**Method**: `create_table_if_not_exists()` (lines 214-377)

When Delta Lake is not configured, LogWriter creates a **parquet format table**:

```python
def create_table_if_not_exists(self, schema: types.StructType) -> None:
    # ... schema creation ...
    
    if not table_exists(self.spark, self.table_fqn):
        # Create empty DataFrame with schema
        empty_df = self.spark.createDataFrame([], schema)
        
        # Check if Delta Lake is available
        delta_configured = _is_delta_lake_available(self.spark)
        
        if delta_configured:
            # Delta table creation (not the failing path)
            empty_df.write.format("delta").mode("append").saveAsTable(self.table_fqn)
        else:
            # ❌ THIS IS THE FAILING PATH - Parquet format table
            self.logger.warning(
                f"Delta Lake is not configured. Creating regular Spark table {self.table_fqn} instead."
            )
            # Create table with parquet format
            empty_df.write.format("parquet").mode("overwrite").saveAsTable(self.table_fqn)
            # ✅ Table creation succeeds
            self.logger.info(f"Regular Spark table created successfully: {self.table_fqn}")
```

**Key Point**: The table is created with `.format("parquet")` when Delta Lake is not available.

### 2. Writing Data to the Table

**Method**: `write_dataframe()` (lines 414-520)

When appending data, LogWriter uses the same format:

```python
def write_dataframe(
    self,
    df: DataFrame,
    write_mode: WriteMode = WriteMode.APPEND,
) -> WriteResult:
    # Prepare DataFrame
    df_prepared = self._prepare_dataframe_for_write(df)
    
    # Check Delta availability
    delta_configured = _is_delta_lake_available(self.spark)
    
    if delta_configured:
        # Delta write (not the failing path)
        writer = df_prepared.write.format("delta").mode(write_mode.value)
    else:
        # ❌ THIS IS THE FAILING PATH - Parquet format write
        writer = df_prepared.write.format("parquet").mode(write_mode.value)  # "append"
    
    # Execute write
    writer.saveAsTable(self.table_fqn)  # ✅ Write reports success, no exception
    
    # Return success
    return {
        "table_name": self.table_fqn,
        "write_mode": write_mode.value,
        "rows_written": len(log_rows),  # ✅ Returns correct count (e.g., 2)
        "success": True,
    }
```

**Key Point**: The append write uses `.format("parquet")` and reports success.

### 3. The Problem: Reading Back Data

After the write completes successfully:

```python
# Immediately after write_dataframe() completes:
log_df = spark.table("test_schema.pipeline_logs")  # ✅ No error, table exists
count = log_df.count()  # ❌ Returns 0, but we just wrote 2 rows!
```

**The Issue**: 
- Table exists (no "Table not found" error)
- Write operation reports success with correct row count
- But `spark.table().count()` returns 0
- This suggests the data is written but not immediately visible in the table catalog

## Detailed Code Path Comparison

### Working Path (Simple DataFrame - No Format Specified)

```python
# Step 1: Create table (implicit format)
df.write.mode("overwrite").saveAsTable("test_schema.test_table")
# Uses default/managed table format

# Step 2: Append data (implicit format)
df2.write.mode("append").saveAsTable("test_schema.test_table")
# Uses same format as table

# Step 3: Read back
spark.table("test_schema.test_table").count()  # ✅ Works in 3.17.2
```

### Failing Path (LogWriter - Explicit Parquet Format)

```python
# Step 1: Create empty table with parquet format
empty_df.write.format("parquet").mode("overwrite").saveAsTable("test_schema.pipeline_logs")
# ✅ Table created successfully

# Step 2: Append data with parquet format
df.write.format("parquet").mode("append").saveAsTable("test_schema.pipeline_logs")
# ✅ Write reports success, rows_written = 2

# Step 3: Read back
spark.table("test_schema.pipeline_logs").count()  # ❌ Returns 0 (should be 2)
```

## Key Differences

1. **Format specification**: LogWriter explicitly uses `.format("parquet")`, simple case uses default
2. **Two-step creation**: LogWriter creates empty table first, then appends (simple case does both in one step)
3. **Table type**: LogWriter creates parquet format tables when Delta is unavailable

## Evidence from Logs

When running the failing test, logs show:

```
INFO - Creating table if not exists: test_schema.pipeline_logs
WARNING - Delta Lake is not configured. Creating regular Spark table test_schema.pipeline_logs instead.
INFO - Regular Spark table created successfully: test_schema.pipeline_logs
INFO - Writing DataFrame to test_schema.pipeline_logs with mode append
INFO - Successfully wrote 6 rows to test_schema.pipeline_logs
```

But then:
```python
log_df = spark.table("test_schema.pipeline_logs")
count = log_df.count()  # Returns 0, not 6
```

## Impact

This bug affects:
- **LogWriter functionality**: Cannot read back pipeline execution logs
- **Any code using explicit parquet format**: Tables created with `.format("parquet")` may have visibility issues
- **Data persistence**: Data appears to be written but is not queryable

## Workaround

No reliable workaround exists. The data is written but not immediately visible. Potential workarounds (not tested):
- Use Delta Lake format instead of parquet (if available)
- Add delays before querying (unreliable)
- Use different write methods (if available)

## Related Issues

- Related to issue #112 (general table visibility bug)
- Issue #112 was partially fixed in 3.17.2 for simple cases
- This issue is the remaining case for parquet format tables

## Test Results

| Test Case | sparkless 3.17.1 | sparkless 3.17.2 | PySpark | Status |
|-----------|------------------|------------------|---------|--------|
| Simple append (no format) | ❌ Fails | ✅ Works | ✅ Works | Fixed |
| LogWriter parquet append | ❌ Fails | ❌ Fails | ✅ Works | **Still Broken** |

## Minimal Reproduction

**Note**: The minimal reproduction case actually **works** in sparkless 3.17.2, but the issue manifests in the LogWriter integration context. This suggests a context-specific or timing issue.

### Working Minimal Case

```python
from sparkless import SparkSession

spark = SparkSession('test')
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

# Create empty table with parquet format (like LogWriter does)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])
empty_df = spark.createDataFrame([], schema)

# Create table with parquet format
empty_df.write.format("parquet").mode("overwrite").saveAsTable("test_schema.test_table")

# Append data with parquet format
data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
df = spark.createDataFrame(data, schema)
df.write.format("parquet").mode("append").saveAsTable("test_schema.test_table")

# Read back - THIS WORKS in 3.17.2
result = spark.table("test_schema.test_table")
count = result.count()  # ✅ Returns 2 (correct!)
```

### Failing Integration Case

The issue only manifests when LogWriter is used in a test/integration context:

```python
# In test context (pytest with fixtures)
log_writer = LogWriter(spark, schema="test_schema", table_name="pipeline_logs")
log_result = log_writer.write_log_rows(log_rows, run_id="test_run")
# Reports: {"success": True, "rows_written": 6}

# Immediately after:
log_df = spark.table("test_schema.pipeline_logs")
count = log_df.count()  # ❌ Returns 0 (should be 6)
```

**Hypothesis**: The issue may be related to:
- Session state management in test contexts
- Table metadata caching/refresh timing
- Schema/table registration in sparkless catalog after writes


# DataFrame.filter() returns 0 rows on tables created with saveAsTable() using complex schemas

## Summary

In sparkless version 3.17.5, when a DataFrame is written to a table using `saveAsTable()` with a complex schema (29+ fields including TimestampType fields), and then read back using `table()`, the `filter()` method returns 0 rows even when the data matches the filter condition.

**Key finding:** The table persistence works correctly (data is written and can be counted), but filtering fails.

## Environment

- **sparkless version:** 3.17.5
- **Python version:** 3.11.13
- **Operating System:** macOS (darwin 24.6.0)
- **Mode:** Mock mode (default sparkless behavior)

## Minimal Reproduction

```python
from sparkless import SparkSession
from sparkless.spark_types import (
    StructType, StructField, StringType, TimestampType, 
    FloatType, IntegerType
)
from datetime import datetime

# Create Spark session
spark = SparkSession.builder.appName("filter_bug_test").getOrCreate()

# Create schema
schema = "test_schema"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Create DataFrame with complex schema (29 fields)
complex_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_mode", StringType(), False),
    StructField("run_started_at", TimestampType(), True),
    StructField("run_ended_at", TimestampType(), True),
    StructField("execution_id", StringType(), False),
    StructField("pipeline_id", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("phase", StringType(), False),
    StructField("step_name", StringType(), False),
    StructField("step_type", StringType(), False),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_secs", FloatType(), False),
    StructField("table_fqn", StringType(), True),
    StructField("write_mode", StringType(), True),
    StructField("input_rows", IntegerType(), True),
    StructField("output_rows", IntegerType(), True),
    StructField("rows_written", IntegerType(), True),
    StructField("rows_processed", IntegerType(), True),
    StructField("table_total_rows", IntegerType(), True),
    StructField("valid_rows", IntegerType(), True),
    StructField("invalid_rows", IntegerType(), True),
    StructField("validation_rate", FloatType(), True),
    StructField("success", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("memory_usage_mb", FloatType(), True),
    StructField("cpu_usage_percent", FloatType(), True),
    StructField("metadata", StringType(), True),
    StructField("created_at", StringType(), True),
])

current_time = datetime.now()
data = [
    ("initial_run_1", "initial", current_time, current_time, "exec_1", "pipeline_1", 
     schema, "bronze", "step_1", "transform", current_time, current_time, 30.0,
     None, None, 100, 100, 100, 100, None, 100, 0, 100.0, "true", None, None, None, 
     "{}", current_time.isoformat()),
    ("initial_run_1", "initial", current_time, current_time, "exec_2", "pipeline_1",
     schema, "silver", "step_2", "transform", current_time, current_time, 25.0,
     None, None, 100, 100, 100, 100, None, 100, 0, 100.0, "true", None, None, None,
     "{}", current_time.isoformat()),
    ("other_run", "initial", current_time, current_time, "exec_3", "pipeline_1",
     schema, "gold", "step_3", "transform", current_time, current_time, 20.0,
     None, None, 100, 100, 100, 100, None, 100, 0, 100.0, "true", None, None, None,
     "{}", current_time.isoformat()),
]

df = spark.createDataFrame(data, complex_schema)
print(f"Created DataFrame: {df.count()} rows")  # Output: 3 ✅

# Write to table using Delta format
table_fqn = f"{schema}.test_table"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_fqn)
print("✅ Write completed")

# Read back
read_df = spark.table(table_fqn)
total_count = read_df.count()
print(f"Read back: {total_count} rows")  # Output: 3 ✅

# Verify data exists
all_rows = read_df.collect()
run_ids = [row.run_id for row in all_rows]
print(f"Actual run_id values: {run_ids}")  # Output: ['initial_run_1', 'initial_run_1', 'other_run'] ✅

# Try to filter (THIS FAILS)
filtered = read_df.filter(read_df.run_id == "initial_run_1")
filtered_count = filtered.count()
print(f"Filtered count: {filtered_count}")  # Output: 0 ❌ (Expected: 2)

# Also fails with F.col()
from sparkless import functions as F
filtered2 = read_df.filter(F.col("run_id") == "initial_run_1")
filtered_count2 = filtered2.count()
print(f"Filtered count (F.col): {filtered_count2}")  # Output: 0 ❌ (Expected: 2)
```

## Expected Behavior

After writing 3 rows to a table (2 with `run_id='initial_run_1'`, 1 with `run_id='other_run'`), filtering for `run_id == 'initial_run_1'` should return 2 rows.

## Actual Behavior

- `saveAsTable()` writes data successfully ✅
- `table()` reads data successfully (count returns 3) ✅
- `collect()` shows correct data ✅
- `filter()` returns **0 rows** ❌ (should return 2)

## Additional Observations

1. **Simple schemas work:** Filtering works correctly with simple schemas (3-5 fields)
2. **Complex schemas fail:** Filtering fails with complex schemas (29+ fields)
3. **Data is present:** The data exists and can be accessed via `collect()`
4. **Both filter methods fail:** Both `df.column == value` and `F.col('column') == value` fail
5. **Simple DataFrames work:** Filtering works on DataFrames created directly (not from `table()`)

## Real-World Impact

This breaks any code that:
1. Writes data to tables using `saveAsTable()` with complex schemas
2. Needs to filter the data after reading it back
3. Runs in mock mode for testing

### Example from Our Codebase

We have a logging system that writes pipeline execution logs:

```python
# Write log rows to table (works)
log_writer.write_log_rows(log_rows, run_id="run_1")
# Reports: "Successfully wrote 6 rows" ✅

# Read back (works)
log_df = spark.table("schema.pipeline_logs")
count = log_df.count()  # Returns 6 ✅

# Filter by run_id (FAILS)
filtered = log_df.filter(log_df.run_id == "run_1")
filtered_count = filtered.count()  # Returns 0 ❌ (Expected: 6)
```

This causes 4 test failures in our test suite.

## Reproduction Script

A complete reproduction script is available:
- File: `reproduce_sparkless_filter_issue.py` (included below)
- Can be run directly: `python reproduce_sparkless_filter_issue.py`

The script demonstrates:
- Table has 3 rows ✅
- run_id values are correct: `['initial_run_1', 'initial_run_1', 'other_run']` ✅
- Both filter methods return 0 rows ❌

## Requested Fix

Please fix `DataFrame.filter()` to work correctly on DataFrames read from tables created with `saveAsTable()`, especially when using complex schemas with many fields and mixed data types (StringType, TimestampType, IntegerType, FloatType, etc.).

**Specific areas to investigate:**
1. Filter evaluation on DataFrames from `table()` vs direct DataFrames
2. Complex schema handling (29+ fields)
3. Mixed data type support in filter expressions
4. TimestampType field handling in filters

---

**Priority:** High (blocks filtering of table data in mock mode)
**Severity:** High (workarounds exist but are inefficient - filtering is essential functionality)


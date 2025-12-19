# Issue: `DataFrame.filter()` returns 0 rows on tables created with `saveAsTable()` using complex schemas

## Summary

In sparkless version 3.17.5, when a DataFrame is written to a table using `saveAsTable()` with a complex schema (29+ fields including TimestampType fields), and then read back using `table()`, the `filter()` method returns 0 rows even when the data matches the filter condition.

**Key finding:** Table persistence works correctly (data is written and can be counted), but filtering fails. This breaks any code that needs to filter table data after reading it back.

## Environment

- **sparkless version:** 3.17.5
- **Python version:** 3.11.13
- **Operating System:** macOS (darwin 24.6.0)
- **Mode:** Mock mode (default sparkless behavior)

## Steps to Reproduce

**✅ REPRODUCED:** The issue is with `DataFrame.filter()` on DataFrames read from tables created with `saveAsTable()` using complex schemas.

**Key observations:**
- Table persistence works: `saveAsTable()` writes data, `table()` reads it, `count()` returns correct number
- Data is correct: `collect()` shows the data exists with correct values
- Filtering fails: `filter()` returns 0 rows even when data matches the condition
- Simple schemas work: Filtering works with simple schemas (3-5 fields)
- Complex schemas fail: Filtering fails with complex schemas (29+ fields with TimestampType, etc.)

### Minimal Reproduction

```python
from sparkless import SparkSession
from sparkless.spark_types import (
    StructType, StructField, StringType, TimestampType, 
    FloatType, IntegerType
)
from datetime import datetime

spark = SparkSession.builder.appName("test").getOrCreate()
schema = "test_schema"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Create DataFrame with complex schema (29 fields, matching LogWriter)
complex_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_mode", StringType(), False),
    StructField("run_started_at", TimestampType(), True),
    # ... 26 more fields including TimestampType, IntegerType, FloatType
])

current_time = datetime.now()
data = [
    ("initial_run_1", "initial", current_time, ...),  # 2 rows
    ("other_run", "initial", current_time, ...),      # 1 row
]

df = spark.createDataFrame(data, complex_schema)
print(f"Created: {df.count()} rows")  # Output: 3 ✅

# Write to table
table_fqn = f"{schema}.test_table"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_fqn)
print("✅ Write completed")

# Read back
read_df = spark.table(table_fqn)
total_count = read_df.count()
print(f"Read: {total_count} rows")  # Output: 3 ✅

# Verify data exists
all_rows = read_df.collect()
run_ids = [row.run_id for row in all_rows]
print(f"run_id values: {run_ids}")  # Output: ['initial_run_1', 'initial_run_1', 'other_run'] ✅

# Try to filter (THIS FAILS)
filtered = read_df.filter(read_df.run_id == "initial_run_1")
filtered_count = filtered.count()
print(f"Filtered: {filtered_count} rows")  # Output: 0 ❌ (Expected: 2)

# Also fails with F.col()
from sparkless import functions as F
filtered2 = read_df.filter(F.col("run_id") == "initial_run_1")
filtered_count2 = filtered2.count()
print(f"Filtered (F.col): {filtered_count2} rows")  # Output: 0 ❌ (Expected: 2)
```

### Reproduction Status

**⚠️ IMPORTANT:** We have been unable to reproduce this issue in isolation, despite trying multiple approaches:

1. ✅ Simple `saveAsTable()` + `table()` - **WORKS**
2. ✅ Delta format with append mode - **WORKS**
3. ✅ Explicit schema creation - **WORKS**
4. ✅ Creating empty table first, then appending - **WORKS**
5. ✅ Using actual LogWriter class - **WORKS**

**However, the issue occurs consistently in our test suite:**
- Test: `tests/system/test_full_pipeline_with_logging.py::TestFullPipelineWithLogging::test_full_pipeline_with_logging`
- `log_writer.write_log_rows()` reports: `rows_written=6, success=True` ✅
- `spark.table("schema.pipeline_logs").count()` returns: `0` ❌ (expected: 6)

**Possible causes we cannot rule out:**
1. **Test fixture behavior**: The test uses pytest fixtures that may affect Spark session state
2. **Parallel test execution**: Tests run with `-n 10` (pytest-xdist), may cause state conflicts
3. **Session isolation**: Different Spark session instances between write and read
4. **Timing/race conditions**: Write completes but read happens before state is committed
5. **Test environment setup**: Something in the test environment (fixtures, conftest.py) affects behavior

**We can provide:**
- Full test code that reproduces the issue
- Test fixtures and conftest.py setup
- Ability to run the failing test in our environment
- Access to our codebase for debugging

See reproduction scripts:
- `reproduce_sparkless_table_issue.py` - Simple reproduction (works)
- `reproduce_sparkless_issue_minimal.py` - Detailed pattern matching (works)
- `reproduce_with_logwriter.py` - Using actual LogWriter (works)

### Expected Behavior

After writing 3 rows to a table (2 with `run_id='initial_run_1'`, 1 with `run_id='other_run'`), filtering for `run_id == 'initial_run_1'` should return 2 rows.

### Actual Behavior

- `saveAsTable()` writes data successfully ✅
- `table()` reads data successfully (count returns 3) ✅
- `collect()` shows correct data ✅
- `filter()` returns **0 rows** ❌ (should return 2)

## Real-World Impact

This issue affects any code that:
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
assert filtered_count == 6  # ❌ Test fails
```

This causes 4 test failures in our test suite that filter log data by `run_id`.

## Additional Observations

1. **Table persistence works**: `saveAsTable()` writes data, `table()` reads it, `count()` returns correct number ✅
2. **Data is correct**: `collect()` shows the data exists with correct values ✅
3. **Filtering fails**: Both `df.column == value` and `F.col('column') == value` return 0 rows ❌
4. **Simple schemas work**: Filtering works correctly with simple schemas (3-5 fields)
5. **Complex schemas fail**: Filtering fails with complex schemas (29+ fields with TimestampType, IntegerType, FloatType, etc.)
6. **Direct DataFrames work**: Filtering works on DataFrames created directly (not from `table()`)
7. **Both filter methods fail**: Both attribute access and `F.col()` fail

## Workaround

Currently, we cannot filter table data in mock mode. We have to:
- Use `collect()` and filter in Python instead of DataFrame.filter()
- Skip tests that require filtering table data
- Run tests in real PySpark mode instead of mock mode

**Example workaround:**
```python
# Instead of: log_df.filter(log_df.run_id == "run_1")
# Use: [row.asDict() for row in log_df.collect() if row.run_id == "run_1"]
```

## Requested Fix

Please fix `DataFrame.filter()` to work correctly on DataFrames read from tables created with `saveAsTable()`, especially when using complex schemas with many fields and mixed data types.

**Specific areas to investigate:**
1. **Filter evaluation on table DataFrames** - why does filtering work on direct DataFrames but not on DataFrames from `table()`?
2. **Complex schema handling** - does the number of fields (29+) or specific field types (TimestampType) affect filter evaluation?
3. **Mixed data type support** - does having StringType, TimestampType, IntegerType, FloatType in the same schema cause issues?
4. **Filter expression evaluation** - is the filter expression being evaluated correctly on table DataFrames?

**Reproduction:**
- Complete reproduction script: `reproduce_sparkless_filter_issue.py`
- Can be run directly: `python reproduce_sparkless_filter_issue.py`
- Demonstrates the bug with a minimal example

## Reproduction Scripts

Complete reproduction scripts are available:
- **Minimal reproduction**: `reproduce_sparkless_filter_issue.py` - Demonstrates the bug with a minimal example
- **LogWriter schema test**: `test_logwriter_schema_filter.py` - Tests with exact LogWriter schema
- **Schema qualification test**: `test_schema_qualified_tables.py` - Tests schema-qualified table names

All scripts can be run directly and demonstrate the bug clearly.

## Related Issues

This appears to be related to how sparkless evaluates filter expressions on DataFrames read from tables. The filter evaluation may not work correctly when:
- The DataFrame comes from `table()` (vs direct creation)
- The schema is complex (29+ fields)
- The schema contains mixed data types (StringType, TimestampType, IntegerType, FloatType)

---

**Priority:** High (blocks filtering of table data in mock mode)
**Severity:** High (workarounds exist but are inefficient - filtering is essential functionality)


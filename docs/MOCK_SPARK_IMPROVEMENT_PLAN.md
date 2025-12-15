# Mock-Spark Improvement Plan

**Version:** 1.0  
**Date:** December 2024  
**Target Audience:** mock-spark developers  
**Purpose:** Detailed improvement plan based on real-world usage in SparkForge project

## Executive Summary

This document outlines critical improvements needed in mock-spark to better support PySpark compatibility, particularly for data pipeline frameworks like SparkForge. The improvements focus on schema evolution, type casting, SQL operations, and DataFrame writer options that are essential for testing complex data transformation pipelines.

## Table of Contents

1. [Schema Evolution Support](#schema-evolution-support)
2. [Type Casting Improvements](#type-casting-improvements)
3. [SQL DELETE Operation Support](#sql-delete-operation-support)
4. [DataFrame Writer Options](#dataframe-writer-options)
5. [Column Addition with Type Preservation](#column-addition-with-type-preservation)
6. [Table Metadata Operations](#table-metadata-operations)
7. [Implementation Priority](#implementation-priority)
8. [Testing Recommendations](#testing-recommendations)

---

## 1. Schema Evolution Support

### Current Limitation

mock-spark does not support adding null columns with specific data types during schema evolution. This prevents proper schema merging when new columns are added to existing tables.

### Problem Statement

When a DataFrame needs to be merged with an existing table schema (e.g., during schema evolution), PySpark allows adding missing columns with null values of the correct type:

```python
# PySpark - This works
existing_schema = existing_table.schema
for field in existing_schema.fields:
    if field.name not in df.columns:
        df = df.withColumn(
            field.name,
            F.lit(None).cast(field.dataType)  # Casts None to the exact type
        )
```

In mock-spark, this operation fails or doesn't preserve the correct type, making schema evolution impossible.

### Real-World Example from SparkForge

**File:** `src/pipeline_builder/execution.py` (lines 856-884)

```python
# Current workaround in SparkForge
if is_mock_spark():
    # For mock-spark, we can't easily add null columns with specific types
    # because mock-spark doesn't support the same casting operations.
    # Instead, we'll skip adding these columns and let the schema
    # evolution happen naturally through Delta Lake's mergeSchema.
    # This is a known limitation of mock-spark.
    self.logger.debug(
        f"Skipping column '{col_name}' addition in mock-spark "
        f"(mock-spark limitation with schema evolution)"
    )
else:
    # For real PySpark, cast to the exact type from existing schema
    merged_df = merged_df.withColumn(
        col_name,
        F.lit(None).cast(field),
    )
```

### Proposed Solution

**1.1: Support `F.lit(None).cast(dataType)`**

Implement proper type casting for null literals:

```python
# Expected behavior
from mock_spark import SparkSession, functions as F
from mock_spark.spark_types import StringType, IntegerType, TimestampType

spark = SparkSession()
df = spark.createDataFrame([("Alice", 25)], ["name", "age"])

# Should work: add a null column with specific type
df_with_timestamp = df.withColumn(
    "created_at",
    F.lit(None).cast(TimestampType())
)

# Verify the schema
assert "created_at" in df_with_timestamp.columns
assert isinstance(df_with_timestamp.schema["created_at"].dataType, TimestampType)
```

**Implementation Details:**

- When `F.lit(None)` is used with `.cast(dataType)`, create a column with the specified type
- All rows should have `None` values, but the column type should be preserved
- The schema should reflect the correct data type

**1.2: Support `mergeSchema` Option in DataFrameWriter**

Implement the `mergeSchema` option for append mode writes:

```python
# Expected behavior
df.write.mode("append").option("mergeSchema", "true").saveAsTable("my_table")
```

When `mergeSchema="true"`:
- If the table exists, merge the DataFrame schema with the existing table schema
- Add missing columns from the DataFrame to the table (with nulls for existing rows)
- Add missing columns from the table to the DataFrame (with nulls for new rows)
- Preserve all existing columns

**1.3: Support `overwriteSchema` Option**

Implement the `overwriteSchema` option for append/overwrite mode:

```python
# Expected behavior
df.write.mode("append").option("overwriteSchema", "true").saveAsTable("my_table")
```

When `overwriteSchema="true"`:
- Allow writing DataFrames with different schemas
- Automatically evolve the table schema to match the DataFrame
- Preserve existing data where possible

### Test Cases

```python
def test_schema_evolution_with_null_columns():
    """Test adding null columns with specific types."""
    spark = SparkSession()
    
    # Create initial table
    df1 = spark.createDataFrame([("Alice", 25)], ["name", "age"])
    df1.write.mode("overwrite").saveAsTable("test.evolution_test")
    
    # Add new column with null values of correct type
    from mock_spark.spark_types import TimestampType
    df2 = spark.createDataFrame([("Bob", 30)], ["name", "age"])
    df2 = df2.withColumn("created_at", F.lit(None).cast(TimestampType()))
    
    # Should be able to append with schema evolution
    df2.write.mode("append").option("mergeSchema", "true").saveAsTable("test.evolution_test")
    
    # Verify result
    result = spark.table("test.evolution_test")
    assert "created_at" in result.columns
    assert result.count() == 2
    # First row should have None for created_at
    rows = result.collect()
    assert rows[0]["created_at"] is None
```

---

## 2. Type Casting Improvements

### Current Limitation

mock-spark's type casting system doesn't fully support all PySpark casting operations, particularly for complex types and null handling.

### Problem Statement

PySpark allows casting between compatible types and handles null values gracefully. mock-spark needs better support for:

1. Casting null literals to specific types
2. Casting between compatible numeric types
3. Casting string to timestamp/date
4. Preserving type information through transformations

### Real-World Example

**File:** `src/pipeline_builder/execution.py` (line 878)

```python
# This fails in mock-spark but works in PySpark
merged_df = merged_df.withColumn(
    col_name,
    F.lit(None).cast(field.dataType),  # field.dataType could be any type
)
```

### Proposed Solution

**2.1: Enhanced `cast()` Method**

Support casting for all basic types:

```python
# String to Integer
df = df.withColumn("age_int", F.col("age_str").cast(IntegerType()))

# String to Timestamp
df = df.withColumn("ts", F.col("date_str").cast(TimestampType()))

# Integer to Double
df = df.withColumn("value_double", F.col("value_int").cast(DoubleType()))

# Null to any type
df = df.withColumn("new_col", F.lit(None).cast(StringType()))
```

**2.2: Type Compatibility Matrix**

Implement a type compatibility matrix similar to PySpark:

| From Type | To Type | Should Work |
|-----------|---------|-------------|
| StringType | IntegerType | ✅ (if parseable) |
| StringType | DoubleType | ✅ (if parseable) |
| StringType | TimestampType | ✅ (if parseable) |
| IntegerType | DoubleType | ✅ |
| IntegerType | StringType | ✅ |
| Any | Any (via None) | ✅ (with cast) |

**2.3: Error Handling**

Provide clear error messages for invalid casts:

```python
# Should raise clear error
try:
    df.withColumn("bad", F.col("name").cast(IntegerType()))
except AnalysisException as e:
    assert "cannot cast" in str(e).lower()
    assert "StringType" in str(e)
    assert "IntegerType" in str(e)
```

### Test Cases

```python
def test_null_literal_casting():
    """Test casting None to various types."""
    spark = SparkSession()
    df = spark.createDataFrame([("Alice",)], ["name"])
    
    # Cast None to different types
    df = df.withColumn("str_col", F.lit(None).cast(StringType()))
    df = df.withColumn("int_col", F.lit(None).cast(IntegerType()))
    df = df.withColumn("ts_col", F.lit(None).cast(TimestampType()))
    
    # Verify types in schema
    schema = df.schema
    assert isinstance(schema["str_col"].dataType, StringType)
    assert isinstance(schema["int_col"].dataType, IntegerType)
    assert isinstance(schema["ts_col"].dataType, TimestampType)
    
    # Verify values are None
    row = df.first()
    assert row["str_col"] is None
    assert row["int_col"] is None
    assert row["ts_col"] is None

def test_numeric_type_casting():
    """Test casting between numeric types."""
    spark = SparkSession()
    df = spark.createDataFrame([(25, 3.14)], ["age", "pi"])
    
    # Integer to Double
    df = df.withColumn("age_double", F.col("age").cast(DoubleType()))
    assert isinstance(df.schema["age_double"].dataType, DoubleType)
    
    # Double to Integer (should truncate)
    df = df.withColumn("pi_int", F.col("pi").cast(IntegerType()))
    assert isinstance(df.schema["pi_int"].dataType, IntegerType)
    assert df.first()["pi_int"] == 3
```

---

## 3. SQL DELETE Operation Support

### Current Limitation

mock-spark does not support SQL `DELETE` operations, which are essential for Delta Lake table operations and data pipeline patterns.

### Problem Statement

Many data pipeline frameworks use `DELETE FROM table` to clear data before appending new data (DELETE + INSERT pattern). This is common in:

- Delta Lake table maintenance
- Incremental data processing
- Schema evolution scenarios

### Real-World Example from SparkForge

**File:** `src/pipeline_builder/execution.py` (lines 940-959)

```python
# Current workaround
try:
    self.spark.sql(f"DELETE FROM {output_table}")
    delete_succeeded = True
except Exception as e:
    # DELETE might fail for non-Delta tables (e.g., Parquet)
    # If there are schema changes, drop and recreate the table
    error_msg = str(e).lower()
    if "does not support delete" in error_msg or "unsupported_feature" in error_msg:
        self.logger.info(
            f"Table '{output_table}' does not support DELETE. "
            f"Dropping and recreating table for schema evolution."
        )
        drop_table(self.spark, output_table)
        delete_succeeded = True
```

### Proposed Solution

**3.1: Basic DELETE Support**

Implement `DELETE FROM table` without WHERE clause (deletes all rows):

```python
# Expected behavior
spark = SparkSession()
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.write.mode("overwrite").saveAsTable("test.people")

# Verify data exists
assert spark.table("test.people").count() == 2

# Delete all rows
spark.sql("DELETE FROM test.people")

# Verify table is empty but schema remains
result = spark.table("test.people")
assert result.count() == 0
assert "name" in result.columns
assert "age" in result.columns
```

**3.2: DELETE with WHERE Clause**

Support conditional deletes:

```python
# Delete specific rows
spark.sql("DELETE FROM test.people WHERE age < 30")

# Verify only Bob remains
result = spark.table("test.people")
assert result.count() == 1
assert result.first()["name"] == "Bob"
```

**3.3: Error Handling**

Provide appropriate errors for invalid DELETE operations:

```python
# Should raise error for non-existent table
try:
    spark.sql("DELETE FROM test.nonexistent")
except AnalysisException as e:
    assert "table or view not found" in str(e).lower()

# Should raise error for views (if views don't support DELETE)
try:
    spark.sql("CREATE VIEW test.my_view AS SELECT * FROM test.people")
    spark.sql("DELETE FROM test.my_view")
except AnalysisException as e:
    assert "delete" in str(e).lower()
```

### Implementation Details

1. **Table State Management:**
   - DELETE should remove rows but preserve table schema
   - Table metadata (columns, types) should remain unchanged
   - Table should still be queryable after DELETE (returns empty result)

2. **Transaction-like Behavior:**
   - DELETE should be atomic (all or nothing)
   - If DELETE fails partway through, table should remain in original state

3. **Performance Considerations:**
   - For mock-spark, DELETE can be implemented by clearing the internal data store
   - No need for complex transaction logs (unlike real Delta Lake)

### Test Cases

```python
def test_delete_all_rows():
    """Test DELETE FROM table removes all rows."""
    spark = SparkSession()
    df = spark.createDataFrame(
        [("Alice", 25), ("Bob", 30), ("Charlie", 35)],
        ["name", "age"]
    )
    df.write.mode("overwrite").saveAsTable("test.people")
    
    # Delete all
    spark.sql("DELETE FROM test.people")
    
    # Verify
    result = spark.table("test.people")
    assert result.count() == 0
    assert len(result.columns) == 2

def test_delete_with_where():
    """Test DELETE FROM table WHERE condition."""
    spark = SparkSession()
    df = spark.createDataFrame(
        [("Alice", 25), ("Bob", 30), ("Charlie", 35)],
        ["name", "age"]
    )
    df.write.mode("overwrite").saveAsTable("test.people")
    
    # Delete rows where age < 30
    spark.sql("DELETE FROM test.people WHERE age < 30")
    
    # Verify
    result = spark.table("test.people")
    assert result.count() == 2
    rows = result.collect()
    names = [row["name"] for row in rows]
    assert "Alice" not in names
    assert "Bob" in names
    assert "Charlie" in names

def test_delete_preserves_schema():
    """Test DELETE preserves table schema."""
    spark = SparkSession()
    from mock_spark.spark_types import StringType, IntegerType
    
    df = spark.createDataFrame([("Alice", 25)], ["name", "age"])
    df.write.mode("overwrite").saveAsTable("test.people")
    
    # Delete all
    spark.sql("DELETE FROM test.people")
    
    # Verify schema is preserved
    result = spark.table("test.people")
    schema = result.schema
    assert isinstance(schema["name"].dataType, StringType)
    assert isinstance(schema["age"].dataType, IntegerType)
```

---

## 4. DataFrame Writer Options

### Current Limitation

mock-spark's DataFrameWriter doesn't fully support all PySpark writer options, particularly `mergeSchema` and `overwriteSchema`.

### Problem Statement

PySpark DataFrameWriter supports several options that control how data is written:

- `mergeSchema`: Merge DataFrame schema with existing table schema
- `overwriteSchema`: Overwrite table schema to match DataFrame
- `partitionBy`: Partition data by columns
- `bucketBy`: Bucket data by columns

mock-spark needs better support for these options to enable proper schema evolution testing.

### Real-World Example from SparkForge

**File:** `src/pipeline_builder/execution.py` (lines 933-935, 963-965)

```python
# Current workaround - mock-spark doesn't support overwriteSchema properly
if is_mock_spark():
    # Mock-spark: use overwrite mode to replace table with new schema
    writer = _create_dataframe_writer(
        output_df, self.spark, "overwrite", overwriteSchema="true"
    )
else:
    # Real PySpark: try DELETE + INSERT pattern for Delta Lake
    # ... uses append mode with overwriteSchema
    writer = _create_dataframe_writer(
        output_df, self.spark, "append", overwriteSchema="true"
    )
```

### Proposed Solution

**4.1: `mergeSchema` Option**

```python
# Expected behavior
spark = SparkSession()

# Create initial table
df1 = spark.createDataFrame([("Alice", 25)], ["name", "age"])
df1.write.mode("overwrite").saveAsTable("test.people")

# Append with new column
df2 = spark.createDataFrame([("Bob", 30, "engineer")], ["name", "age", "job"])
df2.write.mode("append").option("mergeSchema", "true").saveAsTable("test.people")

# Result should have all columns
result = spark.table("test.people")
assert "name" in result.columns
assert "age" in result.columns
assert "job" in result.columns
assert result.count() == 2

# Alice should have None for job
rows = result.collect()
alice_row = next(r for r in rows if r["name"] == "Alice")
assert alice_row["job"] is None
```

**4.2: `overwriteSchema` Option**

```python
# Expected behavior
spark = SparkSession()

# Create initial table
df1 = spark.createDataFrame([("Alice", 25)], ["name", "age"])
df1.write.mode("overwrite").saveAsTable("test.people")

# Overwrite with different schema
df2 = spark.createDataFrame(
    [("Bob", "engineer", 50000)],
    ["name", "job", "salary"]
)
df2.write.mode("append").option("overwriteSchema", "true").saveAsTable("test.people")

# Result should have new schema
result = spark.table("test.people")
assert "name" in result.columns
assert "job" in result.columns
assert "salary" in result.columns
# age column should be gone (schema overwritten)
assert "age" not in result.columns
```

**4.3: Option Validation**

Provide clear errors for invalid option combinations:

```python
# Should raise error for invalid combination
try:
    df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("test.table")
except AnalysisException as e:
    # mergeSchema typically only works with append mode
    assert "mergeSchema" in str(e).lower() or "mode" in str(e).lower()
```

### Implementation Details

1. **Schema Merging Logic:**
   - When `mergeSchema="true"`:
     - Read existing table schema
     - Merge with DataFrame schema
     - Add missing columns from table to DataFrame (with nulls)
     - Add missing columns from DataFrame to table (with nulls for existing rows)
     - Write merged DataFrame

2. **Schema Overwriting Logic:**
   - When `overwriteSchema="true"`:
     - Replace table schema with DataFrame schema
     - Existing data may be lost if columns don't match
     - Preserve data where column names and types match

3. **Mode Combinations:**
   - `append` + `mergeSchema`: Merge schemas, append data
   - `append` + `overwriteSchema`: Overwrite schema, append data
   - `overwrite` + `overwriteSchema`: Overwrite schema and data
   - `overwrite` + `mergeSchema`: May not make sense (should error or ignore)

### Test Cases

```python
def test_merge_schema_append():
    """Test mergeSchema with append mode."""
    spark = SparkSession()
    
    # Initial table
    df1 = spark.createDataFrame([("Alice", 25)], ["name", "age"])
    df1.write.mode("overwrite").saveAsTable("test.people")
    
    # Append with new column
    df2 = spark.createDataFrame([("Bob", 30, "NYC")], ["name", "age", "city"])
    df2.write.mode("append").option("mergeSchema", "true").saveAsTable("test.people")
    
    # Verify merged schema
    result = spark.table("test.people")
    assert set(result.columns) == {"name", "age", "city"}
    assert result.count() == 2
    
    # Verify data
    rows = result.collect()
    alice = next(r for r in rows if r["name"] == "Alice")
    bob = next(r for r in rows if r["name"] == "Bob")
    assert alice["city"] is None
    assert bob["city"] == "NYC"

def test_overwrite_schema_append():
    """Test overwriteSchema with append mode."""
    spark = SparkSession()
    
    # Initial table
    df1 = spark.createDataFrame([("Alice", 25)], ["name", "age"])
    df1.write.mode("overwrite").saveAsTable("test.people")
    
    # Append with different schema
    df2 = spark.createDataFrame([("Bob", "engineer")], ["name", "job"])
    df2.write.mode("append").option("overwriteSchema", "true").saveAsTable("test.people")
    
    # Verify new schema
    result = spark.table("test.people")
    assert "name" in result.columns
    assert "job" in result.columns
    assert "age" not in result.columns  # Old column removed
```

---

## 5. Column Addition with Type Preservation

### Current Limitation

mock-spark doesn't properly support adding columns with specific types, especially when the column needs to be added to match an existing table schema.

### Problem Statement

During schema evolution, it's common to need to add columns to a DataFrame to match an existing table schema. The columns should have the correct types and null values.

### Real-World Example from SparkForge

**File:** `src/pipeline_builder/execution.py` (lines 852-884)

```python
# This is the pattern we need to support
for col_name, field in existing_columns.items():
    if col_name not in merged_df.columns:
        # Add missing column with null values of the correct type
        merged_df = merged_df.withColumn(
            col_name,
            F.lit(None).cast(field.dataType),  # field.dataType is a StructField
        )
```

### Proposed Solution

**5.1: Support Adding Columns from StructField**

Allow using `StructField` objects directly in casting:

```python
from mock_spark.spark_types import StructField, StringType, IntegerType

# Create a field
field = StructField("new_col", IntegerType(), nullable=True)

# Add column using the field's dataType
df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

# Verify
assert "new_col" in df.columns
assert isinstance(df.schema["new_col"].dataType, IntegerType)
```

**5.2: Preserve Nullability**

When adding columns, preserve the nullability from the source schema:

```python
# If existing table has nullable column, new rows should have None
# If existing table has non-nullable column, should handle appropriately
existing_field = StructField("status", StringType(), nullable=False)
df = df.withColumn(existing_field.name, F.lit(None).cast(existing_field.dataType))

# Note: In real PySpark, adding None to non-nullable column might cause issues
# mock-spark should either:
# 1. Allow it (more permissive)
# 2. Raise an error (strict)
# 3. Use a default value (if specified)
```

### Test Cases

```python
def test_add_column_from_struct_field():
    """Test adding column using StructField."""
    from mock_spark.spark_types import StructField, TimestampType
    
    spark = SparkSession()
    df = spark.createDataFrame([("Alice",)], ["name"])
    
    # Add column from StructField
    field = StructField("created_at", TimestampType(), nullable=True)
    df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    
    # Verify
    assert "created_at" in df.columns
    assert isinstance(df.schema["created_at"].dataType, TimestampType)
    assert df.first()["created_at"] is None

def test_schema_merge_with_type_preservation():
    """Test merging schemas preserves types correctly."""
    spark = SparkSession()
    from mock_spark.spark_types import StringType, IntegerType, DoubleType
    
    # Existing table schema
    existing_df = spark.createDataFrame(
        [("Alice", 25, 50000.0)],
        ["name", "age", "salary"]
    )
    existing_df.write.mode("overwrite").saveAsTable("test.employees")
    
    # New DataFrame with different columns
    new_df = spark.createDataFrame(
        [("Bob", "engineer")],
        ["name", "job"]
    )
    
    # Add missing columns to match existing schema
    existing_schema = spark.table("test.employees").schema
    for field in existing_schema.fields:
        if field.name not in new_df.columns:
            new_df = new_df.withColumn(
                field.name,
                F.lit(None).cast(field.dataType)
            )
    
    # Verify types are preserved
    assert isinstance(new_df.schema["age"].dataType, IntegerType)
    assert isinstance(new_df.schema["salary"].dataType, DoubleType)
    assert new_df.first()["age"] is None
    assert new_df.first()["salary"] is None
```

---

## 6. Table Metadata Operations

### Current Limitation

Some table metadata operations may not work consistently, particularly around schema inspection and table existence checks.

### Problem Statement

Data pipeline frameworks need to:

1. Check if a table exists
2. Read table schema without reading data
3. Inspect column types and nullability
4. Refresh table metadata after writes

### Proposed Solution

**6.1: Enhanced `table()` Method**

Ensure `spark.table()` works reliably:

```python
# Should work immediately after write
df.write.mode("overwrite").saveAsTable("test.my_table")
table_df = spark.table("test.my_table")  # Should work without delay

# Should work for empty tables
empty_df = spark.createDataFrame([], schema)
empty_df.write.mode("overwrite").saveAsTable("test.empty_table")
result = spark.table("test.empty_table")
assert result.count() == 0
assert len(result.columns) > 0
```

**6.2: Schema Inspection**

Provide reliable schema access:

```python
# Get schema without reading data
table_df = spark.table("test.my_table")
schema = table_df.schema

# Access field information
for field in schema.fields:
    print(f"{field.name}: {field.dataType}, nullable={field.nullable}")

# Access by name
age_field = schema["age"]
assert isinstance(age_field.dataType, IntegerType)
```

**6.3: REFRESH TABLE Support**

Support `REFRESH TABLE` command for metadata updates:

```python
# After writing, refresh to ensure latest metadata
df.write.mode("append").saveAsTable("test.my_table")
spark.sql("REFRESH TABLE test.my_table")

# Should see latest data
result = spark.table("test.my_table")
assert result.count() == expected_count
```

### Test Cases

```python
def test_table_immediate_access():
    """Test table is accessible immediately after write."""
    spark = SparkSession()
    df = spark.createDataFrame([("Alice", 25)], ["name", "age"])
    
    df.write.mode("overwrite").saveAsTable("test.people")
    
    # Should work immediately
    result = spark.table("test.people")
    assert result.count() == 1
    assert "name" in result.columns

def test_schema_inspection():
    """Test schema can be inspected reliably."""
    spark = SparkSession()
    from mock_spark.spark_types import StringType, IntegerType
    
    df = spark.createDataFrame([("Alice", 25)], ["name", "age"])
    df.write.mode("overwrite").saveAsTable("test.people")
    
    # Inspect schema
    table_df = spark.table("test.people")
    schema = table_df.schema
    
    assert len(schema.fields) == 2
    assert schema["name"].dataType == StringType()
    assert schema["age"].dataType == IntegerType()
```

---

## 7. Implementation Priority

### High Priority (Critical for Schema Evolution)

1. **`F.lit(None).cast(dataType)` support** - Enables schema merging
2. **`mergeSchema` option** - Essential for schema evolution
3. **`overwriteSchema` option** - Needed for schema changes
4. **DELETE FROM table support** - Common pattern in data pipelines

### Medium Priority (Important for Compatibility)

5. **Enhanced type casting** - Better PySpark compatibility
6. **Column addition from StructField** - Cleaner API
7. **Table metadata operations** - Better reliability

### Low Priority (Nice to Have)

8. **Advanced writer options** (partitionBy, bucketBy)
9. **Transaction-like behavior** for writes
10. **Performance optimizations**

---

## 8. Testing Recommendations

### Test Structure

Create comprehensive test suites for each improvement:

```python
# tests/test_schema_evolution.py
class TestSchemaEvolution:
    def test_add_null_column_with_type()
    def test_merge_schema_append()
    def test_overwrite_schema()
    def test_schema_evolution_preserves_existing_data()

# tests/test_type_casting.py
class TestTypeCasting:
    def test_null_literal_casting()
    def test_numeric_type_casting()
    def test_string_to_timestamp()
    def test_invalid_cast_errors()

# tests/test_sql_operations.py
class TestSQLOperations:
    def test_delete_all_rows()
    def test_delete_with_where()
    def test_delete_preserves_schema()
    def test_delete_nonexistent_table_error()
```

### Integration Tests

Test with real-world scenarios:

```python
def test_complete_schema_evolution_scenario():
    """Test a complete schema evolution scenario like SparkForge uses."""
    spark = SparkSession()
    
    # Step 1: Create initial table
    df1 = spark.createDataFrame(
        [("user1", "Alice", 100)],
        ["user_id", "name", "value"]
    )
    df1.write.mode("overwrite").saveAsTable("test.events")
    
    # Step 2: Add new column in transform
    df2 = spark.createDataFrame(
        [("user2", "Bob", 200)],
        ["user_id", "name", "value"]
    )
    df2 = df2.withColumn("processed_at", F.current_timestamp())
    
    # Step 3: Merge with existing schema
    existing_table = spark.table("test.events")
    existing_schema = existing_table.schema
    
    # Add missing columns from existing schema
    for field in existing_schema.fields:
        if field.name not in df2.columns:
            df2 = df2.withColumn(
                field.name,
                F.lit(None).cast(field.dataType)
            )
    
    # Step 4: Write with schema evolution
    df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("test.events")
    
    # Step 5: Verify result
    result = spark.table("test.events")
    assert "user_id" in result.columns
    assert "name" in result.columns
    assert "value" in result.columns
    assert "processed_at" in result.columns
    assert result.count() == 1
```

### Compatibility Tests

Ensure improvements don't break existing functionality:

```python
def test_backward_compatibility():
    """Ensure new features don't break existing code."""
    # Test that existing mock-spark code still works
    # Test that new features are opt-in where possible
    pass
```

---

## 9. Example: Complete Schema Evolution Flow

This section shows how all improvements work together:

```python
from mock_spark import SparkSession, functions as F
from mock_spark.spark_types import StringType, IntegerType, TimestampType

def complete_schema_evolution_example():
    """Complete example showing schema evolution with all improvements."""
    spark = SparkSession()
    
    # ===== PHASE 1: Initial Load =====
    print("Phase 1: Initial load with basic schema")
    df1 = spark.createDataFrame(
        [("user1", "Alice", 100), ("user2", "Bob", 200)],
        ["user_id", "name", "value"]
    )
    df1.write.mode("overwrite").saveAsTable("analytics.events")
    
    result1 = spark.table("analytics.events")
    print(f"Columns: {result1.columns}")
    print(f"Count: {result1.count()}")
    # Output: Columns: ['user_id', 'name', 'value'], Count: 2
    
    # ===== PHASE 2: Schema Evolution =====
    print("\nPhase 2: Add new column 'processed_at'")
    
    # New DataFrame with additional column
    df2 = spark.createDataFrame(
        [("user3", "Charlie", 300)],
        ["user_id", "name", "value"]
    )
    df2 = df2.withColumn("processed_at", F.current_timestamp())
    
    # Read existing schema
    existing_table = spark.table("analytics.events")
    existing_schema = existing_table.schema
    
    # Merge schemas: add missing columns from existing table
    for field in existing_schema.fields:
        if field.name not in df2.columns:
            df2 = df2.withColumn(
                field.name,
                F.lit(None).cast(field.dataType)  # NEW: This should work!
            )
    
    # Write with schema evolution
    df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analytics.events")
    
    result2 = spark.table("analytics.events")
    print(f"Columns: {result2.columns}")
    print(f"Count: {result2.count()}")
    # Output: Columns: ['user_id', 'name', 'value', 'processed_at'], Count: 1
    
    # ===== PHASE 3: Append with Schema Merge =====
    print("\nPhase 3: Append new data with schema merge")
    
    df3 = spark.createDataFrame(
        [("user4", "Diana", 400, None)],  # processed_at is None
        ["user_id", "name", "value", "processed_at"]
    )
    
    # Use mergeSchema to preserve existing columns
    df3.write.mode("append").option("mergeSchema", "true").saveAsTable("analytics.events")
    
    result3 = spark.table("analytics.events")
    print(f"Columns: {result3.columns}")
    print(f"Count: {result3.count()}")
    # Output: Columns: ['user_id', 'name', 'value', 'processed_at'], Count: 2
    
    # ===== PHASE 4: DELETE + INSERT Pattern =====
    print("\nPhase 4: Use DELETE + INSERT pattern")
    
    # Clear existing data
    spark.sql("DELETE FROM analytics.events")  # NEW: This should work!
    
    # Insert fresh data
    df4 = spark.createDataFrame(
        [("user5", "Eve", 500, None)],
        ["user_id", "name", "value", "processed_at"]
    )
    df4.write.mode("append").saveAsTable("analytics.events")
    
    result4 = spark.table("analytics.events")
    print(f"Columns: {result4.columns}")
    print(f"Count: {result4.count()}")
    # Output: Columns: ['user_id', 'name', 'value', 'processed_at'], Count: 1
    
    print("\n✅ All schema evolution operations completed successfully!")

if __name__ == "__main__":
    complete_schema_evolution_example()
```

---

## 10. Migration Guide for mock-spark Users

### Before (Current Limitations)

```python
# ❌ This doesn't work in current mock-spark
df = df.withColumn("new_col", F.lit(None).cast(TimestampType()))
# Error: Cannot cast None to TimestampType

# ❌ This doesn't work
spark.sql("DELETE FROM my_table")
# Error: DELETE operation not supported

# ❌ This doesn't work properly
df.write.mode("append").option("mergeSchema", "true").saveAsTable("my_table")
# Schema merging doesn't work correctly
```

### After (With Improvements)

```python
# ✅ This will work
df = df.withColumn("new_col", F.lit(None).cast(TimestampType()))
# Successfully adds column with correct type

# ✅ This will work
spark.sql("DELETE FROM my_table")
# Successfully deletes all rows, preserves schema

# ✅ This will work
df.write.mode("append").option("mergeSchema", "true").saveAsTable("my_table")
# Successfully merges schemas and appends data
```

---

## 11. Additional Considerations

### Performance

While performance is less critical for mock-spark (it's for testing), consider:

- Efficient schema merging algorithms
- Fast table lookups for schema inspection
- Minimal overhead for type casting

### Error Messages

Provide clear, actionable error messages:

```python
# Good error message
try:
    df.withColumn("bad", F.col("name").cast(IntegerType()))
except AnalysisException as e:
    # Should say something like:
    # "Cannot cast column 'name' of type StringType to IntegerType. 
    #  Column contains non-numeric values: ['Alice', 'Bob']"
    pass
```

### Documentation

Update mock-spark documentation to include:

- Schema evolution examples
- Type casting reference
- SQL operations guide
- Migration guide from PySpark

---

## 12. Conclusion

These improvements will significantly enhance mock-spark's compatibility with PySpark, making it a more reliable tool for testing data pipeline frameworks. The improvements are prioritized based on real-world usage patterns and can be implemented incrementally.

### Key Benefits

1. **Better PySpark Compatibility** - More code will work without modification
2. **Schema Evolution Support** - Critical for modern data pipelines
3. **Reduced Workarounds** - Less special-case code needed in frameworks
4. **Better Testing** - More comprehensive test coverage possible

### Next Steps

1. Review and prioritize improvements
2. Create detailed implementation specs for each item
3. Implement high-priority items first
4. Add comprehensive test coverage
5. Update documentation
6. Release incrementally with version bumps

---

## Appendix A: Reference Implementation Patterns

### Pattern 1: Schema Merging

```python
def merge_schemas(existing_schema, new_df):
    """Merge existing table schema with new DataFrame."""
    existing_columns = {f.name: f for f in existing_schema.fields}
    new_columns = set(new_df.columns)
    
    # Add missing columns from existing schema
    for col_name, field in existing_columns.items():
        if col_name not in new_columns:
            new_df = new_df.withColumn(
                col_name,
                F.lit(None).cast(field.dataType)
            )
    
    # Note: Missing columns from new_df will be added by mergeSchema option
    return new_df
```

### Pattern 2: DELETE + INSERT

```python
def delete_and_insert(spark, table_name, new_df):
    """Delete all rows and insert new data."""
    try:
        spark.sql(f"DELETE FROM {table_name}")
        new_df.write.mode("append").saveAsTable(table_name)
    except Exception as e:
        if "does not support delete" in str(e).lower():
            # Fallback: drop and recreate
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            new_df.write.mode("overwrite").saveAsTable(table_name)
        else:
            raise
```

### Pattern 3: Schema Evolution Detection

```python
def detect_schema_evolution(existing_table, new_df):
    """Detect if schema evolution is needed."""
    existing_cols = set(existing_table.columns)
    new_cols = set(new_df.columns)
    
    new_columns = new_cols - existing_cols
    missing_columns = existing_cols - new_cols
    
    return {
        "needs_evolution": len(new_columns) > 0 or len(missing_columns) > 0,
        "new_columns": new_columns,
        "missing_columns": missing_columns
    }
```

---

## Appendix B: Related Issues and Workarounds

### Issue: Column Type Inference

**Problem:** When adding null columns, type inference may fail.

**Current Workaround:**
```python
# Explicitly specify type
df = df.withColumn("new_col", F.lit(None).cast(StringType()))
```

**Proposed Solution:** Support type inference from context or explicit casting.

### Issue: Schema Validation

**Problem:** No validation that merged schemas are compatible.

**Proposed Solution:** Add optional schema validation with clear error messages.

---

## Contact and Feedback

For questions or feedback on this improvement plan, please contact the SparkForge development team or open an issue in the mock-spark repository.

**Document Version:** 1.0  
**Last Updated:** December 2024  
**Maintained By:** SparkForge Project

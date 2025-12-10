# Mock-Spark Improvement Plan: PySpark Compatibility for Delta Lake Schema Evolution

**Document Version:** 1.0  
**Date:** December 9, 2024  
**Target Audience:** Mock-Spark Developers  
**Purpose:** Detailed improvement plan to enhance mock-spark compatibility with PySpark, specifically for Delta Lake schema evolution features

---

## Executive Summary

This document outlines critical improvements needed in mock-spark to achieve full compatibility with PySpark's Delta Lake schema evolution capabilities. The issues were discovered during testing of the PipelineBuilder framework, which relies on Delta Lake's automatic schema evolution when adding new columns to existing tables.

**Key Issues Identified:**
1. `F.lit(None)` fails with `'NoneType' object has no attribute '_jvm'` error
2. `overwriteSchema` option not properly supported in `saveAsTable` operations
3. Table catalog synchronization delays after `saveAsTable` operations
4. Schema merging during overwrite mode doesn't preserve existing columns
5. Type casting operations fail when JVM is not available

**Impact:** These limitations prevent mock-spark from being a drop-in replacement for PySpark in Delta Lake-based data pipelines that require schema evolution.

---

## Table of Contents

1. [Issue #1: F.lit(None) JVM Dependency](#issue-1-flitnone-jvm-dependency)
2. [Issue #2: overwriteSchema Option Support](#issue-2-overwriteschema-option-support)
3. [Issue #3: Table Catalog Synchronization](#issue-3-table-catalog-synchronization)
4. [Issue #4: Schema Merging in Overwrite Mode](#issue-4-schema-merging-in-overwrite-mode)
5. [Issue #5: Type Casting Without JVM](#issue-5-type-casting-without-jvm)
6. [Test Cases for Validation](#test-cases-for-validation)
7. [Implementation Priority](#implementation-priority)
8. [References and Examples](#references-and-examples)

---

## Issue #1: F.lit(None) JVM Dependency

### Problem Description

When attempting to add null columns to a DataFrame using `F.lit(None)`, mock-spark raises:
```
'NoneType' object has no attribute '_jvm'
```

This occurs because mock-spark's implementation of `F.lit()` attempts to access PySpark's JVM-based type system, which doesn't exist in mock-spark.

### Current Behavior

**PySpark (Working):**
```python
from pyspark.sql import functions as F
df = df.withColumn("new_col", F.lit(None).cast(StringType()))
# Successfully adds a null column of StringType
```

**Mock-Spark (Failing):**
```python
from mock_spark import functions as F
df = df.withColumn("new_col", F.lit(None))
# Raises: 'NoneType' object has no attribute '_jvm'
```

### Root Cause Analysis

1. **Location:** `mock_spark/functions.py` - `lit()` function implementation
2. **Issue:** The function likely calls PySpark's internal JVM methods or attempts to infer types using JVM-based type system
3. **Impact:** Prevents adding null columns, which is essential for schema evolution

### Detailed Fix Requirements

#### 1.1: Implement Native lit() Function

**File:** `mock_spark/functions.py`

**Current Implementation (Hypothetical):**
```python
def lit(value):
    # Likely calls PySpark's lit() which requires JVM
    from pyspark.sql.functions import lit as pyspark_lit
    return pyspark_lit(value)  # This fails for None
```

**Required Implementation:**
```python
def lit(value):
    """
    Create a Column with literal value.
    
    Args:
        value: Literal value (can be None, int, str, bool, etc.)
    
    Returns:
        Column object with literal value
    """
    if value is None:
        # Return a Column that represents NULL
        # Should work without JVM dependency
        return Column(Literal(None, NullType()))
    elif isinstance(value, bool):
        return Column(Literal(value, BooleanType()))
    elif isinstance(value, int):
        return Column(Literal(value, IntegerType()))
    elif isinstance(value, float):
        return Column(Literal(value, DoubleType()))
    elif isinstance(value, str):
        return Column(Literal(value, StringType()))
    else:
        # For other types, attempt to infer or use generic type
        return Column(Literal(value, StringType()))  # Fallback
```

#### 1.2: Implement NullType Support

**File:** `mock_spark/types.py` (or equivalent)

**Required:**
```python
class NullType(DataType):
    """Represents NULL type in mock-spark."""
    
    def __init__(self):
        super().__init__()
        self.typeName = "null"
    
    def __repr__(self):
        return "NullType()"
    
    def __eq__(self, other):
        return isinstance(other, NullType)
```

#### 1.3: Update Literal Class

**File:** `mock_spark/functions.py` or `mock_spark/column.py`

**Required:**
```python
class Literal:
    """Represents a literal value in an expression."""
    
    def __init__(self, value, data_type=None):
        self.value = value
        if data_type is None:
            # Infer type from value
            if value is None:
                self.data_type = NullType()
            elif isinstance(value, bool):
                self.data_type = BooleanType()
            elif isinstance(value, int):
                self.data_type = IntegerType()
            elif isinstance(value, float):
                self.data_type = DoubleType()
            elif isinstance(value, str):
                self.data_type = StringType()
            else:
                self.data_type = StringType()  # Default
        else:
            self.data_type = data_type
    
    def __repr__(self):
        return f"Literal({self.value}, {self.data_type})"
```

### Test Case

```python
def test_lit_none():
    """Test that F.lit(None) works without JVM."""
    from mock_spark import SparkSession, functions as F
    from mock_spark.types import StringType
    
    spark = SparkSession("test")
    df = spark.createDataFrame([(1, "test")], ["id", "name"])
    
    # This should work without errors
    result = df.withColumn("null_col", F.lit(None))
    assert "null_col" in result.columns
    
    # Should also work with casting
    result2 = df.withColumn("null_str", F.lit(None).cast(StringType()))
    assert "null_str" in result2.columns
```

### Expected Outcome

After fix:
- `F.lit(None)` should work without JVM dependency
- `F.lit(None).cast(dataType)` should work for all supported types
- Null columns can be added to DataFrames for schema evolution

---

## Issue #2: overwriteSchema Option Support

### Problem Description

Delta Lake's `overwriteSchema` option allows overwriting a table while preserving schema evolution (adding new columns, keeping existing ones). Mock-spark's `saveAsTable` doesn't properly handle this option, causing schema mismatches.

### Current Behavior

**PySpark (Working):**
```python
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("schema.table")
# Successfully overwrites table with new schema, preserving existing columns
```

**Mock-Spark (Failing):**
```python
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("schema.table")
# Option is ignored, table is overwritten with only new columns, existing columns lost
```

### Root Cause Analysis

1. **Location:** `mock_spark/writer.py` or `mock_spark/dataframe.py` - `saveAsTable` implementation
2. **Issue:** The `overwriteSchema` option is not parsed or handled in the write operation
3. **Impact:** Schema evolution fails, existing columns are lost during overwrite operations

### Detailed Fix Requirements

#### 2.1: Parse Write Options

**File:** `mock_spark/dataframe.py` or `mock_spark/writer.py`

**Current Implementation (Hypothetical):**
```python
class DataFrameWriter:
    def saveAsTable(self, name):
        # Only handles mode, ignores options
        mode = self._mode  # "overwrite" or "append"
        # Write logic doesn't check for overwriteSchema option
```

**Required Implementation:**
```python
class DataFrameWriter:
    def __init__(self, df):
        self._df = df
        self._mode = "error"  # default
        self._options = {}
        self._partition_by = []
    
    def mode(self, mode):
        """Set write mode."""
        self._mode = mode
        return self
    
    def option(self, key, value):
        """Set write option."""
        self._options[key] = value
        return self
    
    def saveAsTable(self, name):
        """
        Save DataFrame as table with support for Delta Lake options.
        
        Args:
            name: Fully qualified table name (schema.table)
        """
        # Parse schema and table name
        if "." in name:
            schema_name, table_name = name.split(".", 1)
        else:
            schema_name = "default"
            table_name = name
        
        # Check if table exists
        table_exists = self._check_table_exists(schema_name, table_name)
        
        # Handle overwriteSchema option
        overwrite_schema = self._options.get("overwriteSchema", "false").lower() == "true"
        merge_schema = self._options.get("mergeSchema", "false").lower() == "true"
        
        if self._mode == "overwrite":
            if table_exists and (overwrite_schema or merge_schema):
                # Read existing table schema
                existing_table = self._spark.table(name)
                existing_schema = existing_table.schema
                current_schema = self._df.schema
                
                # Merge schemas
                merged_df = self._merge_schemas(self._df, existing_schema, current_schema)
                
                # Write merged DataFrame
                self._write_table(merged_df, schema_name, table_name, mode="overwrite")
            else:
                # Normal overwrite
                self._write_table(self._df, schema_name, table_name, mode="overwrite")
        elif self._mode == "append":
            if table_exists and merge_schema:
                # Merge schemas for append mode
                existing_table = self._spark.table(name)
                existing_schema = existing_table.schema
                current_schema = self._df.schema
                
                merged_df = self._merge_schemas(self._df, existing_schema, current_schema)
                self._write_table(merged_df, schema_name, table_name, mode="append")
            else:
                # Normal append (schema must match)
                self._write_table(self._df, schema_name, table_name, mode="append")
        else:
            # error, ignore, etc.
            self._write_table(self._df, schema_name, table_name, mode=self._mode)
```

#### 2.2: Implement Schema Merging Logic

**File:** `mock_spark/writer.py` or `mock_spark/schema.py`

**Required:**
```python
def _merge_schemas(self, df, existing_schema, current_schema):
    """
    Merge existing table schema with current DataFrame schema.
    
    This implements Delta Lake's schema evolution:
    - Preserves all columns from existing schema
    - Adds new columns from current schema
    - Fills missing columns with null values
    
    Args:
        df: Current DataFrame to write
        existing_schema: Schema of existing table
        current_schema: Schema of current DataFrame
    
    Returns:
        DataFrame with merged schema
    """
    from mock_spark import functions as F
    
    existing_columns = {f.name: f for f in existing_schema.fields}
    current_columns = {f.name: f for f in current_schema.fields}
    
    # Find missing columns (in existing but not in current)
    missing_columns = set(existing_columns.keys()) - set(current_columns.keys())
    new_columns = set(current_columns.keys()) - set(existing_columns.keys())
    
    merged_df = df
    
    # Add missing columns with null values
    for col_name, field in existing_columns.items():
        if col_name not in merged_df.columns:
            # Add null column of the correct type
            merged_df = merged_df.withColumn(
                col_name,
                F.lit(None).cast(field.dataType)
            )
    
    # Select columns in order: existing first, then new
    all_columns = list(existing_columns.keys()) + sorted(new_columns)
    merged_df = merged_df.select(*all_columns)
    
    return merged_df
```

#### 2.3: Update Storage Backend

**File:** `mock_spark/storage.py` or equivalent

**Required Changes:**
- When `overwriteSchema=true`, read existing table before overwriting
- Merge schemas before writing
- Ensure all existing columns are preserved in the new table

### Test Case

```python
def test_overwrite_schema_option():
    """Test that overwriteSchema option preserves existing columns."""
    from mock_spark import SparkSession
    
    spark = SparkSession("test")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    # Create initial table
    df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    df1.write.mode("overwrite").saveAsTable("test_schema.users")
    
    # Verify initial schema
    table1 = spark.table("test_schema.users")
    assert set(table1.columns) == {"id", "name"}
    
    # Add new column with overwriteSchema
    df2 = spark.createDataFrame([(1, "Alice", 25)], ["id", "name", "age"])
    df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("test_schema.users")
    
    # Verify schema evolution: existing columns preserved, new column added
    table2 = spark.table("test_schema.users")
    assert "id" in table2.columns
    assert "name" in table2.columns
    assert "age" in table2.columns
    assert len(table2.columns) == 3
```

### Expected Outcome

After fix:
- `overwriteSchema=true` option is recognized and processed
- Existing table columns are preserved when overwriting
- New columns are added to the schema
- Schema merging works correctly for both overwrite and append modes

---

## Issue #3: Table Catalog Synchronization

### Problem Description

After calling `saveAsTable()`, there's a delay before the table becomes available in the catalog via `spark.table()`. This causes "Table or view not found" errors when trying to read the table immediately after writing.

### Current Behavior

**PySpark (Working):**
```python
df.write.mode("overwrite").saveAsTable("schema.table")
table = spark.table("schema.table")  # Works immediately
```

**Mock-Spark (Failing):**
```python
df.write.mode("overwrite").saveAsTable("schema.table")
table = spark.table("schema.table")  # Raises: Table or view not found
# Works after a delay or retry
```

### Root Cause Analysis

1. **Location:** `mock_spark/storage.py` - Table registration logic
2. **Issue:** Table is written to storage but not immediately registered in the catalog
3. **Impact:** Pipeline execution fails when trying to read tables immediately after writing

### Detailed Fix Requirements

#### 3.1: Synchronous Catalog Registration

**File:** `mock_spark/storage.py`

**Current Implementation (Hypothetical):**
```python
def save_table(self, schema, table, df, mode):
    # Write DataFrame to storage
    self._write_dataframe(schema, table, df, mode)
    # Catalog registration happens asynchronously or with delay
    # This causes the table to not be immediately available
```

**Required Implementation:**
```python
def save_table(self, schema, table, df, mode):
    """
    Save DataFrame as table with immediate catalog registration.
    
    Args:
        schema: Schema name
        table: Table name
        df: DataFrame to save
        mode: Write mode (overwrite, append, etc.)
    """
    # Ensure schema exists
    if not self.schema_exists(schema):
        self.create_schema(schema)
    
    # Write DataFrame to storage
    self._write_dataframe(schema, table, df, mode)
    
    # IMMEDIATELY register table in catalog
    # This must happen synchronously, not asynchronously
    self._register_table_in_catalog(schema, table, df.schema)
    
    # Verify table is immediately accessible
    assert self.table_exists(schema, table), \
        "Table should be immediately available after saveAsTable"
```

#### 3.2: Implement Catalog Registration

**File:** `mock_spark/catalog.py` or `mock_spark/storage.py`

**Required:**
```python
def _register_table_in_catalog(self, schema, table, schema_obj):
    """
    Register table in catalog immediately after writing.
    
    Args:
        schema: Schema name
        table: Table name
        schema_obj: DataFrame schema
    """
    # Get or create catalog entry
    if schema not in self._catalog:
        self._catalog[schema] = {}
    
    # Register table with schema
    self._catalog[schema][table] = {
        "schema": schema_obj,
        "path": self._get_table_path(schema, table),
        "created_at": datetime.now(),
    }
    
    # Ensure catalog is immediately queryable
    # This should be a synchronous operation
    self._flush_catalog()
```

#### 3.3: Update table() Method

**File:** `mock_spark/session.py`

**Required:**
```python
def table(self, table_name):
    """
    Get table as DataFrame with immediate availability.
    
    Args:
        table_name: Fully qualified table name (schema.table)
    
    Returns:
        DataFrame with table data
    """
    # Parse table name
    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema = self.storage.get_current_schema()
        table = table_name
    
    # Check catalog first (should be immediately available)
    if self.storage.table_exists(schema, table):
        # Read from storage using catalog metadata
        return self.storage.read_table(schema, table)
    else:
        # If not in catalog, try to discover from storage
        # This handles edge cases but shouldn't be the primary path
        if self.storage._table_exists_in_storage(schema, table):
            # Register it in catalog for future access
            df = self.storage._read_table_from_storage(schema, table)
            self.storage._register_table_in_catalog(schema, table, df.schema)
            return df
        else:
            raise AnalysisException(f"Table or view not found: {table_name}")
```

### Test Case

```python
def test_immediate_table_access():
    """Test that table is immediately accessible after saveAsTable."""
    from mock_spark import SparkSession
    
    spark = SparkSession("test")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    df = spark.createDataFrame([(1, "test")], ["id", "name"])
    
    # Write table
    df.write.mode("overwrite").saveAsTable("test_schema.test_table")
    
    # Should be immediately accessible (no delay, no retry needed)
    table = spark.table("test_schema.test_table")
    assert table is not None
    assert table.count() == 1
    assert "id" in table.columns
    assert "name" in table.columns
```

### Expected Outcome

After fix:
- Tables are immediately available in catalog after `saveAsTable()`
- No retry logic needed when reading tables after writing
- Catalog registration is synchronous and atomic with write operation

---

## Issue #4: Schema Merging in Overwrite Mode

### Problem Description

When overwriting a table with `mode("overwrite")`, mock-spark should merge schemas to preserve existing columns while adding new ones. Currently, it only writes the new schema, losing existing columns.

### Current Behavior

**PySpark with Delta Lake (Working):**
```python
# Initial table: [id, name]
df1.write.mode("overwrite").saveAsTable("schema.table")

# Overwrite with new columns: [id, name, age, city]
# With overwriteSchema=true: Result is [id, name, age, city] (all columns preserved)
df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("schema.table")
```

**Mock-Spark (Failing):**
```python
# Initial table: [id, name]
df1.write.mode("overwrite").saveAsTable("schema.table")

# Overwrite with new columns: [id, age]
# Result: [id, age] (name column is lost)
df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("schema.table")
```

### Root Cause Analysis

1. **Location:** `mock_spark/writer.py` or `mock_spark/storage.py`
2. **Issue:** Overwrite mode doesn't check for existing schema before writing
3. **Impact:** Schema evolution fails, data loss occurs when columns are filtered by validation

### Detailed Fix Requirements

#### 4.1: Implement Schema-Aware Overwrite

**File:** `mock_spark/writer.py`

**Required:**
```python
def _write_table(self, df, schema, table, mode):
    """
    Write DataFrame to table with schema-aware overwrite.
    
    Args:
        df: DataFrame to write
        schema: Schema name
        table: Table name
        mode: Write mode
    """
    table_exists = self._table_exists(schema, table)
    
    if mode == "overwrite":
        if table_exists:
            # Read existing schema
            existing_df = self._spark.table(f"{schema}.{table}")
            existing_schema = existing_df.schema
            
            # Check if overwriteSchema option is set
            if self._options.get("overwriteSchema", "false").lower() == "true":
                # Merge schemas before overwriting
                df = self._merge_schemas_for_overwrite(df, existing_schema)
            
            # Perform overwrite
            self._storage.overwrite_table(schema, table, df)
        else:
            # Table doesn't exist, create new
            self._storage.create_table(schema, table, df)
    else:
        # Other modes (append, error, etc.)
        self._storage.write_table(schema, table, df, mode)
```

#### 4.2: Schema Merging Algorithm

**File:** `mock_spark/schema.py`

**Required:**
```python
def merge_schemas(existing_schema, new_schema, fill_missing_with_null=True):
    """
    Merge two schemas, preserving all columns.
    
    Algorithm:
    1. Identify columns in existing but not in new (missing columns)
    2. Identify columns in new but not in existing (new columns)
    3. For missing columns: add with null values if fill_missing_with_null=True
    4. Return merged schema with: existing columns + new columns
    
    Args:
        existing_schema: Schema of existing table
        new_schema: Schema of new DataFrame
        fill_missing_with_null: If True, add missing columns with null values
    
    Returns:
        Merged schema
    """
    existing_fields = {f.name: f for f in existing_schema.fields}
    new_fields = {f.name: f for f in new_schema.fields}
    
    # All columns: existing first, then new (in sorted order)
    all_field_names = list(existing_fields.keys()) + sorted(
        set(new_fields.keys()) - set(existing_fields.keys())
    )
    
    # Build merged schema
    merged_fields = []
    for field_name in all_field_names:
        if field_name in existing_fields:
            # Use existing field definition (preserves type, nullable, etc.)
            merged_fields.append(existing_fields[field_name])
        else:
            # New field
            merged_fields.append(new_fields[field_name])
    
    return StructType(merged_fields)
```

### Test Case

```python
def test_schema_merge_on_overwrite():
    """Test that overwrite preserves existing columns when overwriteSchema=true."""
    from mock_spark import SparkSession
    
    spark = SparkSession("test")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    # Initial table with columns: [id, name, value]
    df1 = spark.createDataFrame(
        [(1, "Alice", 100), (2, "Bob", 200)],
        ["id", "name", "value"]
    )
    df1.write.mode("overwrite").saveAsTable("test_schema.users")
    
    # Overwrite with new columns but missing some existing
    # New DataFrame has: [id, age, city] (missing name and value)
    df2 = spark.createDataFrame(
        [(1, 25, "NYC"), (2, 30, "LA")],
        ["id", "age", "city"]
    )
    
    # With overwriteSchema=true, should preserve name and value (with nulls)
    df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("test_schema.users")
    
    # Verify all columns exist
    result = spark.table("test_schema.users")
    assert "id" in result.columns
    assert "name" in result.columns  # Preserved from original
    assert "value" in result.columns  # Preserved from original
    assert "age" in result.columns  # New column
    assert "city" in result.columns  # New column
    
    # Verify null values for missing columns
    rows = result.collect()
    assert rows[0]["name"] is None  # Should be null (wasn't in new DataFrame)
    assert rows[0]["value"] is None  # Should be null
```

### Expected Outcome

After fix:
- Overwrite mode with `overwriteSchema=true` preserves all existing columns
- Missing columns are filled with null values
- New columns are added to the schema
- Schema evolution works as expected

---

## Issue #5: Type Casting Without JVM

### Problem Description

Type casting operations like `.cast(StringType())` fail when they attempt to access PySpark's JVM-based type system, which doesn't exist in mock-spark.

### Current Behavior

**PySpark (Working):**
```python
from pyspark.sql.types import StringType
df = df.withColumn("col", F.lit(None).cast(StringType()))
# Works correctly
```

**Mock-Spark (Failing):**
```python
from mock_spark.types import StringType
df = df.withColumn("col", F.lit(None).cast(StringType()))
# May fail if cast() method tries to access JVM
```

### Root Cause Analysis

1. **Location:** `mock_spark/column.py` - `cast()` method
2. **Issue:** Cast operation may call PySpark internals that require JVM
3. **Impact:** Prevents type-safe schema evolution

### Detailed Fix Requirements

#### 5.1: Implement Native Cast Operation

**File:** `mock_spark/column.py`

**Required:**
```python
class Column:
    def cast(self, dataType):
        """
        Cast column to a different data type.
        
        Args:
            dataType: Target data type (StringType, IntegerType, etc.)
        
        Returns:
            New Column with cast operation
        """
        # Create a Cast expression without JVM dependency
        return Column(CastExpression(self._expr, dataType))
```

#### 5.2: Implement Cast Expression

**File:** `mock_spark/expressions.py` or `mock_spark/column.py`

**Required:**
```python
class CastExpression:
    """Represents a type cast operation."""
    
    def __init__(self, expression, target_type):
        self.expression = expression
        self.target_type = target_type
    
    def evaluate(self, row):
        """Evaluate cast operation on a row."""
        value = self.expression.evaluate(row)
        
        if value is None:
            return None
        
        # Perform type conversion
        if isinstance(self.target_type, StringType):
            return str(value) if value is not None else None
        elif isinstance(self.target_type, IntegerType):
            return int(value) if value is not None else None
        elif isinstance(self.target_type, DoubleType):
            return float(value) if value is not None else None
        elif isinstance(self.target_type, BooleanType):
            return bool(value) if value is not None else None
        elif isinstance(self.target_type, NullType):
            return None
        else:
            # Default: convert to string
            return str(value) if value is not None else None
```

### Test Case

```python
def test_type_casting():
    """Test that type casting works without JVM."""
    from mock_spark import SparkSession, functions as F
    from mock_spark.types import StringType, IntegerType, DoubleType
    
    spark = SparkSession("test")
    df = spark.createDataFrame([(1, "test")], ["id", "name"])
    
    # Test casting None to different types
    df1 = df.withColumn("null_str", F.lit(None).cast(StringType()))
    df2 = df.withColumn("null_int", F.lit(None).cast(IntegerType()))
    df3 = df.withColumn("null_double", F.lit(None).cast(DoubleType()))
    
    assert "null_str" in df1.columns
    assert "null_int" in df2.columns
    assert "null_double" in df3.columns
    
    # Test casting actual values
    df4 = df.withColumn("id_str", F.col("id").cast(StringType()))
    assert df4.select("id_str").collect()[0]["id_str"] == "1"
```

### Expected Outcome

After fix:
- `.cast(dataType)` works without JVM dependency
- All standard types (String, Integer, Double, Boolean, Null) are supported
- Type conversions work correctly for both null and non-null values

---

## Test Cases for Validation

### Comprehensive Test Suite

Create a test file `tests/test_delta_lake_schema_evolution.py`:

```python
"""
Comprehensive tests for Delta Lake schema evolution in mock-spark.

These tests validate that mock-spark behaves like PySpark for schema evolution.
"""

import pytest
from mock_spark import SparkSession, functions as F
from mock_spark.types import StringType, IntegerType


class TestDeltaLakeSchemaEvolution:
    """Test Delta Lake schema evolution features."""
    
    def test_basic_schema_evolution(self):
        """Test basic schema evolution: add new columns."""
        spark = SparkSession("test")
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        
        # Initial table
        df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        df1.write.mode("overwrite").saveAsTable("test_schema.users")
        
        # Add new column
        df2 = spark.createDataFrame([(1, "Alice", 25)], ["id", "name", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("test_schema.users")
        
        result = spark.table("test_schema.users")
        assert set(result.columns) == {"id", "name", "age"}
    
    def test_preserve_existing_columns(self):
        """Test that existing columns are preserved when adding new ones."""
        spark = SparkSession("test")
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        
        # Initial: [id, name, value]
        df1 = spark.createDataFrame(
            [(1, "Alice", 100)],
            ["id", "name", "value"]
        )
        df1.write.mode("overwrite").saveAsTable("test_schema.data")
        
        # Overwrite with: [id, age] (missing name and value)
        df2 = spark.createDataFrame([(1, 25)], ["id", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("test_schema.data")
        
        result = spark.table("test_schema.data")
        assert "id" in result.columns
        assert "name" in result.columns  # Should be preserved
        assert "value" in result.columns  # Should be preserved
        assert "age" in result.columns  # New column
    
    def test_immediate_table_access(self):
        """Test that table is immediately accessible after saveAsTable."""
        spark = SparkSession("test")
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        
        df = spark.createDataFrame([(1, "test")], ["id", "name"])
        df.write.mode("overwrite").saveAsTable("test_schema.immediate")
        
        # Should work immediately, no retry needed
        table = spark.table("test_schema.immediate")
        assert table.count() == 1
    
    def test_lit_none_works(self):
        """Test that F.lit(None) works without JVM."""
        spark = SparkSession("test")
        df = spark.createDataFrame([(1,)], ["id"])
        
        # Should not raise JVM error
        result = df.withColumn("null_col", F.lit(None))
        assert "null_col" in result.columns
    
    def test_type_casting_works(self):
        """Test that type casting works without JVM."""
        from mock_spark.types import StringType, IntegerType
        
        spark = SparkSession("test")
        df = spark.createDataFrame([(1,)], ["id"])
        
        # Should work without JVM
        result = df.withColumn("id_str", F.col("id").cast(StringType()))
        assert result.select("id_str").collect()[0]["id_str"] == "1"
```

---

## Implementation Priority

### Priority 1: Critical (Blocks Schema Evolution)

1. **Issue #1: F.lit(None) JVM Dependency**
   - **Impact:** High - Prevents adding null columns
   - **Effort:** Medium
   - **Dependencies:** None
   - **Estimated Time:** 2-3 days

2. **Issue #3: Table Catalog Synchronization**
   - **Impact:** High - Breaks pipeline execution flow
   - **Effort:** Low-Medium
   - **Dependencies:** None
   - **Estimated Time:** 1-2 days

### Priority 2: High (Required for Full Compatibility)

3. **Issue #2: overwriteSchema Option Support**
   - **Impact:** High - Core Delta Lake feature
   - **Effort:** Medium-High
   - **Dependencies:** Issue #1, Issue #5
   - **Estimated Time:** 3-4 days

4. **Issue #5: Type Casting Without JVM**
   - **Impact:** Medium-High - Required for type-safe operations
   - **Effort:** Medium
   - **Dependencies:** None
   - **Estimated Time:** 2-3 days

### Priority 3: Medium (Enhancement)

5. **Issue #4: Schema Merging in Overwrite Mode**
   - **Impact:** Medium - Improves user experience
   - **Effort:** Medium
   - **Dependencies:** Issue #1, Issue #2, Issue #5
   - **Estimated Time:** 2-3 days

**Total Estimated Time:** 10-15 days of development work

---

## References and Examples

### PySpark Delta Lake Documentation

- [Delta Lake Schema Evolution](https://docs.delta.io/latest/delta-update.html#schema-evolution)
- [Delta Lake Overwrite Schema](https://docs.delta.io/latest/delta-batch.html#overwrite-schema)

### Mock-Spark Code Locations (Hypothetical)

Based on typical mock library structure:

```
mock_spark/
├── functions.py          # F.lit(), F.col(), etc.
├── column.py             # Column class, cast() method
├── types.py              # DataType classes
├── writer.py             # DataFrameWriter, saveAsTable()
├── storage.py            # Table storage and catalog
├── catalog.py             # Catalog management
└── session.py             # SparkSession, table() method
```

### Example Working PySpark Code

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, IntegerType

spark = SparkSession.builder.appName("test").getOrCreate()
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

# Initial table
df1 = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "value"])
df1.write.mode("overwrite").saveAsTable("test_schema.users")

# Schema evolution: add new columns, preserve existing
df2 = spark.createDataFrame(
    [(1, "Alice", 100, 25, "NYC")],
    ["id", "name", "value", "age", "city"]
)
df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("test_schema.users")

# Table is immediately accessible
table = spark.table("test_schema.users")
assert set(table.columns) == {"id", "name", "value", "age", "city"}
```

### Expected Mock-Spark Behavior

After all fixes, the same code should work identically:

```python
from mock_spark import SparkSession, functions as F
from mock_spark.types import StringType, IntegerType

spark = SparkSession("test")
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

# Same code, should work identically
df1 = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "value"])
df1.write.mode("overwrite").saveAsTable("test_schema.users")

df2 = spark.createDataFrame(
    [(1, "Alice", 100, 25, "NYC")],
    ["id", "name", "value", "age", "city"]
)
df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("test_schema.users")

# Should work immediately, no errors
table = spark.table("test_schema.users")
assert set(table.columns) == {"id", "name", "value", "age", "city"}
```

---

## Additional Considerations

### Performance

- Catalog registration should be fast (synchronous, in-memory operation)
- Schema merging should not significantly impact write performance
- Consider caching schema information to avoid repeated reads

### Backward Compatibility

- All fixes should maintain backward compatibility with existing mock-spark code
- New options should be optional (default behavior unchanged)
- Existing tests should continue to pass

### Testing Strategy

1. **Unit Tests:** Test each component in isolation
2. **Integration Tests:** Test full workflow (write → read → schema evolution)
3. **Compatibility Tests:** Compare behavior with PySpark side-by-side
4. **Regression Tests:** Ensure existing functionality still works

### Documentation Updates

After implementing fixes, update:
- Mock-spark README with Delta Lake support notes
- API documentation for new options and behaviors
- Migration guide for users upgrading

---

## Conclusion

These improvements will make mock-spark a true drop-in replacement for PySpark in Delta Lake-based data pipelines. The fixes are focused, well-defined, and can be implemented incrementally. Priority should be given to Issues #1 and #3 as they block basic functionality, followed by Issues #2, #5, and #4 for full compatibility.

**Contact:** For questions or clarifications about this improvement plan, please refer to the test cases in the PipelineBuilder repository or open an issue with the mock-spark project.

---

**Document End**

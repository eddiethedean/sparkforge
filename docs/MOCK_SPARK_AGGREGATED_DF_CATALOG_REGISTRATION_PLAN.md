# Mock-Spark Aggregated DataFrame Catalog Registration Bug

**Document Version:** 1.0  
**Date:** December 2024  
**Target Audience:** Mock-Spark Developers  
**Priority:** High  
**Issue:** Aggregated DataFrames (from `groupBy().agg()`) are not registered in the catalog after `saveAsTable()`

---

## Executive Summary

Mock-spark has a critical bug where DataFrames created through aggregation operations (`groupBy().agg()`) are not registered in the catalog after calling `saveAsTable()`, even though the table data is written successfully. This breaks compatibility with PySpark, where aggregated tables are immediately queryable after writing.

**Impact:** This prevents mock-spark from being a drop-in replacement for PySpark in data pipelines that use aggregations (common in gold/analytics layers), requiring workarounds and retry logic in consuming applications.

---

## Problem Description

### Current Behavior

**PySpark (Expected Behavior):**
```python
source_df = spark.createDataFrame([('user1', 100), ('user2', 200)], ['user_id', 'value'])
agg_df = source_df.groupBy('user_id').agg(F.count('*').alias('count'))
agg_df.write.mode('overwrite').saveAsTable('schema.aggregated_table')
table = spark.table('schema.aggregated_table')  # ✅ Works immediately
```

**Mock-Spark (Current Broken Behavior):**
```python
source_df = spark.createDataFrame([('user1', 100), ('user2', 200)], ['user_id', 'value'])
agg_df = source_df.groupBy('user_id').agg(F.count('*').alias('count'))
agg_df.write.mode('overwrite').saveAsTable('schema.aggregated_table')
table = spark.table('schema.aggregated_table')  # ❌ Raises: Table or view not found
# Table data is written, but catalog registration fails
```

### Affected Operations

All aggregation operations fail to register in catalog:

- `groupBy().agg(F.count('*'))`
- `groupBy().agg(F.sum('column'))`
- `groupBy().agg(F.max('column'))`
- `groupBy().agg(F.avg('column'))`
- `groupBy().agg(F.min('column'))`
- Any combination of aggregation functions

### Working Operations

These operations correctly register in catalog:

- Simple DataFrames (no transformations)
- DataFrames with `withColumn()`
- DataFrames with `select()`
- DataFrames with `filter()`

---

## Root Cause Analysis

### Technical Root Cause

The issue appears to be in mock-spark's catalog registration logic when handling DataFrames that result from aggregation operations. The table data is successfully written to storage, but the catalog registration step fails or is skipped for aggregated DataFrames.

**Hypothesis:**

1. Aggregated DataFrames may have a different internal representation or schema structure
2. The catalog registration code may not properly handle the schema of aggregated DataFrames
3. There may be a bug in how mock-spark extracts schema information from aggregated DataFrames for catalog registration

### Code Flow Analysis

**Expected Flow (PySpark):**
```
groupBy().agg() → Aggregated DataFrame
  ↓
saveAsTable() called
  ↓
Write DataFrame to storage
  ↓
Extract schema from aggregated DataFrame
  ↓
Register table in catalog with schema (synchronous, atomic)
  ↓
Return (table is now immediately queryable)
```

**Current Flow (Mock-Spark - Broken):**
```
groupBy().agg() → Aggregated DataFrame
  ↓
saveAsTable() called
  ↓
Write DataFrame to storage ✅ (succeeds)
  ↓
Extract schema from aggregated DataFrame ❌ (fails or returns incorrect schema)
  ↓
Register table in catalog ❌ (fails silently or skipped)
  ↓
Return (table written but not in catalog)
```

### Where the Fix Needs to Happen

Based on typical mock-spark architecture, the fix needs to be in:

1. **`mock_spark/dataframe.py`** or **`mock_spark/writer.py`**: The `DataFrameWriter.saveAsTable()` method - schema extraction for aggregated DataFrames
2. **`mock_spark/catalog.py`**: The catalog registration logic - handling aggregated DataFrame schemas
3. **`mock_spark/backend/polars/storage.py`**: The storage layer - ensuring aggregated DataFrame schemas are correctly extracted before catalog registration

---

## Detailed Implementation Requirements

### Requirement 1: Fix Schema Extraction for Aggregated DataFrames

**File:** `mock_spark/writer.py` or `mock_spark/dataframe.py`

**Current Implementation (Hypothetical - needs verification):**
```python
class DataFrameWriter:
    def saveAsTable(self, name):
        """Save DataFrame as table."""
        # Write to storage
        self._storage.write_table(name, self._df, self._mode)
        
        # Extract schema - THIS MAY FAIL FOR AGGREGATED DATAFRAMES
        schema = self._df.schema  # May return incorrect schema for aggregated DFs
        
        # Register in catalog
        self._spark.catalog._register_table(schema, table, schema, path)
```

**Required Implementation:**
```python
class DataFrameWriter:
    def saveAsTable(self, name):
        """
        Save DataFrame as table with immediate catalog registration.
        
        This method must correctly handle aggregated DataFrames and ensure
        their schemas are properly extracted and registered in the catalog.
        
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
        
        # CRITICAL: Extract schema BEFORE writing (for aggregated DataFrames)
        # Aggregated DataFrames may need special handling to extract correct schema
        df_schema = self._extract_schema_for_catalog(self._df)
        
        # Write DataFrame to storage (synchronous)
        table_path = self._storage.write_table(schema_name, table_name, self._df, self._mode)
        
        # CRITICAL: Register table in catalog IMMEDIATELY with correct schema
        # This must work for aggregated DataFrames
        self._spark.catalog._register_table(
            schema_name, 
            table_name, 
            df_schema,  # Use extracted schema, not self._df.schema directly
            table_path
        )
        
        # Verify table is immediately accessible
        if not self._spark.catalog.tableExists(f"{schema_name}.{table_name}"):
            raise RuntimeError(
                f"Catalog registration failed: table '{schema_name}.{table_name}' "
                f"not found in catalog immediately after saveAsTable()"
            )
    
    def _extract_schema_for_catalog(self, df):
        """
        Extract schema from DataFrame for catalog registration.
        
        This method handles special cases like aggregated DataFrames that may
        have different internal schema representations.
        
        Args:
            df: DataFrame to extract schema from
        
        Returns:
            StructType schema suitable for catalog registration
        """
        # Try standard schema extraction first
        try:
            schema = df.schema
            # Verify schema is valid (has fields)
            if schema and hasattr(schema, 'fields') and len(schema.fields) > 0:
                return schema
        except Exception as e:
            # If standard extraction fails, try alternative methods
            pass
        
        # For aggregated DataFrames, try to get schema from the underlying data
        # This may require accessing the internal representation differently
        try:
            # Attempt to infer schema from a sample or from the DataFrame's internal structure
            # The exact implementation depends on mock-spark's internal architecture
            if hasattr(df, '_internal_df') or hasattr(df, '_df'):
                # Access underlying Polars DataFrame or internal representation
                internal_df = getattr(df, '_internal_df', None) or getattr(df, '_df', None)
                if internal_df is not None:
                    # Extract schema from internal representation
                    schema = self._extract_schema_from_internal(internal_df)
                    if schema:
                        return schema
        except Exception:
            pass
        
        # If all else fails, try to infer schema from a sample
        try:
            sample = df.limit(1).collect()
            if sample:
                # Infer schema from sample data
                # This is a fallback method
                return self._infer_schema_from_sample(sample, df.columns)
        except Exception:
            pass
        
        # Last resort: raise error with helpful message
        raise RuntimeError(
            f"Could not extract schema from DataFrame for catalog registration. "
            f"DataFrame type: {type(df)}, columns: {df.columns if hasattr(df, 'columns') else 'unknown'}"
        )
```

### Requirement 2: Ensure Catalog Registration Handles All Schema Types

**File:** `mock_spark/catalog.py`

**Required:**
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
        
        This method must correctly handle schemas from aggregated DataFrames,
        ensuring all field types and nullable flags are properly preserved.
        
        Args:
            schema: Schema/database name
            table: Table name
            schema_obj: DataFrame schema (StructType) - may be from aggregated DataFrame
            table_path: Path where table data is stored
        """
        # Ensure schema exists in catalog
        if schema not in self._catalog:
            self._catalog[schema] = {}
        
        # Validate schema_obj is a valid StructType
        if not isinstance(schema_obj, StructType):
            raise TypeError(
                f"Expected StructType for table schema, got {type(schema_obj)}. "
                f"This may indicate a bug in schema extraction for aggregated DataFrames."
            )
        
        # Validate schema has fields
        if not hasattr(schema_obj, 'fields') or len(schema_obj.fields) == 0:
            raise ValueError(
                f"Schema for table '{schema}.{table}' has no fields. "
                f"This may indicate a bug in schema extraction for aggregated DataFrames."
            )
        
        # Register table with full metadata
        self._catalog[schema][table] = {
            "schema": schema_obj,
            "path": table_path,
            "mode": "managed",
            "created_at": datetime.now(),
            "last_modified": datetime.now(),
        }
        
        # Flush catalog to ensure it's immediately queryable
        self._flush_catalog_cache()
        
        # Verify registration succeeded
        assert schema in self._catalog, f"Schema '{schema}' not in catalog"
        assert table in self._catalog[schema], f"Table '{table}' not in catalog"
        
        # Additional validation: verify schema fields match what was registered
        registered_schema = self._catalog[schema][table]["schema"]
        assert len(registered_schema.fields) == len(schema_obj.fields), \
            f"Schema field count mismatch: registered {len(registered_schema.fields)}, expected {len(schema_obj.fields)}"
```

### Requirement 3: Test Aggregated DataFrame Schema Extraction

**File:** `mock_spark/tests/` (new test file or existing test file)

**Required Test:**
```python
def test_aggregated_dataframe_catalog_registration():
    """Test that aggregated DataFrames are correctly registered in catalog."""
    from mock_spark import SparkSession
    from mock_spark.sql import functions as F
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    # Create source data
    source_df = spark.createDataFrame(
        [('user1', 100), ('user2', 200), ('user1', 150)],
        ['user_id', 'value']
    )
    
    # Test various aggregation functions
    aggregation_tests = [
        ('count', source_df.groupBy('user_id').agg(F.count('*').alias('count'))),
        ('sum', source_df.groupBy('user_id').agg(F.sum('value').alias('total'))),
        ('max', source_df.groupBy('user_id').agg(F.max('value').alias('max_val'))),
        ('min', source_df.groupBy('user_id').agg(F.min('value').alias('min_val'))),
        ('avg', source_df.groupBy('user_id').agg(F.avg('value').alias('avg_val'))),
        ('multiple', source_df.groupBy('user_id').agg(
            F.count('*').alias('count'),
            F.sum('value').alias('total'),
            F.avg('value').alias('avg')
        )),
    ]
    
    for test_name, agg_df in aggregation_tests:
        table_name = f"test_schema.agg_{test_name}"
        
        # Write aggregated DataFrame
        agg_df.write.mode('overwrite').saveAsTable(table_name)
        
        # CRITICAL: Should be immediately accessible (no delay, no retry)
        table = spark.table(table_name)
        
        # Verify table is accessible and has correct data
        assert table is not None, f"Table {table_name} is None"
        assert table.count() > 0, f"Table {table_name} has no rows"
        
        # Verify schema matches
        expected_columns = set(agg_df.columns)
        actual_columns = set(table.columns)
        assert expected_columns == actual_columns, \
            f"Column mismatch for {test_name}: expected {expected_columns}, got {actual_columns}"
        
        # Verify data integrity
        agg_data = sorted(agg_df.collect(), key=lambda x: x['user_id'])
        table_data = sorted(table.collect(), key=lambda x: x['user_id'])
        assert len(agg_data) == len(table_data), \
            f"Row count mismatch for {test_name}"
```

---

## Test Cases

### Test Case 1: Basic Aggregation

```python
def test_basic_groupby_agg_catalog_registration():
    """Test basic groupBy().agg() catalog registration."""
    from mock_spark import SparkSession
    from mock_spark.sql import functions as F
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    source = spark.createDataFrame([('user1', 100), ('user2', 200)], ['user_id', 'value'])
    agg_df = source.groupBy('user_id').agg(F.count('*').alias('count'))
    
    agg_df.write.mode('overwrite').saveAsTable('test_schema.basic_agg')
    
    # Must work immediately
    table = spark.table('test_schema.basic_agg')
    assert table.count() == 2
    assert 'user_id' in table.columns
    assert 'count' in table.columns
```

### Test Case 2: Multiple Aggregations

```python
def test_multiple_agg_functions():
    """Test DataFrame with multiple aggregation functions."""
    from mock_spark import SparkSession
    from mock_spark.sql import functions as F
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    source = spark.createDataFrame(
        [('user1', 100), ('user2', 200), ('user1', 150)],
        ['user_id', 'value']
    )
    
    agg_df = source.groupBy('user_id').agg(
        F.count('*').alias('count'),
        F.sum('value').alias('total'),
        F.avg('value').alias('average'),
        F.max('value').alias('maximum')
    )
    
    agg_df.write.mode('overwrite').saveAsTable('test_schema.multi_agg')
    
    # Must work immediately
    table = spark.table('test_schema.multi_agg')
    assert table.count() == 2
    assert set(table.columns) == {'user_id', 'count', 'total', 'average', 'maximum'}
```

### Test Case 3: Nested Aggregations

```python
def test_nested_aggregations():
    """Test complex nested aggregation operations."""
    from mock_spark import SparkSession
    from mock_spark.sql import functions as F
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    source = spark.createDataFrame(
        [('user1', 'A', 100), ('user1', 'B', 200), ('user2', 'A', 150)],
        ['user_id', 'category', 'value']
    )
    
    # First aggregation
    first_agg = source.groupBy('user_id', 'category').agg(F.sum('value').alias('category_total'))
    
    # Second aggregation on first result
    second_agg = first_agg.groupBy('user_id').agg(F.sum('category_total').alias('user_total'))
    
    second_agg.write.mode('overwrite').saveAsTable('test_schema.nested_agg')
    
    # Must work immediately
    table = spark.table('test_schema.nested_agg')
    assert table.count() == 2
    assert 'user_id' in table.columns
    assert 'user_total' in table.columns
```

---

## Expected Behavior After Fix

### Success Criteria

1. **Immediate Availability**: Aggregated DataFrames written via `saveAsTable()` are immediately accessible via `spark.table()` with zero delay
2. **All Aggregation Functions**: Works for `count`, `sum`, `max`, `min`, `avg`, and any combination
3. **Schema Preservation**: Aggregated DataFrame schemas are correctly preserved in catalog
4. **No Retry Logic Needed**: Applications should not need retry logic or delays when reading aggregated tables after writing
5. **Consistent with Simple DataFrames**: Aggregated DataFrames behave identically to simple DataFrames for catalog registration

### Performance Considerations

- Schema extraction should be fast (ideally O(1) or O(n) where n is number of columns)
- No significant performance penalty compared to simple DataFrames
- Catalog registration should be synchronous and atomic, just like for simple DataFrames

---

## References

### Related Code in PipelineBuilder

- **Gold Step Execution**: [src/pipeline_builder/execution.py:1429-1446](src/pipeline_builder/execution.py:1429-1446)
- **Table Registration**: [src/pipeline_builder/execution.py:1136-1169](src/pipeline_builder/execution.py:1136-1169)
- **Test Case**: [tests/system/test_schema_evolution_without_override.py:400-476](tests/system/test_schema_evolution_without_override.py:400-476)

### PySpark Reference Behavior

In PySpark, aggregated DataFrames are handled identically to simple DataFrames:

- Schema extraction works the same way
- Catalog registration works the same way
- Tables are immediately queryable after `saveAsTable()`

---

## Implementation Priority

**Priority: HIGH**

This issue blocks full PySpark compatibility for any pipeline that uses aggregations (which is common in analytics/gold layers). It should be addressed in the next mock-spark release.

---

## Questions for Mock-Spark Developers

1. How does mock-spark internally represent aggregated DataFrames? (Polars, custom structure, etc.)
2. Is there a difference in how schemas are stored/accessed for aggregated vs. simple DataFrames?
3. Are there any known issues with schema extraction for transformed DataFrames?
4. Is the catalog registration code path different for aggregated DataFrames, or is it the same code that's failing?

---

## Conclusion

Fixing the aggregated DataFrame catalog registration bug is critical for mock-spark to be a true drop-in replacement for PySpark in analytics workloads. The fix requires ensuring that schema extraction and catalog registration work correctly for aggregated DataFrames, matching the behavior of simple DataFrames.

This improvement will eliminate the need for workarounds in consuming applications and enable data pipelines that rely on aggregations to work seamlessly with mock-spark.


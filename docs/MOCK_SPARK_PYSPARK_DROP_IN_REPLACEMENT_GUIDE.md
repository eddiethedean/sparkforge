# Mock-Spark PySpark Drop-In Replacement Guide

**For Mock-Spark Developers**

This document provides detailed specifications for making mock-spark a true drop-in replacement for PySpark in testing scenarios. All behaviors described here are based on real-world test failures where mock-spark passed tests that failed with actual PySpark, indicating gaps in mock-spark's PySpark compatibility.

## Table of Contents

1. [Core Principles](#core-principles)
2. [String Concatenation Behavior](#string-concatenation-behavior)
3. [DataFrame Materialization and Column Availability](#dataframe-materialization-and-column-availability)
4. [Schema Creation and Management](#schema-creation-and-management)
5. [Type System Compatibility](#type-system-compatibility)
6. [Function API Compatibility](#function-api-compatibility)
7. [Error Handling and Exception Types](#error-handling-and-exception-types)
8. [Performance Characteristics](#performance-characteristics)
9. [Expression Parsing and Evaluation](#expression-parsing-and-evaluation)
10. [Empty DataFrame Handling](#empty-dataframe-handling)
11. [Union Operation Strictness](#union-operation-strictness)
12. [SparkContext and JVM Requirements](#sparkcontext-and-jvm-requirements)
13. [Catalog and Storage API](#catalog-and-storage-api)
14. [Testing Recommendations](#testing-recommendations)

---

## Core Principles

### Principle 1: Fail Fast, Fail Loud
Mock-spark should fail in the same way PySpark fails. If PySpark raises an exception for invalid input, mock-spark should raise the same exception (or a compatible one), not silently accept it.

### Principle 2: Type and Schema Strictness
Mock-spark should enforce the same type and schema strictness as PySpark. This includes:
- Requiring explicit schemas for empty DataFrames
- Enforcing schema compatibility in union operations
- Validating column types match expected types

### Principle 3: Lazy Evaluation Semantics
Mock-spark should respect PySpark's lazy evaluation model. Columns created with `withColumn()` should not be immediately available until the DataFrame is materialized (via `collect()`, `count()`, `cache()`, etc.).

### Principle 4: API Parity
Mock-spark should provide the same APIs as PySpark, or clearly document differences. When APIs differ, mock-spark should provide compatibility shims.

---

## String Concatenation Behavior

### Issue
PySpark's string concatenation using the `+` operator fails when DataFrames are cached or materialized, producing `null` values. Mock-spark currently handles this correctly, but the behavior should match PySpark's failure mode.

### Current Behavior (Incorrect)
```python
from mock_spark import SparkSession, functions as F

spark = SparkSession()
df = spark.createDataFrame([("John", "Doe")], ["first_name", "last_name"])

# This works in mock-spark
df2 = df.withColumn("full_name", F.col("first_name") + F.lit(" ") + F.col("last_name"))
df2_cached = df2.cache()
result = df2_cached.collect()[0]
# result["full_name"] = "John Doe"  # ✅ Works in mock-spark
```

### PySpark Behavior (Expected)
```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.createDataFrame([("John", "Doe")], ["first_name", "last_name"])

# This FAILS in PySpark when cached
df2 = df.withColumn("full_name", F.col("first_name") + F.lit(" ") + F.col("last_name"))
df2_cached = df2.cache()
_ = df2_cached.count()  # Force materialization
result = df2_cached.collect()[0]
# result["full_name"] = None  # ❌ Returns None/null in PySpark
```

### Correct PySpark Approach
```python
# PySpark requires F.concat() for reliable string concatenation
df2 = df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
df2_cached = df2.cache()
_ = df2_cached.count()
result = df2_cached.collect()[0]
# result["full_name"] = "John Doe"  # ✅ Works correctly
```

### Required Mock-Spark Behavior

**Option A (Recommended)**: Match PySpark's failure mode
```python
# Mock-spark should produce None/null when using + operator with cached DataFrames
df2 = df.withColumn("full_name", F.col("first_name") + F.lit(" ") + F.col("last_name"))
df2_cached = df2.cache()
_ = df2_cached.count()
result = df2_cached.collect()[0]
assert result["full_name"] is None  # Should match PySpark behavior
```

**Option B (Alternative)**: Raise a warning and recommend `F.concat()`
```python
# Mock-spark could raise a DeprecationWarning or UserWarning
import warnings
with warnings.catch_warnings(record=True) as w:
    df2 = df.withColumn("full_name", F.col("first_name") + F.lit(" ") + F.col("last_name"))
    df2_cached = df2.cache()
    _ = df2_cached.count()
    assert len(w) > 0
    assert "concat" in str(w[0].message).lower()
```

### Test Case
```python
def test_string_concatenation_with_caching():
    """Test that string concatenation behaves like PySpark when DataFrame is cached."""
    spark = SparkSession()
    df = spark.createDataFrame([("Alice", "Smith")], ["first", "last"])
    
    # Using + operator
    df_plus = df.withColumn("name", F.col("first") + F.lit(" ") + F.col("last"))
    df_plus_cached = df_plus.cache()
    _ = df_plus_cached.count()  # Force materialization
    result_plus = df_plus_cached.collect()[0]
    
    # Using F.concat()
    df_concat = df.withColumn("name", F.concat(F.col("first"), F.lit(" "), F.col("last")))
    df_concat_cached = df_concat.cache()
    _ = df_concat_cached.count()
    result_concat = df_concat_cached.collect()[0]
    
    # Both should work, but + should match PySpark's behavior (None after caching)
    # OR both should work if mock-spark is more lenient but warns
    assert result_concat["name"] == "Alice Smith"
    # If matching PySpark: assert result_plus["name"] is None
    # If more lenient: assert result_plus["name"] == "Alice Smith" (with warning)
```

---

## DataFrame Materialization and Column Availability

### Issue
PySpark requires DataFrames to be materialized before validation can access columns created in transforms. Mock-spark's CTE optimization can make columns appear available when they're not actually materialized.

### Current Behavior (Incorrect)
```python
from mock_spark import SparkSession, functions as F

spark = SparkSession()
df = spark.createDataFrame([(1, "A")], ["id", "value"])

# Transform creates new column
df_transformed = df.withColumn("computed", F.col("id") * 2)

# In mock-spark, this might work without materialization
# Validation can access "computed" column immediately
validation_result = df_transformed.filter(F.col("computed") > 0)
# ✅ Works in mock-spark (incorrectly)
```

### PySpark Behavior (Expected)
```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.createDataFrame([(1, "A")], ["id", "value"])

# Transform creates new column
df_transformed = df.withColumn("computed", F.col("id") * 2)

# In PySpark, columns are available after materialization
# But validation might fail if DataFrame isn't materialized
df_cached = df_transformed.cache()
_ = df_cached.count()  # Force materialization
validation_result = df_cached.filter(F.col("computed") > 0)
# ✅ Works after materialization
```

### Required Mock-Spark Behavior

Mock-spark should require explicit materialization before columns created in transforms are accessible for validation:

```python
from mock_spark import SparkSession, functions as F

spark = SparkSession()
df = spark.createDataFrame([(1, "A")], ["id", "value"])

# Transform creates new column
df_transformed = df.withColumn("computed", F.col("id") * 2)

# Option 1: Require materialization (strict mode)
try:
    validation_result = df_transformed.filter(F.col("computed") > 0)
    # Should raise an error or require materialization first
except Exception as e:
    assert "materialized" in str(e).lower() or "cache" in str(e).lower()

# Option 2: Auto-materialize with warning (lenient mode)
import warnings
with warnings.catch_warnings(record=True) as w:
    validation_result = df_transformed.filter(F.col("computed") > 0)
    assert len(w) > 0
    assert "materialize" in str(w[0].message).lower()

# After explicit materialization, should always work
df_cached = df_transformed.cache()
_ = df_cached.count()
validation_result = df_cached.filter(F.col("computed") > 0)
# ✅ Should always work
```

### Test Case
```python
def test_column_availability_after_transform():
    """Test that computed columns require materialization before validation."""
    spark = SparkSession()
    df = spark.createDataFrame([(1, "test")], ["id", "name"])
    
    # Create computed column
    df_transformed = df.withColumn("doubled", F.col("id") * 2)
    
    # Attempt validation without materialization
    # Should either fail or warn
    try:
        result = df_transformed.filter(F.col("doubled") > 0).collect()
        # If it succeeds, should have warned
        import warnings
        # Check that warning was issued
    except Exception as e:
        # If it fails, error should mention materialization
        assert "materialize" in str(e).lower() or "cache" in str(e).lower()
    
    # After materialization, should work
    df_cached = df_transformed.cache()
    _ = df_cached.count()
    result = df_cached.filter(F.col("doubled") > 0).collect()
    assert len(result) == 1
    assert result[0]["doubled"] == 2
```

---

## Schema Creation and Management

### Issue
PySpark uses SQL commands for schema creation (`CREATE SCHEMA IF NOT EXISTS`), while mock-spark provides a `storage.create_schema()` method that doesn't exist in PySpark.

### Current Behavior (Incorrect)
```python
from mock_spark import SparkSession

spark = SparkSession()
# Mock-spark has this API
spark.storage.create_schema("my_schema")  # ✅ Works in mock-spark
```

### PySpark Behavior (Expected)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
# PySpark uses SQL
spark.sql("CREATE SCHEMA IF NOT EXISTS my_schema")  # ✅ Works in PySpark
# spark.storage.create_schema()  # ❌ AttributeError: 'SparkSession' object has no attribute 'storage'
```

### Required Mock-Spark Behavior

Mock-spark should provide BOTH APIs for compatibility:

```python
from mock_spark import SparkSession

spark = SparkSession()

# Option 1: Support storage API (for mock-spark convenience)
spark.storage.create_schema("my_schema")  # ✅ Should work

# Option 2: Support SQL API (for PySpark compatibility)
spark.sql("CREATE SCHEMA IF NOT EXISTS my_schema")  # ✅ Should also work

# Both should create the same schema
schemas = spark.catalog.listDatabases()
assert any(db.name == "my_schema" for db in schemas)
```

### Implementation Example
```python
class MockSparkSession:
    def __init__(self):
        self._storage = MockStorage()
        self._catalog = MockCatalog()
    
    @property
    def storage(self):
        """Mock-spark specific storage API."""
        return self._storage
    
    @property
    def catalog(self):
        """PySpark-compatible catalog API."""
        return self._catalog
    
    def sql(self, query: str):
        """Execute SQL, including schema creation."""
        query_upper = query.upper().strip()
        if query_upper.startswith("CREATE SCHEMA") or query_upper.startswith("CREATE DATABASE"):
            # Parse schema name from SQL
            schema_name = self._parse_schema_name_from_sql(query)
            self._storage.create_schema(schema_name)
            return None  # CREATE statements don't return DataFrames
        # ... handle other SQL
```

### Test Case
```python
def test_schema_creation_apis():
    """Test that both storage and SQL APIs work for schema creation."""
    spark = SparkSession()
    
    # Test storage API
    spark.storage.create_schema("schema1")
    assert "schema1" in [db.name for db in spark.catalog.listDatabases()]
    
    # Test SQL API
    spark.sql("CREATE SCHEMA IF NOT EXISTS schema2")
    assert "schema2" in [db.name for db in spark.catalog.listDatabases()]
    
    # Test that both create the same schema
    spark.storage.create_schema("schema3")
    spark.sql("CREATE SCHEMA IF NOT EXISTS schema3")  # Should be idempotent
    schemas = [db.name for db in spark.catalog.listDatabases()]
    assert schemas.count("schema3") == 1  # Should not create duplicate
```

---

## Type System Compatibility

### Issue
Mock-spark's `StructType`, `StructField`, and other type classes are not compatible with PySpark's types. Tests that import types conditionally fail when mock-spark types are used with PySpark operations.

### Current Behavior (Incorrect)
```python
from mock_spark import StructType, StructField, StringType

schema = StructType([StructField("name", StringType(), True)])
# This schema object is a mock-spark type

# When used with PySpark operations, fails
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([], schema)  # ❌ TypeError: schema should be StructType...
```

### PySpark Behavior (Expected)
```python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([StructField("name", StringType(), True)])
# This is a PySpark type

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([], schema)  # ✅ Works
```

### Required Mock-Spark Behavior

Mock-spark types should be compatible with PySpark type checking, OR mock-spark should provide a compatibility layer:

**Option A: Type Compatibility**
```python
# Mock-spark types should pass isinstance checks
from mock_spark import StructType as MockStructType
from pyspark.sql.types import StructType as PySparkStructType

mock_schema = MockStructType([...])
# Should pass PySpark's type checks
assert isinstance(mock_schema, PySparkStructType)  # Or compatible check
```

**Option B: Import Alias**
```python
# Mock-spark could provide pyspark.sql.types as an alias
from mock_spark.sql import types  # Provides compatible types
# OR
import mock_spark.sql.types as pyspark_types  # Drop-in replacement
```

### Implementation Example
```python
# In mock_spark/__init__.py or mock_spark/sql/types.py
try:
    # Try to import PySpark types for compatibility
    from pyspark.sql.types import (
        StructType as _PySparkStructType,
        StructField as _PySparkStructField,
        StringType as _PySparkStringType,
    )
    
    # Make mock-spark types inherit from PySpark types
    class StructType(_PySparkStructType):
        # Mock-spark implementation
        pass
    
    class StructField(_PySparkStructField):
        # Mock-spark implementation
        pass
    
except ImportError:
    # If PySpark not available, use standalone implementation
    # But ensure API compatibility
    class StructType:
        # Implementation that matches PySpark API
        pass
```

### Test Case
```python
def test_type_compatibility():
    """Test that mock-spark types work with PySpark operations."""
    from mock_spark import SparkSession, StructType, StructField, StringType
    
    spark = SparkSession()
    schema = StructType([StructField("name", StringType(), True)])
    
    # Should work with mock-spark
    df1 = spark.createDataFrame([{"name": "test"}], schema)
    assert df1.collect()[0]["name"] == "test"
    
    # If PySpark available, should also work
    try:
        from pyspark.sql import SparkSession as PySparkSession
        pyspark = PySparkSession.builder.getOrCreate()
        df2 = pyspark.createDataFrame([{"name": "test"}], schema)
        assert df2.collect()[0]["name"] == "test"
    except ImportError:
        pass  # PySpark not available, skip
```

---

## Function API Compatibility

### Issue
Mock-spark's `functions` module should match PySpark's `pyspark.sql.functions` API exactly, including parameter types, return types, and error handling.

### Current Behavior (Incorrect)
```python
from mock_spark import functions as F

# Mock-spark might accept Python expressions
expr = F.expr("col('user_id').isNotNull()")  # ✅ Works in mock-spark
```

### PySpark Behavior (Expected)
```python
from pyspark.sql import functions as F

# PySpark requires SQL syntax
expr = F.expr("user_id IS NOT NULL")  # ✅ Works in PySpark
# F.expr("col('user_id').isNotNull()")  # ❌ ParseException
```

### Required Mock-Spark Behavior

Mock-spark's `F.expr()` should parse SQL like PySpark, not Python expressions:

```python
from mock_spark import SparkSession, functions as F

spark = SparkSession()
df = spark.createDataFrame([(1, "test"), (None, "test2")], ["id", "name"])

# Should accept SQL syntax
expr_sql = F.expr("id IS NOT NULL")
result = df.filter(expr_sql).collect()
assert len(result) == 1
assert result[0]["id"] == 1

# Should reject Python-like syntax (or convert it)
try:
    expr_python = F.expr("col('id').isNotNull()")
    # Should raise ParseException or convert to SQL
except Exception as e:
    assert "parse" in str(e).lower() or "syntax" in str(e).lower()
```

### Implementation Example
```python
class Functions:
    def expr(self, sql_expression: str):
        """
        Parse SQL expression like PySpark.
        
        Args:
            sql_expression: SQL expression string (e.g., "id IS NOT NULL")
        
        Returns:
            Column expression
        
        Raises:
            ParseException: If SQL syntax is invalid
        """
        # Parse SQL expression
        # Convert to internal expression format
        # Return Column object
        try:
            parsed = self._parse_sql(sql_expression)
            return Column(parsed)
        except ParseError as e:
            raise ParseException(f"Invalid SQL expression: {sql_expression}") from e
    
    def _parse_sql(self, sql: str):
        """Parse SQL expression to internal format."""
        # Implement SQL parser
        # Handle: IS NOT NULL, =, >, <, AND, OR, etc.
        pass
```

### Test Case
```python
def test_expr_sql_parsing():
    """Test that F.expr() parses SQL like PySpark."""
    from mock_spark import SparkSession, functions as F
    
    spark = SparkSession()
    df = spark.createDataFrame([(1, "A"), (2, "B"), (None, "C")], ["id", "name"])
    
    # Valid SQL expressions
    valid_exprs = [
        "id IS NOT NULL",
        "id > 1",
        "id = 1",
        "name = 'A'",
        "id IS NOT NULL AND name = 'A'",
    ]
    
    for expr_str in valid_exprs:
        expr = F.expr(expr_str)
        result = df.filter(expr).collect()
        # Should not raise exception
        assert isinstance(result, list)
    
    # Invalid expressions (Python-like syntax)
    invalid_exprs = [
        "col('id').isNotNull()",
        "df['id'] > 1",
        "id.isNotNull()",
    ]
    
    for expr_str in invalid_exprs:
        try:
            expr = F.expr(expr_str)
            # If it doesn't raise, should at least warn
            import warnings
            # Check for warning
        except ParseException:
            pass  # Expected
```

---

## Error Handling and Exception Types

### Issue
PySpark raises `Py4JJavaError` for JVM-related errors, while mock-spark raises Python exceptions. Tests that check for specific exception types fail.

### Current Behavior (Incorrect)
```python
from mock_spark import functions as F

try:
    F.col(None)  # Invalid input
except TypeError:
    # Mock-spark raises TypeError
    pass
```

### PySpark Behavior (Expected)
```python
from pyspark.sql import functions as F

try:
    F.col(None)  # Invalid input
except Exception as e:
    # PySpark raises Py4JJavaError
    from py4j.protocol import Py4JJavaError
    assert isinstance(e, Py4JJavaError)
```

### Required Mock-Spark Behavior

Mock-spark should raise compatible exceptions. For JVM-related operations, mock-spark could:

**Option A: Raise Py4JJavaError-compatible exceptions**
```python
# Create a mock Py4JJavaError class
class MockPy4JJavaError(Exception):
    """Mock Py4JJavaError for compatibility."""
    def __init__(self, message, java_exception=None):
        self.message = message
        self.java_exception = java_exception
        super().__init__(message)

# In functions implementation
def col(name):
    if name is None:
        raise MockPy4JJavaError("Column name cannot be None")
    # ...
```

**Option B: Raise standard Python exceptions but document compatibility**
```python
# Raise standard exceptions but make them compatible
def col(name):
    if name is None:
        # Raise TypeError but make it catchable as Py4JJavaError
        error = TypeError("Column name cannot be None")
        error.__class__ = type('Py4JJavaError', (TypeError,), {})
        raise error
```

### Test Case
```python
def test_error_handling_compatibility():
    """Test that mock-spark errors are compatible with PySpark error handling."""
    from mock_spark import functions as F
    
    # Test invalid input
    try:
        F.col(None)
        assert False, "Should have raised exception"
    except Exception as e:
        # Should be catchable as both TypeError and Py4JJavaError
        assert isinstance(e, (TypeError, ValueError, AttributeError))
        
        # If Py4J available, should also be catchable as Py4JJavaError
        try:
            from py4j.protocol import Py4JJavaError
            # Mock-spark error should be compatible
            # (either isinstance or has same attributes)
        except ImportError:
            pass  # Py4J not available
```

---

## Performance Characteristics

### Issue
Mock-spark is much faster than PySpark due to no JVM overhead. Performance tests that expect PySpark-like timing fail.

### Current Behavior
```python
from mock_spark import functions as F
import time

start = time.time()
for i in range(1000):
    F.col(f"col_{i}")
end = time.time()

duration = end - start
# Mock-spark: ~0.01s (very fast)
# PySpark: ~1-2s (JVM overhead)
```

### Required Mock-Spark Behavior

Mock-spark should provide a "realistic mode" that simulates PySpark's performance characteristics:

```python
from mock_spark import SparkSession

# Default: Fast mode (for quick tests)
spark_fast = SparkSession()
# Very fast execution

# Realistic mode: Simulate PySpark performance
spark_realistic = SparkSession(performance_mode="realistic")
# Adds artificial delays to match PySpark timing
# 1000 function calls: ~1-2s instead of ~0.01s
```

### Implementation Example
```python
class SparkSession:
    def __init__(self, app_name="MockSpark", performance_mode="fast"):
        self.performance_mode = performance_mode
        self._jvm_overhead = 0.001 if performance_mode == "realistic" else 0.00001
    
    def _simulate_jvm_overhead(self, operations=1):
        """Simulate JVM overhead for realistic performance."""
        if self.performance_mode == "realistic":
            import time
            time.sleep(self._jvm_overhead * operations)

class Functions:
    def __init__(self, spark_session=None):
        self._spark = spark_session
    
    def col(self, name):
        if self._spark and self._spark.performance_mode == "realistic":
            self._spark._simulate_jvm_overhead()
        # ... implementation
```

### Test Case
```python
def test_performance_modes():
    """Test that mock-spark can simulate PySpark performance."""
    from mock_spark import SparkSession, functions as F
    import time
    
    # Fast mode (default)
    spark_fast = SparkSession(performance_mode="fast")
    F_fast = Functions(spark_fast)
    
    start = time.time()
    for i in range(1000):
        F_fast.col(f"col_{i}")
    duration_fast = time.time() - start
    
    # Realistic mode
    spark_realistic = SparkSession(performance_mode="realistic")
    F_realistic = Functions(spark_realistic)
    
    start = time.time()
    for i in range(1000):
        F_realistic.col(f"col_{i}")
    duration_realistic = time.time() - start
    
    # Realistic should be slower
    assert duration_realistic > duration_fast
    # Realistic should be in PySpark range (~1-2s for 1000 calls)
    assert 0.5 < duration_realistic < 3.0
```

---

## Expression Parsing and Evaluation

### Issue
Mock-spark's `F.expr()` accepts Python-like expressions, while PySpark requires SQL syntax.

### Detailed Examples

#### Example 1: IS NOT NULL
```python
# PySpark (correct)
F.expr("user_id IS NOT NULL")

# Mock-spark (incorrect - currently accepts)
F.expr("col('user_id').isNotNull()")  # Should fail or convert

# Mock-spark (correct - should be)
F.expr("user_id IS NOT NULL")  # Match PySpark
```

#### Example 2: Comparisons
```python
# PySpark
F.expr("age > 18")
F.expr("age >= 18")
F.expr("age < 65")
F.expr("age <= 65")
F.expr("age = 25")
F.expr("age != 25")

# Mock-spark should match exactly
```

#### Example 3: Logical Operators
```python
# PySpark
F.expr("age > 18 AND age < 65")
F.expr("status = 'active' OR status = 'pending'")
F.expr("NOT (deleted = true)")

# Mock-spark should support AND, OR, NOT
```

#### Example 4: String Operations
```python
# PySpark
F.expr("LENGTH(name) > 0")
F.expr("TRIM(name) = ''")
F.expr("UPPER(status) = 'ACTIVE'")

# Mock-spark should support SQL string functions
```

### Implementation Requirements

1. **SQL Parser**: Implement a SQL expression parser that handles:
   - Column references (unquoted identifiers)
   - Literals (strings, numbers, booleans, NULL)
   - Operators (comparison, arithmetic, logical)
   - Functions (IS NULL, IS NOT NULL, LENGTH, TRIM, etc.)
   - Parentheses for grouping

2. **Error Messages**: Match PySpark's error messages:
   ```python
   # PySpark error
   ParseException: mismatched input ')' expecting {'ADD', 'AFTER', ...}
   
   # Mock-spark should produce similar errors
   ```

3. **Case Sensitivity**: SQL keywords should be case-insensitive:
   ```python
   F.expr("id IS NOT NULL")  # ✅
   F.expr("id is not null")  # ✅ Should also work
   F.expr("id Is Not Null")  # ✅ Should also work
   ```

---

## Empty DataFrame Handling

### Issue
PySpark requires explicit `StructType` schemas for empty DataFrames. Mock-spark should enforce the same requirement.

### Current Behavior (Incorrect)
```python
from mock_spark import SparkSession

spark = SparkSession()
# Mock-spark might accept this
df = spark.createDataFrame([], ["col1", "col2"])  # ✅ Works in mock-spark
```

### PySpark Behavior (Expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()
# PySpark requires explicit schema
try:
    df = spark.createDataFrame([], ["col1", "col2"])
    # ❌ ValueError: can not infer schema from empty dataset
except ValueError as e:
    assert "can not infer schema" in str(e)

# Correct approach
schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", StringType(), True),
])
df = spark.createDataFrame([], schema)  # ✅ Works
```

### Required Mock-Spark Behavior

```python
from mock_spark import SparkSession
from mock_spark import StructType, StructField, StringType

spark = SparkSession()

# Should raise error for empty DataFrame without schema
try:
    df = spark.createDataFrame([], ["col1", "col2"])
    assert False, "Should have raised ValueError"
except ValueError as e:
    assert "can not infer schema" in str(e) or "empty dataset" in str(e)

# Should work with explicit schema
schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", StringType(), True),
])
df = spark.createDataFrame([], schema)  # ✅ Should work
assert df.schema == schema
```

### Test Case
```python
def test_empty_dataframe_schema_requirement():
    """Test that empty DataFrames require explicit schemas."""
    from mock_spark import SparkSession, StructType, StructField, StringType, IntegerType
    
    spark = SparkSession()
    
    # Should fail without schema
    try:
        df = spark.createDataFrame([], ["name", "age"])
        assert False, "Should require schema for empty DataFrame"
    except ValueError as e:
        assert "schema" in str(e).lower() or "empty" in str(e).lower()
    
    # Should work with schema
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ])
    df = spark.createDataFrame([], schema)
    assert len(df.collect()) == 0
    assert df.schema == schema
    
    # Should work with non-empty data (schema optional)
    df2 = spark.createDataFrame([("Alice", 25)], ["name", "age"])
    assert len(df2.collect()) == 1
```

---

## Union Operation Strictness

### Issue
PySpark enforces strict schema compatibility for `union` operations. Mock-spark should match this strictness.

### Current Behavior (Incorrect)
```python
from mock_spark import SparkSession

spark = SparkSession()
df1 = spark.createDataFrame([("A", 1)], ["col1", "col2"])
df2 = spark.createDataFrame([("B",)], ["col1"])  # Different schema

# Mock-spark might allow this
df_union = df1.union(df2)  # ✅ Might work (incorrectly)
```

### PySpark Behavior (Expected)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([("A", 1)], ["col1", "col2"])
df2 = spark.createDataFrame([("B",)], ["col1"])  # Different schema

# PySpark requires same number of columns
try:
    df_union = df1.union(df2)
    # ❌ AnalysisException: Union can only be performed on tables with the same number of columns
except Exception as e:
    assert "same number of columns" in str(e) or "compatible" in str(e)
```

### Required Mock-Spark Behavior

```python
from mock_spark import SparkSession

spark = SparkSession()
df1 = spark.createDataFrame([("A", 1)], ["col1", "col2"])
df2 = spark.createDataFrame([("B",)], ["col1"])

# Should fail - different number of columns
try:
    df_union = df1.union(df2)
    assert False, "Should have raised AnalysisException"
except AnalysisException as e:
    assert "same number of columns" in str(e) or "compatible" in str(e)

# Should also check column names and types
df3 = spark.createDataFrame([("C", 2)], ["col1", "col2"])
df_union = df1.union(df3)  # ✅ Should work - same schema
```

### Test Case
```python
def test_union_schema_strictness():
    """Test that union operations enforce schema compatibility."""
    from mock_spark import SparkSession
    
    spark = SparkSession()
    
    # Same schema - should work
    df1 = spark.createDataFrame([("A", 1)], ["name", "value"])
    df2 = spark.createDataFrame([("B", 2)], ["name", "value"])
    df_union = df1.union(df2)
    assert len(df_union.collect()) == 2
    
    # Different number of columns - should fail
    df3 = spark.createDataFrame([("C",)], ["name"])
    try:
        df_union2 = df1.union(df3)
        assert False, "Should have raised AnalysisException"
    except AnalysisException as e:
        assert "columns" in str(e).lower()
    
    # Same number but different names - should fail
    df4 = spark.createDataFrame([("D", 3)], ["name", "id"])
    try:
        df_union3 = df1.union(df4)
        # PySpark allows this but uses first DataFrame's column names
        # Mock-spark should match PySpark behavior
    except AnalysisException:
        pass  # Also acceptable if strict about names
```

---

## SparkContext and JVM Requirements

### Issue
PySpark requires an active SparkContext for function calls. Mock-spark should simulate this requirement.

### Current Behavior (Incorrect)
```python
from mock_spark import functions as F

# Mock-spark might allow this without SparkSession
col_expr = F.col("test")  # ✅ Works (incorrectly)
```

### PySpark Behavior (Expected)
```python
from pyspark.sql import functions as F

# PySpark requires active SparkContext
try:
    col_expr = F.col("test")
    # ❌ RuntimeError: Cannot perform expression 'col(test)': No active SparkSession found
except RuntimeError as e:
    assert "SparkSession" in str(e) or "SparkContext" in str(e)
```

### Required Mock-Spark Behavior

Mock-spark should require an active SparkSession for function calls:

```python
from mock_spark import SparkSession, functions as F

# Without SparkSession - should fail
try:
    col_expr = F.col("test")
    assert False, "Should require SparkSession"
except RuntimeError as e:
    assert "SparkSession" in str(e) or "SparkContext" in str(e)

# With SparkSession - should work
spark = SparkSession()
F_with_session = Functions(spark)
col_expr = F_with_session.col("test")  # ✅ Should work

# Or use functions from spark session
col_expr2 = spark.sql.functions.col("test")  # ✅ Should work
```

### Implementation Example
```python
class Functions:
    _active_session = None
    
    @classmethod
    def set_active_session(cls, session):
        """Set the active SparkSession for function calls."""
        cls._active_session = session
    
    def col(self, name):
        """Get column by name. Requires active SparkSession."""
        if self._active_session is None:
            # Check if there's a global session
            try:
                from mock_spark import _global_session
                if _global_session is None:
                    raise RuntimeError(
                        "Cannot perform expression 'col({})': No active SparkSession found".format(name)
                    )
            except (ImportError, AttributeError):
                raise RuntimeError(
                    "Cannot perform expression 'col({})': No active SparkSession found".format(name)
                )
        # ... implementation
```

### Test Case
```python
def test_sparksession_requirement():
    """Test that function calls require active SparkSession."""
    from mock_spark import SparkSession, functions as F
    
    # Without session - should fail
    try:
        expr = F.col("test")
        assert False, "Should require SparkSession"
    except RuntimeError as e:
        assert "SparkSession" in str(e)
    
    # With session - should work
    spark = SparkSession()
    # Set as active session
    F.set_active_session(spark)
    expr = F.col("test")
    assert expr is not None
```

---

## Catalog and Storage API

### Issue
PySpark uses `catalog.createDatabase()` and SQL for schema management, while mock-spark provides `storage.create_schema()`. Both should be supported.

### Required Implementation

```python
class MockSparkSession:
    def __init__(self):
        self._storage = MockStorage()
        self._catalog = MockCatalog(self._storage)
    
    @property
    def storage(self):
        """Mock-spark specific storage API."""
        return self._storage
    
    @property
    def catalog(self):
        """PySpark-compatible catalog API."""
        return self._catalog
    
    def sql(self, query: str):
        """Execute SQL, including schema operations."""
        query_upper = query.upper().strip()
        
        # Handle CREATE SCHEMA/DATABASE
        if query_upper.startswith("CREATE SCHEMA") or query_upper.startswith("CREATE DATABASE"):
            schema_name = self._parse_schema_name(query)
            if_exists = "IF NOT EXISTS" in query_upper
            self._storage.create_schema(schema_name, if_exists=if_exists)
            return None
        
        # Handle other SQL...
    
    def _parse_schema_name(self, query: str):
        """Parse schema name from CREATE SCHEMA/DATABASE query."""
        # Extract schema name from SQL
        import re
        match = re.search(r'(?:SCHEMA|DATABASE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)', query, re.IGNORECASE)
        if match:
            return match.group(1)
        raise ValueError(f"Could not parse schema name from: {query}")

class MockCatalog:
    def __init__(self, storage):
        self._storage = storage
    
    def createDatabase(self, name: str, path: str = None, ignoreIfExists: bool = False):
        """PySpark-compatible database creation."""
        self._storage.create_schema(name, if_exists=ignoreIfExists)
    
    def listDatabases(self):
        """List all databases/schemas."""
        schemas = self._storage.list_schemas()
        return [Database(name=s) for s in schemas]

class Database:
    def __init__(self, name: str):
        self.name = name
```

### Test Case
```python
def test_catalog_and_storage_apis():
    """Test that both catalog and storage APIs work."""
    from mock_spark import SparkSession
    
    spark = SparkSession()
    
    # Test storage API
    spark.storage.create_schema("schema1")
    assert "schema1" in [db.name for db in spark.catalog.listDatabases()]
    
    # Test catalog API
    spark.catalog.createDatabase("schema2", ignoreIfExists=True)
    assert "schema2" in [db.name for db in spark.catalog.listDatabases()]
    
    # Test SQL API
    spark.sql("CREATE SCHEMA IF NOT EXISTS schema3")
    assert "schema3" in [db.name for db in spark.catalog.listDatabases()]
    
    # All should create the same schemas
    schemas = [db.name for db in spark.catalog.listDatabases()]
    assert "schema1" in schemas
    assert "schema2" in schemas
    assert "schema3" in schemas
```

---

## Datetime Type Inference and Casting

### Issue
PySpark's `to_timestamp()` function requires string input. If a column is already inferred as datetime type, `to_timestamp()` fails. Mock-spark should match this behavior.

### Current Behavior (Incorrect)
```python
from mock_spark import SparkSession, functions as F

spark = SparkSession()
# Mock-spark might infer datetime from ISO string
df = spark.createDataFrame([("2024-01-01T10:00:00",)], ["timestamp_str"])
# Column might be inferred as datetime type

# Mock-spark might accept datetime type
df2 = df.withColumn("parsed", F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss"))
# ✅ Might work (incorrectly)
```

### PySpark Behavior (Expected)
```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("2024-01-01T10:00:00",)], ["timestamp_str"])
# PySpark might infer as datetime

# PySpark requires string type
try:
    df2 = df.withColumn("parsed", F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss"))
    # ❌ invalid series dtype: expected `String`, got `datetime[μs]`
except Exception as e:
    assert "String" in str(e) or "datetime" in str(e)
```

### Required Mock-Spark Behavior

Mock-spark should require explicit string casting before `to_timestamp()`:

```python
from mock_spark import SparkSession, functions as F

spark = SparkSession()
df = spark.createDataFrame([("2024-01-01T10:00:00",)], ["timestamp_str"])

# Should fail if column is not string type
try:
    df2 = df.withColumn("parsed", F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss"))
    # If timestamp_str is datetime type, should raise error
except Exception as e:
    assert "String" in str(e) or "cast" in str(e).lower()

# Should work with explicit cast
df2 = df.withColumn(
    "parsed",
    F.to_timestamp(F.col("timestamp_str").cast("string"), "yyyy-MM-dd'T'HH:mm:ss")
)
# ✅ Should work
```

### Test Case
```python
def test_to_timestamp_type_requirement():
    """Test that to_timestamp requires string type."""
    from mock_spark import SparkSession, functions as F
    
    spark = SparkSession()
    
    # Create DataFrame with datetime-like string
    df = spark.createDataFrame([("2024-01-01T10:00:00",)], ["timestamp_str"])
    
    # If column is inferred as datetime, should fail
    try:
        df2 = df.withColumn("parsed", F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss"))
        # Check if it worked or should have failed
        result = df2.collect()[0]
        # If it worked, timestamp_str must have been string type
        assert result["parsed"] is not None
    except Exception as e:
        # If it failed, error should mention string requirement
        assert "String" in str(e) or "cast" in str(e).lower()
    
    # With explicit cast, should always work
    df3 = df.withColumn(
        "parsed",
        F.to_timestamp(F.col("timestamp_str").cast("string"), "yyyy-MM-dd'T'HH:mm:ss")
    )
    result = df3.collect()[0]
    assert result["parsed"] is not None
```

---

## Testing Recommendations

### 1. Create PySpark Compatibility Test Suite

Create a test suite that runs the same tests against both PySpark and mock-spark:

```python
import pytest
import os

@pytest.fixture(params=["mock", "pyspark"])
def spark_session(request):
    """Fixture that provides both mock-spark and PySpark sessions."""
    if request.param == "mock":
        from mock_spark import SparkSession
        return SparkSession()
    else:
        from pyspark.sql import SparkSession
        return SparkSession.builder.appName("test").getOrCreate()

def test_string_concatenation(spark_session):
    """Test that works with both mock-spark and PySpark."""
    from pyspark.sql import functions as F
    
    df = spark_session.createDataFrame([("John", "Doe")], ["first", "last"])
    df2 = df.withColumn("full", F.concat(F.col("first"), F.lit(" "), F.col("last")))
    result = df2.collect()[0]
    
    # Should work the same in both
    assert result["full"] == "John Doe"
```

### 2. Test Failure Modes

Ensure mock-spark fails in the same way PySpark fails:

```python
def test_empty_dataframe_failure():
    """Test that both raise the same error for empty DataFrames."""
    import pytest
    
    # Test mock-spark
    from mock_spark import SparkSession
    spark_mock = SparkSession()
    with pytest.raises(ValueError, match="can not infer schema"):
        spark_mock.createDataFrame([], ["col1"])
    
    # Test PySpark
    from pyspark.sql import SparkSession as PySparkSession
    spark_real = PySparkSession.builder.getOrCreate()
    with pytest.raises(ValueError, match="can not infer schema"):
        spark_real.createDataFrame([], ["col1"])
```

### 3. Performance Testing

Test that performance modes work correctly:

```python
def test_performance_modes():
    """Test that realistic mode simulates PySpark performance."""
    from mock_spark import SparkSession, functions as F
    import time
    
    spark = SparkSession(performance_mode="realistic")
    F_funcs = Functions(spark)
    
    start = time.time()
    for i in range(1000):
        F_funcs.col(f"col_{i}")
    duration = time.time() - start
    
    # Should be in PySpark range
    assert 0.5 < duration < 3.0, f"Duration {duration} not in expected range"
```

### 4. Integration Testing

Test with real-world scenarios from the test suite:

```python
def test_healthcare_pipeline_scenario():
    """Test the exact scenario from healthcare pipeline test."""
    from mock_spark import SparkSession, functions as F
    from mock_spark import StructType, StructField, StringType
    
    spark = SparkSession()
    
    # Create patient data
    schema = StructType([
        StructField("patient_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("date_of_birth", StringType(), False),
    ])
    
    data = [("PAT-001", "John", "Doe", "1980-01-01")]
    df = spark.createDataFrame(data, schema)
    
    # Transform with string concatenation
    df2 = df.withColumn(
        "full_name",
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
    )
    
    # Cache and materialize
    df2_cached = df2.cache()
    _ = df2_cached.count()
    
    # Validate
    result = df2_cached.collect()[0]
    assert result["full_name"] == "John Doe"  # Should not be None
    assert result["full_name"] is not None
```

---

## Summary Checklist

For mock-spark to be a true drop-in replacement, ensure:

- [ ] String concatenation with `+` fails or warns when DataFrame is cached
- [ ] `F.concat()` works reliably for string concatenation
- [ ] Computed columns require materialization before validation
- [ ] Both `storage.create_schema()` and `sql("CREATE SCHEMA...")` work
- [ ] Types are compatible with PySpark type checking
- [ ] `F.expr()` parses SQL, not Python expressions
- [ ] Errors are compatible with PySpark error types
- [ ] Performance mode can simulate PySpark timing
- [ ] Empty DataFrames require explicit schemas
- [ ] Union operations enforce schema compatibility
- [ ] Function calls require active SparkSession
- [ ] Catalog API matches PySpark's catalog API

---

## Additional Resources

- PySpark Documentation: https://spark.apache.org/docs/latest/api/python/
- PySpark Source Code: https://github.com/apache/spark
- Mock-Spark Issues: See `docs/MOCK_SPARK_MISSED_ISSUES.md` for specific test failures

---

---

## Implementation Priority

Based on the test failures encountered, here's a recommended implementation priority:

### High Priority (Critical for Drop-In Replacement)
1. **String Concatenation with `+` Operator** - Causes validation failures in real pipelines
2. **Empty DataFrame Schema Requirement** - Already partially implemented in 3.10.4+
3. **Union Operation Strictness** - Already partially implemented in 3.10.4+
4. **DataFrame Materialization** - Critical for validation to work correctly

### Medium Priority (Important for API Compatibility)
5. **Schema Creation APIs** - Both `storage` and SQL APIs should work
6. **Type System Compatibility** - Types should work with PySpark operations
7. **Function API Compatibility** - `F.expr()` should parse SQL
8. **SparkContext Requirements** - Functions should require active session

### Lower Priority (Nice to Have)
9. **Error Handling Compatibility** - Py4JJavaError compatibility
10. **Performance Modes** - Realistic performance simulation
11. **Catalog API** - Full PySpark catalog compatibility

---

## Quick Reference: Common Patterns

### Pattern 1: String Concatenation
```python
# ❌ Don't use (fails in PySpark when cached)
df.withColumn("full_name", F.col("first") + F.lit(" ") + F.col("last"))

# ✅ Use instead (works in both)
df.withColumn("full_name", F.concat(F.col("first"), F.lit(" "), F.col("last")))
```

### Pattern 2: Empty DataFrame
```python
# ❌ Don't use (fails in PySpark)
df = spark.createDataFrame([], ["col1", "col2"])

# ✅ Use instead (works in both)
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([StructField("col1", StringType(), True)])
df = spark.createDataFrame([], schema)
```

### Pattern 3: to_timestamp with Type Casting
```python
# ❌ Don't use (fails if column is datetime type)
df.withColumn("parsed", F.to_timestamp(F.col("date_str"), "yyyy-MM-dd"))

# ✅ Use instead (works in both)
df.withColumn("parsed", F.to_timestamp(F.col("date_str").cast("string"), "yyyy-MM-dd"))
```

### Pattern 4: Schema Creation
```python
# ✅ Both should work
spark.storage.create_schema("my_schema")  # Mock-spark convenience
spark.sql("CREATE SCHEMA IF NOT EXISTS my_schema")  # PySpark compatible
```

### Pattern 5: Materialization Before Validation
```python
# ❌ Don't use (may fail in PySpark)
df_transformed = df.withColumn("computed", F.col("id") * 2)
validation_result = df_transformed.filter(F.col("computed") > 0)

# ✅ Use instead (works in both)
df_transformed = df.withColumn("computed", F.col("id") * 2)
df_cached = df_transformed.cache()
_ = df_cached.count()  # Force materialization
validation_result = df_cached.filter(F.col("computed") > 0)
```

---

## Conclusion

Making mock-spark a true drop-in replacement for PySpark requires attention to detail in many areas. The issues documented here were discovered through real-world test failures where mock-spark passed tests that failed with actual PySpark.

**Key Takeaway**: Mock-spark should fail in the same way PySpark fails. It's better to be strict and match PySpark's behavior exactly than to be lenient and allow code that would fail in production.

By implementing the behaviors described in this guide, mock-spark will become a reliable testing tool that catches real PySpark issues before they reach production.

---

**Last Updated**: 2024-12-11  
**Mock-Spark Version**: 3.11.0  
**PySpark Version Tested Against**: 3.5.x  
**Document Version**: 1.0


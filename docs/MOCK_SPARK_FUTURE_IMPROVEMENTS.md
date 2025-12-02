# Future Improvements for mock-spark

Based on real-world usage patterns in this codebase, here are actionable improvements that would make mock-spark even better as a PySpark drop-in replacement.

## 🔴 High Priority Improvements

### 1. Remove or Strengthen Functions() Deprecation

**Current State:**
- `Functions()` class is deprecated but still works
- Shows deprecation warnings in test output (435 warnings in our test suite)
- Can cause confusion about which API to use

**Recommendation:**
```python
# Option A: Remove Functions() entirely in next major version
# Option B: Make deprecation more actionable
class Functions:
    def __init__(self):
        import warnings
        warnings.warn(
            "Functions() is deprecated. Use 'from mock_spark.sql import functions as F' instead. "
            "This matches PySpark where functions is a module, not a class. "
            "Migration: Replace 'Functions()' with 'from mock_spark.sql import functions as F'",
            DeprecationWarning,
            stacklevel=2
        )
        # ... rest of implementation
```

**Impact:** Reduces test noise, clearer migration path

---

### 2. Add DataFrameWriter.delta() Method

**Current State:**
```python
# PySpark
df.write.format("delta").save("path")
df.write.delta("path")  # Convenience method

# mock-spark
df.write.format("delta").save("path")  # ✅ Works
df.write.delta("path")  # ❌ AttributeError
```

**Recommendation:**
```python
class DataFrameWriter:
    def delta(self, path, **options):
        """Save DataFrame in Delta Lake format.
        
        This is a convenience method equivalent to:
        df.write.format("delta").save(path)
        """
        return self.format("delta").options(**options).save(path)
```

**Impact:** Better API parity, reduces code changes when switching engines

---

### 3. Enhanced Error Messages with Migration Guidance

**Current State:**
Error messages are functional but could be more helpful for users migrating from PySpark.

**Recommendation:**
```python
class AnalysisException(Exception):
    def __init__(self, message, error_class=None, message_parameters=None):
        super().__init__(message)
        self.error_class = error_class
        self.message_parameters = message_parameters
        
        # Add helpful migration hints for common errors
        if "table or view not found" in str(message).lower():
            self._hint = (
                "In mock-spark, ensure tables are created using "
                "spark.sql('CREATE TABLE ...') or storage API. "
                "For PySpark compatibility, use standard SQL commands."
            )
```

**Impact:** Better developer experience when debugging

---

### 4. Storage API SQL Compatibility (Documentation)

**Current State:**
- ✅ SQL commands work (CREATE DATABASE, CREATE TABLE, etc.)
- ✅ `.storage` API is available as convenience
- ⚠️ Documentation could be clearer about which to use

**Recommendation:**
Improve documentation to clearly distinguish:
- **PySpark-compatible APIs** (use for compatibility): `spark.sql("CREATE DATABASE ...")`
- **mock-spark convenience APIs** (use for testing): `spark.storage.create_schema(...)`

Both work, but users should know which is more portable.

**Impact:** Clearer guidance for developers choosing between APIs

---

## 🟡 Medium Priority Improvements

### 5. Type Hints Enhancement (Partial)

**Current State:**
- ✅ Basic type hints exist (e.g., `F.col` has return type annotations)
- ⚠️ Could be more comprehensive for:
  - DataFrame operations return types (some overloads missing)
  - Complex function signatures with multiple overloads
  - Type stubs for better IDE support

**Recommendation:**
```python
from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from typing_extensions import Protocol
    
    class Column(Protocol):
        """Column protocol for type checking."""
        pass

class DataFrame:
    @overload
    def select(self, *cols: Column) -> DataFrame: ...
    
    @overload
    def select(self, *cols: str) -> DataFrame: ...
    
    def select(self, *cols) -> DataFrame:
        """Select columns from DataFrame."""
        # Implementation
```

**Impact:** Better IDE autocomplete, type checking support

---

### 6. Performance Hints and Optimization

**Current State:**
mock-spark is fast, but could provide hints about performance characteristics.

**Recommendation:**
```python
class SparkSession:
    def explain(self, extended=False, codegen=False, cost=False, formatted=False):
        """Explain the execution plan.
        
        In mock-spark, this provides a simplified execution plan
        showing DataFrame operations in a readable format.
        """
        # Return a simplified plan showing:
        # - Source operations
        # - Transformations
        # - Output operations
```

**Impact:** Better debugging and understanding of DataFrame operations

---

### 7. Catalog API Completeness

**Current State:**
Basic catalog operations work, but could match PySpark's catalog API more closely.

**Recommendation:**
```python
class Catalog:
    def listDatabases(self):
        """List all databases."""
        # Implementation
        
    def listTables(self, dbName=None):
        """List tables in a database."""
        # Implementation
        
    def tableExists(self, tableName, dbName=None):
        """Check if table exists."""
        # Implementation
        
    def getTable(self, tableName, dbName=None):
        """Get table metadata."""
        # Implementation
```

**Impact:** Better compatibility with PySpark code using catalog operations

---

### 8. Better Documentation for mock-spark Specific Features

**Recommendation:**
Clear documentation distinguishing:
- **PySpark-compatible APIs**: Use these for compatibility
- **mock-spark convenience APIs**: Use these for testing convenience
- **Migration guide**: How to write code that works with both

**Example:**
```markdown
## Storage API (mock-spark convenience)

The `.storage` API is a convenience feature specific to mock-spark:

```python
# mock-spark convenience (faster for tests)
spark.storage.create_schema("test")

# PySpark compatible (works with both)
spark.sql("CREATE DATABASE IF NOT EXISTS test")
```

**When to use:** Use PySpark-compatible APIs for code that needs to work with both engines.
Use convenience APIs when writing mock-spark-specific test utilities.
```

**Impact:** Clearer guidance for developers

---

## 🟢 Low Priority / Nice to Have

### 9. Lazy Evaluation Simulation

**Current State:**
mock-spark executes operations eagerly (good for testing), but could optionally simulate lazy evaluation.

**Recommendation:**
```python
# Optional lazy evaluation mode for testing lazy evaluation bugs
spark = SparkSession.builder\
    .config("spark.sql.execution.lazy", True)\
    .getOrCreate()

# Operations are queued until an action (collect, show, etc.)
df = spark.read.parquet("data")  # No execution
df = df.filter(F.col("age") > 18)  # Still no execution
df.show()  # Now executes all operations
```

**Impact:** Can test lazy evaluation edge cases

---

### 10. Query Plan Visualization

**Recommendation:**
```python
df.explain(extended=True)  # Returns formatted string
df.explain(mode="graph")   # Returns graphviz DOT format for visualization
```

**Impact:** Better understanding of DataFrame transformations

---

## Summary: Top 5 Recommended Improvements

1. **Remove Functions() deprecation** or make it more actionable (reduces test noise)
2. **Add DataFrameWriter.delta()** method (better API parity)
3. **Enhanced error messages** with migration guidance (better DX)
4. **Storage API documentation** - clarify when to use SQL vs convenience API
5. **Enhanced type hints** - add more overloads and comprehensive stubs for IDE support

## Testing Compatibility Checklist

When implementing improvements, verify:
- ✅ Import paths match PySpark exactly (only package name differs)
- ✅ API methods have same signatures
- ✅ Error types and messages are similar
- ✅ Type hints are compatible
- ✅ SQL commands work identically
- ✅ Documentation clearly distinguishes compatibility vs convenience features

---

## Feedback from Real Usage

Based on 1713+ tests in this codebase:

**What Works Great:**
- Module structure (3.8.0 fixed this!)
- Type compatibility
- Basic DataFrame operations
- Exception handling

**What Needs Improvement:**
- Deprecation warnings (too noisy) - Functions() still shows warnings
- Missing convenience methods (delta()) - DataFrameWriter.delta() not available
- Error message clarity - could provide better migration guidance

**What Would Be Nice:**
- Better type hints for IDEs
- Performance profiling hints
- Lazy evaluation simulation option


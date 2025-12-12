# Mock-Spark Drop-In Replacement Improvement Plan

**Document Version:** 1.0  
**Date:** 2024-12-11  
**Target Audience:** mock-spark library developers  
**Goal:** Achieve near-perfect drop-in replacement compatibility with PySpark

## Executive Summary

This document outlines improvements needed for `mock-spark` to become a seamless drop-in replacement for PySpark. These recommendations are based on real-world compatibility challenges encountered while developing SparkForge, a data pipeline framework that supports both PySpark and mock-spark execution modes.

**Current Status:** mock-spark 3.12.0 provides excellent compatibility for most use cases, but several API differences require conditional code or workarounds in production codebases.

**Priority:** High-value improvements that eliminate the need for conditional logic when switching between PySpark and mock-spark.

---

## Table of Contents

1. [Catalog API Compatibility](#1-catalog-api-compatibility)
2. [Storage API Standardization](#2-storage-api-standardization)
3. [Import Path Consistency](#3-import-path-consistency)
4. [Exception Type Alignment](#4-exception-type-alignment)
5. [SparkSession Context Management](#5-sparksession-context-management)
6. [Type System Consistency](#6-type-system-consistency)
7. [Delta Lake Compatibility](#7-delta-lake-compatibility)
8. [Function API Parity](#8-function-api-parity)
9. [Testing Recommendations](#9-testing-recommendations)

---

## 1. Catalog API Compatibility

### Current Issue

**Problem:** `spark.catalog.createDatabase()` exists in mock-spark but not in PySpark, forcing users to write conditional code.

**Example from SparkForge:**
```python
def _create_database(spark, schema_name: str):
    """Helper to create database that works for both mock-spark and pyspark."""
    try:
        # Try mock-spark API first
        if hasattr(spark.catalog, "createDatabase"):
            spark.catalog.createDatabase(schema_name)
        else:
            # Use SQL for pyspark
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    except Exception as e:
        print(f"⚠️  Could not create database {schema_name}: {e}")
```

**PySpark Equivalent:**
```python
# PySpark only supports SQL
spark.sql("CREATE DATABASE IF NOT EXISTS test_schema")
```

### Recommended Solution

**Option A (Recommended):** Add `createDatabase()` method to mock-spark's `catalog` that internally uses SQL, matching PySpark's behavior:

```python
# In mock-spark catalog implementation
class Catalog:
    def createDatabase(self, dbName: str, ignoreIfExists: bool = True, path: Optional[str] = None):
        """
        Create a database (alias for SQL).
        This matches PySpark's SQL-only approach for database creation.
        """
        if ignoreIfExists:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbName}")
        else:
            self.spark.sql(f"CREATE DATABASE {dbName}")
```

**Option B (Alternative):** Document that `createDatabase()` is deprecated and recommend SQL-only approach. Remove it in a future major version.

### Priority
**High** - Affects all code that creates databases/schemas. Currently requires conditional logic in every codebase.

### Compatibility Note
This change would make mock-spark's API more consistent with PySpark, but would break existing code that relies on `createDatabase()`. Consider a deprecation period with warnings.

---

## 2. Storage API Standardization

### Current Issue

**Problem:** mock-spark provides a convenient `spark.storage` API (`create_schema`, `create_table`, `insert_data`, `query_table`, `table_exists`) that doesn't exist in PySpark, causing test failures and preventing code portability.

**Example from SparkForge tests:**
```python
# mock-spark only - not portable to PySpark
mock_spark_session.storage.create_schema("bronze")
mock_spark_session.storage.create_table("bronze", "raw_events", schema_fields)
mock_spark_session.storage.insert_data("bronze", "raw_events", sample_data)
bronze_data = mock_spark_session.storage.query_table("bronze", "raw_events")
exists = mock_spark_session.storage.table_exists("bronze", "raw_events")
```

**PySpark Equivalent:**
```python
# PySpark requires SQL/DataFrame API
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
df.write.saveAsTable("bronze.raw_events")
# No direct insert_data/query_table equivalent
```

### Recommended Solution

**Option A (Recommended):** Keep `spark.storage` as an extension API but document it clearly as mock-spark-specific. Consider making it opt-in via a flag or separate import.

**Option B:** Remove `spark.storage` entirely and provide helper utilities as separate functions/modules that work with standard PySpark APIs.

**Option C (Best for drop-in replacement):** Implement `spark.storage` as a thin wrapper around PySpark-compatible APIs, but document that PySpark users should use standard DataFrame/SQL APIs directly.

### Priority
**Medium** - This is a convenience API that doesn't break PySpark compatibility, but creates portability issues. Better documentation and examples showing PySpark equivalents would help.

### Recommendation
**Keep it, but:**
1. Add clear documentation that `spark.storage` is a mock-spark convenience API
2. Provide PySpark migration examples for each storage operation
3. Consider adding a compatibility mode that warns when `spark.storage` is used

---

## 3. Import Path Consistency

### Current Issue

**Problem:** Different import paths require conditional imports in user code.

**Current State:**
```python
import os

if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructType, StructField
else:
    from mock_spark.functions import F
    from mock_spark import StringType, StructType, StructField
```

### Recommended Solution

**Option A (Recommended):** Provide `pyspark.sql.functions` and `pyspark.sql.types` aliases in mock-spark:

```python
# mock-spark should support:
from pyspark.sql import functions as F  # Works via __init__.py alias
from pyspark.sql.types import StringType, StructType  # Works via types module alias
```

This could be implemented via:
1. A `pyspark` compatibility package/module
2. Or by making mock-spark installable as `pyspark` in test environments
3. Or by providing a compatibility shim package

**Option B:** Create a standardized compatibility layer package that both PySpark and mock-spark can implement.

### Priority
**Very High** - This is one of the most common sources of conditional code. Eliminating it would dramatically improve drop-in replacement status.

### Implementation Considerations
- PySpark's namespace is `pyspark`, not `mock_spark`
- Consider a `mock-spark[pyspark-alias]` extra that installs namespace packages
- May require Python namespace packages or `pkg_resources` namespace handling

---

## 4. Exception Type Alignment

### Current Issue

**Problem:** Different exception module paths and potentially different exception class signatures.

**Current State:**
```python
# PySpark
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

# mock-spark
from mock_spark.errors import AnalysisException, IllegalArgumentException

# SparkForge workaround
if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql.utils import AnalysisException
else:
    from mock_spark.errors import AnalysisException
```

### Recommended Solution

**Option A (Recommended):** Provide exceptions under `pyspark.sql.utils` namespace in mock-spark:

```python
# mock-spark should support:
from pyspark.sql.utils import AnalysisException, IllegalArgumentException
```

**Option B:** Ensure exception classes have identical signatures and behavior, and document the import path difference clearly.

### Priority
**High** - Exception handling is critical for production code, and path differences create maintenance burden.

### Additional Considerations
- Verify exception message formats match
- Ensure stack trace handling is consistent
- Test exception chaining behavior

---

## 5. SparkSession Context Management

### Current Issue

**Problem:** Column expressions (`F.col()`, etc.) sometimes fail with "No active SparkSession found" errors, particularly when used in closures or delayed evaluation contexts.

**Example Error:**
```
RuntimeError: Cannot perform column expression 'id': No active SparkSession found.
```

**Context:** This often occurs when:
- Column expressions are created outside the immediate execution context
- Used in lambda functions or closures
- Referenced in validation rules before DataFrame operations

### Recommended Solution

**Option A:** Implement a global SparkSession registry similar to PySpark's `SparkSession.getActiveSession()`:

```python
class SparkSession:
    _active_session = None
    
    @classmethod
    def getActiveSession(cls):
        """Get the currently active SparkSession (PySpark-compatible)."""
        return cls._active_session
    
    def __enter__(self):
        SparkSession._active_session = self
        return self
    
    def __exit__(self, *args):
        SparkSession._active_session = None
```

**Option B:** Ensure column expressions can work without an active session context (store session reference in Column objects).

### Priority
**Medium** - Affects advanced use cases but can usually be worked around. However, it's a common source of confusion for users porting PySpark code.

---

## 6. Type System Consistency

### Current Issue

**Problem:** Type imports come from different modules, and type behavior may differ slightly.

**Current State:**
```python
# PySpark
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

# mock-spark
from mock_spark import StringType, IntegerType, StructType, StructField
# OR
from mock_spark.spark_types import StringType, IntegerType, StructType, StructField
```

### Recommended Solution

1. **Consistent Import Path:** Support `from pyspark.sql.types import ...` (see Import Path Consistency above)
2. **Type Behavior Parity:** Ensure types behave identically:
   - String representation matches
   - Serialization format matches
   - Comparison behavior matches
   - Schema inference matches

### Priority
**High** - Types are fundamental to DataFrame operations and schema definitions.

---

## 7. Delta Lake Compatibility

### Current Issue

**Problem:** Delta Lake operations are PySpark-specific and not available in mock-spark. Code must handle missing Delta Lake gracefully.

**Example from SparkForge:**
```python
try:
    from delta.tables import DeltaTable
    HAS_DELTA = True
except (ImportError, AttributeError, RuntimeError):
    # Catch ImportError, AttributeError (delta-spark compatibility issues),
    # and RuntimeError (Spark session not initialized)
    DeltaTable = None
    HAS_DELTA = False
```

### Recommended Solution

**Option A:** Provide a mock Delta Lake implementation that supports basic operations (`DeltaTable.forPath()`, `merge()`, `update()`, `delete()`, `optimize()`, `vacuum()`).

**Option B:** Document Delta Lake as PySpark-only and provide clear guidance on feature detection.

**Option C:** Create a separate `mock-delta` package that provides Delta Lake compatibility.

### Priority
**Low-Medium** - Delta Lake is an optional feature, but many production pipelines depend on it. A basic mock implementation would significantly improve compatibility.

### Recommended Approach
Start with Option B (documentation), then consider Option A for a future release if there's sufficient demand.

---

## 8. Function API Parity

### Current Issue

**Problem:** Some PySpark functions may have different signatures or behavior in mock-spark.

**Areas to Verify:**
- `F.col()` behavior with nested column references
- Window functions (`F.row_number()`, `F.rank()`, etc.)
- Aggregation functions (`F.sum()`, `F.count()`, etc.)
- Date/time functions (`F.to_date()`, `F.date_format()`, etc.)
- String functions (`F.regexp_replace()`, `F.concat()`, etc.)
- Array/map functions (`F.explode()`, `F.map_keys()`, etc.)

### Recommended Solution

1. **Comprehensive Function Testing:** Create a test suite that exercises all PySpark functions with identical test cases for both PySpark and mock-spark.
2. **Documentation:** Clearly document any function limitations or differences.
3. **Deprecation Warnings:** Warn users when using functions with known differences.

### Priority
**High** - Functions are the core API that users interact with. Any differences create bugs that are hard to detect.

---

## 9. Testing Recommendations

### Current Gap

Testing compatibility between PySpark and mock-spark is currently ad-hoc and project-specific.

### Recommended Solution

Create a **PySpark Compatibility Test Suite** as part of mock-spark:

1. **Shared Test Cases:** Test cases that can run against both PySpark and mock-spark
2. **Automated Compatibility Checks:** CI/CD pipeline that runs the same tests against both implementations
3. **Regression Testing:** Ensure new mock-spark versions don't break compatibility

**Example Structure:**
```
mock-spark/
  tests/
    compatibility/
      test_functions.py      # Function API tests
      test_types.py          # Type system tests
      test_catalog.py        # Catalog API tests
      test_exceptions.py     # Exception tests
      test_dataframe.py      # DataFrame operation tests
      test_sql.py            # SQL compatibility tests
```

### Priority
**High** - Automated compatibility testing would catch regressions early and provide confidence to users.

---

## Implementation Roadmap

### Phase 1: Critical Path (Immediate Impact)
1. ✅ **Import Path Consistency** - Enable `from pyspark.sql import ...` imports
2. ✅ **Exception Type Alignment** - Provide exceptions under `pyspark.sql.utils`
3. ✅ **Catalog API Consistency** - Align `createDatabase()` behavior with PySpark

**Estimated Impact:** Eliminates 70% of conditional code in user projects.

### Phase 2: High Value (Short-term)
4. ✅ **Type System Consistency** - Verify and fix type behavior differences
5. ✅ **Function API Parity** - Comprehensive function testing and fixes
6. ✅ **SparkSession Context Management** - Fix column expression context issues

**Estimated Impact:** Eliminates 20% more conditional code, fixes edge cases.

### Phase 3: Feature Completion (Medium-term)
7. ✅ **Storage API Documentation** - Clear docs and PySpark migration examples
8. ✅ **Delta Lake Mock Implementation** - Basic Delta Lake support (optional)
9. ✅ **Compatibility Test Suite** - Automated compatibility testing

**Estimated Impact:** Completes drop-in replacement status, enables advanced features.

---

## Success Metrics

### Before Improvements
- **Conditional Code Required:** ~50+ locations in SparkForge codebase
- **Test Compatibility:** ~95% (with skip markers and workarounds)
- **API Parity:** ~85% (core APIs, missing some edge cases)

### After Phase 1
- **Conditional Code Required:** ~15 locations (70% reduction)
- **Test Compatibility:** ~98% (fewer skip markers needed)
- **API Parity:** ~92% (critical paths aligned)

### After Phase 3 (Full Drop-In Replacement)
- **Conditional Code Required:** <5 locations (90%+ reduction)
- **Test Compatibility:** 100% (no skip markers, full portability)
- **API Parity:** ~98% (all common use cases, Delta Lake optional)

---

## Example: Before and After

### Before (Current State)
```python
import os

# Conditional imports
if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructType
    from pyspark.sql.utils import AnalysisException
else:
    from mock_spark.functions import F
    from mock_spark import StringType, StructType
    from mock_spark.errors import AnalysisException

# Conditional database creation
def create_db(spark, name):
    if hasattr(spark.catalog, "createDatabase"):
        spark.catalog.createDatabase(name)
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")

# Conditional exception handling
try:
    df.select("missing_col")
except AnalysisException:
    pass  # Works in both
```

### After (With Improvements)
```python
# No conditional imports needed - works with both
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType
from pyspark.sql.utils import AnalysisException

# No conditional logic needed - same API
spark.sql("CREATE DATABASE IF NOT EXISTS test_schema")

# Same exception handling
try:
    df.select("missing_col")
except AnalysisException:
    pass  # Works identically in both
```

**Result:** Zero conditional code needed for core operations!

---

## Additional Considerations

### Backward Compatibility
- All changes should maintain backward compatibility with existing mock-spark APIs
- Consider deprecation periods for breaking changes
- Provide migration guides for affected APIs

### Performance
- Ensure compatibility improvements don't degrade mock-spark's performance advantages
- Mock-spark should remain significantly faster than PySpark for test scenarios

### Documentation
- Update documentation to highlight PySpark compatibility
- Provide migration guides for projects moving from PySpark to mock-spark
- Create "PySpark to mock-spark" compatibility checklist

### Community Feedback
- Engage with users who are using mock-spark as a PySpark replacement
- Collect feedback on compatibility pain points
- Prioritize improvements based on real-world usage patterns

---

## References

### SparkForge Compatibility Code
- `tests/conftest.py` - Test fixtures with conditional logic
- `tests/compat.py` - Compatibility layer implementation
- `src/pipeline_builder/compat/` - Compatibility abstractions

### Related Documents
- `docs/MOCK_SPARK_LIMITATIONS.md` - Current known limitations
- `docs/MOCK_SPARK_UPGRADE_PLAN.md` - Previous upgrade planning
- `docs/PYSPARK_OPTIONAL_MIGRATION.md` - PySpark integration approach

---

## Contact and Contributions

For questions or contributions related to this plan:
- **SparkForge Project:** [GitHub Repository]
- **Issues:** Please tag issues with `mock-spark-compatibility`
- **Feedback:** We welcome feedback from the mock-spark community

---

**Document Status:** Living document - will be updated as compatibility improvements are implemented and new issues are discovered.

**Last Updated:** 2024-12-11

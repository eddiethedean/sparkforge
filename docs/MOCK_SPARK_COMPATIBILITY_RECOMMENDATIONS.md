# Recommendations for mock-spark Developers: PySpark Compatibility

## Overview

This document outlines differences between mock-spark and PySpark that prevent drop-in replacement in testing scenarios. These recommendations are based on real-world usage patterns encountered when trying to use mock-spark as a testing replacement for PySpark.

**Status Update (2024)**: mock-spark 3.8.0 has addressed most of the critical compatibility issues! The following recommendations reflect the state prior to 3.8.0, and most have now been implemented.

**Goal**: Make mock-spark as close to a drop-in replacement for PySpark as possible, especially for unit testing scenarios where tests should work with either engine without modification.

## ✅ Fixed in mock-spark 3.8.0

The following issues have been resolved in mock-spark 3.8.0:

1. ✅ **Functions Module**: `mock_spark.sql.functions` is now available as a module (matching PySpark)
2. ✅ **Types Module**: `mock_spark.sql.types` is now available (matching PySpark structure)
3. ✅ **Utils Module**: `mock_spark.sql.utils` is now available for exceptions (matching PySpark structure)

**Imports now work identically:**
```python
# PySpark
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

# mock-spark 3.8.0+ (same structure!)
from mock_spark.sql import functions as F
from mock_spark.sql.types import StructType
from mock_spark.sql.utils import AnalysisException
```

The only difference is the package name (`pyspark` vs `mock_spark`), which is expected and acceptable.

## Priority Levels

- **🔴 Critical**: Blocks drop-in replacement, prevents tests from running
- **🟡 Important**: Causes test failures, requires workarounds
- **🟢 Enhancement**: Nice-to-have for better compatibility

---

## 1. Functions Module vs Class (✅ FIXED in 3.8.0)

### Status: ✅ RESOLVED

**mock-spark 3.8.0+ now provides:**
```python
from mock_spark.sql import functions  # This is a module (like PySpark!)
type(functions)  # <class 'module'>
```

**PySpark-compatible usage:**
```python
# Both work identically now:
from pyspark.sql import functions as F
from mock_spark.sql import functions as F  # Same structure!
```

### Historical Context (pre-3.8.0)

**Previous mock-spark:**
```python
from mock_spark import Functions  # This was a class
type(Functions)  # <class 'type'>
# Could be instantiated: Functions() works
```

**Problem (now fixed):**
Tests often checked `if callable(Functions)` or tried to instantiate `Functions()`, which worked with mock-spark but failed with PySpark.

**Current mock-spark:**
```python
# mock_spark/__init__.py
from mock_spark.functions import Functions

# Usage
from mock_spark import Functions
f = Functions()
```

**Recommended:**
```python
# mock_spark/__init__.py
from mock_spark import functions  # Export as module

# Usage (matches PySpark)
from mock_spark.sql import functions as F
# Or for backward compatibility:
from mock_spark import functions as F
```

**Backward Compatibility**: If the `Functions` class is needed internally, keep it but deprecate direct instantiation. Provide `functions` as the primary interface.

---

## 2. Type Import Structure (✅ FIXED in 3.8.0)

### Status: ✅ RESOLVED

**mock-spark 3.8.0+ now provides:**
```python
from mock_spark.sql.types import StructType, StructField, StringType
# Types are in mock_spark.sql.types module (matching PySpark structure!)
```

**PySpark-compatible usage:**
```python
# Both work identically now:
from pyspark.sql.types import StructType, StructField, StringType
from mock_spark.sql.types import StructType, StructField, StringType  # Same path!
```

### Historical Context (pre-3.8.0)

**Previous mock-spark:**
```python
# Option 1: From main module
from mock_spark import StructType, StructField, StringType

# Option 2: From spark_types submodule
from mock_spark.spark_types import StructType, StructField, StringType
```

**Problem (now fixed):**
PySpark's `createDataFrame()` is strict about type objects. It uses `isinstance()` checks that verify the types are actual PySpark types. When mock-spark types were passed to a real PySpark session, they were rejected, causing tests to fail.

**Example:**
```python
# This fails when spark_session is PySpark but types are from mock-spark
schema = StructType([StructField("id", StringType())])  # mock-spark types
df = spark_session.createDataFrame(data, schema)  # TypeError
```

### Recommended Change

1. **Match PySpark's module structure**: Ensure types can be imported from `mock_spark.sql.types` (or at least provide this as an alias)

2. **Type identity**: Types should pass `isinstance()` checks when used with the corresponding mock-spark session. The type objects should be compatible.

**Recommended structure:**
```python
# mock_spark/sql/types.py (matching PySpark structure)
from mock_spark.spark_types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    # ... etc
)
```

**Usage (matches PySpark exactly):**
```python
from mock_spark.sql.types import StructType, StructField, StringType
```

---

## 3. Exception Module Location (✅ FIXED in 3.8.0)

### Status: ✅ RESOLVED

**mock-spark 3.8.0+ now provides:**
```python
from mock_spark.sql.utils import AnalysisException
# Exceptions are in mock_spark.sql.utils (matching PySpark structure!)
```

**PySpark-compatible usage:**
```python
# Both work identically now:
from pyspark.sql.utils import AnalysisException
from mock_spark.sql.utils import AnalysisException  # Same path!
```

**Backward compatibility:** The old `mock_spark.errors` path still works for older code.

### Historical Context (pre-3.8.0)

**Previous mock-spark:**
```python
from mock_spark.errors import AnalysisException
```

**Problem (now fixed):**
Tests needed to conditionally import exceptions, which required code changes when switching between engines.

### Recommended Change

Provide exceptions in a location that matches PySpark's structure.

**Recommended:**
```python
# mock_spark/sql/utils.py (matching PySpark)
from mock_spark.errors import AnalysisException

# Also available for backward compatibility
from mock_spark.errors import AnalysisException
```

**Usage (matches PySpark):**
```python
from mock_spark.sql.utils import AnalysisException
```

**Backward Compatibility**: Keep `mock_spark.errors` for backward compatibility, but recommend using the PySpark-compatible path.

---

## 4. SparkSession.storage API (🟢 Enhancement)

### Current State

**PySpark:**
```python
# No .storage attribute
spark_session.storage  # AttributeError
# Use SQL commands instead:
spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")
```

**mock-spark:**
```python
# Has .storage attribute
spark_session.storage.create_schema("test_schema")
spark_session.storage.create_table("test_schema", "table", schema)
```

### Problem

Tests written for mock-spark that use `.storage` fail with PySpark, requiring conditional logic or skipping tests.

### Recommended Approach

This is **acceptable as an enhancement** - mock-spark can keep `.storage` as a convenience API. However:

1. **Document it clearly** as mock-spark-specific
2. **Consider adding SQL compatibility**: Allow `.sql()` commands to work with `.storage` operations

**Example:**
```python
# Both should work in mock-spark
spark_session.storage.create_schema("test_schema")
spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")
```

This way, code written for PySpark (using SQL) works with mock-spark, and code written for mock-spark (using `.storage`) also works.

---

## 5. Module Package Structure (🟡 Important)

### Current State

**PySpark structure:**
```
pyspark/
  sql/
    __init__.py
    functions.py (module)
    types.py
    utils.py
    session.py (SparkSession)
```

**mock-spark structure:**
```
mock_spark/
  __init__.py (exports everything)
  functions.py (Functions class)
  spark_types.py (types)
  errors.py (exceptions)
```

### Recommended Structure

Provide a `mock_spark.sql` package that mirrors PySpark's structure:

```
mock_spark/
  __init__.py
  sql/
    __init__.py
    functions.py (module, not class)
    types.py
    utils.py
    session.py
  # Backward compatibility - re-export from new locations
  functions.py -> sql/functions.py
  spark_types.py -> sql/types.py
  errors.py -> sql/utils.py (for exceptions)
```

This allows:
```python
# Works with both PySpark and mock-spark
from pyspark.sql.types import StructType
from mock_spark.sql.types import StructType  # Same import path
```

---

## 6. Type Checking and isinstance() Compatibility (🔴 Critical)

### Problem

PySpark's `createDataFrame()` checks:
```python
if not isinstance(schema, StructType):
    raise TypeError(f"schema should be StructType, got {type(schema)}")
```

When mock-spark types are passed to PySpark sessions, `isinstance()` returns `False` because they're different classes.

### Current Workaround

We work around this by ensuring types match the session:
```python
if SPARK_MODE == "real":
    from pyspark.sql.types import StructType
else:
    from mock_spark import StructType
```

### Recommended Approach

This is challenging because it requires type objects to be recognized across different implementations. Some options:

1. **Document limitation**: Clearly state that types must match the session
2. **Adapter layer**: Provide type adapters that convert mock-spark types to PySpark-compatible types (complex)
3. **Duck typing**: Make mock-spark types pass PySpark's checks (may require extending PySpark types)

**Priority**: Lower - This is a fundamental limitation that may be acceptable if clearly documented.

---

## 7. Exception Class Hierarchy (🟢 Enhancement)

### Current State

**PySpark:**
```python
AnalysisException  # Extends Exception
```

**mock-spark:**
```python
AnalysisException  # Should extend Exception with similar behavior
```

### Recommendation

Ensure exception classes have the same inheritance hierarchy and similar attributes as PySpark exceptions. This helps exception handling code work the same way.

---

## 8. Import Aliases and Compatibility (🟡 Important)

### Recommendation

Provide import aliases that match PySpark's common import patterns:

```python
# mock_spark/__init__.py
from mock_spark.sql.types import *
from mock_spark.sql import functions as F
from mock_spark.sql.utils import AnalysisException

# Allow both:
from mock_spark import StructType  # Convenience
from mock_spark.sql.types import StructType  # PySpark-compatible
```

---

## Implementation Priority

### Phase 1: Critical Fixes
1. ✅ Make `functions` a module (not a class)
2. ✅ Provide `mock_spark.sql.types` module structure
3. ✅ Provide `mock_spark.sql.utils` for exceptions

### Phase 2: Important Improvements
4. ✅ Document `.storage` API as mock-spark-specific
5. ✅ Ensure SQL commands work alongside `.storage`
6. ✅ Add import aliases for PySpark compatibility

### Phase 3: Enhancements
7. ✅ Better exception class compatibility
8. ✅ Consider type adapter layer (if feasible)

---

## Testing Compatibility Checklist

To verify compatibility, tests should be able to switch between engines using only an environment variable:

```python
# This should work with BOTH engines without code changes:
import os
SPARK_MODE = os.environ.get("SPARK_MODE", "mock")

if SPARK_MODE == "real":
    from pyspark.sql.types import StructType
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException
else:
    from mock_spark.sql.types import StructType  # Same path!
    from mock_spark.sql import functions as F     # Same path!
    from mock_spark.sql.utils import AnalysisException  # Same path!
```

Ideally, even this wouldn't be necessary - imports would be identical:
```python
# Works with both (ideal, may not be achievable)
from pyspark.sql.types import StructType  # Or mock_spark.sql.types
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
```

---

## Summary

The main barriers to drop-in replacement are:

1. **Functions as class vs module** - Most critical issue
2. **Type import structure** - Needs to match PySpark's module layout
3. **Exception location** - Should be in `sql.utils` for compatibility
4. **Module structure** - `mock_spark.sql` package would help immensely

By addressing these issues, mock-spark would become a true drop-in replacement for PySpark in testing scenarios, eliminating the need for conditional imports and making test code cleaner and more maintainable.

---

## Additional Notes

### Version Compatibility

Consider maintaining compatibility with multiple PySpark versions. The structure described here matches PySpark 3.2.x, but check compatibility with:
- PySpark 3.0.x
- PySpark 3.1.x
- PySpark 3.3.x
- PySpark 3.4.x+

### Documentation

When these changes are implemented, update mock-spark documentation to:
1. Emphasize PySpark compatibility
2. Provide migration guide from PySpark to mock-spark
3. Document mock-spark-specific features (like `.storage`) clearly
4. Provide examples showing drop-in replacement scenarios

### Breaking Changes

Some of these recommendations may require breaking changes. Consider:
1. Deprecation periods for old import paths
2. Clear migration guides
3. Version numbering to indicate breaking changes
4. Backward compatibility shims where possible

---

## Contact

If mock-spark developers need clarification on any of these recommendations or want to discuss implementation strategies, please reach out. We're happy to help test compatibility and provide feedback on proposed changes.


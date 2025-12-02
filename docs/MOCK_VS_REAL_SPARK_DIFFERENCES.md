# Mock-Spark vs Real PySpark: Test Compatibility Guide

## Overview

Tests in this codebase need to work with both **mock-spark** (for fast testing) and **real PySpark** (for integration testing). This document outlines the key differences that cause test failures and how to fix them.

## Key Differences

### 1. Type Imports

**Problem:** Hardcoded imports from `mock_spark` don't work with real PySpark.

**❌ Wrong:**
```python
from mock_spark import StructType, StructField, StringType
```

**✅ Correct:**
```python
import os

if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql.types import StructType, StructField, StringType
else:
    from mock_spark import StructType, StructField, StringType
```

**Alternative (using module-level imports):**
```python
# At module level, check both SPARK_MODE and SPARKFORGE_ENGINE
_SPARK_MODE = os.environ.get("SPARK_MODE", "mock").lower()
_ENGINE = os.environ.get("SPARKFORGE_ENGINE", "auto").lower()

use_real_spark = _SPARK_MODE == "real" or _ENGINE in ("pyspark", "spark", "real")

if use_real_spark:
    from pyspark.sql.types import StructType, StructField, StringType
else:
    from mock_spark.spark_types import StructType, StructField, StringType
```

### 2. Type Compatibility

**Problem:** PySpark's `createDataFrame()` is strict about type objects. It won't accept a `mock_spark.spark_types.StructType` when running with real PySpark.

**Solution:** Always use the types that match your `spark_session` type. If `SPARK_MODE=real`, use PySpark types; otherwise use mock-spark types.

### 3. Mock-Spark Specific APIs

**Problem:** Mock-spark has APIs that real PySpark doesn't have, like `.storage`:

```python
# Mock-spark only
spark_session.storage.create_schema("test_schema")
spark_session.storage.create_table("test_schema", "table", schema_fields)
```

**✅ Solution - Skip or Adapt:**

**Option 1: Skip test in real mode**
```python
import os
import pytest

def test_mock_only_feature(spark_session):
    if os.environ.get("SPARK_MODE", "mock").lower() == "real":
        pytest.skip("This test uses mock-spark specific APIs")
    
    # Test code using .storage API
    spark_session.storage.create_schema("test_schema")
```

**Option 2: Adapt for both**
```python
import os

def test_table_operations(spark_session):
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
    
    if spark_mode == "real":
        # Use PySpark SQL
        spark_session.sql("CREATE DATABASE IF NOT EXISTS test_schema")
        # ... use SQL commands
    else:
        # Use mock-spark storage API
        spark_session.storage.create_schema("test_schema")
        # ... use storage API
```

### 4. Exception Types

**Problem:** Exception class names differ between mock-spark and PySpark.

**❌ Wrong:**
```python
from mock_spark.errors import AnalysisException
```

**✅ Correct:**
```python
import os

if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql.utils import AnalysisException
else:
    from mock_spark.errors import AnalysisException
```

## Common Fixes Applied

### Fix 1: Module-Level Type Imports

**File:** `tests/unit/test_validation.py`

Changed from checking only `SPARKFORGE_ENGINE` to checking both `SPARK_MODE` and `SPARKFORGE_ENGINE`:

```python
# Before
_ENGINE = os.environ.get("SPARKFORGE_ENGINE", "auto").lower()
if _ENGINE in ("pyspark", "spark", "real"):
    from pyspark.sql.types import ...

# After
_SPARK_MODE = os.environ.get("SPARK_MODE", "mock").lower()
_ENGINE = os.environ.get("SPARKFORGE_ENGINE", "auto").lower()
use_real_spark = _SPARK_MODE == "real" or _ENGINE in ("pyspark", "spark", "real")
if use_real_spark:
    from pyspark.sql.types import ...
```

### Fix 2: Fixture-Level Type Imports

**File:** `tests/unit/test_validation.py` - `sample_dataframe` fixture

Changed from hardcoded mock-spark imports to checking `SPARK_MODE`:

```python
# Before
from mock_spark import StructField, StructType

# After
spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
if spark_mode == "real":
    from pyspark.sql.types import StructType, StructField, ...
else:
    from mock_spark.spark_types import StructType, StructField, ...
```

### Fix 3: Hardcoded Module-Level Imports

**File:** `tests/unit/test_execution_final_coverage.py`

Added conditional import based on `SPARK_MODE`:

```python
# Before
from mock_spark import IntegerType, StructField, StructType, StringType

# After
import os
if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType
else:
    from mock_spark import IntegerType, StructField, StructType, StringType
```

## Testing Checklist

When fixing tests to work with both mock-spark and real PySpark:

- [ ] Check all `from mock_spark import` statements
- [ ] Verify type imports match the `spark_session` type
- [ ] Replace mock-spark specific APIs (`.storage`) with conditional logic
- [ ] Test with `SPARK_MODE=mock` to ensure mock mode still works
- [ ] Test with `SPARK_MODE=real` to ensure real mode works

## Running Tests

```bash
# Test with mock-spark (default)
python -m pytest tests/unit/test_validation.py

# Test with real PySpark
SPARK_MODE=real python -m pytest tests/unit/test_validation.py

# Test both modes
SPARK_MODE=mock python -m pytest tests/unit/test_validation.py
SPARK_MODE=real python -m pytest tests/unit/test_validation.py
```

## Summary

The main issue causing tests to fail with real PySpark but pass with mock-spark is:

1. **Hardcoded type imports** - Always check `SPARK_MODE` before importing types
2. **Type incompatibility** - PySpark won't accept mock-spark type objects
3. **Mock-specific APIs** - Use conditional logic or skip tests that use mock-only features

By following these patterns, tests can work with both mock-spark and real PySpark seamlessly.


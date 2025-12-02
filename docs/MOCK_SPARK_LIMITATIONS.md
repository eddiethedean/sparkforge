# Mock-Spark Limitations

> **Note**: With mock-spark 3.7+, most limitations described in this document have been fixed. Mock-spark 3.7+ uses a Polars backend instead of DuckDB, which resolved threading issues and improved compatibility. This document is kept for historical reference.

This document describes known limitations when using SparkForge with mock-spark instead of real PySpark.

## Overview

SparkForge supports running with either PySpark or mock-spark. While mock-spark provides excellent compatibility for most operations, there are some limitations due to differences in the underlying backend compared to Apache Spark. Most of these limitations have been resolved in mock-spark 3.7+.

## Known Limitations

### 1. regexp_replace SQL Syntax

**Issue**: The DuckDB backend doesn't support PySpark's `regexp_replace` function syntax when converted to SQL.

**Error Example**:
```
(duckdb.duckdb.ParserException) Parser Error: syntax error at or near "regexp_replace"
[SQL: (("order_date" regexp_replace ('\\.\\d+', '')) to_timestamp yyyy-MM-dd'T'HH:mm:ss)]
```

**Root Cause**: When PySpark column operations are converted to SQL for execution in DuckDB, the `regexp_replace` function syntax is not compatible with DuckDB's SQL parser.

**Workarounds**:
1. **Pre-process data**: Clean timestamps/dates before passing to SparkForge
2. **Use string manipulation functions**: Instead of `regexp_replace`, use other string functions that are compatible
3. **Run with real PySpark**: For pipelines that require `regexp_replace`, use `SPARKFORGE_ENGINE=pyspark`

**Affected Tests**: Some builder tests that use `regexp_replace` for date/timestamp parsing are skipped in mock mode.

### 2. AnalysisException Constructor

> **Status**: Fixed in mock-spark 3.7+

**Issue**: In older versions of mock-spark, `AnalysisException` required a `stackTrace` parameter, unlike PySpark's optional parameter. This has been fixed in mock-spark 3.7+.

**Historical Usage** (mock-spark < 3.7):
```python
from mock_spark.errors import AnalysisException
exc = AnalysisException("Table not found", None)  # stackTrace required (can be None)
```

**Current Usage** (mock-spark 3.7+):
```python
from pipeline_builder.compat import AnalysisException
exc = AnalysisException("Table not found")  # stackTrace is now optional, matching PySpark
```

**Note**: The `pipeline_builder.compat` module provides a compatibility layer that handles this automatically.

## Checking Current Engine

To check which engine is currently active:

```python
from pipeline_builder.compat import is_mock_spark, compat_name

if is_mock_spark():
    print(f"Running in mock mode: {compat_name()}")
else:
    print(f"Running in PySpark mode: {compat_name()}")
```

## Environment Variables

Set the engine explicitly using the `SPARKFORGE_ENGINE` environment variable:

- `SPARKFORGE_ENGINE=mock` - Force mock-spark
- `SPARKFORGE_ENGINE=pyspark` - Force PySpark
- `SPARKFORGE_ENGINE=auto` - Auto-detect (default, prefers PySpark if available)

## Recommendations

1. **Development/Testing**: Use mock-spark for faster iteration and testing
2. **Production**: Use real PySpark for full feature support
3. **CI/CD**: Can use mock-spark for most tests, but run integration tests with PySpark
4. **When in doubt**: Test with both engines to ensure compatibility

## Contributing

If you discover additional limitations, please:
1. Document them in this file
2. Add appropriate skip markers to affected tests
3. Update the compatibility layer if possible


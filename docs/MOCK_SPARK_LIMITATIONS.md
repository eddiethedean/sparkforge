# Mock-Spark Limitations

This document describes known limitations when using SparkForge with mock-spark (DuckDB backend) instead of real PySpark.

## Overview

SparkForge supports running with either PySpark or mock-spark. While mock-spark provides excellent compatibility for most operations, there are some limitations due to differences in the underlying DuckDB backend compared to Apache Spark.

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

**Issue**: mock-spark's `AnalysisException` requires a `stackTrace` parameter, unlike PySpark's optional parameter.

**PySpark Usage**:
```python
from pyspark.sql.utils import AnalysisException
exc = AnalysisException("Table not found")  # stackTrace is optional
```

**mock-spark Usage**:
```python
from mock_spark.errors import AnalysisException
exc = AnalysisException("Table not found", None)  # stackTrace required (can be None)
```

**Solution**: When creating `AnalysisException` instances, always pass `None` as the second argument for compatibility:
```python
from pipeline_builder.compat import AnalysisException
exc = AnalysisException("Table not found", None)
```

**Note**: The `pipeline_builder.compat` module provides a compatibility layer that handles this automatically in most cases.

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


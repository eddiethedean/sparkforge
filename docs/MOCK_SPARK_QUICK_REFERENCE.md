# Quick Reference: mock-spark → PySpark Compatibility Issues

## ✅ Status Update: mock-spark 3.8.0

**Most compatibility issues have been resolved in mock-spark 3.8.0!** This document now reflects what was fixed.

## Top 5 Critical Issues (Historical - Fixed in 3.8.0)

### 1. ✅ Functions: Class vs Module (FIXED in 3.8.0)
**Status**: ✅ Resolved
```python
# mock-spark 3.8.0+ (matches PySpark!)
from mock_spark.sql import functions as F
# F is a module, not a class - matches PySpark exactly!

# PySpark
from pyspark.sql import functions as F
# Same structure, only package name differs
```

### 2. ✅ Type Import Path (FIXED in 3.8.0)
**Status**: ✅ Resolved
```python
# mock-spark 3.8.0+ (matches PySpark!)
from mock_spark.sql.types import StructType

# PySpark
from pyspark.sql.types import StructType
# Same import path structure!
```

### 3. ✅ Exception Location (FIXED in 3.8.0)
**Status**: ✅ Resolved
```python
# mock-spark 3.8.0+ (matches PySpark!)
from mock_spark.sql.utils import AnalysisException

# PySpark
from pyspark.sql.utils import AnalysisException
# Same import path structure!
```

### 4. 🟡 Type Compatibility
**Problem**: PySpark rejects mock-spark type objects
```python
# This fails when types don't match session
schema = StructType(...)  # mock-spark type
df = pyspark_session.createDataFrame(data, schema)  # TypeError
```

### 5. 🟢 Storage API
**Problem**: mock-spark has `.storage`, PySpark doesn't
```python
# mock-spark only
spark.storage.create_schema("test")
```

## Recommended Module Structure

```
mock_spark/
  sql/
    __init__.py
    functions.py      # Module (not class)
    types.py          # All type definitions
    utils.py          # Exceptions (AnalysisException, etc.)
    session.py        # SparkSession
  # Backward compatibility exports
  __init__.py         # Re-exports from sql.*
```

## ✅ Ideal Import Compatibility (Achieved in 3.8.0!)

Both now work identically in mock-spark 3.8.0+:

```python
# PySpark
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# mock-spark 3.8.0+
from mock_spark.sql.types import StructType    # ✅ Same path!
from mock_spark.sql import functions as F      # ✅ Same path!
from mock_spark.sql.utils import AnalysisException  # ✅ Same path!
```

**The only difference is the package name (`pyspark` vs `mock_spark`)**, which is expected and acceptable.

## See Also

- Full document: `MOCK_SPARK_COMPATIBILITY_RECOMMENDATIONS.md`
- User guide: `MOCK_VS_REAL_SPARK_DIFFERENCES.md`



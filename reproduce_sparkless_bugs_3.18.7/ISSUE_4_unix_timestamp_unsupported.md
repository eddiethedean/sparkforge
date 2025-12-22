# Unsupported function: unix_timestamp

## Description

The `unix_timestamp()` function is not supported in sparkless, even though it's a standard PySpark function.

## Version

sparkless 3.18.7

## Reproduction

```python
from sparkless import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.appName("bug").getOrCreate()

# Create test data
data = []
for i in range(10):
    data.append({
        "event_id": f"EVT-{i:03d}",
        "event_timestamp": datetime.now(),
    })

df = spark.createDataFrame(data, ["event_id", "event_timestamp"])

# Try to use unix_timestamp() (THIS TRIGGERS THE BUG)
result_df = df.withColumn(
    "unix_ts",
    F.unix_timestamp(F.col("event_timestamp"))  # ❌ Error here
)
```

## Expected Behavior

`unix_timestamp()` should be supported and return the Unix timestamp (seconds since epoch) for the given timestamp column.

## Actual Behavior

```
ValueError: Unsupported function: unix_timestamp
```

## Root Cause

The `unix_timestamp()` function is not implemented in sparkless, even though it's a standard PySpark function.

## Workaround

None known. The function needs to be implemented in sparkless.

## Additional Notes

- `unix_timestamp()` is a commonly used function in PySpark
- This prevents migration of PySpark code that uses this function
- The function should be added to sparkless's function library


# to_date() requires StringType or DateType input, got TimestampType

## Description

`to_date()` function in sparkless doesn't accept `TimestampType` as input, even though PySpark does. This requires an unnecessary cast to string.

## Version

sparkless 3.18.7

## Reproduction

```python
from sparkless import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.appName("bug").getOrCreate()

# Create test data with timestamp
data = []
for i in range(10):
    data.append({
        "event_id": f"EVT-{i:03d}",
        "event_timestamp": datetime.now(),
    })

df = spark.createDataFrame(data, ["event_id", "event_timestamp"])

# Parse timestamp (creates TimestampType column)
parsed_df = df.withColumn(
    "event_timestamp_parsed",
    F.to_timestamp(F.col("event_timestamp").cast("string"), "yyyy-MM-dd HH:mm:ss")
)

# Apply to_date() on TimestampType column (THIS TRIGGERS THE BUG)
result_df = parsed_df.withColumn(
    "event_date",
    F.to_date(F.col("event_timestamp_parsed"))  # ❌ Error here
)
```

## Expected Behavior

`to_date()` should accept `TimestampType` as input, just like PySpark does. The function should extract the date part from a timestamp.

## Actual Behavior

```
ValueError: to_date() requires StringType or DateType input, got TimestampType(nullable=True). Cast the column to string first: F.col('event_timestamp_parsed').cast('string')
```

## Root Cause

sparkless's `to_date()` function only accepts `StringType` or `DateType`, but not `TimestampType`. This is inconsistent with PySpark's behavior.

## Workaround

Cast to string first (but this shouldn't be necessary):

```python
result_df = parsed_df.withColumn(
    "event_date",
    F.to_date(F.col("event_timestamp_parsed").cast("string"))
)
```

However, even this workaround may fail in some cases.

## Additional Notes

- PySpark's `to_date()` accepts `TimestampType` directly
- This is a common pattern in data pipelines (parse timestamp, then extract date)
- The workaround adds unnecessary complexity and may have performance implications


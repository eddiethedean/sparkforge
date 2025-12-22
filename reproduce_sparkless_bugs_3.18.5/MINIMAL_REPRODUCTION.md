# Minimal Reproduction: sparkless Bug with Dropped Columns

## Bug Confirmed ✅

The bug **DOES reproduce** with pure sparkless code when using a larger dataset (150+ rows).

## Minimal Reproduction Code

```python
from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug_reproduction").getOrCreate()

# Create test data (150 rows - bug occurs with larger datasets)
data = []
for i in range(150):
    data.append({
        "impression_id": f"IMP-{i:08d}",
        "campaign_id": f"CAMP-{i % 10:02d}",
        "customer_id": f"CUST-{i % 40:04d}",
        "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
        "channel": ["google", "facebook", "twitter", "email", "display"][i % 5],
        "ad_id": f"AD-{i % 20:03d}",
        "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
        "device_type": ["desktop", "mobile", "tablet"][i % 3],
    })

bronze_df = spark.createDataFrame(data, [
    "impression_id",
    "campaign_id",
    "customer_id",
    "impression_date",  # This column will be dropped
    "channel",
    "ad_id",
    "cost_per_impression",
    "device_type",
])

# Apply transform that uses impression_date then drops it
silver_df = (
    bronze_df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
    .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
    .withColumn("is_mobile", F.when(F.col("device_type") == "mobile", True).otherwise(False))
    .select(
        "impression_id",
        "campaign_id",
        "customer_id",
        "impression_date_parsed",  # New column
        "hour_of_day",
        "day_of_week",
        "channel",
        "ad_id",
        "cost_per_impression",
        "device_type",
        "is_mobile",
        # impression_date is DROPPED - not in select list
    )
)

# ERROR: Any operation that triggers materialization fails
try:
    count = silver_df.count()  # ❌ Fails
except Exception as e:
    print(f"Error: {e}")
    # Error: cannot resolve 'impression_date' given input columns: 
    # [impression_id, campaign_id, customer_id, impression_date_parsed, ...]

try:
    cached = silver_df.cache()  # ❌ Fails
except Exception as e:
    print(f"Error: {e}")
    # Error: cannot resolve 'impression_date' given input columns: [...]

try:
    collected = silver_df.collect()  # ❌ Fails
except Exception as e:
    print(f"Error: {e}")
    # Error: cannot resolve 'impression_date' given input columns: [...]

spark.stop()
```

## Root Cause

1. **Transform uses a column** (e.g., `impression_date`) in operations like `F.regexp_replace(F.col("impression_date"), ...)`
2. **Transform drops the column** via `.select()` (excluding it from the final column list)
3. **Materialization is triggered** when calling operations like `count()`, `cache()`, or `collect()`
4. **During materialization**, sparkless validates all column expressions in the execution plan
5. **It tries to resolve the dropped column** (`impression_date`) which no longer exists
6. **Error**: `"cannot resolve 'impression_date' given input columns: [...]"`

## Key Finding

- **With 2 rows**: Operations work fine ✅
- **With 150+ rows**: Operations fail ❌

This suggests the bug is related to how sparkless handles execution plans with larger datasets, possibly due to:
- Plan optimization that kicks in with more data
- Different evaluation strategy for larger DataFrames
- Or some threshold-based behavior

## Error Location

The error occurs in sparkless's column validation during materialization:
- `sparkless/dataframe/validation/column_validator.py:72` - `validate_column_exists()`
- Called from materialization process when validating expression columns
- Doesn't account for columns that were dropped via `.select()`

## Impact

Any operation that triggers materialization fails when:
- A column is used in transformations
- That column is then dropped via `.select()`
- The DataFrame has a sufficient number of rows (150+)

This is a common pattern in data pipelines where raw columns are transformed and then dropped.


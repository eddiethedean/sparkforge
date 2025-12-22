# Validation fails with "cannot resolve" when validating after transform that drops columns

## Description

When validating a DataFrame after a transform that uses a column and then drops it, sparkless tries to resolve the dropped column during validation, causing a "cannot resolve" error.

## Version

sparkless 3.18.7

## Reproduction

```python
from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug").getOrCreate()

# Create test data (150 rows - bug manifests with larger datasets)
data = []
for i in range(150):
    data.append({
        "impression_id": f"IMP-{i:08d}",
        "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
    })

bronze_df = spark.createDataFrame(data, ["impression_id", "impression_date"])

# Transform that uses impression_date then drops it
silver_df = (
    bronze_df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .select("impression_id", "impression_date_parsed")  # impression_date is DROPPED
)

# Validation (THIS TRIGGERS THE BUG)
validation_predicate = F.col("impression_id").isNotNull() & F.col("impression_date_parsed").isNotNull()

valid_df = silver_df.filter(validation_predicate)  # ❌ Error here
count = valid_df.count()
```

## Expected Behavior

Validation should succeed. The validation only references columns that exist (`impression_id` and `impression_date_parsed`), and `impression_date` was already dropped in the transform.

## Actual Behavior

```
sparkless.core.exceptions.operation.SparkColumnNotFoundError: cannot resolve 'impression_date' given input columns: [impression_id, impression_date_parsed]
```

## Root Cause

When sparkless validates column expressions during materialization, it appears to:
1. Check all column references in the execution plan
2. Include references from the transform (even though the column was dropped)
3. Try to resolve `impression_date` which was used in the transform but dropped

## Workaround

None known. The bug occurs during validation, which is a common operation in data pipelines.

## Additional Notes

- Simple operations like `count()`, `cache()`, `collect()` work fine
- The bug only manifests when validating after transforms that drop columns
- The bug is more likely to occur with larger datasets (150+ rows)


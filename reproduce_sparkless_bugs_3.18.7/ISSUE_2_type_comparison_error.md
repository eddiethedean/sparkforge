# Type comparison error: "cannot compare string with numeric type (i32)"

## Description

sparkless treats numeric columns as strings, causing type comparison errors when comparing numeric columns with numbers.

## Version

sparkless 3.18.7

## Reproduction

```python
from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName("bug").getOrCreate()

# Create test data with numeric column
data = []
for i in range(10):
    data.append({
        "id": f"ID-{i:03d}",
        "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
    })

df = spark.createDataFrame(data, ["id", "cost_per_impression"])

# Check schema
print(df.schema)
# Output: StructType([StructField(name='id', dataType=StringType(nullable=True), nullable=True), 
#                      StructField(name='cost_per_impression', dataType=StringType(nullable=True), nullable=True)])

# Try to compare numeric column with number (THIS TRIGGERS THE BUG)
result_df = df.filter(F.col("cost_per_impression") >= 0)  # ❌ Error here
count = result_df.count()
```

## Expected Behavior

The comparison should succeed. `cost_per_impression` should be treated as a numeric type (DoubleType or DecimalType), and the comparison `>= 0` should work.

## Actual Behavior

```
polars.exceptions.ComputeError: cannot compare string with numeric type (i32)
```

The schema shows `cost_per_impression` as `StringType`, but it should be numeric.

## Root Cause

sparkless infers all columns as `StringType` when creating DataFrames from Python dictionaries, even when the values are numeric. This causes type mismatches when performing numeric operations.

## Workaround

Explicitly cast the column to numeric type:

```python
result_df = df.filter(F.col("cost_per_impression").cast("double") >= 0)
```

However, this shouldn't be necessary - sparkless should infer numeric types correctly.

## Additional Notes

- This affects validation rules that use numeric comparisons (e.g., `["gte", 0]`)
- The workaround works but adds unnecessary complexity
- PySpark correctly infers numeric types from numeric values


# PySpark parity: `date_trunc` in API but unsupported at runtime (Polars backend)

## Summary

`F.date_trunc` is exposed on the sparkless functions module (API parity with PySpark), but the Polars backend does not implement it at materialization time. Code that runs with PySpark fails when run with sparkless with:

```text
ValueError: Unsupported function: date_trunc
```

This is a **runtime parity gap**: the function exists for import/call but is not implemented in the expression translator/materializer.

## Environment

- **sparkless version:** 3.19.1 (observed; may affect other versions)
- **Backend:** Polars (default)
- **Python:** 3.9+

## Steps to reproduce

```python
from sparkless import functions as F
from sparkless.session import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("2024-03-15",)], ["d"])
df = df.withColumn("d", F.to_date("d")).withColumn(
    "month", F.date_trunc("month", F.col("d"))
)
df.show()
```

## Expected behavior

- Same as PySpark: the DataFrame is materialized and `show()` prints a row with a truncated date (e.g. first day of month) in the `month` column.

## Actual behavior

- During materialization, the Polars backend raises:

```text
ValueError: Unsupported function: date_trunc
```

Stack trace points to the expression translator, e.g.:

```text
  ...
  File ".../sparkless/backend/polars/expression_translator.py", line 4517, in _translate_function_call
    raise ValueError(f"Unsupported function: {function_name}")
ValueError: Unsupported function: date_trunc
```

## Notes

- `date_trunc` appears in the sparkless functions namespace (e.g. `"date_trunc" in dir(F)` is true), so the gap is between API presence and backend support, not a missing export.
- PySpark’s `date_trunc(format, timestamp)` is commonly used in pipelines (e.g. truncating to `"month"` or `"day"` for aggregations). Supporting it in the Polars backend would improve drop-in compatibility for tests and local runs that use sparkless.

## Possible resolution

- Implement `date_trunc` in the Polars expression translator/materializer so that calls to `F.date_trunc(format, col)` are translated and executed correctly (e.g. truncate to the first day of month/day/etc. as in PySpark).

Thank you for maintaining sparkless.

# Validation Edge Cases and Troubleshooting

This document covers edge cases in validation and how the framework handles them.

## Column Validation After Transform Drops Column

### Problem

When a Silver step transform drops a column (e.g., `impression_date`), validation rules that reference it will fail because validation happens on the transformed output DataFrame, not the input.

### Solution

The framework now automatically filters out validation rules for columns that don't exist in the transformed output, with a warning. This allows validation to continue with the remaining rules.

### Example

```python
# Transform drops the incremental column
def transform(spark, df, silvers):
    return df.select("user_id", "value")  # Drops "timestamp"

# Validation rules reference the dropped column
rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()],  # This column was dropped
}

# Framework automatically filters out "timestamp" rule and continues
# A warning is logged: "Filtered out rules for missing columns: ['timestamp']"
```

### Behavior

- **If some rules reference existing columns**: Rules for missing columns are filtered out with a warning, validation continues
- **If all rules reference missing columns**: ValidationError is raised with a clear message explaining the issue

## Schema Validation Edge Cases

### Nullable Changes

The framework detects nullable changes but treats them as informational (doesn't fail validation):

- **Nullable → Non-nullable**: More strict, may cause issues if data has nulls
- **Non-nullable → Nullable**: More lenient, usually OK

### Column Reordering

Column order differences are detected but don't fail validation (order doesn't affect functionality).

### Schema Evolution

For INCREMENTAL and FULL_REFRESH modes:
- Schema must match exactly (except for informational differences like nullable changes)
- New columns are not allowed
- Missing columns are not allowed
- Type mismatches cause validation to fail

For INITIAL mode:
- Schema changes are allowed
- New columns can be added
- Missing columns are allowed

## Incremental Column Filtering

### Type Validation

The framework validates that incremental columns are of appropriate types for filtering:

- **Recommended**: Numeric, Date, Timestamp, String
- **Not recommended**: Boolean, Array, Map, Struct, Binary

A warning is logged if the incremental column type may not be suitable for comparison operations.

### Error Messages

Enhanced error messages provide:
- Column type information
- Available columns in the DataFrame
- Suggestions for fixing the issue

### Example Error

```
Silver step processed_events: failed to filter bronze rows using incremental column 'timestamp'.
Error: cannot resolve 'timestamp' given input columns: [...]
Available columns in bronze DataFrame: ['user_id', 'value', 'timestamp_parsed'].
This may indicate that the incremental column was dropped or renamed in a previous transform.
```

## Memory and CPU Metrics

### Collection

Memory and CPU metrics are now collected during step execution:

- **memory_usage_mb**: Memory usage in megabytes
- **cpu_usage_percent**: CPU usage percentage

### Access

Metrics are available in `StepExecutionResult`:

```python
result = engine.execute_step(step, context, mode)
print(f"Memory usage: {result.memory_usage_mb} MB")
print(f"CPU usage: {result.cpu_usage_percent}%")
```

### Requirements

- Requires `psutil` package (included in dependencies)
- Metrics are `None` if `psutil` is unavailable or collection fails

## Best Practices

1. **Validation Rules**: Only reference columns that exist after the transform
2. **Incremental Columns**: Use numeric, date, timestamp, or string types
3. **Schema Changes**: Use INITIAL mode when schema needs to change
4. **Error Handling**: Check error messages for specific guidance


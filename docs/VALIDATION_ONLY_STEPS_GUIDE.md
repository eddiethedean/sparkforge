# Validation-Only Steps Guide: with_silver_rules and with_gold_rules

**Version**: 2.8.0  
**Last Updated**: January 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Overview](#overview)
3. [with_silver_rules](#with_silver_rules)
4. [with_gold_rules](#with_gold_rules)
5. [Accessing Validated Tables](#accessing-validated-tables)
6. [Use Cases](#use-cases)
7. [Examples](#examples)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

---

## Introduction

The `with_silver_rules` and `with_gold_rules` methods allow you to create **validation-only steps** for existing silver and gold tables. These methods work similarly to `with_bronze_rules`, but are designed for tables that already exist in your data warehouse and need to be validated and made available to subsequent transform functions.

### Key Benefits

- **Validation of Existing Tables**: Validate existing silver and gold tables without transformation
- **Access to Validated Data**: Subsequent transform functions can access validated tables via `prior_silvers` and `prior_golds`
- **No Transform Required**: Pure validation steps - no transform function needed
- **Consistent API**: Works like `with_bronze_rules` for consistency

---

## Overview

### What are Validation-Only Steps?

Validation-only steps are pipeline steps that:
- Read data from existing Delta Lake tables
- Apply validation rules to ensure data quality
- Make the validated data available to subsequent transform functions
- Do **not** perform any data transformation

### When to Use

Use `with_silver_rules` and `with_gold_rules` when:

1. **Existing Tables**: You have existing silver or gold tables that need validation
2. **Cross-Layer Dependencies**: Silver transforms need access to validated existing gold tables
3. **Gold-to-Gold Dependencies**: Gold transforms need access to previously validated gold tables
4. **Data Quality Monitoring**: You want to validate existing tables as part of your pipeline

### Architecture

```
Bronze → Silver → Gold
         ↓         ↓
    (existing)  (existing)
         ↓         ↓
    with_silver_rules / with_gold_rules
         ↓         ↓
    Validated tables available via:
    - prior_silvers (for silver/gold transforms)
    - prior_golds (for silver/gold transforms)
```

---

## with_silver_rules

The `with_silver_rules` method creates a validation-only silver step that reads from an existing silver table.

### Method Signature

```python
def with_silver_rules(
    *,
    name: str,
    table_name: str,
    rules: Dict[str, List[Union[str, Column]]],
    description: Optional[str] = None,
    schema: Optional[str] = None,
) -> PipelineBuilder
```

### Parameters

- **name** (str, required): Unique identifier for this silver step
- **table_name** (str, required): Existing Delta table name (without schema)
- **rules** (Dict[str, List], required): Validation rules mapping column names to rule lists
  - Supports PySpark Column expressions: `{"id": [F.col("id").isNotNull()]}`
  - Supports string rules: `{"id": ["not_null"], "value": ["gt", 0]}`
- **description** (str, optional): Description of this silver step
- **schema** (str, optional): Schema name for reading silver data. If not provided, uses builder's default schema

### Returns

- `PipelineBuilder`: Self for method chaining

### Example: Basic Usage

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions

F = get_default_functions()
builder = PipelineBuilder(spark=spark, schema="analytics")

# Add validation-only silver step
builder.with_silver_rules(
    name="existing_clean_events",
    table_name="clean_events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()],
        "value": [F.col("value") > 0]
    }
)
```

**Output:**
```
✅ Added Silver step (validation-only): existing_clean_events
✅ Pipeline validation passed
✅ Step created: existing_clean_events
   - transform is None: True
   - existing: True
   - source_bronze: ''
   - table_name: clean_events
   - rules: ['user_id', 'event_date', 'value']
```

### Example: With String Rules

```python
# String rules are automatically converted to PySpark expressions
builder.with_silver_rules(
    name="validated_events",
    table_name="events",
    rules={
        "user_id": ["not_null"],
        "value": ["gt", 0],
        "status": ["in", ["active", "inactive"]]
    }
)
```

### Example: With Custom Schema

```python
# Read from a different schema
builder.with_silver_rules(
    name="staging_events",
    table_name="events",
    rules={"id": [F.col("id").isNotNull()]},
    schema="staging"  # Read from staging schema
)
```

### Output

The step created by `with_silver_rules`:
- Has `transform=None` (no transform function)
- Has `existing=True` (marks it as an existing table)
- Has `source_bronze=""` (no source bronze required)
- Reads from the specified table during execution
- Makes the validated data available via `prior_silvers` to subsequent steps

---

## with_gold_rules

The `with_gold_rules` method creates a validation-only gold step that reads from an existing gold table.

### Method Signature

```python
def with_gold_rules(
    *,
    name: str,
    table_name: str,
    rules: Dict[str, List[Union[str, Column]]],
    description: Optional[str] = None,
    schema: Optional[str] = None,
) -> PipelineBuilder
```

### Parameters

- **name** (str, required): Unique identifier for this gold step
- **table_name** (str, required): Existing Delta table name (without schema)
- **rules** (Dict[str, List], required): Validation rules mapping column names to rule lists
  - Supports PySpark Column expressions: `{"user_id": [F.col("user_id").isNotNull()]}`
  - Supports string rules: `{"user_id": ["not_null"], "count": ["gt", 0]}`
- **description** (str, optional): Description of this gold step
- **schema** (str, optional): Schema name for reading gold data. If not provided, uses builder's default schema

### Returns

- `PipelineBuilder`: Self for method chaining

### Example: Basic Usage

```python
# Add validation-only gold step
builder.with_gold_rules(
    name="existing_user_metrics",
    table_name="user_metrics",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "total_events": [F.col("total_events") > 0],
        "last_activity": [F.col("last_activity").isNotNull()]
    }
)
```

**Output:**
```
✅ Added Gold step (validation-only): existing_user_metrics
✅ Pipeline validation passed
✅ Step created: existing_user_metrics
   - transform is None: True
   - existing: True
   - table_name: user_metrics
   - rules: ['user_id', 'total_events', 'last_activity']
```

### Example: With String Rules

```python
builder.with_gold_rules(
    name="validated_metrics",
    table_name="metrics",
    rules={
        "user_id": ["not_null"],
        "count": ["gt", 0],
        "revenue": ["gte", 0]
    },
    schema="analytics"
)
```

### Output

The step created by `with_gold_rules`:
- Has `transform=None` (no transform function)
- Has `existing=True` (marks it as an existing table)
- Reads from the specified table during execution
- Makes the validated data available via `prior_golds` to subsequent steps

---

## Accessing Validated Tables

Once you've created validation-only steps using `with_silver_rules` or `with_gold_rules`, subsequent transform functions can access the validated tables via `prior_silvers` and `prior_golds` arguments.

### Silver Transform Functions

Silver transform functions can optionally accept a `prior_golds` parameter to access validated gold tables:

```python
def silver_transform_with_prior_golds(
    spark,
    bronze_df,
    prior_silvers,
    prior_golds=None  # Optional: access validated gold tables
):
    result = bronze_df
    
    # Access validated existing silver tables
    if "existing_silver" in prior_silvers:
        existing_silver = prior_silvers["existing_silver"]
        # Use existing_silver DataFrame
    
    # Access validated existing gold tables
    if prior_golds and "existing_gold" in prior_golds:
        existing_gold = prior_golds["existing_gold"]
        # Use existing_gold DataFrame
    
    return result
```

### Gold Transform Functions

Gold transform functions can optionally accept a `prior_golds` parameter to access previously validated gold tables:

```python
def gold_transform_with_prior_golds(
    spark,
    silvers,
    prior_golds=None  # Optional: access previously validated gold tables
):
    result = silvers["clean_events"]
    
    # Access previously validated gold tables
    if prior_golds:
        for gold_name, gold_df in prior_golds.items():
            # Use gold_df for calculations, joins, etc.
            pass
    
    return result
```

### Backward Compatibility

Transform functions **without** `prior_golds` parameter continue to work as before:

```python
# Old-style transform (still works)
def silver_transform_old_style(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("value") > 0)

# Old-style gold transform (still works)
def gold_transform_old_style(spark, silvers):
    return silvers["clean_events"]
```

The framework automatically detects whether a transform function accepts `prior_golds` and only passes it if the function signature includes it.

---

## Use Cases

### Use Case 1: Validating Existing Silver Tables

You have an existing silver table that was created outside the pipeline, and you want to validate it and make it available to new transforms:

```python
# Validate existing silver table
builder.with_silver_rules(
    name="legacy_clean_events",
    table_name="legacy_clean_events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()]
    }
)

# New silver transform can access it
def new_silver_transform(spark, bronze_df, prior_silvers):
    # Access validated legacy table
    if "legacy_clean_events" in prior_silvers:
        legacy_df = prior_silvers["legacy_clean_events"]
        # Merge, join, or compare with new data
        pass
    return bronze_df
```

### Use Case 2: Gold-to-Gold Dependencies

You have multiple gold tables that depend on each other:

```python
# Validate existing gold table
builder.with_gold_rules(
    name="existing_user_metrics",
    table_name="user_metrics",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)

# New gold transform can access it
def new_gold_transform(spark, silvers, prior_golds=None):
    result = silvers["clean_events"]
    
    # Access previously validated gold table
    if prior_golds and "existing_user_metrics" in prior_golds:
        user_metrics = prior_golds["existing_user_metrics"]
        # Join, aggregate, or enrich with existing metrics
        result = result.join(user_metrics, "user_id", "left")
    
    return result

builder.add_gold_transform(
    name="enhanced_metrics",
    transform=new_gold_transform,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="enhanced_metrics",
    source_silvers=["clean_events"]
)
```

### Use Case 3: Cross-Layer Dependencies

Silver transforms need access to validated gold tables:

```python
# Validate existing gold table
builder.with_gold_rules(
    name="existing_user_profiles",
    table_name="user_profiles",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)

# Silver transform can access gold table
def enriched_silver_transform(
    spark,
    bronze_df,
    prior_silvers,
    prior_golds=None  # Accept prior_golds parameter
):
    result = bronze_df
    
    # Access validated gold table from silver transform
    if prior_golds and "existing_user_profiles" in prior_golds:
        user_profiles = prior_golds["existing_user_profiles"]
        # Enrich silver data with gold data
        result = result.join(user_profiles, "user_id", "left")
    
    return result

builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events",
    transform=enriched_silver_transform,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="enriched_events"
)
```

---

## Examples

### Complete Example: Validation-Only Steps in Pipeline

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions
from pipeline_builder.pipeline.runner import SimplePipelineRunner
from pipeline_builder_base.models import PipelineConfig

F = get_default_functions()

# Create builder
builder = PipelineBuilder(spark=spark, schema="analytics")

# 1. Add bronze step
builder.with_bronze_rules(
    name="events",
    rules={"id": [F.col("id").isNotNull()]},
    incremental_col="timestamp"
)

# 2. Validate existing silver table
builder.with_silver_rules(
    name="existing_clean_events",
    table_name="clean_events",
    rules={"id": [F.col("id").isNotNull()]}
)

# 3. Validate existing gold table
builder.with_gold_rules(
    name="existing_user_metrics",
    table_name="user_metrics",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)

# 4. Add new silver transform that uses prior_silvers and prior_golds
def enriched_silver_transform(
    spark,
    bronze_df,
    prior_silvers,
    prior_golds=None
):
    result = bronze_df
    
    # Use validated existing silver table
    if "existing_clean_events" in prior_silvers:
        existing = prior_silvers["existing_clean_events"]
        result = result.withColumn("has_existing", F.lit(True))
    
    # Use validated existing gold table
    if prior_golds and "existing_user_metrics" in prior_golds:
        metrics = prior_golds["existing_user_metrics"]
        result = result.withColumn("has_metrics", F.lit(True))
    
    return result

builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events",
    transform=enriched_silver_transform,
    rules={"id": [F.col("id").isNotNull()]},
    table_name="enriched_events"
)

# 5. Add new gold transform that uses prior_golds
def enhanced_gold_transform(spark, silvers, prior_golds=None):
    result = silvers["enriched_events"]
    
    # Use previously validated gold table
    if prior_golds and "existing_user_metrics" in prior_golds:
        existing_metrics = prior_golds["existing_user_metrics"]
        # Join or aggregate with existing metrics
        result = result.join(existing_metrics, "user_id", "left")
    
    return result

builder.add_gold_transform(
    name="enhanced_metrics",
    transform=enhanced_gold_transform,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="enhanced_metrics",
    source_silvers=["enriched_events"]
)

# 6. Build and run pipeline
pipeline = builder.to_pipeline()
config = PipelineConfig.create_default(schema="analytics")
runner = SimplePipelineRunner(spark, config)

report = runner.run_pipeline(
    pipeline.steps,
    bronze_sources={"events": source_df}
)

print(f"Status: {report.status}")
print(f"Steps executed: {len(report.steps) if report.steps else 0}")
```

**Output:**
```
✅ Added Bronze step: events
✅ Added Silver step (validation-only): existing_clean_events
✅ Added Gold step (validation-only): existing_user_metrics
✅ Added Silver step: enriched_events (source: events)
✅ Added Gold step: enhanced_metrics (sources: ['enriched_events'])
✅ Pipeline validation passed
📋 Execution order (5 steps): events → existing_clean_events → existing_user_metrics → enriched_events → enhanced_metrics
🚀 Pipeline built successfully with 1 bronze, 2 silver, 2 gold steps

🚀 Running pipeline...
✅ Pipeline execution completed
   Status: PipelineStatus.COMPLETED
```

### Example: Multiple Prior Golds

```python
# Validate multiple existing gold tables
builder.with_gold_rules(
    name="user_metrics",
    table_name="user_metrics",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)

builder.with_gold_rules(
    name="product_metrics",
    table_name="product_metrics",
    rules={"product_id": [F.col("product_id").isNotNull()]}
)

# Gold transform can access all prior golds
def comprehensive_gold_transform(spark, silvers, prior_golds=None):
    result = silvers["clean_events"]
    
    if prior_golds:
        # Access all previously validated gold tables
        user_metrics = prior_golds.get("user_metrics")
        product_metrics = prior_golds.get("product_metrics")
        
        if user_metrics:
            result = result.join(user_metrics, "user_id", "left")
        if product_metrics:
            result = result.join(product_metrics, "product_id", "left")
    
    return result
```

### Example: Error Handling

```python
# Validation-only step will raise error if table doesn't exist
try:
    builder.with_silver_rules(
        name="nonexistent_table",
        table_name="nonexistent_table",
        rules={"id": [F.col("id").isNotNull()]}
    )
    
    # When executed, this will raise ExecutionError if table doesn't exist
    pipeline = builder.to_pipeline()
    runner = SimplePipelineRunner(spark, config)
    report = runner.run_pipeline(pipeline.steps, bronze_sources={})
except ExecutionError as e:
    print(f"Table not found: {e}")
    # Handle error: create table, use different table, etc.
```

---

## Best Practices

### 1. Validate Tables Before Use

Always ensure the table exists before creating a validation-only step:

```python
from pipeline_builder.table_operations import table_exists, fqn

table_fqn = fqn("analytics", "clean_events")
if table_exists(spark, table_fqn):
    builder.with_silver_rules(
        name="existing_clean_events",
        table_name="clean_events",
        rules={"id": [F.col("id").isNotNull()]}
    )
else:
    # Handle missing table: create it, use different table, etc.
    pass
```

### 2. Use Descriptive Names

Use clear, descriptive names for validation-only steps:

```python
# Good: Clear that this is an existing/legacy table
builder.with_silver_rules(
    name="legacy_clean_events",
    table_name="clean_events",
    rules={...}
)

# Bad: Unclear if this is existing or new
builder.with_silver_rules(
    name="clean_events",
    table_name="clean_events",
    rules={...}
)
```

### 3. Comprehensive Validation Rules

Define comprehensive validation rules for existing tables:

```python
builder.with_silver_rules(
    name="existing_clean_events",
    table_name="clean_events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()],
        "value": [F.col("value") > 0],
        "status": [F.col("status").isin(["active", "inactive"])]
    }
)
```

### 4. Document Dependencies

Document which transforms depend on validation-only steps:

```python
# Document that enriched_events depends on existing_clean_events
def enriched_silver_transform(spark, bronze_df, prior_silvers):
    """
    Enriches bronze events with data from existing_clean_events.
    
    Dependencies:
    - existing_clean_events (validation-only step)
    """
    if "existing_clean_events" not in prior_silvers:
        raise ValueError("existing_clean_events must be validated first")
    
    existing = prior_silvers["existing_clean_events"]
    # ... use existing DataFrame
    return bronze_df
```

### 5. Handle Optional Dependencies

Make dependencies optional when possible:

```python
def flexible_silver_transform(spark, bronze_df, prior_silvers, prior_golds=None):
    result = bronze_df
    
    # Optional: Use existing silver if available
    if "existing_clean_events" in prior_silvers:
        existing = prior_silvers["existing_clean_events"]
        result = result.join(existing, "user_id", "left")
    
    # Optional: Use existing gold if available
    if prior_golds and "existing_user_metrics" in prior_golds:
        metrics = prior_golds["existing_user_metrics"]
        result = result.join(metrics, "user_id", "left")
    
    return result
```

### 6. Use Watermark Columns

Specify watermark columns for incremental processing compatibility:

```python
builder.with_silver_rules(
    name="existing_clean_events",
    table_name="clean_events",
    rules={"id": [F.col("id").isNotNull()]}
)
```

---

## Troubleshooting

### Error: Table Does Not Exist

**Error Message:**
```
ExecutionError: Validation-only silver step 'existing_silver' requires existing table 'analytics.clean_events', but table does not exist
```

**Solution:**
1. Verify the table exists: `spark.table("analytics.clean_events").show()`
2. Check schema name: Ensure the schema is correct
3. Check table name: Ensure the table name matches exactly
4. Create the table if needed: Use `df.write.saveAsTable("analytics.clean_events")`

### Error: Schema Not Found

**Error Message:**
```
ExecutionError: Validation-only silver step 'existing_silver' requires schema to read from table
```

**Solution:**
1. Provide schema parameter: `builder.with_silver_rules(..., schema="analytics")`
2. Or set default schema: `builder = PipelineBuilder(spark=spark, schema="analytics")`

### Error: Transform Function Not Accessing prior_golds

**Issue:** Transform function doesn't receive `prior_golds` even though validation-only gold steps exist.

**Solution:**
1. Add `prior_golds=None` parameter to transform function signature:
   ```python
   def transform(spark, bronze_df, prior_silvers, prior_golds=None):
       # Now prior_golds will be passed
   ```
2. Ensure validation-only gold steps are executed before the transform that needs them
3. Check execution order: Gold steps must execute before silver steps that need them (unusual but possible)

### Error: Duplicate Step Name

**Error Message:**
```
ExecutionError: Silver step 'clean_events' already exists
```

**Solution:**
1. Use a different name for the validation-only step:
   ```python
   builder.with_silver_rules(
       name="existing_clean_events",  # Different name
       table_name="clean_events",
       rules={...}
   )
   ```
2. Or remove the existing step first if it's no longer needed

### Validation Rules Not Applied

**Issue:** Validation rules don't seem to be working.

**Solution:**
1. Check rule syntax: Use PySpark Column expressions or valid string rules
2. Verify column names: Ensure column names in rules match table columns
3. Check execution mode: Validation is skipped in `VALIDATION_ONLY` mode
4. Review validation results: Check `StepExecutionResult.validation_rate`

---

## Summary

- **`with_silver_rules`**: Creates validation-only silver steps for existing silver tables
- **`with_gold_rules`**: Creates validation-only gold steps for existing gold tables
- **No Transform Required**: These steps only validate, they don't transform data
- **Access via prior_silvers/prior_golds**: Subsequent transforms can access validated tables
- **Backward Compatible**: Existing transforms without `prior_golds` continue to work
- **Table Must Exist**: The table must exist before execution, or an error is raised

For more information, see the [User Guide](USER_GUIDE.md) and [Stepwise Execution Guide](STEPWISE_EXECUTION_GUIDE.md).

# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

# SparkForge Quick Reference

A quick reference guide for SparkForge developers and users.

## Installation

```bash
pip install sparkforge
```

## Basic Pipeline (Traditional)

```python
from sparkforge import PipelineBuilder
from pyspark.sql import functions as F

# Setup
spark = SparkSession.builder.appName("My Pipeline").getOrCreate()
builder = PipelineBuilder(spark=spark, schema="my_schema")

# Bronze
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)

# Silver
builder.add_silver_transform(
    name="silver_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
    rules={"status": [F.col("status").isNotNull()]},
    table_name="silver_events"
)

# Gold
builder.add_gold_transform(
    name="gold_summary",
    source_silvers=["silver_events"],
    transform=lambda spark, silvers: silvers["silver_events"].groupBy("category").count(),
    rules={"category": [F.col("category").isNotNull()]},
    table_name="gold_summary"
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"events": source_df})
```

## Simplified Pipeline (New!)

```python
from sparkforge import PipelineBuilder

# Quick setup with preset configuration
builder = PipelineBuilder.for_development(spark=spark, schema="my_schema")

# Bronze with helper methods
builder.with_bronze_rules(
    name="events",
    rules=PipelineBuilder.not_null_rules(["user_id", "timestamp"])
)

# Silver - source_bronze auto-inferred!
builder.add_silver_transform(
    name="silver_events",
    transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
    rules=PipelineBuilder.not_null_rules(["status"]),
    table_name="silver_events"
)

# Gold - source_silvers auto-inferred!
builder.add_gold_transform(
    name="gold_summary",
    transform=lambda spark, silvers: list(silvers.values())[0].groupBy("category").count(),
    rules=PipelineBuilder.not_null_rules(["category"]),
    table_name="gold_summary"
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"events": source_df})
```

## Multi-Schema Pipeline (New!)

```python
from sparkforge import PipelineBuilder

# Cross-schema data flows for multi-tenant applications
builder = PipelineBuilder(spark=spark, schema="default")

# Bronze: Read from raw_data schema
builder.with_bronze_rules(
    name="events",
    rules=PipelineBuilder.not_null_rules(["user_id"]),
    schema="raw_data"  # Read from different schema
)

# Silver: Write to processing schema
builder.add_silver_transform(
    name="clean_events",
    transform=clean_events,
    rules=PipelineBuilder.not_null_rules(["user_id"]),
    table_name="clean_events",
    schema="processing"  # Write to different schema
)

# Gold: Write to analytics schema
builder.add_gold_transform(
    name="daily_metrics",
    transform=daily_metrics,
    rules=PipelineBuilder.not_null_rules(["user_id"]),
    table_name="daily_metrics",
    schema="analytics"  # Write to different schema
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"events": source_df})
```

### Multi-Schema Use Cases
- **Multi-Tenant SaaS**: Each tenant gets their own schema
- **Environment Separation**: Dev/staging/prod schema isolation
- **Data Lake Architecture**: Raw → Processing → Analytics schemas
- **Compliance**: Meet data residency requirements
- **Microservices**: Service-specific schema boundaries

## New User Experience Features

### Preset Configurations
```python
# Development (relaxed validation, verbose logging)
builder = PipelineBuilder.for_development(spark=spark, schema="my_schema")

# Production (strict validation, optimized settings)
builder = PipelineBuilder.for_production(spark=spark, schema="my_schema")

# Testing (minimal validation, sequential execution)
builder = PipelineBuilder.for_testing(spark=spark, schema="my_schema")
```

### Validation Helper Methods
```python
# Common validation patterns
rules = PipelineBuilder.not_null_rules(["user_id", "timestamp"])
rules = PipelineBuilder.positive_number_rules(["amount", "quantity"])
rules = PipelineBuilder.string_not_empty_rules(["name", "description"])
rules = PipelineBuilder.timestamp_rules(["created_at", "updated_at"])
```

### Timestamp Column Detection
```python
# Auto-detect timestamp columns for watermarking
schema = df.schema
timestamp_cols = PipelineBuilder.detect_timestamp_columns(schema)
# Returns: ["timestamp", "created_at", "event_time", ...]
```

## Enterprise Features

### Security (Automatic)

```python
from sparkforge import PipelineBuilder

# Security is enabled automatically - no configuration needed
builder = PipelineBuilder(spark=spark, schema="my_schema")
# All inputs are automatically validated and protected
```

### Performance Caching (Automatic)

```python
from sparkforge import PipelineBuilder

# Caching is enabled automatically - no configuration needed
builder = PipelineBuilder(spark=spark, schema="my_schema")
# Validation results are automatically cached for better performance
```

### Advanced Security Configuration

```python
from sparkforge import SecurityConfig, get_security_manager

# Configure security
security_config = SecurityConfig(
    enable_input_validation=True,
    enable_sql_injection_protection=True,
    enable_audit_logging=True
)
security_manager = get_security_manager(security_config)

# Validate inputs
security_manager.validate_table_name("my_table")
security_manager.validate_sql_expression("col('id').isNotNull()")
```

### Performance Cache Configuration

```python
from sparkforge import CacheConfig, get_performance_cache, CacheStrategy

# Configure caching
cache_config = CacheConfig(
    max_size_mb=512,
    ttl_seconds=3600,
    strategy=CacheStrategy.LRU
)
cache = get_performance_cache(cache_config)

# Cache operations
cache.put("key", value, ttl_seconds=1800)
result = cache.get("key")
cache.invalidate("key")
```

### Dynamic Parallel Execution

```python
from sparkforge import (
    DynamicParallelExecutor, ExecutionTask, TaskPriority,
    create_execution_task
)

# Create executor
executor = DynamicParallelExecutor()

# Create tasks
tasks = [
    create_execution_task("task1", function1, priority=TaskPriority.HIGH),
    create_execution_task("task2", function2, priority=TaskPriority.NORMAL)
]

# Execute with dynamic optimization
result = executor.execute_parallel(tasks)
print(f"Success rate: {result['metrics']['success_rate']:.1f}%")
```

## Execution Modes

```python
# Full refresh
result = pipeline.initial_load(bronze_sources={"events": source_df})

# Incremental
result = pipeline.run_incremental(bronze_sources={"events": new_data_df})

# Full refresh (force)
result = pipeline.run_full_refresh(bronze_sources={"events": source_df})

# Validation only
result = pipeline.run_validation_only(bronze_sources={"events": source_df})
```

## Step-by-Step Debugging

```python
# Execute individual steps
bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
silver_result = pipeline.execute_silver_step("silver_events")
gold_result = pipeline.execute_gold_step("gold_summary")

# Get step information
step_info = pipeline.get_step_info("silver_events")
steps = pipeline.list_steps()
```

## Validation Rules

```python
# Basic validation
rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "age": [F.col("age").between(0, 120)],
    "email": [F.col("email").rlike("^[^@]+@[^@]+\\.[^@]+$")]
}

# Custom validation
def custom_validation(spark, df, rules):
    invalid = df.filter(~F.col("user_id").isNotNull())
    if invalid.count() > 0:
        raise ValidationError("Invalid user records")
    return df
```

## Column Filtering Control

```python
from sparkforge.validation import apply_column_rules

# Default: Only keep columns with validation rules
valid_df, invalid_df, stats = apply_column_rules(
    df=input_df,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    stage="bronze",
    step="test",
    filter_columns_by_rules=True  # DEFAULT
)

# Preserve all columns for downstream steps
valid_df, invalid_df, stats = apply_column_rules(
    df=input_df,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    stage="bronze",
    step="test",
    filter_columns_by_rules=False  # Keep all columns
)
```

## Configuration

```python
# Basic configuration
builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    min_bronze_rate=95.0,
    min_silver_rate=98.0,
    min_gold_rate=99.0,
    enable_parallel_silver=True,
    max_parallel_workers=4,
    verbose=True
)

# Advanced configuration
from sparkforge.models import ValidationThresholds, ParallelConfig

thresholds = ValidationThresholds(bronze=90.0, silver=95.0, gold=98.0)
parallel_config = ParallelConfig(max_workers=8, enable_parallel_execution=True)

builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    validation_thresholds=thresholds,
    parallel_config=parallel_config
)
```

## Delta Lake Features

```python
# ACID transactions (automatic)
result = pipeline.run_incremental(bronze_sources={"events": source_df})

# Time travel
spark.sql("DESCRIBE HISTORY my_schema.events")
spark.sql("SELECT * FROM my_schema.events VERSION AS OF 1")

# Schema evolution (automatic)
def add_column(spark, df, silvers):
    return df.withColumn("new_field", F.lit("default"))
```

## Parallel Execution

```python
# Silver layer parallelization
builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    enable_parallel_silver=True,
    max_parallel_workers=4
)

# Unified execution (cross-layer)
pipeline = (builder
    .enable_unified_execution(
        max_workers=8,
        enable_parallel_execution=True,
        enable_dependency_optimization=True
    )
    .to_pipeline()
)

result = pipeline.run_unified(bronze_sources={"events": source_df})
```

## Monitoring & Logging

```python
# Execution results
result = pipeline.run_incremental(bronze_sources={"events": source_df})
print(f"Success: {result.success}")
print(f"Rows written: {result.totals['total_rows_written']}")
print(f"Duration: {result.totals['total_duration_secs']:.2f}s")

# Structured logging
from sparkforge import LogWriter
log_writer = LogWriter(spark=spark, table_name="my_schema.pipeline_logs")
log_writer.log_pipeline_execution(result)

# Performance monitoring
from sparkforge.performance import performance_monitor, time_operation

@time_operation("my_transform")
def my_transform(spark, df):
    return df.filter(F.col("status") == "active")

with performance_monitor("data_processing", max_duration=300):
    result = pipeline.run_incremental(bronze_sources={"events": source_df})
```

## Common Patterns

### Bronze with Incremental Processing

```python
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    incremental_col="timestamp"  # Enable incremental
)
```

### Silver with Watermarking

```python
builder.add_silver_transform(
    name="silver_events",
    source_bronze="events",
    transform=my_transform,
    rules={"status": [F.col("status").isNotNull()]},
    table_name="silver_events",
    watermark_col="timestamp"  # For streaming
)
```

### Silver with Silver Dependencies

```python
builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.join(silvers["user_profiles"], "user_id"),
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="enriched_events",
    source_silvers=["user_profiles"]  # Depend on Silver step
)
```

### Bronze without Datetime (Full Refresh)

```python
# Bronze without incremental column
builder.with_bronze_rules(
    name="events_no_datetime",
    rules={"user_id": [F.col("user_id").isNotNull()]}
    # No incremental_col - forces full refresh
)

# Silver will use overwrite mode automatically
builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events_no_datetime",
    transform=my_transform,
    rules={"status": [F.col("status").isNotNull()]},
    table_name="enriched_events"
)
```

## Error Handling

```python
# Check execution results
result = pipeline.run_incremental(bronze_sources={"events": source_df})
if not result.success:
    print(f"Pipeline failed: {result.error_message}")
    print(f"Failed steps: {result.failed_steps}")

# Debug specific step
bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
if not bronze_result.validation_result.validation_passed:
    print(f"Bronze validation failed: {bronze_result.validation_result.validation_rate:.2f}%")
```

## Testing

```python
# Test individual steps
bronze_result = pipeline.execute_bronze_step("events", input_data=test_df)
silver_result = pipeline.execute_silver_step("silver_events")
gold_result = pipeline.execute_gold_step("gold_summary")

# Check step outputs
executor = pipeline.create_step_executor()
silver_output = executor.get_step_output("silver_events")
silver_output.show()
```

## Performance Tips

1. **Enable parallel execution** for independent steps
2. **Use appropriate partitioning** strategies
3. **Monitor execution times** and optimize slow steps
4. **Use Delta Lake optimization** features
5. **Profile individual steps** for bottlenecks

## Common Issues

### Validation Failures
- Check validation rules and thresholds
- Debug specific steps with `execute_*_step()`
- Verify data quality in source data

### Performance Issues
- Enable parallel execution
- Check step dependencies
- Profile individual steps
- Optimize slow transformations

### Dependency Issues
- Check step dependencies with `get_step_info()`
- Verify Silver-to-Silver dependencies
- Use `list_steps()` to see all steps

---

**For more details, see the [User Guide](USER_GUIDE.md) and [API Reference](README.md).**

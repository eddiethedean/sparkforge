# SparkForge User Guide

A comprehensive guide to building robust data pipelines with SparkForge's Bronze → Silver → Gold architecture.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [Pipeline Building](#pipeline-building)
4. [Execution Modes](#execution-modes)
5. [Data Validation](#data-validation)
6. [Delta Lake Integration](#delta-lake-integration)
7. [Parallel Execution](#parallel-execution)
8. [Step-by-Step Debugging](#step-by-step-debugging)
9. [Monitoring & Logging](#monitoring--logging)
10. [Advanced Features](#advanced-features)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)
13. [Examples](#examples)

## Quick Start

### Installation

```bash
pip install sparkforge
```

### Basic Pipeline

```python
from sparkforge import PipelineBuilder
from pyspark.sql import functions as F

# Initialize Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("My Pipeline") \
    .master("local[*]") \
    .getOrCreate()

# Create pipeline
builder = PipelineBuilder(spark=spark, schema="my_schema")

# Define transforms
def silver_transform(spark, bronze_df):
    return bronze_df.filter(F.col("status") == "active")

def gold_transform(spark, silvers):
    events_df = silvers["silver_events"]
    return events_df.groupBy("category").count()

# Build and run pipeline
pipeline = (builder
    .with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]}
    )
    .add_silver_transform(
        name="silver_events",
        source_bronze="events",
        transform=silver_transform,
        rules={"status": [F.col("status").isNotNull()]},
        table_name="silver_events"
    )
    .add_gold_transform(
        name="gold_summary",
        transform=gold_transform,
        rules={"category": [F.col("category").isNotNull()]},
        table_name="gold_summary"
    )
    .to_pipeline()
)

# Execute pipeline
result = pipeline.initial_load(bronze_sources={"events": source_df})
print(f"Pipeline completed: {result.success}")
```

## Core Concepts

### Medallion Architecture

SparkForge implements the Bronze → Silver → Gold data architecture:

- **Bronze Layer**: Raw data ingestion with basic validation
- **Silver Layer**: Cleaned and enriched data with business logic
- **Gold Layer**: Aggregated and business-ready datasets

### Key Components

- **PipelineBuilder**: Fluent API for building pipelines
- **PipelineRunner**: Executes pipelines with different modes
- **StepExecutor**: Individual step execution and debugging
- **ValidationEngine**: Data quality validation
- **LogWriter**: Structured logging and monitoring

## Pipeline Building

### Bronze Layer

Define data ingestion rules and validation:

```python
# Basic bronze step
builder.with_bronze_rules(
    name="user_events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "event_type": [F.col("event_type").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    }
)

# Bronze with incremental processing
builder.with_bronze_rules(
    name="user_events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    incremental_col="timestamp"  # Enable incremental processing
)
```

### Silver Layer

Transform and enrich data:

```python
def enrich_events(spark, bronze_df, prior_silvers):
    return (bronze_df
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("is_premium", F.col("user_id").startswith("premium_"))
        .filter(F.col("event_type").isin(["click", "view", "purchase"]))
    )

builder.add_silver_transform(
    name="enriched_events",
    source_bronze="user_events",
    transform=enrich_events,
    rules={
        "processed_at": [F.col("processed_at").isNotNull()],
        "is_premium": [F.col("is_premium").isNotNull()]
    },
    table_name="enriched_events",
    watermark_col="timestamp"  # For streaming/incremental processing
)
```

### Gold Layer

Create business-ready aggregations:

```python
def daily_analytics(spark, silvers):
    events_df = silvers["enriched_events"]
    return (events_df
        .groupBy("event_type", F.date_trunc("day", "timestamp").alias("date"))
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.sum("revenue").alias("total_revenue")
        )
    )

builder.add_gold_transform(
    name="daily_analytics",
    transform=daily_analytics,
    rules={
        "event_type": [F.col("event_type").isNotNull()],
        "date": [F.col("date").isNotNull()]
    },
    table_name="daily_analytics",
    source_silvers=["enriched_events"]
)
```

## Execution Modes

### Initial Load

Full refresh of all data:

```python
result = pipeline.initial_load(bronze_sources={"events": source_df})
```

### Incremental Processing

Process only new/changed data:

```python
result = pipeline.run_incremental(bronze_sources={"events": new_data_df})
```

### Full Refresh

Force complete reprocessing:

```python
result = pipeline.run_full_refresh(bronze_sources={"events": source_df})
```

### Validation Only

Check data quality without writing:

```python
result = pipeline.run_validation_only(bronze_sources={"events": source_df})
```

## Data Validation

### Validation Rules

Define column-level validation rules:

```python
rules = {
    "user_id": [
        F.col("user_id").isNotNull(),
        F.col("user_id").rlike("^[a-zA-Z0-9_]+$")
    ],
    "age": [
        F.col("age").isNotNull(),
        F.col("age").between(0, 120)
    ],
    "email": [
        F.col("email").isNotNull(),
        F.col("email").rlike("^[^@]+@[^@]+\\.[^@]+$")
    ]
}
```

### Validation Thresholds

Set quality thresholds per layer:

```python
builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    min_bronze_rate=95.0,    # 95% data quality for Bronze
    min_silver_rate=98.0,    # 98% data quality for Silver
    min_gold_rate=99.0       # 99% data quality for Gold
)
```

### Custom Validation

```python
def custom_validation(spark, df, rules):
    # Custom validation logic
    invalid_records = df.filter(~F.col("user_id").isNotNull())
    if invalid_records.count() > 0:
        raise ValidationError("Found invalid user records")
    return df

builder.add_silver_transform(
    name="validated_events",
    source_bronze="events",
    transform=custom_validation,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="validated_events"
)
```

## Delta Lake Integration

### ACID Transactions

```python
# Delta Lake tables automatically support ACID transactions
result = pipeline.run_incremental(bronze_sources={"events": source_df})

# Access Delta Lake features
print(f"Tables created: {result.totals['tables_created']}")
print(f"Rows written: {result.totals['total_rows_written']}")
```

### Schema Evolution

```python
# Delta Lake handles schema evolution automatically
# Add new columns without breaking existing data
def add_new_column(spark, df, prior_silvers):
    return df.withColumn("new_field", F.lit("default_value"))
```

### Time Travel

```python
# Access historical versions
spark.sql("DESCRIBE HISTORY my_schema.events")
spark.sql("SELECT * FROM my_schema.events VERSION AS OF 1")
```

## Parallel Execution

### Silver Layer Parallelization

Independent Silver steps run in parallel:

```python
builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    enable_parallel_silver=True,
    max_parallel_workers=4
)
```

### Unified Execution

Cross-layer parallelization based on dependencies:

```python
pipeline = (builder
    .enable_unified_execution(
        max_workers=8,
        enable_parallel_execution=True,
        enable_dependency_optimization=True
    )
    .to_pipeline()
)

result = pipeline.run_unified(bronze_sources={"events": source_df})
print(f"Parallel efficiency: {result.metrics.parallel_efficiency:.2f}%")
```

## Step-by-Step Debugging

### Individual Step Execution

Debug specific steps without running the entire pipeline:

```python
# Execute Bronze step
bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
print(f"Bronze status: {bronze_result.status.value}")

# Execute Silver step
silver_result = pipeline.execute_silver_step("silver_events")
print(f"Silver output rows: {silver_result.output_count}")

# Execute Gold step
gold_result = pipeline.execute_gold_step("gold_summary")
print(f"Gold duration: {gold_result.duration_seconds:.2f}s")
```

### Step Information

```python
# Get step details
step_info = pipeline.get_step_info("silver_events")
print(f"Step type: {step_info['type']}")
print(f"Dependencies: {step_info['dependencies']}")

# List all steps
steps = pipeline.list_steps()
print(f"Bronze steps: {steps['bronze']}")
print(f"Silver steps: {steps['silver']}")
print(f"Gold steps: {steps['gold']}")
```

### Inspect Intermediate Data

```python
# Get step output for inspection
executor = pipeline.create_step_executor()
silver_output = executor.get_step_output("silver_events")
silver_output.show()
silver_output.printSchema()
```

## Monitoring & Logging

### Execution Results

```python
result = pipeline.run_incremental(bronze_sources={"events": source_df})

# Access execution metrics
print(f"Success: {result.success}")
print(f"Total rows written: {result.totals['total_rows_written']}")
print(f"Execution time: {result.totals['total_duration_secs']:.2f}s")

# Stage-specific metrics
bronze_stats = result.stage_stats['bronze']
print(f"Bronze validation rate: {bronze_stats.validation_rate:.2f}%")
```

### Structured Logging

```python
from sparkforge import LogWriter

# Configure logging
log_writer = LogWriter(
    spark=spark,
    table_name="my_schema.pipeline_logs",
    use_delta=True
)

# Log pipeline execution
log_writer.log_pipeline_execution(result)
```

### Performance Monitoring

```python
from sparkforge.performance import performance_monitor, time_operation

# Context manager
with performance_monitor("data_processing", max_duration=300):
    result = pipeline.run_incremental(bronze_sources={"events": source_df})

# Decorator
@time_operation("custom_transform")
def my_transform(spark, df):
    return df.filter(F.col("status") == "active")
```

## Advanced Features

### Bronze Tables Without Datetime

Support for full refresh Bronze tables:

```python
# Bronze without incremental column
builder.with_bronze_rules(
    name="events_no_datetime",
    rules={"user_id": [F.col("user_id").isNotNull()]}
    # No incremental_col - forces full refresh
)

# Silver will automatically use overwrite mode
builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events_no_datetime",
    transform=lambda spark, df, silvers: df,
    rules={"status": [F.col("status").isNotNull()]},
    table_name="enriched_events"
)
```

### Complex Dependencies

Handle complex Silver-to-Silver dependencies:

```python
# Silver step depending on another Silver step
builder.add_silver_transform(
    name="user_profiles",
    source_bronze="users",
    transform=create_user_profiles,
    rules={"profile_id": [F.col("profile_id").isNotNull()]},
    table_name="user_profiles"
)

builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.join(
        silvers["user_profiles"], "user_id", "left"
    ),
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="enriched_events",
    source_silvers=["user_profiles"]  # Depend on Silver step
)
```

### Custom Configuration

```python
from sparkforge.models import ValidationThresholds, ParallelConfig

# Custom validation thresholds
thresholds = ValidationThresholds(bronze=90.0, silver=95.0, gold=98.0)

# Custom parallel configuration
parallel_config = ParallelConfig(
    max_workers=8,
    enable_parallel_execution=True,
    enable_dependency_optimization=True
)

builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    validation_thresholds=thresholds,
    parallel_config=parallel_config
)
```

## Best Practices

### 1. Data Quality

- Set appropriate validation thresholds
- Use comprehensive validation rules
- Monitor data quality metrics
- Implement data quality alerts

### 2. Performance

- Enable parallel execution for independent steps
- Use appropriate partitioning strategies
- Monitor execution times and optimize slow steps
- Use Delta Lake optimization features

### 3. Error Handling

- Implement comprehensive error handling
- Use retry mechanisms for transient failures
- Log detailed error information
- Implement circuit breakers for external dependencies

### 4. Monitoring

- Use structured logging
- Monitor key performance metrics
- Set up alerts for failures
- Track data quality trends

### 5. Testing

- Test individual steps in isolation
- Use realistic test data
- Test error scenarios
- Validate data quality rules

## Troubleshooting

### Common Issues

#### 1. Validation Failures

```python
# Check validation results
result = pipeline.run_incremental(bronze_sources={"events": source_df})
if not result.success:
    print(f"Validation failed: {result.validation_errors}")
    # Debug specific step
    bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
    print(f"Bronze validation rate: {bronze_result.validation_result.validation_rate}")
```

#### 2. Performance Issues

```python
# Profile individual steps
with performance_monitor("slow_step"):
    result = pipeline.execute_silver_step("slow_silver")

# Check parallel execution
print(f"Parallel efficiency: {result.metrics.parallel_efficiency}")
```

#### 3. Dependency Issues

```python
# Check step dependencies
step_info = pipeline.get_step_info("problematic_step")
print(f"Dependencies: {step_info['dependencies']}")
print(f"Dependents: {step_info['dependents']}")
```

### Debug Mode

```python
# Enable verbose logging
builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    verbose=True
)
```

## Examples

### E-commerce Analytics Pipeline

```python
from sparkforge import PipelineBuilder
from pyspark.sql import functions as F

# Initialize
spark = SparkSession.builder.appName("E-commerce Analytics").getOrCreate()
builder = PipelineBuilder(spark=spark, schema="ecommerce")

# Bronze: Raw events
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "product_id": [F.col("product_id").isNotNull()],
        "event_type": [F.col("event_type").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    },
    incremental_col="timestamp"
)

# Silver: Enriched events
def enrich_events(spark, bronze_df, prior_silvers):
    return (bronze_df
        .withColumn("event_date", F.date_trunc("day", "timestamp"))
        .withColumn("is_purchase", F.col("event_type") == "purchase")
        .withColumn("revenue", F.when(F.col("is_purchase"), F.col("price")).otherwise(0))
    )

builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events",
    transform=enrich_events,
    rules={
        "event_date": [F.col("event_date").isNotNull()],
        "revenue": [F.col("revenue") >= 0]
    },
    table_name="enriched_events",
    watermark_col="timestamp"
)

# Gold: Daily analytics
def daily_analytics(spark, silvers):
    events_df = silvers["enriched_events"]
    return (events_df
        .groupBy("event_date", "product_id")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("revenue").alias("daily_revenue"),
            F.countDistinct("user_id").alias("unique_users")
        )
    )

builder.add_gold_transform(
    name="daily_analytics",
    transform=daily_analytics,
    rules={
        "event_date": [F.col("event_date").isNotNull()],
        "daily_revenue": [F.col("daily_revenue") >= 0]
    },
    table_name="daily_analytics",
    source_silvers=["enriched_events"]
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.run_incremental(bronze_sources={"events": events_df})
```

### Real-time Streaming Pipeline

```python
# For streaming data
def process_streaming_events(spark, bronze_df, prior_silvers):
    return (bronze_df
        .withWatermark("timestamp", "1 hour")
        .groupBy(
            F.window("timestamp", "1 hour"),
            F.col("user_id")
        )
        .agg(F.count("*").alias("event_count"))
    )

builder.add_silver_transform(
    name="hourly_user_events",
    source_bronze="events",
    transform=process_streaming_events,
    rules={"event_count": [F.col("event_count") > 0]},
    table_name="hourly_user_events",
    watermark_col="timestamp"
)
```

## Conclusion

SparkForge provides a powerful, flexible framework for building robust data pipelines with the Bronze → Silver → Gold architecture. This guide covers the essential concepts and features you need to get started and build production-ready data pipelines.

For more information, see the [API Reference](README.md#api-reference) and [Examples](examples/) directory.

---

**Happy Pipeline Building! 🚀**

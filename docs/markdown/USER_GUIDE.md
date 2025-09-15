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

# SparkForge User Guide

A comprehensive guide to building robust data pipelines with SparkForge's Bronze → Silver → Gold architecture.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [Pipeline Building](#pipeline-building)
4. [Multi-Schema Support](#multi-schema-support)
5. [Execution Modes](#execution-modes)
6. [Data Validation](#data-validation)
7. [Delta Lake Integration](#delta-lake-integration)
8. [Parallel Execution](#parallel-execution)
9. [Step-by-Step Debugging](#step-by-step-debugging)
10. [Monitoring & Logging](#monitoring--logging)
11. [Enterprise Security](#enterprise-security)
12. [Performance Optimization](#performance-optimization)
13. [Advanced Parallel Execution](#advanced-parallel-execution)
14. [Advanced Features](#advanced-features)
15. [Best Practices](#best-practices)
16. [Troubleshooting](#troubleshooting)
17. [Examples](#examples)

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

## Multi-Schema Support

SparkForge supports cross-schema data flows, enabling sophisticated data architectures for multi-tenant applications, environment separation, and data lake implementations.

### Overview

Multi-schema support allows you to:
- **Read from different schemas** for bronze and silver data sources
- **Write to different schemas** for silver and gold data outputs
- **Create cross-schema data flows** for complex architectures
- **Isolate data by tenant, environment, or purpose**

### Basic Multi-Schema Usage

All pipeline methods support an optional `schema` parameter:

```python
from sparkforge import PipelineBuilder

builder = PipelineBuilder(spark=spark, schema="default")

# Bronze: Read from raw_data schema
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    schema="raw_data"  # Read from different schema
)

# Silver: Write to processing schema
builder.add_silver_transform(
    name="clean_events",
    transform=clean_events,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="clean_events",
    schema="processing"  # Write to different schema
)

# Gold: Write to analytics schema
builder.add_gold_transform(
    name="daily_metrics",
    transform=daily_metrics,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="daily_metrics",
    schema="analytics"  # Write to different schema
)
```

### Schema Parameters

#### `with_bronze_rules(schema=None)`
- **Purpose**: Specify which schema to read bronze data from
- **Default**: Uses builder's default schema if not specified
- **Example**: `schema="raw_data"` reads from `raw_data` schema

#### `with_silver_rules(schema=None)`
- **Purpose**: Specify which schema to read existing silver data from
- **Default**: Uses builder's default schema if not specified
- **Example**: `schema="staging"` reads from `staging` schema

#### `add_silver_transform(schema=None)`
- **Purpose**: Specify which schema to write silver data to
- **Default**: Uses builder's default schema if not specified
- **Example**: `schema="processing"` writes to `processing` schema

#### `add_gold_transform(schema=None)`
- **Purpose**: Specify which schema to write gold data to
- **Default**: Uses builder's default schema if not specified
- **Example**: `schema="analytics"` writes to `analytics` schema

### Schema Validation

SparkForge automatically validates schemas when provided:

```python
# This will validate that 'raw_data' schema exists
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    schema="raw_data"  # Validates schema exists
)
```

**Error Handling:**
- If schema doesn't exist: `StepError` with helpful suggestions
- If schema access denied: `StepError` with permission guidance
- If schema name invalid: `StepError` with naming suggestions

### Use Cases

#### Multi-Tenant SaaS Applications

Each tenant gets their own isolated schema:

```python
def create_tenant_pipeline(tenant_id: str):
    builder = PipelineBuilder(spark=spark, schema="default")

    # Tenant-specific schemas
    raw_schema = f"{tenant_id}_raw"
    processing_schema = f"{tenant_id}_processing"
    analytics_schema = f"{tenant_id}_analytics"

    # Bronze: Read from tenant's raw data
    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]},
        schema=raw_schema
    )

    # Silver: Process in tenant's processing schema
    builder.add_silver_transform(
        name="clean_events",
        transform=clean_events,
        rules={"user_id": [F.col("user_id").isNotNull()]},
        table_name="clean_events",
        schema=processing_schema
    )

    # Gold: Analytics in tenant's analytics schema
    builder.add_gold_transform(
        name="daily_metrics",
        transform=daily_metrics,
        rules={"user_id": [F.col("user_id").isNotNull()]},
        table_name="daily_metrics",
        schema=analytics_schema
    )

    return builder.to_pipeline()
```

#### Data Lake Architecture

Organize data into logical layers:

```python
# Bronze (raw) → Silver (processing) → Gold (analytics)
builder = PipelineBuilder(spark=spark, schema="default")

# Raw data ingestion
builder.with_bronze_rules(
    name="raw_events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    schema="raw_data"  # Raw data layer
)

# Data processing and cleaning
builder.add_silver_transform(
    name="clean_events",
    transform=clean_events,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="clean_events",
    schema="processing"  # Processing layer
)

# Business analytics
builder.add_gold_transform(
    name="daily_metrics",
    transform=daily_metrics,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="daily_metrics",
    schema="analytics"  # Analytics layer
)
```

#### Environment Separation

Separate dev, staging, and production data:

```python
def create_environment_pipeline(environment: str):
    builder = PipelineBuilder(spark=spark, schema="default")

    # Environment-specific schemas
    raw_schema = f"{environment}_raw"
    processing_schema = f"{environment}_processing"
    analytics_schema = f"{environment}_analytics"

    # All steps use environment-specific schemas
    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]},
        schema=raw_schema
    )

    builder.add_silver_transform(
        name="clean_events",
        transform=clean_events,
        rules={"user_id": [F.col("user_id").isNotNull()]},
        table_name="clean_events",
        schema=processing_schema
    )

    builder.add_gold_transform(
        name="daily_metrics",
        transform=daily_metrics,
        rules={"user_id": [F.col("user_id").isNotNull()]},
        table_name="daily_metrics",
        schema=analytics_schema
    )

    return builder.to_pipeline()
```

### Schema Management

#### Creating Schemas

```python
# Create schemas before using them
spark.sql("CREATE SCHEMA IF NOT EXISTS raw_data")
spark.sql("CREATE SCHEMA IF NOT EXISTS processing")
spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")
```

#### Schema Validation

```python
# Check if schema exists
try:
    spark.sql("DESCRIBE SCHEMA my_schema")
    print("Schema exists")
except:
    print("Schema does not exist")
```

### Best Practices

1. **Consistent Naming**: Use consistent schema naming conventions
2. **Environment Prefixes**: Use prefixes like `dev_`, `staging_`, `prod_`
3. **Tenant Isolation**: Use tenant-specific schemas for multi-tenant apps
4. **Data Lake Layers**: Use `raw_`, `processing_`, `analytics_` prefixes
5. **Schema Validation**: Always validate schemas exist before pipeline execution
6. **Error Handling**: Handle schema validation errors gracefully

### Backward Compatibility

All existing code continues to work without changes:

```python
# This still works exactly as before
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)
# Uses builder's default schema automatically
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

### Column Filtering Behavior

By default, validation rules filter the output DataFrame to only include columns that have
validation rules defined. This behavior can be controlled with the `filter_columns_by_rules`
parameter:

```python
from sparkforge.validation import apply_column_rules

# Default behavior: Only keep columns with rules
valid_df, invalid_df, stats = apply_column_rules(
    df=input_df,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    stage="bronze",
    step="test",
    filter_columns_by_rules=True  # DEFAULT
)

# Preserve all columns
valid_df, invalid_df, stats = apply_column_rules(
    df=input_df,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    stage="bronze",
    step="test",
    filter_columns_by_rules=False
)
```

**When to use each approach:**
- `filter_columns_by_rules=True` (default): Use when downstream steps only need validated columns
- `filter_columns_by_rules=False`: Use when downstream steps need access to all original columns

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

## Enterprise Security

SparkForge includes comprehensive security features to protect your data pipelines from common vulnerabilities and ensure compliance.

### Automatic Security Features

Security is enabled by default and works transparently:

```python
from sparkforge import PipelineBuilder

# Security is automatically enabled - no configuration needed
builder = PipelineBuilder(spark=spark, schema="my_schema")

# All inputs are automatically validated and protected
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)
```

### Input Validation

Automatic validation of all user inputs:

```python
from sparkforge import SecurityConfig, get_security_manager

# Configure security settings
security_config = SecurityConfig(
    enable_input_validation=True,
    enable_sql_injection_protection=True,
    enable_audit_logging=True,
    max_table_name_length=128,
    max_schema_name_length=64
)

security_manager = get_security_manager(security_config)

# Validate table names
security_manager.validate_table_name("my_table")  # ✅ Valid
security_manager.validate_table_name("'; DROP TABLE users; --")  # ❌ Blocked

# Validate SQL expressions
security_manager.validate_sql_expression("col('id').isNotNull()")  # ✅ Valid
security_manager.validate_sql_expression("DROP TABLE users")  # ❌ Blocked
```

### SQL Injection Protection

Built-in protection against SQL injection attacks:

```python
# These expressions are automatically blocked:
# - DROP TABLE statements
# - UNION SELECT attacks
# - EXEC commands
# - Script injection attempts

# Safe expressions are allowed:
rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "age": [F.col("age") > 18],
    "status": [F.col("status").like("active%")]
}
```

### Access Control

Role-based access control for pipeline operations:

```python
from sparkforge.security import AccessLevel

# Grant permissions
security_manager.grant_permission("user1", AccessLevel.READ, "my_schema.events")
security_manager.grant_permission("user2", AccessLevel.WRITE, "my_schema.silver_events")

# Check permissions
if security_manager.check_access_permission("user1", AccessLevel.READ, "my_schema.events"):
    # User has read access
    pass
```

### Audit Logging

Comprehensive audit trails for compliance:

```python
# Audit logs are automatically generated for:
# - Input validation attempts
# - SQL injection attempts
# - Access control decisions
# - Pipeline execution events

# View audit logs
audit_logs = security_manager.get_audit_logs(
    start_date="2024-01-01",
    end_date="2024-01-31"
)
```

## Performance Optimization

SparkForge includes intelligent performance optimization features that work automatically to improve pipeline execution speed and resource utilization.

### Automatic Performance Caching

Validation results are automatically cached to avoid redundant computations:

```python
from sparkforge import PipelineBuilder

# Caching is enabled automatically - no configuration needed
builder = PipelineBuilder(spark=spark, schema="my_schema")

# Validation results are cached and reused
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)
```

### Intelligent Caching Configuration

Configure caching behavior for optimal performance:

```python
from sparkforge import CacheConfig, get_performance_cache, CacheStrategy

# Configure performance cache
cache_config = CacheConfig(
    max_size_mb=512,           # Maximum cache size
    ttl_seconds=3600,          # Time-to-live for cache entries
    strategy=CacheStrategy.LRU, # Eviction strategy
    enable_compression=True     # Enable data compression
)

cache = get_performance_cache(cache_config)

# Cache statistics
stats = cache.get_stats()
print(f"Cache hit rate: {stats['hit_rate']:.2%}")
print(f"Cache size: {stats['current_size_mb']:.1f} MB")
```

### Memory Management

Automatic memory management and cleanup:

```python
# Cache automatically manages memory usage
# - LRU eviction when memory limit is reached
# - TTL-based expiration for stale data
# - Compression to reduce memory footprint

# Manual cache management
cache.clear()  # Clear all cache entries
cache.invalidate("validation_result_key")  # Remove specific entry
```

## Advanced Parallel Execution

SparkForge includes advanced parallel execution capabilities with dynamic worker allocation and intelligent task scheduling.

### Dynamic Worker Allocation

Automatically adjust the number of parallel workers based on workload and system resources:

```python
from sparkforge import DynamicParallelExecutor, ExecutionTask, TaskPriority

# Create dynamic executor
executor = DynamicParallelExecutor()

# Create tasks with different priorities
tasks = [
    ExecutionTask(
        task_id="critical_task",
        function=critical_function,
        priority=TaskPriority.CRITICAL,
        estimated_duration=30.0
    ),
    ExecutionTask(
        task_id="normal_task",
        function=normal_function,
        priority=TaskPriority.NORMAL,
        estimated_duration=10.0
    )
]

# Execute with dynamic optimization
result = executor.execute_parallel(tasks)
print(f"Executed {result['metrics']['successful_tasks']} tasks successfully!")
```

### Task Prioritization

Intelligent task scheduling based on priority and dependencies:

```python
from sparkforge import create_execution_task, TaskPriority

# Create tasks with priorities
critical_task = create_execution_task(
    "critical_processing",
    critical_function,
    priority=TaskPriority.CRITICAL,
    dependencies={"data_validation"}  # Must complete first
)

normal_task = create_execution_task(
    "normal_processing",
    normal_function,
    priority=TaskPriority.NORMAL,
    dependencies={"critical_processing"}  # Depends on critical task
)

# Tasks are automatically scheduled based on dependencies and priority
```

### Resource Monitoring

Real-time monitoring of system resources and performance:

```python
# Get performance metrics
metrics = executor.get_performance_metrics()
print(f"Worker count: {metrics['worker_count']}")
print(f"Success rate: {metrics['success_rate']:.1f}%")
print(f"Average efficiency: {metrics['average_efficiency']:.2f}")

# Get optimization recommendations
recommendations = executor.get_optimization_recommendations()
for rec in recommendations:
    print(f"💡 {rec}")
```

### Work-Stealing Algorithms

Optimal resource utilization across workers:

```python
# Workers automatically steal work from busy workers
# - Load balancing across all workers
# - Optimal resource utilization
# - Reduced idle time

# Monitor worker efficiency
worker_efficiencies = metrics['worker_efficiencies']
for worker_id, efficiency in worker_efficiencies.items():
    print(f"Worker {worker_id}: {efficiency:.2f} efficiency")
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

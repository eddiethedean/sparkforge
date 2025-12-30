# SparkForge Pipeline Builder - User Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Core Concepts](#core-concepts)
4. [Building Pipelines](#building-pipelines)
5. [Execution Modes](#execution-modes)
6. [Validation Rules](#validation-rules)
7. [Logging and Monitoring](#logging-and-monitoring)
8. [Common Workflows](#common-workflows)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)

## Introduction

SparkForge Pipeline Builder is a framework for building and executing data pipelines using Apache Spark and Delta Lake. It implements the Medallion Architecture (Bronze → Silver → Gold) with built-in validation, logging, and monitoring capabilities.

### Key Features

- **Medallion Architecture**: Bronze (raw), Silver (cleaned), Gold (aggregated) data layers
- **Dual Engine Support**: Works with PySpark (production) and sparkless (testing/development)
- **Automatic Validation**: Configurable validation rules at each layer
- **Incremental Processing**: Efficient processing of new/updated data
- **Parallel Execution**: Automatic dependency analysis and concurrent step execution
- **Comprehensive Logging**: Built-in LogWriter for tracking pipeline executions
- **Type Safety**: Full mypy compliance and runtime type checks

## Getting Started

### Installation

```bash
# Install the package
pip install pipeline-builder

# Or install with extras
pip install pipeline-builder[pyspark]  # For PySpark support
pip install pipeline-builder[dev,test]  # For development
```

### Basic Setup

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine

# Configure the engine (required before importing pipeline components)
configure_engine()

# Create a Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("MyPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create a pipeline builder
builder = PipelineBuilder(spark=spark, schema="my_schema")
```

## Core Concepts

### Medallion Architecture

The framework implements the Medallion Architecture pattern:

- **Bronze Layer**: Raw, unprocessed data from source systems
  - Validates incoming data
  - Stores data as-is (no transformations)
  - Acts as a landing zone

- **Silver Layer**: Cleaned and enriched data
  - Transforms bronze data
  - Applies business rules
  - Validates transformed data
  - Stores cleaned data in Delta tables

- **Gold Layer**: Aggregated and business-ready data
  - Aggregates silver data
  - Creates analytics-ready datasets
  - Stores aggregated metrics and summaries

### Pipeline Components

- **Steps**: Individual processing units (Bronze, Silver, or Gold)
- **Transforms**: Functions that process data between layers
- **Validation Rules**: Rules that ensure data quality
- **Execution Modes**: How the pipeline runs (initial, incremental, etc.)

## Building Pipelines

### Step 1: Define Bronze Steps

Bronze steps validate incoming raw data:

```python
from pyspark.sql import functions as F

# Define validation rules
bronze_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()],
    "value": [F.col("value") > 0],
}

# Add bronze step
builder.with_bronze_rules(
    name="raw_events",
    rules=bronze_rules,
    incremental_col="timestamp",  # Column for incremental processing
)
```

### Step 2: Define Silver Steps

Silver steps transform and clean bronze data:

```python
def silver_transform(spark, bronze_df, silvers):
    """Transform bronze data: add event_date and filter."""
    # Convert timestamp to date
    bronze_df = bronze_df.withColumn(
        "timestamp_str", F.col("timestamp").cast("string")
    )
    result_df = bronze_df.withColumn(
        "event_date",
        F.to_date(
            F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
        ),
    )
    # Filter and select columns
    return result_df.filter(F.col("user_id").isNotNull()).select(
        "user_id", "action", "event_date", "value", "timestamp_str"
    ).withColumnRenamed("timestamp_str", "timestamp")

# Define validation rules for silver output
silver_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "value": [F.col("value").isNotNull()],
    "event_date": [F.col("event_date").isNotNull()],
    "action": [F.col("action").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()],
}

# Add silver step
builder.add_silver_transform(
    name="processed_events",
    source_bronze="raw_events",
    transform=silver_transform,
    rules=silver_rules,
    table_name="silver_processed_events",
)
```

**Important**: Include validation rules for all columns you want to preserve. Columns without validation rules will be filtered out.

### Step 3: Define Gold Steps

Gold steps aggregate silver data:

```python
def gold_transform(spark, silvers):
    """Aggregate data from silver step."""
    silver = silvers.get("processed_events")
    if silver is not None:
        return silver.groupBy("user_id").agg(
            F.count("*").alias("event_count"),
            F.sum("value").alias("total_value"),
            F.avg("value").alias("avg_value"),
        )
    return spark.createDataFrame([], ["user_id", "event_count", "total_value", "avg_value"])

# Define validation rules for gold output
gold_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "event_count": [F.col("event_count") > 0],
    "total_value": [F.col("total_value") > 0],
}

# Add gold step
builder.add_gold_transform(
    name="user_metrics",
    source_silvers=["processed_events"],
    transform=gold_transform,
    rules=gold_rules,
    table_name="gold_user_metrics",
)
```

### Step 4: Validate and Build

```python
# Validate pipeline configuration
validation_errors = builder.validate_pipeline()
if validation_errors:
    print(f"Pipeline validation failed: {validation_errors}")
    exit(1)

# Build the pipeline
pipeline = builder.to_pipeline()
```

## Execution Modes

### Initial Load (Full Refresh)

Use `run_initial_load()` for:
- First-time data load
- Full refresh of existing data
- Rebuilding tables from scratch

**Behavior**: Overwrites existing tables with new data

```python
# Create source data
source_df = spark.createDataFrame(
    data, ["user_id", "action", "timestamp", "value"]
)

# Execute initial load
result = pipeline.run_initial_load(
    bronze_sources={"raw_events": source_df}
)

# Check results
if result.status.value == "completed":
    print("Pipeline executed successfully")
    print(f"Bronze rows processed: {result.bronze_results['raw_events']['rows_processed']}")
    print(f"Silver rows written: {result.silver_results['processed_events']['rows_written']}")
    print(f"Gold rows written: {result.gold_results['user_metrics']['rows_written']}")
```

### Incremental Load

Use `run_incremental()` for:
- Daily/hourly updates
- Processing only new data
- Efficient updates to existing tables

**Behavior**: Appends new data to existing tables (filters based on `incremental_col`)

```python
# Create incremental data (new records)
incremental_df = spark.createDataFrame(
    new_data, ["user_id", "action", "timestamp", "value"]
)

# Execute incremental load
result = pipeline.run_incremental(
    bronze_sources={"raw_events": incremental_df}
)

# Check results
if result.status.value == "completed":
    print("Incremental load completed")
    print(f"New rows processed: {result.bronze_results['raw_events']['rows_processed']}")
```

### Typical Workflow Pattern

```python
# 1. Initial load (one-time setup)
initial_result = pipeline.run_initial_load(
    bronze_sources={"raw_events": historical_data}
)

# 2. Regular incremental loads (scheduled)
incremental_result = pipeline.run_incremental(
    bronze_sources={"raw_events": new_data}
)
```

## Validation Rules

### Rule Syntax

Validation rules are dictionaries mapping column names to lists of PySpark expressions:

```python
rules = {
    "column_name": [
        F.col("column_name").isNotNull(),  # Not null check
        F.col("column_name") > 0,          # Value range check
        F.length(F.col("column_name")) > 5,  # String length check
    ]
}
```

### Common Validation Patterns

```python
# Not null validation
rules = {
    "user_id": [F.col("user_id").isNotNull()],
}

# Range validation
rules = {
    "age": [
        F.col("age").isNotNull(),
        F.col("age") >= 0,
        F.col("age") <= 120,
    ],
}

# String validation
rules = {
    "email": [
        F.col("email").isNotNull(),
        F.col("email").contains("@"),
        F.length(F.col("email")) > 5,
    ],
}

# Multiple conditions
rules = {
    "value": [
        F.col("value").isNotNull(),
        F.col("value") > 0,
        F.col("value") < 1000000,
    ],
}
```

### Validation Behavior

- **Bronze Layer**: Validates incoming raw data
- **Silver Layer**: Validates transformed data
- **Gold Layer**: Validates aggregated data
- **Filtering**: Invalid rows are filtered out (not written to tables)
- **Missing Columns**: Rules for columns that don't exist after transforms are automatically filtered out with a warning

## Logging and Monitoring

### Setting Up LogWriter

```python
from pipeline_builder.writer.core import LogWriter

# Create LogWriter
log_writer = LogWriter(
    spark=spark,
    schema="my_schema",
    table_name="pipeline_logs"
)
```

### Logging Pipeline Executions

```python
from pipeline_builder.writer.models import create_log_rows_from_pipeline_report

# Execute pipeline
result = pipeline.run_initial_load(
    bronze_sources={"raw_events": source_df}
)

# Convert result to log rows
log_rows = create_log_rows_from_pipeline_report(
    result,
    run_id="my_run_123",
    run_mode="initial"  # or "incremental"
)

# Write logs
write_result = log_writer.write_log_rows(log_rows, run_id="my_run_123")

if write_result["success"]:
    print(f"Logged {write_result['rows_written']} log entries")
```

### Querying Logs

```python
# Query pipeline logs
log_df = spark.table("my_schema.pipeline_logs")

# Filter by run
run_logs = log_df.filter(log_df.run_id == "my_run_123")

# Filter by step
step_logs = log_df.filter(log_df.step_name == "processed_events")

# Get metrics
metrics = log_writer.get_metrics()
print(f"Total writes: {metrics['total_writes']}")
print(f"Total rows written: {metrics['total_rows_written']}")
```

## Common Workflows

### Workflow 1: Initial Load with Multiple Runs

Useful for testing table overwrite behavior:

```python
# Run 1: Initial load with 100 records
result1 = pipeline.run_initial_load(
    bronze_sources={"raw_events": df_100_records}
)
# Table has 100 rows

# Run 2: Initial load with 150 records (overwrites)
result2 = pipeline.run_initial_load(
    bronze_sources={"raw_events": df_150_records}
)
# Table now has 150 rows (not 250)

# Run 3: Initial load with 200 records (overwrites)
result3 = pipeline.run_initial_load(
    bronze_sources={"raw_events": df_200_records}
)
# Table now has 200 rows (not 450)
```

### Workflow 2: Initial + Incremental Pattern

The most common production pattern:

```python
# Step 1: Initial load (historical data)
initial_result = pipeline.run_initial_load(
    bronze_sources={"raw_events": historical_data}
)
# Table has 1000 rows

# Step 2: Daily incremental loads
for day in range(1, 8):
    daily_data = get_daily_data(day)
    incremental_result = pipeline.run_incremental(
        bronze_sources={"raw_events": daily_data}
    )
    # Table accumulates: 1000 + 100 + 100 + ... = 1700 rows after 7 days
```

### Workflow 3: Handling Edge Cases

```python
# Empty DataFrame handling
empty_df = spark.createDataFrame([], schema)
result = pipeline.run_initial_load(
    bronze_sources={"raw_events": empty_df}
)
# Pipeline completes successfully with 0 rows

# Null value handling
# Validation rules automatically filter out nulls
rules = {
    "user_id": [F.col("user_id").isNotNull()],
}
# Rows with null user_id are filtered out

# Missing columns after transform
# Rules for missing columns are automatically filtered with a warning
# Pipeline continues with remaining valid rules
```

## Best Practices

### 1. Always Include Validation Rules for All Columns

```python
# Good: Rules for all columns
silver_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "value": [F.col("value").isNotNull()],
    "event_date": [F.col("event_date").isNotNull()],
    "action": [F.col("action").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()],
}

# Bad: Missing rules (columns will be filtered out)
silver_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    # Missing: event_date, action, timestamp
}
```

### 2. Use Incremental Columns for Efficient Processing

```python
# Always specify incremental_col for bronze steps
builder.with_bronze_rules(
    name="raw_events",
    rules=bronze_rules,
    incremental_col="timestamp",  # Required for incremental processing
)
```

### 3. Handle Empty DataFrames Gracefully

```python
# Check if DataFrame is empty before processing
if source_df.count() > 0:
    result = pipeline.run_initial_load(
        bronze_sources={"raw_events": source_df}
    )
else:
    print("No data to process")
```

### 4. Validate Pipeline Before Execution

```python
# Always validate before building
validation_errors = builder.validate_pipeline()
if validation_errors:
    print(f"Validation errors: {validation_errors}")
    # Fix errors before proceeding
```

### 5. Log All Executions

```python
# Log every pipeline execution
log_rows = create_log_rows_from_pipeline_report(
    result,
    run_id=f"run_{datetime.now().isoformat()}",
    run_mode="initial"  # or "incremental"
)
log_writer.write_log_rows(log_rows, run_id=run_id)
```

### 6. Use Unique Schemas for Testing

```python
# Use unique schemas to avoid conflicts
from uuid import uuid4
schema = f"test_schema_{uuid4().hex[:8]}"
builder = PipelineBuilder(spark=spark, schema=schema)
```

## Troubleshooting

### Issue: Columns Missing After Transform

**Symptom**: Validation rules reference columns that don't exist after transform

**Solution**: 
- Check your transform function - ensure it selects all required columns
- Add validation rules for all columns you want to preserve
- The framework will automatically filter out rules for missing columns with a warning

```python
# Transform must select all columns needed for validation
def silver_transform(spark, bronze_df, silvers):
    return bronze_df.select(
        "user_id", "action", "event_date", "value", "timestamp"  # All columns
    )

# Rules must include all columns from transform
silver_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "event_date": [F.col("event_date").isNotNull()],
    "action": [F.col("action").isNotNull()],
    "value": [F.col("value").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()],
}
```

### Issue: Tables Not Overwriting in Initial Mode

**Symptom**: Multiple initial runs accumulate data instead of overwriting

**Solution**:
- Ensure you're using `run_initial_load()` (not `run_incremental()`)
- Check that tables are being written in OVERWRITE mode
- Verify table counts match expected values after each run

### Issue: Incremental Processing Not Filtering

**Symptom**: Incremental runs process all data instead of just new data

**Solution**:
- Ensure `incremental_col` is specified in bronze step
- Verify incremental column is a comparable type (timestamp, date, integer, etc.)
- Check that incremental data has timestamps greater than existing data

### Issue: Validation Rules Not Working

**Symptom**: Invalid data passes validation

**Solution**:
- Verify rule syntax is correct (list of PySpark expressions)
- Check that rules reference correct column names
- Ensure rules are applied to the correct layer (bronze/silver/gold)

### Issue: Empty DataFrame Errors

**Symptom**: Pipeline fails with empty DataFrame

**Solution**:
- Provide explicit schema when creating empty DataFrames
- Handle empty DataFrames gracefully in your code
- Pipeline should complete successfully even with 0 rows

```python
# For PySpark, provide schema for empty DataFrames
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("value", DoubleType(), True),
])
empty_df = spark.createDataFrame([], schema)
```

## Examples

### Complete Example: Event Processing Pipeline

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.writer.core import LogWriter
from pipeline_builder.writer.models import create_log_rows_from_pipeline_report
from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4

# Setup
spark = SparkSession.builder.appName("EventPipeline").getOrCreate()
schema = "analytics"
log_writer = LogWriter(spark, schema=schema, table_name="pipeline_logs")

# Build pipeline
builder = PipelineBuilder(spark=spark, schema=schema)

# Bronze: Validate raw events
builder.with_bronze_rules(
    name="raw_events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "value": [F.col("value") > 0],
    },
    incremental_col="timestamp",
)

# Silver: Transform and clean
def silver_transform(spark, bronze_df, silvers):
    bronze_df = bronze_df.withColumn(
        "timestamp_str", F.col("timestamp").cast("string")
    )
    return bronze_df.withColumn(
        "event_date",
        F.to_date(F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss"))
    ).select("user_id", "action", "event_date", "value", "timestamp_str").withColumnRenamed("timestamp_str", "timestamp")

builder.add_silver_transform(
    name="processed_events",
    source_bronze="raw_events",
    transform=silver_transform,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "value": [F.col("value").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
    },
    table_name="silver_processed_events",
)

# Gold: Aggregate
def gold_transform(spark, silvers):
    silver = silvers.get("processed_events")
    return silver.groupBy("user_id").agg(
        F.count("*").alias("event_count"),
        F.sum("value").alias("total_value"),
        F.avg("value").alias("avg_value"),
    )

builder.add_gold_transform(
    name="user_metrics",
    source_silvers=["processed_events"],
    transform=gold_transform,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "event_count": [F.col("event_count") > 0],
    },
    table_name="gold_user_metrics",
)

# Validate and build
validation_errors = builder.validate_pipeline()
if validation_errors:
    raise ValueError(f"Pipeline validation failed: {validation_errors}")

pipeline = builder.to_pipeline()

# Execute: Initial load
source_df = spark.createDataFrame(data, ["user_id", "action", "timestamp", "value"])
result = pipeline.run_initial_load(bronze_sources={"raw_events": source_df})

# Log execution
log_rows = create_log_rows_from_pipeline_report(
    result,
    run_id=f"initial_{uuid4().hex[:8]}",
    run_mode="initial"
)
log_writer.write_log_rows(log_rows, run_id=f"initial_{uuid4().hex[:8]}")

print(f"Pipeline completed: {result.status.value}")
print(f"Rows processed: {result.bronze_results['raw_events']['rows_processed']}")
```

## Additional Resources

- **API Reference**: See `docs/writer_api_reference.md` for LogWriter API details
- **Validation Guide**: See `docs/markdown/VALIDATION_EDGE_CASES.md` for validation troubleshooting
- **Examples**: See `tests/system/` for complete workflow examples
- **Error Handling**: See `src/pipeline_builder/errors.py` for error types and handling

## Support

For issues, questions, or contributions, please refer to the project repository.


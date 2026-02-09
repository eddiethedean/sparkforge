# SparkForge Pipeline Builder - Comprehensive User Guide

**Version**: 2.8.0  
**Last Updated**: January 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Core Concepts](#core-concepts)
4. [Service-Oriented Architecture](#service-oriented-architecture)
5. [Building Pipelines](#building-pipelines)
6. [Execution Modes](#execution-modes)
7. [Stepwise Execution and Debugging](#stepwise-execution-and-debugging) - See [Stepwise Execution Guide](guides/STEPWISE_EXECUTION_GUIDE.md) for details
8. [Validation-Only Steps](#validation-only-steps) - See [Validation-Only Steps Guide](guides/VALIDATION_ONLY_STEPS_GUIDE.md) for details
9. [Validation Rules](#validation-rules)
10. [Logging and Monitoring](#logging-and-monitoring)
11. [Common Workflows](#common-workflows)
12. [Best Practices](#best-practices)
13. [Troubleshooting](#troubleshooting)

---

## Introduction

SparkForge Pipeline Builder is a production-ready framework for building and executing data pipelines using Apache Spark and Delta Lake. It implements the **Medallion Architecture** (Bronze → Silver → Gold) with built-in validation, logging, and monitoring capabilities.

### Key Features

- **Medallion Architecture**: Bronze (raw), Silver (cleaned), Gold (aggregated) data layers
- **Dual Engine Support**: Works with PySpark (production) and mock Spark (testing/development)
- **Automatic Validation**: Configurable validation rules at each layer
- **Incremental Processing**: Efficient processing of new/updated data
- **Dependency-Aware Execution**: Automatic dependency analysis and sequential execution
- **Comprehensive Logging**: Built-in LogWriter for tracking pipeline executions
- **Service-Oriented Architecture**: Clean separation of concerns with dedicated services
- **Type Safety**: Full mypy compliance and runtime type checks
- **Validation-Only Steps**: Validate existing tables and access them via `prior_silvers` and `prior_golds`

### Why PipelineBuilder?

- **70% Less Boilerplate**: Clean API vs raw Spark code
- **Built-in Validation**: Data quality checks at every layer
- **Dependency-Aware Execution**: Automatic dependency analysis and sequential execution
- **Production Ready**: Error handling, monitoring, alerting
- **Delta Lake Integration**: ACID transactions, time travel, schema evolution
- **Service Architecture**: Modular design for maintainability and testability

---

## Getting Started

### Installation

```bash
# Install the package
pip install pipeline-builder

# Or install with extras
pip install pipeline-builder[pyspark]  # For PySpark support
pip install pipeline-builder[dev,test]  # For development
```

### Engine Configuration

**Important**: You must configure the engine before using pipeline components. This allows the framework to work with both real PySpark and mock Spark for testing.

```python
from pipeline_builder.engine_config import configure_engine
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MyPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configure the engine (required before importing pipeline components)
configure_engine(spark=spark)

# Now you can use pipeline components
from pipeline_builder import PipelineBuilder
builder = PipelineBuilder(spark=spark, schema="my_schema")
```

### Basic Setup

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pyspark.sql import SparkSession

# Configure the engine (required)
spark = SparkSession.builder \
    .appName("MyPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

configure_engine(spark=spark)

# Create a pipeline builder
builder = PipelineBuilder(spark=spark, schema="my_schema")
```

---

## Core Concepts

### Medallion Architecture

The framework implements the Medallion Architecture pattern:

- **Bronze Layer**: Raw, unprocessed data from source systems
  - Validates incoming data
  - Stores data as-is (no transformations)
  - Acts as a landing zone
  - Data remains in-memory (not persisted)

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

---

## Service-Oriented Architecture

The PipelineBuilder uses a service-oriented architecture internally for clean separation of concerns and improved maintainability.

### Architecture Overview

```
PipelineBuilder
├── ExecutionEngine (orchestration)
│   ├── Step Executors
│   │   ├── BronzeStepExecutor
│   │   ├── SilverStepExecutor
│   │   └── GoldStepExecutor
│   ├── ExecutionValidator (validation service)
│   ├── TableService (table operations)
│   ├── WriteService (write operations)
│   ├── SchemaManager (schema management)
│   ├── TransformService (transformations)
│   ├── ExecutionReporter (reporting)
│   └── ErrorHandler (error handling)
└── StepFactory (step creation)
```

### Key Services

#### Step Executors

Step executors handle the execution logic for each step type:

- **BronzeStepExecutor**: Validates raw data without transformation or writing
- **SilverStepExecutor**: Transforms bronze data and handles incremental processing
- **GoldStepExecutor**: Aggregates silver data into final metrics/aggregates

**Example**:
```python
# Step executors are used internally by ExecutionEngine
# You don't typically interact with them directly, but understanding
# their role helps with debugging and customization
from pipeline_builder.step_executors import BronzeStepExecutor

# Executors handle:
# - Step-specific validation
# - Data transformation
# - Write operations
# - Error handling
```

#### ExecutionValidator

Validates data during pipeline execution according to step rules:

```python
from pipeline_builder.validation.execution_validator import ExecutionValidator

# The validator:
# - Applies validation rules to DataFrames
# - Calculates validation rates
# - Checks against thresholds
# - Provides detailed error messages
```

#### TableService

Manages table operations and schema:

```python
from pipeline_builder.storage.table_service import TableService

# The service handles:
# - Table existence checks
# - Schema validation
# - Table lifecycle operations
# - Fully qualified name (FQN) construction
```

#### WriteService

Handles all write operations to Delta Lake:

```python
from pipeline_builder.storage.write_service import WriteService

# The service handles:
# - Write mode determination (overwrite/append)
# - Schema validation for write mode
# - Schema overrides
# - Delta Lake operations
```

#### ErrorHandler

Centralized error handling for consistent error wrapping:

```python
from pipeline_builder.errors.error_handler import ErrorHandler

# The handler provides:
# - Consistent error wrapping
# - Context addition to errors
# - Helpful error messages with suggestions
```

### Benefits of Service Architecture

1. **Separation of Concerns**: Each service has a single responsibility
2. **Testability**: Services can be tested independently
3. **Maintainability**: Changes to one service don't affect others
4. **Extensibility**: Easy to add new services or modify existing ones
5. **Reusability**: Services can be reused across different contexts

---

## Building Pipelines

### Step 1: Define Bronze Steps

Bronze steps validate incoming raw data:

```python
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

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

**String Rules Support**:

You can also use human-readable string rules:

```python
# String rules are automatically converted to PySpark expressions
builder.with_bronze_rules(
    name="raw_events",
    rules={
        "user_id": ["not_null"],
        "value": ["gt", 0],
        "status": ["in", ["active", "inactive"]]
    },
    incremental_col="timestamp"
)
```

### Step 2: Define Silver Steps

Silver steps transform and clean bronze data:

```python
def silver_transform(spark, bronze_df, prior_silvers):
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

### Step 2b: Validation-Only Silver Steps (Optional)

For existing silver tables that need validation and access by subsequent transforms:

```python
# Validate existing silver table
builder.with_silver_rules(
    name="existing_clean_events",
    table_name="clean_events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()]
    }
)
```

**Output:**
```
✅ Added Silver step (validation-only): existing_clean_events
✅ Pipeline validation passed
```

**Note**: See [Validation-Only Steps Guide](guides/VALIDATION_ONLY_STEPS_GUIDE.md) for comprehensive documentation on `with_silver_rules` and `with_gold_rules`.

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

### Step 3b: Validation-Only Gold Steps (Optional)

For existing gold tables that need validation and access by subsequent transforms:

```python
# Validate existing gold table
builder.with_gold_rules(
    name="existing_user_metrics",
    table_name="user_metrics",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "total_events": [F.col("total_events") > 0]
    }
)
```

**Output:**
```
✅ Added Gold step (validation-only): existing_user_metrics
✅ Pipeline validation passed
```

**Note**: See [Validation-Only Steps Guide](guides/VALIDATION_ONLY_STEPS_GUIDE.md) for comprehensive documentation on `with_silver_rules` and `with_gold_rules`, including how to access validated tables via `prior_silvers` and `prior_golds`.

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

---

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

### Full Refresh

Use `run_full_refresh()` for:
- Reprocessing all data
- Schema changes
- Data corrections

**Behavior**: Overwrites existing tables with reprocessed data

```python
# Execute full refresh
result = pipeline.run_full_refresh(
    bronze_sources={"raw_events": source_df}
)
```

### Validation Only

Use `run_validation_only()` for:
- Testing validation rules
- Quality checks without writing
- Development and testing

**Behavior**: Validates data without writing to tables

```python
# Execute validation only
result = pipeline.run_validation_only(
    bronze_sources={"raw_events": source_df}
)

# Check validation results
print(f"Validation rate: {result.bronze_results['raw_events']['validation_rate']}%")
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

---

## Stepwise Execution and Debugging

> **📖 For a comprehensive guide to stepwise execution, see [Stepwise Execution Guide](guides/STEPWISE_EXECUTION_GUIDE.md)**

### Overview

Stepwise execution allows you to run individual pipeline steps, override parameters at runtime, and control execution flow—significantly speeding up the debugging and development cycle.

**Key Features:**
- **Run until a step**: Execute up to a specific step and stop
- **Run a single step**: Execute only one step, loading dependencies automatically
- **Rerun with overrides**: Rerun a step with different parameters without changing code
- **Write control**: Skip table writes for faster iteration during development

### Quick Example

```python
from pipeline_builder.pipeline.runner import SimplePipelineRunner
from pipeline_builder_base.models import PipelineConfig

config = PipelineConfig.create_default(schema="analytics")
runner = SimplePipelineRunner(spark, config)

# Run until a specific step
report, context = runner.run_until(
    "clean_events",
    steps=[bronze_step, silver_step, gold_step],
    bronze_sources={"events": source_df}
)

# Run a single step
report, context = runner.run_step(
    "clean_events",
    steps=all_steps,
    context=context
)

# Rerun with parameter overrides
step_params = {"clean_events": {"threshold": 0.9}}
report, context = runner.rerun_step(
    "clean_events",
    steps=all_steps,
    context=context,
    step_params=step_params
)
```

### When to Use

- **Development**: Testing new transform functions
- **Debugging**: Isolating issues in specific steps
- **Parameter Tuning**: Finding optimal parameter values
- **Quick Iteration**: Fast feedback cycles during development

For detailed documentation, examples, and best practices, see the [Stepwise Execution Guide](guides/STEPWISE_EXECUTION_GUIDE.md).

---

## Validation-Only Steps

For detailed information on validation-only steps (`with_silver_rules` and `with_gold_rules`), see the [Validation-Only Steps Guide](guides/VALIDATION_ONLY_STEPS_GUIDE.md).

### Quick Overview

Validation-only steps allow you to:
- Validate existing silver and gold tables without transformation
- Access validated tables in subsequent transforms via `prior_silvers` and `prior_golds`
- Create dependencies between transforms and existing tables

**Example:**
```python
# Validate existing silver table
builder.with_silver_rules(
    name="existing_clean_events",
    table_name="clean_events",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)

# Validate existing gold table
builder.with_gold_rules(
    name="existing_user_metrics",
    table_name="user_metrics",
    rules={"user_id": [F.col("user_id").isNotNull()]}
)

# Access validated tables in transforms
def enriched_transform(spark, bronze_df, prior_silvers, prior_golds=None):
    # Access validated existing silver
    if "existing_clean_events" in prior_silvers:
        existing = prior_silvers["existing_clean_events"]
    
    # Access validated existing gold
    if prior_golds and "existing_user_metrics" in prior_golds:
        metrics = prior_golds["existing_user_metrics"]
    
    return bronze_df
```

**Output:**
```
📊 Accessed existing_clean_events: 2 rows
📊 Accessed existing_user_metrics: 2 rows
```

See the [Validation-Only Steps Guide](guides/VALIDATION_ONLY_STEPS_GUIDE.md) for comprehensive documentation, examples, and best practices.

---

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

### String Rules

You can use human-readable string rules that are automatically converted:

```python
rules = {
    "user_id": ["not_null"],                    # F.col("user_id").isNotNull()
    "age": ["gt", 0],                           # F.col("age") > 0
    "status": ["in", ["active", "inactive"]],   # F.col("status").isin([...])
    "value": ["between", 0, 100],                # F.col("value").between(0, 100)
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

---

## Logging and Monitoring

### Setting Up LogWriter

The LogWriter provides a simplified API for logging pipeline executions:

```python
from pipeline_builder.writer import LogWriter

# Create LogWriter with new simplified API (recommended)
log_writer = LogWriter(
    spark=spark,
    schema="my_schema",
    table_name="pipeline_logs"
)
```

**Note**: The old API using `WriterConfig` is deprecated but still supported:

```python
# Old API (deprecated, emits warning)
from pipeline_builder.writer import LogWriter, WriterConfig, WriteMode
config = WriterConfig(table_schema="my_schema", table_name="pipeline_logs")
log_writer = LogWriter(spark=spark, config=config)  # DeprecationWarning
```

### Logging Pipeline Executions

```python
# Execute pipeline
result = pipeline.run_initial_load(
    bronze_sources={"raw_events": source_df}
)

# Log execution using simplified API
log_result = log_writer.append(result, run_id="my_run_123")

if log_result["success"]:
    print(f"Logged {log_result['rows_written']} log entries")
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

### Analytics and Monitoring

```python
# Analyze quality trends
quality_trends = log_writer.analyze_quality_trends(days=30)

# Analyze execution trends
execution_trends = log_writer.analyze_execution_trends(days=30)

# Detect anomalies
anomalies = log_writer.detect_quality_anomalies()

# Generate performance report
report = log_writer.generate_performance_report()
```

---

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

### Workflow 3: Multi-Source Pipeline

```python
# Multiple bronze sources
builder.with_bronze_rules(name="users", rules={...})
builder.with_bronze_rules(name="orders", rules={...})
builder.with_bronze_rules(name="products", rules={...})

# Silver steps can depend on multiple sources
builder.add_silver_transform(
    name="clean_orders",
    source_bronze="orders",
    transform=lambda spark, df, silvers: df.join(
        silvers.get("clean_users", spark.createDataFrame([], schema)),
        "user_id"
    ),
    rules={...},
    table_name="clean_orders"
)

# Execute with multiple sources
result = pipeline.run_initial_load(
    bronze_sources={
        "users": users_df,
        "orders": orders_df,
        "products": products_df
    }
)
```

### Workflow 4: Handling Edge Cases

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

---

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

### 3. Configure Engine Before Use

```python
# Always configure engine first
from pipeline_builder.engine_config import configure_engine
configure_engine(spark=spark)

# Then import and use pipeline components
from pipeline_builder import PipelineBuilder
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
from pipeline_builder.writer import LogWriter

log_writer = LogWriter(spark=spark, schema="monitoring", table_name="logs")
result = pipeline.run_initial_load(bronze_sources={"events": source_df})
log_writer.append(result, run_id=f"run_{datetime.now().isoformat()}")
```

### 6. Use Unique Schemas for Testing

```python
# Use unique schemas to avoid conflicts
from uuid import uuid4
schema = f"test_schema_{uuid4().hex[:8]}"
builder = PipelineBuilder(spark=spark, schema=schema)
```

### 7. Handle Errors Gracefully

```python
# Check execution results
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

if result.status.value == "completed":
    print("Success!")
else:
    print(f"Pipeline failed: {result.errors}")
    # Handle errors appropriately
```

---

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

### Issue: Engine Not Configured

**Symptom**: Errors about missing engine configuration

**Solution**:
- Always call `configure_engine(spark=spark)` before importing pipeline components
- Ensure Spark session is created before engine configuration
- Check that Delta Lake extensions are configured in Spark session

---

## Complete Example: Event Processing Pipeline

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.writer import LogWriter
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4

# Setup
spark = SparkSession.builder.appName("EventPipeline").getOrCreate()

# Configure engine (required)
configure_engine(spark=spark)

# Get functions
F = get_default_functions()

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
log_writer.append(result, run_id=f"initial_{uuid4().hex[:8]}")

print(f"Pipeline completed: {result.status.value}")
print(f"Rows processed: {result.bronze_results['raw_events']['rows_processed']}")
```

---

## Additional Resources

- **API Reference**: See `docs/api_reference.rst` for detailed API documentation
- **Architecture Guide**: See `docs/Architecture.md` for architecture details
- **Writer Guide**: See `docs/writer_api_reference.md` for LogWriter API details
- **Examples**: See `examples/` directory for complete workflow examples
- **Troubleshooting**: See `docs/COMPREHENSIVE_TROUBLESHOOTING_GUIDE.md` for detailed troubleshooting

---

## Support

For issues, questions, or contributions, please refer to the project repository.

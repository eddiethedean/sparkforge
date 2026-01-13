# SparkForge Enhanced API Reference

This document provides comprehensive API documentation for SparkForge with detailed examples, usage patterns, and best practices.

## Table of Contents

1. [Engine Configuration](#engine-configuration)
2. [Core Classes](#core-classes)
3. [Step Executors](#step-executors)
4. [Services](#services)
5. [Data Models](#data-models)
6. [Execution Engine](#execution-engine)
7. [Validation System](#validation-system)
8. [Error Handling](#error-handling)
9. [LogWriter](#logwriter)
10. [Utility Functions](#utility-functions)
11. [Examples and Use Cases](#examples-and-use-cases)
12. [Migration Guide](#migration-guide)

## Engine Configuration

### configure_engine

**Required**: You must configure the engine before using any PipelineBuilder components.

```python
from pipeline_builder.engine_config import configure_engine
from pyspark.sql import SparkSession

# Create Spark session with Delta Lake
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configure engine (required!)
configure_engine(spark=spark)
```

**Parameters:**
- `spark` (SparkSession): Spark session to use for pipeline execution

**Example with Mock Spark (Testing):**

```python
from pipeline_builder.engine_config import configure_engine
from mock_spark import MockSparkSession

mock_spark = MockSparkSession()
configure_engine(spark=mock_spark)
```

## Core Classes

### PipelineBuilder

The main class for building data pipelines with the Medallion Architecture.

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions
from pyspark.sql import SparkSession

# Configure engine first
spark = SparkSession.builder.getOrCreate()
configure_engine(spark=spark)
F = get_default_functions()

# Create builder
builder = PipelineBuilder(spark=spark, schema="analytics")
```

#### Methods

##### `__init__(spark: SparkSession, schema: str, min_bronze_rate: float = 95.0, min_silver_rate: float = 98.0, min_gold_rate: float = 99.0, verbose: bool = True)`

Initialize the pipeline builder.

**Parameters:**
- `spark` (SparkSession): Spark session
- `schema` (str): Database schema name
- `min_bronze_rate` (float): Minimum validation rate for Bronze layer (default: 95.0)
- `min_silver_rate` (float): Minimum validation rate for Silver layer (default: 98.0)
- `min_gold_rate` (float): Minimum validation rate for Gold layer (default: 99.0)
- `verbose` (bool): Enable verbose logging (default: True)

**Example:**
```python
builder = PipelineBuilder(
    spark=spark,
    schema="analytics",
    min_bronze_rate=90.0,
    min_silver_rate=95.0,
    min_gold_rate=97.0,
    verbose=False
)
```

##### `with_bronze_rules(name: str, rules: Dict[str, List], incremental_col: Optional[str] = None) -> PipelineBuilder`

Add validation rules for a Bronze step.

**Parameters:**
- `name` (str): Name of the bronze step
- `rules` (Dict[str, List]): Validation rules (PySpark expressions or string rules)
- `incremental_col` (str, optional): Column name for incremental processing

**Returns:**
- `PipelineBuilder`: Self for method chaining

**Example:**
```python
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

# Using PySpark expressions
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "amount": [F.col("amount") > 0]
    },
    incremental_col="timestamp"
)

# Using string rules (simplified syntax)
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": ["not_null"],
        "amount": ["gt", 0],
        "status": ["in", ["active", "inactive"]]
    },
    incremental_col="timestamp"
)
```

##### `add_silver_transform(name: str, source_bronze: str, transform: Callable, rules: Dict[str, List], table_name: str, source_silvers: Optional[List[str]] = None) -> PipelineBuilder`

Add a Silver transformation step.

**Parameters:**
- `name` (str): Name of the silver step
- `source_bronze` (str): Source bronze step name
- `transform` (Callable): Transform function with signature `(spark, bronze_df, prior_silvers) -> DataFrame`
- `rules` (Dict[str, List]): Validation rules
- `table_name` (str): Target table name
- `source_silvers` (List[str], optional): Additional source silver steps

**Returns:**
- `PipelineBuilder`: Self for method chaining

**Example:**
```python
def clean_events(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return bronze_df.filter(F.col("status") == "active")

builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_events,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "status": [F.col("status").isNotNull()]
    },
    table_name="clean_events"
)
```

##### `add_gold_transform(name: str, transform: Callable, rules: Dict[str, List], table_name: str, source_silvers: List[str]) -> PipelineBuilder`

Add a Gold transformation step.

**Parameters:**
- `name` (str): Name of the gold step
- `transform` (Callable): Transform function with signature `(spark, silvers) -> DataFrame`
- `rules` (Dict[str, List]): Validation rules
- `table_name` (str): Target table name
- `source_silvers` (List[str]): Source silver step names

**Returns:**
- `PipelineBuilder`: Self for method chaining

**Example:**
```python
def daily_metrics(spark, silvers):
    F = get_default_functions()
    return silvers["clean_events"].groupBy("date").agg(
        F.count("*").alias("count"),
        F.sum("amount").alias("total_amount")
    )

builder.add_gold_transform(
    name="daily_metrics",
    transform=daily_metrics,
    rules={
        "date": [F.col("date").isNotNull()],
        "count": [F.col("count") > 0]
    },
    table_name="daily_metrics",
    source_silvers=["clean_events"]
)
```

##### `validate_pipeline() -> List[str]`

Validate the entire pipeline configuration.

**Returns:**
- `List[str]`: List of validation errors (empty if valid)

**Example:**
```python
errors = builder.validate_pipeline()
if errors:
    print("Validation errors:")
    for error in errors:
        print(f"- {error}")
else:
    print("Pipeline is valid!")
```

##### `to_pipeline() -> Pipeline`

Build and return the pipeline.

**Returns:**
- `Pipeline`: Executable pipeline object

**Example:**
```python
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": source_df})
```

## Step Executors

### BronzeStepExecutor

Executor for Bronze layer steps. Validates raw data without transformation.

```python
from pipeline_builder.step_executors.bronze import BronzeStepExecutor
from pipeline_builder.models import BronzeStep, PipelineConfig

config = PipelineConfig.create_default(schema="analytics")
executor = BronzeStepExecutor(spark=spark, config=config)

bronze_step = BronzeStep(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    incremental_col="timestamp"
)

result = executor.execute(step=bronze_step, sources={"events": source_df})
```

### SilverStepExecutor

Executor for Silver layer steps. Transforms bronze data and handles incremental processing.

```python
from pipeline_builder.step_executors.silver import SilverStepExecutor

executor = SilverStepExecutor(spark=spark, config=config)

silver_step = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=clean_events_func,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="clean_events"
)

result = executor.execute(step=silver_step, sources={"events": bronze_df})
```

### GoldStepExecutor

Executor for Gold layer steps. Aggregates silver data into final metrics.

```python
from pipeline_builder.step_executors.gold import GoldStepExecutor

executor = GoldStepExecutor(spark=spark, config=config)

gold_step = GoldStep(
    name="daily_metrics",
    transform=daily_metrics_func,
    rules={"date": [F.col("date").isNotNull()]},
    table_name="daily_metrics",
    source_silvers=["clean_events"]
)

result = executor.execute(step=gold_step, sources={"clean_events": silver_df})
```

## Services

### ExecutionValidator

Service for validating data during pipeline execution.

```python
from pipeline_builder.validation.execution_validator import ExecutionValidator

validator = ExecutionValidator(spark=spark, config=config)

# Validate step output
validation_result = validator.validate_step_output(
    df=output_df,
    step=bronze_step,
    stage="bronze"
)

if validation_result.validation_passed:
    print(f"Validation passed: {validation_result.validation_rate:.2f}%")
else:
    print(f"Validation failed: {validation_result.errors}")
```

### TableService

Service for table operations and schema management.

```python
from pipeline_builder.storage.table_service import TableService

table_service = TableService(spark=spark, config=config)

# Ensure schema exists
table_service.ensure_schema_exists()

# Check if table exists
if table_service.table_exists("clean_events"):
    print("Table exists")

# Get table schema
schema = table_service.get_table_schema("clean_events")
print(f"Schema: {schema}")
```

### WriteService

Service for writing DataFrames to tables.

```python
from pipeline_builder.storage.write_service import WriteService

write_service = WriteService(spark=spark, config=config)

# Write step output
result = write_service.write_step_output(
    df=output_df,
    step=silver_step,
    mode="overwrite"
)

print(f"Rows written: {result['rows_written']}")
```

### TransformService

Service for applying transformations to DataFrames.

```python
from pipeline_builder.transformation.transform_service import TransformService

transform_service = TransformService(spark=spark, config=config)

# Apply silver transform
transformed_df = transform_service.apply_silver_transform(
    step=silver_step,
    bronze_df=bronze_df,
    prior_silvers={}
)

# Apply gold transform
aggregated_df = transform_service.apply_gold_transform(
    step=gold_step,
    silvers={"clean_events": silver_df}
)
```

### ExecutionReporter

Service for creating execution reports.

```python
from pipeline_builder.reporting.execution_reporter import ExecutionReporter

reporter = ExecutionReporter(spark=spark, config=config)

# Create execution summary
summary = reporter.create_execution_summary(
    context=execution_context,
    step_results=[bronze_result, silver_result, gold_result]
)

print(f"Total rows: {summary.total_rows_written}")
print(f"Duration: {summary.duration_seconds:.2f}s")
```

### ErrorHandler

Centralized error handler for pipeline operations.

```python
from pipeline_builder.errors.error_handler import ErrorHandler

error_handler = ErrorHandler()

# Use as context manager
with error_handler.handle_errors(step_name="bronze_events", stage="bronze"):
    # Your code here
    result = execute_step()

# Use as decorator
@error_handler.wrap_error(step_name="silver_clean", stage="silver")
def my_transform():
    # Your code here
    return transformed_df
```

## Data Models

### PipelineConfig

Main pipeline configuration.

```python
from pipeline_builder.models import PipelineConfig, ValidationThresholds

# Create default config
config = PipelineConfig.create_default(schema="analytics")

# Create custom config
config = PipelineConfig(
    schema="analytics",
    thresholds=ValidationThresholds(bronze=90.0, silver=95.0, gold=97.0),
    verbose=False
)

# Validate config
config.validate()
```

### ValidationThresholds

Validation thresholds for each pipeline layer.

```python
from pipeline_builder.models import ValidationThresholds

# Create default thresholds
thresholds = ValidationThresholds.create_default()

# Create strict thresholds
thresholds = ValidationThresholds.create_strict()

# Create loose thresholds
thresholds = ValidationThresholds.create_loose()

# Custom thresholds
thresholds = ValidationThresholds(bronze=90.0, silver=95.0, gold=97.0)
```

### BronzeStep, SilverStep, GoldStep

Step configuration models. See [Core Classes](#core-classes) for usage examples.

## Execution Engine

### ExecutionEngine

The execution engine orchestrates pipeline execution using step executors and services.

```python
from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models import PipelineConfig

config = PipelineConfig.create_default(schema="analytics")
engine = ExecutionEngine(spark=spark, config=config)

# Execute a single step
result = engine.execute_step(
    step=bronze_step,
    sources={"events": source_df},
    mode=ExecutionMode.INITIAL
)

# Execute entire pipeline
result = engine.execute_pipeline(
    steps=[bronze_step, silver_step, gold_step],
    sources={"events": source_df},
    mode=ExecutionMode.INITIAL
)
```

## Validation System

### String Rules

Simplified validation syntax using string rules:

```python
rules = {
    "user_id": ["not_null"],                    # F.col("user_id").isNotNull()
    "age": ["gt", 0],                           # F.col("age") > 0
    "status": ["in", ["active", "inactive"]],   # F.col("status").isin([...])
    "score": ["between", 0, 100],               # F.col("score").between(0, 100)
    "email": ["not_empty"],                      # F.length(F.col("email")) > 0
    "timestamp": ["not_null", "is_timestamp"]   # Multiple rules
}
```

### PySpark Expressions

Advanced validation using PySpark expressions:

```python
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "age": [F.col("age") > 0, F.col("age") < 150],
    "email": [F.col("email").contains("@")]
}
```

## Error Handling

### Custom Exceptions

```python
from pipeline_builder.models.exceptions import (
    PipelineConfigurationError,
    PipelineExecutionError
)

try:
    pipeline = builder.to_pipeline()
    result = pipeline.run_initial_load(bronze_sources={"events": df})
except PipelineConfigurationError as e:
    print(f"Configuration error: {e}")
except PipelineExecutionError as e:
    print(f"Execution error: {e}")
```

## LogWriter

### LogWriter (New Simplified API)

```python
from pipeline_builder.writer import LogWriter

# Create LogWriter
writer = LogWriter(
    spark=spark,
    schema="monitoring",
    table_name="pipeline_logs"
)

# Append execution result
result = pipeline.run_initial_load(bronze_sources={"events": source_df})
write_result = writer.append(result)

# Create or overwrite table
write_result = writer.create_table(result)

# Show recent logs
writer.show_logs(limit=10)

# Analyze quality trends
trends = writer.analyze_quality_trends(days=7)
```

## Utility Functions

### get_default_functions

Get the default functions object from the configured engine.

```python
from pipeline_builder.functions import get_default_functions

F = get_default_functions()

# Use functions
df = df.withColumn("upper_name", F.upper(F.col("name")))
```

### Table Operations

```python
from pipeline_builder.table_operations import (
    fqn,
    table_exists,
    read_table,
    write_overwrite_table
)

# Generate fully qualified name
table_fqn = fqn("analytics", "clean_events")

# Check if table exists
if table_exists(spark, table_fqn):
    # Read table
    df = read_table(spark, table_fqn)
    
    # Write table
    rows_written = write_overwrite_table(df, table_fqn)
```

## Examples and Use Cases

### Complete Pipeline Example

```python
from pipeline_builder.engine_config import configure_engine
from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions
from pyspark.sql import SparkSession

# 1. Configure engine
spark = SparkSession.builder.getOrCreate()
configure_engine(spark=spark)
F = get_default_functions()

# 2. Create sample data
data = [
    ("user1", "click", 10.0, "2024-01-01 10:00:00"),
    ("user2", "purchase", 25.0, "2024-01-01 11:00:00"),
]
source_df = spark.createDataFrame(data, ["user_id", "action", "amount", "timestamp"])

# 3. Build pipeline
builder = PipelineBuilder(spark=spark, schema="analytics")

# Bronze
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "amount": [F.col("amount") > 0]
    },
    incremental_col="timestamp"
)

# Silver
def clean_events(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return bronze_df.filter(F.col("action").isin(["click", "purchase"]))

builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_events,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="clean_events"
)

# Gold
def daily_summary(spark, silvers):
    F = get_default_functions()
    return silvers["clean_events"].groupBy("action").agg(
        F.count("*").alias("count"),
        F.sum("amount").alias("total_amount")
    )

builder.add_gold_transform(
    name="daily_summary",
    transform=daily_summary,
    rules={"action": [F.col("action").isNotNull()]},
    table_name="daily_summary",
    source_silvers=["clean_events"]
)

# 4. Validate and execute
errors = builder.validate_pipeline()
if errors:
    print(f"Validation errors: {errors}")
else:
    pipeline = builder.to_pipeline()
    result = pipeline.run_initial_load(bronze_sources={"events": source_df})
    print(f"Pipeline completed: {result.status.value}")
    print(f"Rows written: {result.metrics.total_rows_written}")
```

### Incremental Processing

```python
# Run incremental load
new_data = spark.createDataFrame([...], schema)
result = pipeline.run_incremental(bronze_sources={"events": new_data})
```

### Full Refresh

```python
# Reprocess all data
result = pipeline.run_full_refresh(bronze_sources={"events": all_data})
```

### Validation Only

```python
# Validate without writing
result = pipeline.run_validation_only(bronze_sources={"events": test_data})
```

## Migration Guide

### From Old API to New API

**Old API (Deprecated):**

```python
# Old way
builder.add_bronze_step("events", transform_func, rules)
builder.add_silver_step("clean", transform_func, rules, source_bronze="events")
builder.add_gold_step("metrics", "table", transform_func, rules, source_silvers=["clean"])

result = pipeline.initial_load(bronze_sources={"events": df})
```

**New API:**

```python
# New way
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions

configure_engine(spark=spark)
F = get_default_functions()

builder.with_bronze_rules(name="events", rules=rules, incremental_col="timestamp")
builder.add_silver_transform(
    name="clean",
    source_bronze="events",
    transform=transform_func,
    rules=rules,
    table_name="clean_events"
)
builder.add_gold_transform(
    name="metrics",
    transform=transform_func,
    rules=rules,
    table_name="metrics",
    source_silvers=["clean"]
)

result = pipeline.run_initial_load(bronze_sources={"events": df})
```

### Key Changes

1. **Engine Configuration Required**: Must call `configure_engine(spark=spark)` before use
2. **Method Names**: 
   - `add_bronze_step` → `with_bronze_rules`
   - `add_silver_step` → `add_silver_transform`
   - `add_gold_step` → `add_gold_transform`
3. **Transform Signatures**:
   - Silver: `(spark, bronze_df, prior_silvers) -> DataFrame`
   - Gold: `(spark, silvers) -> DataFrame`
4. **Execution Methods**:
   - `initial_load()` → `run_initial_load()`
   - `incremental()` → `run_incremental()`
   - `full_refresh()` → `run_full_refresh()`
   - `validation_only()` → `run_validation_only()`
5. **Parallel Execution Removed**: Sequential execution with dependency-aware ordering
6. **Service-Oriented Architecture**: Internal services (ExecutionValidator, TableService, etc.) are now documented

This enhanced API reference provides comprehensive documentation for all SparkForge components with detailed examples and usage patterns. For additional information, refer to the specific class documentation or the main user guide.

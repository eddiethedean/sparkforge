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

# SparkForge API Reference

Complete API reference for SparkForge data pipeline framework.

## Table of Contents

1. [Core Classes](#core-classes)
2. [PipelineBuilder](#pipelinebuilder)
3. [Multi-Schema Support](#multi-schema-support)
4. [PipelineRunner](#pipelinerunner)
5. [StepExecutor](#stepexecutor)
6. [Models](#models)
7. [Validation](#validation)
8. [Performance](#performance)
9. [Logging](#logging)
10. [Security](#security)
11. [Performance Cache](#performance-cache)
12. [Dynamic Parallel Execution](#dynamic-parallel-execution)
13. [Exceptions](#exceptions)

## Core Classes

### PipelineBuilder

Main class for building data pipelines with fluent API.

```python
from sparkforge import PipelineBuilder

builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    min_bronze_rate=95.0,
    min_silver_rate=98.0,
    min_gold_rate=99.0,
    enable_parallel_silver=True,
    max_parallel_workers=4,
    verbose=False
)
```

#### Methods

##### `for_development(spark, schema, **kwargs)` (Class Method)

Create a PipelineBuilder configured for development with relaxed validation and verbose logging.

**Parameters:**
- `spark` (SparkSession): Spark session
- `schema` (str): Database schema name
- `**kwargs`: Additional configuration options

**Returns:** `PipelineBuilder` instance

**Example:**
```python
builder = PipelineBuilder.for_development(spark=spark, schema="dev_schema")
```

##### `for_production(spark, schema, **kwargs)` (Class Method)

Create a PipelineBuilder configured for production with strict validation and optimized settings.

**Parameters:**
- `spark` (SparkSession): Spark session
- `schema` (str): Database schema name
- `**kwargs`: Additional configuration options

**Returns:** `PipelineBuilder` instance

**Example:**
```python
builder = PipelineBuilder.for_production(spark=spark, schema="prod_schema")
```

##### `for_testing(spark, schema, **kwargs)` (Class Method)

Create a PipelineBuilder configured for testing with minimal validation and sequential execution.

**Parameters:**
- `spark` (SparkSession): Spark session
- `schema` (str): Database schema name
- `**kwargs`: Additional configuration options

**Returns:** `PipelineBuilder` instance

**Example:**
```python
builder = PipelineBuilder.for_testing(spark=spark, schema="test_schema")
```

##### `not_null_rules(columns)` (Static Method)

Create validation rules for non-null constraints.

**Parameters:**
- `columns` (List[str]): List of column names

**Returns:** `dict` of validation rules

**Example:**
```python
rules = PipelineBuilder.not_null_rules(["user_id", "timestamp"])
```

##### `positive_number_rules(columns)` (Static Method)

Create validation rules for positive number constraints.

**Parameters:**
- `columns` (List[str]): List of column names

**Returns:** `dict` of validation rules

**Example:**
```python
rules = PipelineBuilder.positive_number_rules(["amount", "quantity"])
```

##### `string_not_empty_rules(columns)` (Static Method)

Create validation rules for non-empty string constraints.

**Parameters:**
- `columns` (List[str]): List of column names

**Returns:** `dict` of validation rules

**Example:**
```python
rules = PipelineBuilder.string_not_empty_rules(["name", "description"])
```

##### `timestamp_rules(columns)` (Static Method)

Create validation rules for timestamp constraints.

**Parameters:**
- `columns` (List[str]): List of column names

**Returns:** `dict` of validation rules

**Example:**
```python
rules = PipelineBuilder.timestamp_rules(["created_at", "updated_at"])
```

##### `detect_timestamp_columns(schema)` (Static Method)

Detect timestamp columns based on common naming patterns.

**Parameters:**
- `schema`: DataFrame schema or list of column names

**Returns:** `List[str]` of detected timestamp column names

**Example:**
```python
timestamp_cols = PipelineBuilder.detect_timestamp_columns(df.schema)
```

##### `with_bronze_rules(name, rules, incremental_col=None, description=None, schema=None)`

Define Bronze layer data ingestion rules.

**Parameters:**
- `name` (str): Bronze step name
- `rules` (dict): Validation rules for columns
- `incremental_col` (str, optional): Column for incremental processing
- `description` (str, optional): Description of this Bronze step
- `schema` (str, optional): Schema name for reading bronze data. If not provided, uses the builder's default schema.

**Returns:** `PipelineBuilder` (for chaining)

**Example:**
```python
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    },
    incremental_col="timestamp",
    schema="raw_data"  # Read from different schema
)
```

##### `add_silver_transform(name, transform, rules, table_name, source_bronze=None, watermark_col=None, description=None, depends_on=None, schema=None)`

Add Silver layer transformation step with auto-inference of source_bronze.

**Parameters:**
- `name` (str): Silver step name
- `transform` (callable): Transformation function
- `rules` (dict): Validation rules
- `table_name` (str): Output table name
- `source_bronze` (str, optional): Source Bronze step name (auto-inferred if not provided)
- `watermark_col` (str, optional): Watermark column for streaming
- `description` (str, optional): Description of this Silver step
- `depends_on` (list, optional): List of other Silver step names that must complete first
- `schema` (str, optional): Schema name for writing silver data. If not provided, uses the builder's default schema.
- `source_silvers` (list, optional): Dependent Silver steps

**Returns:** `PipelineBuilder` (for chaining)

**Example:**
```python
def silver_transform(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("status") == "active")

builder.add_silver_transform(
    name="silver_events",
    source_bronze="events",
    transform=silver_transform,
    rules={"status": [F.col("status").isNotNull()]},
    table_name="silver_events",
    watermark_col="timestamp"
)
```

##### `add_gold_transform(name, transform, rules, table_name, source_silvers=None, description=None, schema=None)`

Add Gold layer aggregation step with auto-inference of source_silvers.

**Parameters:**
- `name` (str): Gold step name
- `transform` (callable): Aggregation function
- `rules` (dict): Validation rules
- `table_name` (str): Output table name
- `source_silvers` (list, optional): Source Silver steps (auto-inferred if not provided)
- `description` (str, optional): Description of this Gold step
- `schema` (str, optional): Schema name for writing gold data. If not provided, uses the builder's default schema.

**Returns:** `PipelineBuilder` (for chaining)

**Example:**
```python
def gold_transform(spark, silvers):
    events_df = silvers["silver_events"]
    return events_df.groupBy("category").count()

builder.add_gold_transform(
    name="gold_summary",
    transform=gold_transform,
    rules={"category": [F.col("category").isNotNull()]},
    table_name="gold_summary",
    source_silvers=["silver_events"],
    schema="analytics"  # Write to different schema
)
```

##### `enable_unified_execution(max_workers=4, enable_parallel_execution=True, enable_dependency_optimization=True)`

Enable unified cross-layer parallel execution.

**Parameters:**
- `max_workers` (int): Maximum parallel workers
- `enable_parallel_execution` (bool): Enable parallel execution
- `enable_dependency_optimization` (bool): Enable dependency optimization

**Returns:** `PipelineBuilder` (for chaining)

##### `to_pipeline()`

Build and return the pipeline.

**Returns:** `PipelineRunner`

## Multi-Schema Support

SparkForge supports cross-schema data flows, enabling multi-tenant applications, environment separation, and data lake architectures.

### Schema Parameters

All pipeline methods support optional `schema` parameters:

#### `with_bronze_rules(schema=None)`
- **Purpose**: Read bronze data from a specific schema
- **Default**: Uses builder's default schema if not specified
- **Example**: `schema="raw_data"` reads from `raw_data` schema

#### `with_silver_rules(schema=None)`
- **Purpose**: Read existing silver data from a specific schema
- **Default**: Uses builder's default schema if not specified
- **Example**: `schema="staging"` reads from `staging` schema

#### `add_silver_transform(schema=None)`
- **Purpose**: Write silver data to a specific schema
- **Default**: Uses builder's default schema if not specified
- **Example**: `schema="processing"` writes to `processing` schema

#### `add_gold_transform(schema=None)`
- **Purpose**: Write gold data to a specific schema
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

### Multi-Schema Examples

#### Multi-Tenant SaaS Application
```python
# Each tenant gets their own schema
tenant_id = "tenant_123"
builder = PipelineBuilder(spark=spark, schema="default")

# Read from tenant-specific raw data
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    schema=f"{tenant_id}_raw"  # tenant_123_raw
)

# Write to tenant-specific processing
builder.add_silver_transform(
    name="clean_events",
    transform=clean_events,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="clean_events",
    schema=f"{tenant_id}_processing"  # tenant_123_processing
)
```

#### Data Lake Architecture
```python
# Bronze (raw) → Silver (processing) → Gold (analytics)
builder = PipelineBuilder(spark=spark, schema="default")

# Bronze: Raw data ingestion
builder.with_bronze_rules(
    name="raw_events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    schema="raw_data"  # Raw data layer
)

# Silver: Data processing and cleaning
builder.add_silver_transform(
    name="clean_events",
    transform=clean_events,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="clean_events",
    schema="processing"  # Processing layer
)

# Gold: Business analytics
builder.add_gold_transform(
    name="daily_metrics",
    transform=daily_metrics,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="daily_metrics",
    schema="analytics"  # Analytics layer
)
```

#### Environment Separation
```python
# Dev/Staging/Prod schema separation
environment = "dev"  # or "staging" or "prod"
builder = PipelineBuilder(spark=spark, schema="default")

# All steps use environment-specific schemas
builder.with_bronze_rules(
    name="events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    schema=f"{environment}_raw"
)

builder.add_silver_transform(
    name="clean_events",
    transform=clean_events,
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="clean_events",
    schema=f"{environment}_processing"
)
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

### PipelineRunner

Executes pipelines with different modes and provides step debugging.

```python
pipeline = builder.to_pipeline()
```

#### Methods

##### `initial_load(bronze_sources)`

Execute full refresh of all data.

**Parameters:**
- `bronze_sources` (dict): Bronze data sources

**Returns:** `ExecutionResult`

##### `run_incremental(bronze_sources)`

Execute incremental processing.

**Parameters:**
- `bronze_sources` (dict): New Bronze data

**Returns:** `ExecutionResult`

##### `run_full_refresh(bronze_sources)`

Force complete reprocessing.

**Parameters:**
- `bronze_sources` (dict): Bronze data sources

**Returns:** `ExecutionResult`

##### `run_validation_only(bronze_sources)`

Check data quality without writing.

**Parameters:**
- `bronze_sources` (dict): Bronze data sources

**Returns:** `ExecutionResult`

##### `run_unified(bronze_sources)`

Execute with unified parallel processing.

**Parameters:**
- `bronze_sources` (dict): Bronze data sources

**Returns:** `UnifiedExecutionResult`

##### `execute_bronze_step(name, input_data)`

Execute individual Bronze step.

**Parameters:**
- `name` (str): Bronze step name
- `input_data` (DataFrame): Input data

**Returns:** `StepExecutionResult`

##### `execute_silver_step(name, force_input=False)`

Execute individual Silver step.

**Parameters:**
- `name` (str): Silver step name
- `force_input` (bool): Force input data generation

**Returns:** `StepExecutionResult`

##### `execute_gold_step(name)`

Execute individual Gold step.

**Parameters:**
- `name` (str): Gold step name

**Returns:** `StepExecutionResult`

##### `get_step_info(name)`

Get information about a specific step.

**Parameters:**
- `name` (str): Step name

**Returns:** `dict`

##### `list_steps()`

List all pipeline steps.

**Returns:** `dict`

##### `create_step_executor()`

Create step executor for debugging.

**Returns:** `StepExecutor`

### StepExecutor

Provides detailed step execution and debugging capabilities.

```python
executor = pipeline.create_step_executor()
```

#### Methods

##### `get_step_output(step_name)`

Get output DataFrame for a step.

**Parameters:**
- `step_name` (str): Step name

**Returns:** `DataFrame`

##### `list_completed_steps()`

List completed steps.

**Returns:** `list`

##### `list_failed_steps()`

List failed steps.

**Returns:** `list`

##### `clear_execution_state()`

Clear execution state.

## Models

### ValidationThresholds

Data quality thresholds for each layer.

```python
from sparkforge.models import ValidationThresholds

thresholds = ValidationThresholds(
    bronze=95.0,
    silver=98.0,
    gold=99.0
)
```

### ParallelConfig

Parallel execution configuration.

```python
from sparkforge.models import ParallelConfig

config = ParallelConfig(
    max_workers=8,
    enable_parallel_execution=True,
    enable_dependency_optimization=True
)
```

### ExecutionResult

Result of pipeline execution.

```python
result = pipeline.run_incremental(bronze_sources={"events": source_df})

# Properties
result.success                    # bool: Execution success
result.error_message             # str: Error message if failed
result.totals                    # dict: Execution totals
result.stage_stats              # dict: Stage-specific statistics
result.metrics                  # PipelineMetrics: Performance metrics
result.failed_steps             # list: Failed step names
```

### StepExecutionResult

Result of individual step execution.

```python
step_result = pipeline.execute_bronze_step("events", input_data=source_df)

# Properties
step_result.status               # StepStatus: Execution status
step_result.output_count        # int: Output row count
step_result.duration_seconds    # float: Execution duration
step_result.validation_result   # ValidationResult: Validation results
step_result.error_message       # str: Error message if failed
```

### PipelineMetrics

Performance metrics for pipeline execution.

```python
metrics = result.metrics

# Properties
metrics.total_duration          # float: Total execution time
metrics.parallel_efficiency    # float: Parallel efficiency percentage
metrics.step_durations         # dict: Step-specific durations
metrics.memory_usage           # dict: Memory usage statistics
```

## Validation

### Validation Functions

```python
from sparkforge.validation import (
    and_all_rules,
    validate_dataframe_schema,
    get_dataframe_info,
    apply_column_rules,
    assess_data_quality,
    safe_divide
)
```

#### `and_all_rules(rules)`

Combine validation rules with AND logic.

**Parameters:**
- `rules` (list): List of validation rules

**Returns:** `Column`

#### `apply_column_rules(df, rules, stage, step, filter_columns_by_rules=True)`

Apply validation rules to a DataFrame and return valid/invalid DataFrames with statistics.

**Parameters:**
- `df` (DataFrame): DataFrame to validate
- `rules` (ColumnRules): Dictionary mapping column names to validation rules
- `stage` (str): Pipeline stage name ("bronze", "silver", or "gold")
- `step` (str): Step name within the stage
- `filter_columns_by_rules` (bool): If True (default), output DataFrames will only contain
  columns that have validation rules defined. If False, all columns from the input
  DataFrame will be preserved.

**Returns:**
- `valid_df` (DataFrame): Records that passed validation
- `invalid_df` (DataFrame): Records that failed validation
- `stats` (StageStats): Validation statistics

**Column Filtering Behavior:**
By default, validation rules filter the output DataFrame to only include columns that have
validation rules defined. This can be controlled with the `filter_columns_by_rules` parameter:

```python
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

#### `validate_dataframe_schema(df, expected_schema)`

Validate DataFrame schema.

**Parameters:**
- `df` (DataFrame): DataFrame to validate
- `expected_schema` (StructType): Expected schema

**Returns:** `bool`

#### `assess_data_quality(df, rules, threshold=95.0)`

Assess data quality against rules.

**Parameters:**
- `df` (DataFrame): DataFrame to assess
- `rules` (dict): Validation rules
- `threshold` (float): Quality threshold

**Returns:** `ValidationResult`

## Performance

### Performance Monitoring

```python
from sparkforge.performance import (
    now_dt,
    format_duration,
    time_operation,
    performance_monitor,
    time_write_operation,
    monitor_performance
)
```

#### `time_operation(operation_name)`

Decorator for timing operations.

**Parameters:**
- `operation_name` (str): Operation name for logging

**Returns:** `decorator`

**Example:**
```python
@time_operation("data_transform")
def my_transform(spark, df):
    return df.filter(F.col("status") == "active")
```

#### `performance_monitor(operation_name, max_duration=None)`

Context manager for performance monitoring.

**Parameters:**
- `operation_name` (str): Operation name
- `max_duration` (float, optional): Maximum duration threshold

**Returns:** `context manager`

**Example:**
```python
with performance_monitor("data_processing", max_duration=300):
    result = pipeline.run_incremental(bronze_sources={"events": source_df})
```

#### `monitor_performance(operation_name, max_duration=None)`

Decorator factory for performance monitoring.

**Parameters:**
- `operation_name` (str): Operation name
- `max_duration` (float, optional): Maximum duration threshold

**Returns:** `decorator`

**Example:**
```python
@monitor_performance("my_operation", max_duration=60)
def my_function():
    return "result"
```

## Logging

### LogWriter

Structured logging for pipeline execution.

```python
from sparkforge import LogWriter

log_writer = LogWriter(
    spark=spark,
    table_name="my_schema.pipeline_logs",
    use_delta=True
)
```

#### Methods

##### `log_pipeline_execution(result)`

Log pipeline execution results.

**Parameters:**
- `result` (ExecutionResult): Execution result to log

##### `log_step_execution(step_result)`

Log individual step execution.

**Parameters:**
- `step_result` (StepExecutionResult): Step result to log

### PipelineLogger

Internal logging for pipeline operations.

```python
from sparkforge.logger import PipelineLogger

logger = PipelineLogger(
    spark=spark,
    table_name="my_schema.pipeline_logs"
)
```

## Exceptions

### ValidationError

Raised when data validation fails.

```python
from sparkforge.exceptions import ValidationError

raise ValidationError("Data validation failed")
```

### PipelineValidationError

Raised when pipeline validation fails.

```python
from sparkforge.exceptions import PipelineValidationError

raise PipelineValidationError("Pipeline configuration invalid")
```

### TableOperationError

Raised when table operations fail.

```python
from sparkforge.exceptions import TableOperationError

raise TableOperationError("Table write operation failed")
```

## Enums

### StepType

```python
from sparkforge.models import StepType

StepType.BRONZE
StepType.SILVER
StepType.GOLD
```

### StepStatus

```python
from sparkforge.models import StepStatus

StepStatus.PENDING
StepStatus.RUNNING
StepStatus.COMPLETED
StepStatus.FAILED
StepStatus.CANCELLED
```

### ExecutionMode

```python
from sparkforge.models import ExecutionMode

ExecutionMode.SEQUENTIAL
ExecutionMode.PARALLEL
ExecutionMode.ADAPTIVE
ExecutionMode.BATCH
```

### WriteMode

```python
from sparkforge.models import WriteMode

WriteMode.OVERWRITE
WriteMode.APPEND
WriteMode.MERGE
```

## Constants

### PIPELINE_LOG_SCHEMA

Schema for pipeline log tables.

```python
from sparkforge import PIPELINE_LOG_SCHEMA
```

## Utility Functions

### Reporting

```python
from sparkforge.reporting import create_validation_dict, create_write_dict

# Create validation report
validation_dict = create_validation_dict(validation_result)

# Create write operation report
write_dict = create_write_dict(write_result)
```

### Data Utils

```python
from sparkforge.data_utils import (
    create_sample_dataframe,
    validate_dataframe,
    get_dataframe_stats
)

# Create sample data
df = create_sample_dataframe(spark, schema, num_rows=1000)

# Validate DataFrame
is_valid = validate_dataframe(df, rules)

# Get DataFrame statistics
stats = get_dataframe_stats(df)
```

## Security

### SecurityManager

Comprehensive security management for data pipelines.

```python
from sparkforge import SecurityManager, SecurityConfig, get_security_manager

# Create security manager
security_config = SecurityConfig(
    enable_input_validation=True,
    enable_sql_injection_protection=True,
    enable_audit_logging=True,
    max_table_name_length=128,
    max_schema_name_length=64
)

security_manager = SecurityManager(security_config)
```

#### Methods

##### `validate_table_name(name)`
Validate table name for security.

**Parameters:**
- `name` (str): Table name to validate

**Returns:** `str` - Validated table name

**Raises:** `InputValidationError` if invalid

##### `validate_sql_expression(expression)`
Validate SQL expression for injection attacks.

**Parameters:**
- `expression` (str): SQL expression to validate

**Returns:** `str` - Validated expression

**Raises:** `SQLInjectionError` if malicious

##### `grant_permission(user, level, resource)`
Grant access permission to user.

**Parameters:**
- `user` (str): User identifier
- `level` (AccessLevel): Permission level
- `resource` (str): Resource identifier

##### `check_access_permission(user, level, resource)`
Check if user has permission.

**Parameters:**
- `user` (str): User identifier
- `level` (AccessLevel): Required permission level
- `resource` (str): Resource identifier

**Returns:** `bool` - True if permission granted

### SecurityConfig

Configuration for security features.

```python
from sparkforge import SecurityConfig

config = SecurityConfig(
    enable_input_validation=True,
    enable_sql_injection_protection=True,
    enable_audit_logging=True,
    max_table_name_length=128,
    max_schema_name_length=64,
    allowed_sql_functions={"col", "lit", "when", "otherwise"},
    audit_retention_days=90
)
```

### AccessLevel

Enumeration of access levels.

```python
from sparkforge.security import AccessLevel

# Available levels
AccessLevel.READ      # Read-only access
AccessLevel.WRITE     # Write access
AccessLevel.ADMIN     # Administrative access
AccessLevel.EXECUTE   # Execution access
```

## Performance Cache

### PerformanceCache

Intelligent caching system with TTL and LRU eviction.

```python
from sparkforge import PerformanceCache, CacheConfig, CacheStrategy, get_performance_cache

# Create cache
cache_config = CacheConfig(
    max_size_mb=512,
    ttl_seconds=3600,
    strategy=CacheStrategy.LRU,
    enable_compression=True
)

cache = PerformanceCache(cache_config)
```

#### Methods

##### `put(key, value, ttl_seconds=None)`
Store value in cache.

**Parameters:**
- `key` (Any): Cache key
- `value` (Any): Value to cache
- `ttl_seconds` (int, optional): Time-to-live in seconds

##### `get(key)`
Retrieve value from cache.

**Parameters:**
- `key` (Any): Cache key

**Returns:** `Any` - Cached value or None

##### `invalidate(key)`
Remove value from cache.

**Parameters:**
- `key` (Any): Cache key to remove

##### `clear()`
Clear all cache entries.

##### `get_stats()`
Get cache statistics.

**Returns:** `dict` - Cache statistics

### CacheConfig

Configuration for performance cache.

```python
from sparkforge import CacheConfig, CacheStrategy

config = CacheConfig(
    max_size_mb=512,           # Maximum cache size
    ttl_seconds=3600,          # Default TTL
    strategy=CacheStrategy.LRU, # Eviction strategy
    enable_compression=True,    # Enable compression
    max_entries=10000          # Maximum entries
)
```

### CacheStrategy

Enumeration of cache eviction strategies.

```python
from sparkforge.performance_cache import CacheStrategy

# Available strategies
CacheStrategy.LRU  # Least Recently Used
CacheStrategy.TTL  # Time To Live
CacheStrategy.FIFO # First In First Out
```

## Dynamic Parallel Execution

### DynamicParallelExecutor

Advanced parallel execution with dynamic worker allocation.

```python
from sparkforge import DynamicParallelExecutor, ExecutionTask, TaskPriority

# Create executor
executor = DynamicParallelExecutor()

# Create tasks
tasks = [
    ExecutionTask("task1", function1, priority=TaskPriority.HIGH),
    ExecutionTask("task2", function2, priority=TaskPriority.NORMAL)
]

# Execute parallel
result = executor.execute_parallel(tasks)
```

#### Methods

##### `execute_parallel(tasks, wait_for_completion=True, timeout=None)`
Execute tasks in parallel with dynamic optimization.

**Parameters:**
- `tasks` (List[ExecutionTask]): Tasks to execute
- `wait_for_completion` (bool): Wait for all tasks to complete
- `timeout` (float, optional): Timeout in seconds

**Returns:** `dict` - Execution results and metrics

##### `get_performance_metrics()`
Get current performance metrics.

**Returns:** `dict` - Performance metrics

##### `get_optimization_recommendations()`
Get optimization recommendations.

**Returns:** `List[str]` - Optimization recommendations

### ExecutionTask

Represents a task to be executed.

```python
from sparkforge import ExecutionTask, TaskPriority, create_execution_task

# Create task directly
task = ExecutionTask(
    task_id="my_task",
    function=my_function,
    args=(arg1, arg2),
    kwargs={"param": "value"},
    priority=TaskPriority.HIGH,
    dependencies={"prerequisite_task"},
    estimated_duration=30.0,
    memory_requirement_mb=256.0,
    timeout_seconds=300
)

# Or use helper function
task = create_execution_task(
    "my_task",
    my_function,
    arg1, arg2,
    priority=TaskPriority.HIGH,
    param="value"
)
```

### TaskPriority

Enumeration of task priority levels.

```python
from sparkforge import TaskPriority

# Available priorities
TaskPriority.CRITICAL   # Must complete first
TaskPriority.HIGH       # High priority
TaskPriority.NORMAL     # Normal priority
TaskPriority.LOW        # Low priority
TaskPriority.BACKGROUND # Background tasks
```

### DynamicWorkerPool

Dynamic worker pool with intelligent allocation.

```python
from sparkforge import DynamicWorkerPool

# Create worker pool
pool = DynamicWorkerPool(
    min_workers=1,
    max_workers=16,
    logger=logger
)

# Submit task
task_id = pool.submit_task(task)

# Wait for completion
success = pool.wait_for_completion(timeout=300.0)

# Get metrics
metrics = pool.get_performance_metrics()
```

---

**For more examples and usage patterns, see the [User Guide](USER_GUIDE.md) and [Quick Reference](QUICK_REFERENCE.md).**

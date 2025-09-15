# SparkForge API Reference

Complete API reference for SparkForge data pipeline framework.

## Table of Contents

1. [Core Classes](#core-classes)
2. [PipelineBuilder](#pipelinebuilder)
3. [PipelineRunner](#pipelinerunner)
4. [StepExecutor](#stepexecutor)
5. [Models](#models)
6. [Validation](#validation)
7. [Performance](#performance)
8. [Logging](#logging)
9. [Security](#security)
10. [Performance Cache](#performance-cache)
11. [Dynamic Parallel Execution](#dynamic-parallel-execution)
12. [Exceptions](#exceptions)

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

##### `with_bronze_rules(name, rules, incremental_col=None)`

Define Bronze layer data ingestion rules.

**Parameters:**
- `name` (str): Bronze step name
- `rules` (dict): Validation rules for columns
- `incremental_col` (str, optional): Column for incremental processing

**Returns:** `PipelineBuilder` (for chaining)

**Example:**
```python
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    },
    incremental_col="timestamp"
)
```

##### `add_silver_transform(name, source_bronze, transform, rules, table_name, watermark_col=None, source_silvers=None)`

Add Silver layer transformation step.

**Parameters:**
- `name` (str): Silver step name
- `source_bronze` (str): Source Bronze step name
- `transform` (callable): Transformation function
- `rules` (dict): Validation rules
- `table_name` (str): Output table name
- `watermark_col` (str, optional): Watermark column for streaming
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

##### `add_gold_transform(name, transform, rules, table_name, source_silvers)`

Add Gold layer aggregation step.

**Parameters:**
- `name` (str): Gold step name
- `transform` (callable): Aggregation function
- `rules` (dict): Validation rules
- `table_name` (str): Output table name
- `source_silvers` (list): Source Silver steps

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
    source_silvers=["silver_events"]
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

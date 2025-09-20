# SparkForge Enhanced API Reference

This document provides comprehensive API documentation for SparkForge with detailed examples, usage patterns, and best practices.

## Table of Contents

1. [Core Classes](#core-classes)
2. [Data Models](#data-models)
3. [Validation System](#validation-system)
4. [Pipeline Builder](#pipeline-builder)
5. [Pipeline Runner](#pipeline-runner)
6. [Execution Engine](#execution-engine)
7. [Performance Monitoring](#performance-monitoring)
8. [Error Handling](#error-handling)
9. [Utility Functions](#utility-functions)
10. [Examples and Use Cases](#examples-and-use-cases)

## Core Classes

### PipelineBuilder

The main class for building data pipelines with the Medallion Architecture.

```python
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig

# Basic usage
config = PipelineConfig(
    schema="analytics",
    quality_thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
    parallel=ParallelConfig(enabled=True, max_workers=4)
)

builder = PipelineBuilder(config)
```

#### Methods

##### `__init__(config: PipelineConfig)`

Initialize the pipeline builder with configuration.

**Parameters:**
- `config` (PipelineConfig): Pipeline configuration object

**Example:**
```python
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig

config = PipelineConfig(
    schema="analytics",
    quality_thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
    parallel=ParallelConfig(enabled=True, max_workers=4),
    performance_monitoring=True
)

builder = PipelineBuilder(config)
```

##### `add_bronze_step(name: str, transform_func: Callable, validation_rules: Dict[str, List]) -> None`

Add a bronze step to the pipeline.

**Parameters:**
- `name` (str): Name of the bronze step
- `transform_func` (Callable): Transform function for data processing
- `validation_rules` (Dict[str, List]): Validation rules for data quality

**Example:**
```python
from pyspark.sql import functions as F

def bronze_transform(df):
    return df.withColumn("processed_at", F.current_timestamp())

validation_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()],
    "amount": [F.col("amount") > 0]
}

builder.add_bronze_step("user_events", bronze_transform, validation_rules)
```

##### `add_silver_step(name: str, transform_func: Callable, validation_rules: Dict[str, List], source_bronze: str = None) -> None`

Add a silver step to the pipeline.

**Parameters:**
- `name` (str): Name of the silver step
- `transform_func` (Callable): Transform function for data processing
- `validation_rules` (Dict[str, List]): Validation rules for data quality
- `source_bronze` (str, optional): Source bronze step name

**Example:**
```python
def silver_transform(df):
    return df.withColumn("user_segment", 
                        F.when(F.col("total_spent") > 1000, "premium")
                         .when(F.col("total_spent") > 500, "standard")
                         .otherwise("basic"))

validation_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "total_spent": [F.col("total_spent") >= 0],
    "user_segment": [F.col("user_segment").isin(["premium", "standard", "basic"])]
}

builder.add_silver_step("user_profiles", silver_transform, validation_rules, source_bronze="user_events")
```

##### `add_gold_step(name: str, table_name: str, transform_func: Callable, validation_rules: Dict[str, List], source_silvers: List[str] = None) -> None`

Add a gold step to the pipeline.

**Parameters:**
- `name` (str): Name of the gold step
- `table_name` (str): Target table name
- `transform_func` (Callable): Transform function for data processing
- `validation_rules` (Dict[str, List]): Validation rules for data quality
- `source_silvers` (List[str], optional): Source silver step names

**Example:**
```python
def gold_transform(df):
    return df.groupBy("user_segment").agg(
        F.count("*").alias("user_count"),
        F.avg("total_spent").alias("avg_spent")
    )

validation_rules = {
    "user_segment": [F.col("user_segment").isNotNull()],
    "user_count": [F.col("user_count") > 0],
    "avg_spent": [F.col("avg_spent") >= 0]
}

builder.add_gold_step("user_analytics", "user_analytics_table", gold_transform, validation_rules, source_silvers=["user_profiles"])
```

##### `get_pipeline() -> PipelineConfig`

Get the current pipeline configuration.

**Returns:**
- `PipelineConfig`: Current pipeline configuration

**Example:**
```python
pipeline_config = builder.get_pipeline()
print(f"Schema: {pipeline_config.schema}")
print(f"Quality thresholds: {pipeline_config.quality_thresholds}")
```

##### `validate_pipeline() -> ValidationResult`

Validate the entire pipeline configuration.

**Returns:**
- `ValidationResult`: Validation result with errors, warnings, and recommendations

**Example:**
```python
validation_result = builder.validate_pipeline()
if not validation_result.is_valid:
    print("Validation errors:")
    for error in validation_result.errors:
        print(f"- {error}")
```

##### `get_performance_summary() -> PerformanceSummary`

Get performance summary for the pipeline.

**Returns:**
- `PerformanceSummary`: Performance metrics and statistics

**Example:**
```python
performance_summary = builder.get_performance_summary()
print(f"Total execution time: {performance_summary.total_time}s")
print(f"Memory peak: {performance_summary.peak_memory_mb}MB")
print(f"Throughput: {performance_summary.records_per_second} records/s")
```

## Data Models

### PipelineConfig

Configuration for the entire pipeline.

```python
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig

config = PipelineConfig(
    schema="analytics",
    quality_thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
    parallel=ParallelConfig(enabled=True, max_workers=4),
    performance_monitoring=True,
    debug_mode=False
)
```

#### Attributes

- `schema` (str): Database schema name
- `quality_thresholds` (ValidationThresholds): Quality thresholds for each tier
- `parallel` (ParallelConfig): Parallel processing configuration
- `performance_monitoring` (bool): Enable performance monitoring
- `debug_mode` (bool): Enable debug mode

### ValidationThresholds

Quality thresholds for data validation.

```python
from sparkforge.models import ValidationThresholds

thresholds = ValidationThresholds(
    bronze=80.0,  # 80% quality threshold for bronze tier
    silver=85.0,  # 85% quality threshold for silver tier
    gold=90.0     # 90% quality threshold for gold tier
)
```

#### Methods

##### `get_threshold(phase: PipelinePhase) -> float`

Get threshold for a specific pipeline phase.

**Parameters:**
- `phase` (PipelinePhase): Pipeline phase (BRONZE, SILVER, or GOLD)

**Returns:**
- `float`: Quality threshold for the phase

**Example:**
```python
from sparkforge.models import PipelinePhase

bronze_threshold = thresholds.get_threshold(PipelinePhase.BRONZE)
print(f"Bronze threshold: {bronze_threshold}%")
```

### ParallelConfig

Configuration for parallel processing.

```python
from sparkforge.models import ParallelConfig

parallel_config = ParallelConfig(
    enabled=True,      # Enable parallel processing
    max_workers=4      # Maximum number of parallel workers
)
```

#### Attributes

- `enabled` (bool): Enable parallel processing
- `max_workers` (int): Maximum number of parallel workers

### BronzeStep

Represents a bronze tier step in the pipeline.

```python
from sparkforge.models import BronzeStep
from pyspark.sql import functions as F

def bronze_transform(df):
    return df.withColumn("processed_at", F.current_timestamp())

validation_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()]
}

bronze_step = BronzeStep(
    name="user_events",
    transform=bronze_transform,
    rules=validation_rules
)
```

#### Attributes

- `name` (str): Step name
- `transform` (Callable): Transform function
- `rules` (Dict[str, List]): Validation rules
- `incremental_col` (str, optional): Column for incremental processing

#### Methods

##### `validate() -> None`

Validate the bronze step configuration.

**Raises:**
- `ValidationError`: If validation fails

**Example:**
```python
try:
    bronze_step.validate()
    print("Bronze step validation passed")
except ValidationError as e:
    print(f"Validation failed: {e}")
```

### SilverStep

Represents a silver tier step in the pipeline.

```python
from sparkforge.models import SilverStep

def silver_transform(df):
    return df.withColumn("user_segment", 
                        F.when(F.col("total_spent") > 1000, "premium")
                         .otherwise("standard"))

silver_step = SilverStep(
    name="user_profiles",
    transform=silver_transform,
    rules=validation_rules,
    source_bronze="user_events"
)
```

#### Attributes

- `name` (str): Step name
- `transform` (Callable): Transform function
- `rules` (Dict[str, List]): Validation rules
- `source_bronze` (str): Source bronze step name
- `depends_on_silvers` (List[str], optional): Dependent silver steps
- `can_run_parallel` (bool): Can run in parallel with other steps
- `execution_group` (str, optional): Execution group for ordering

### GoldStep

Represents a gold tier step in the pipeline.

```python
from sparkforge.models import GoldStep

def gold_transform(df):
    return df.groupBy("user_segment").agg(
        F.count("*").alias("user_count"),
        F.avg("total_spent").alias("avg_spent")
    )

gold_step = GoldStep(
    name="user_analytics",
    table_name="user_analytics_table",
    transform=gold_transform,
    rules=validation_rules,
    source_silvers=["user_profiles"]
)
```

#### Attributes

- `name` (str): Step name
- `table_name` (str): Target table name
- `transform` (Callable): Transform function
- `rules` (Dict[str, List]): Validation rules
- `source_silvers` (List[str], optional): Source silver step names

## Validation System

### UnifiedValidator

Central validation system for SparkForge.

```python
from sparkforge.validation import UnifiedValidator

validator = UnifiedValidator()
```

#### Methods

##### `validate_pipeline(pipeline_config: PipelineConfig) -> ValidationResult`

Validate the entire pipeline configuration.

**Parameters:**
- `pipeline_config` (PipelineConfig): Pipeline configuration to validate

**Returns:**
- `ValidationResult`: Validation result with errors, warnings, and recommendations

**Example:**
```python
validation_result = validator.validate_pipeline(pipeline_config)
if validation_result.is_valid:
    print("Pipeline validation passed")
else:
    print("Validation errors:")
    for error in validation_result.errors:
        print(f"- {error}")
```

##### `add_validator(name: str, validator_func: Callable) -> None`

Add a custom validator function.

**Parameters:**
- `name` (str): Validator name
- `validator_func` (Callable): Validator function

**Example:**
```python
def custom_validator(data):
    # Custom validation logic
    return True

validator.add_validator("custom_check", custom_validator)
```

### Validation Functions

#### `assess_data_quality(df: DataFrame, rules: Dict[str, List], threshold: float = 80.0) -> Dict`

Assess data quality against validation rules.

**Parameters:**
- `df` (DataFrame): DataFrame to validate
- `rules` (Dict[str, List]): Validation rules
- `threshold` (float): Quality threshold

**Returns:**
- `Dict`: Quality assessment results

**Example:**
```python
from sparkforge.validation import assess_data_quality

quality_result = assess_data_quality(df, validation_rules, threshold=85.0)
print(f"Quality score: {quality_result['quality_score']}%")
print(f"Valid records: {quality_result['valid_records']}")
print(f"Total records: {quality_result['total_records']}")
```

#### `validate_dataframe_schema(df: DataFrame, expected_columns: List[str]) -> bool`

Validate DataFrame schema against expected columns.

**Parameters:**
- `df` (DataFrame): DataFrame to validate
- `expected_columns` (List[str]): Expected column names

**Returns:**
- `bool`: True if schema is valid

**Example:**
```python
from sparkforge.validation import validate_dataframe_schema

expected_columns = ["user_id", "timestamp", "amount", "category"]
is_valid = validate_dataframe_schema(df, expected_columns)
if not is_valid:
    print("Schema validation failed")
```

#### `apply_validation_rules(df: DataFrame, rules: Dict[str, List], stage: str, step: str) -> Tuple[DataFrame, DataFrame, Dict]`

Apply validation rules to a DataFrame.

**Parameters:**
- `df` (DataFrame): DataFrame to validate
- `rules` (Dict[str, List]): Validation rules
- `stage` (str): Pipeline stage
- `step` (str): Step name

**Returns:**
- `Tuple[DataFrame, DataFrame, Dict]`: Valid DataFrame, invalid DataFrame, and statistics

**Example:**
```python
from sparkforge.validation import apply_validation_rules

valid_df, invalid_df, stats = apply_validation_rules(
    df, validation_rules, "bronze", "user_events"
)

print(f"Valid records: {stats['valid_rows']}")
print(f"Invalid records: {stats['invalid_rows']}")
print(f"Validation rate: {stats['validation_rate']}%")
```

## Pipeline Runner

### SimplePipelineRunner

Simplified pipeline runner for executing data pipelines.

```python
from sparkforge.pipeline.runner import SimplePipelineRunner

runner = SimplePipelineRunner()
```

#### Methods

##### `run_pipeline(builder: PipelineBuilder) -> ExecutionResult`

Run the complete pipeline.

**Parameters:**
- `builder` (PipelineBuilder): Pipeline builder instance

**Returns:**
- `ExecutionResult`: Execution result with performance metrics

**Example:**
```python
execution_result = runner.run_pipeline(builder)
print(f"Pipeline executed successfully in {execution_result.execution_time}s")
print(f"Total records processed: {execution_result.total_records}")
```

##### `run_step(step: Union[BronzeStep, SilverStep, GoldStep], input_df: DataFrame = None) -> ExecutionResult`

Run a single pipeline step.

**Parameters:**
- `step` (Union[BronzeStep, SilverStep, GoldStep]): Step to execute
- `input_df` (DataFrame, optional): Input DataFrame

**Returns:**
- `ExecutionResult`: Execution result

**Example:**
```python
execution_result = runner.run_step(bronze_step, input_df)
print(f"Step executed in {execution_result.execution_time}s")
```

## Execution Engine

### SimpleExecutionEngine

Simplified execution engine for processing pipeline steps.

```python
from sparkforge.execution import SimpleExecutionEngine

engine = SimpleExecutionEngine()
```

#### Methods

##### `execute_step(step: Union[BronzeStep, SilverStep, GoldStep], input_df: DataFrame = None) -> DataFrame`

Execute a single pipeline step.

**Parameters:**
- `step` (Union[BronzeStep, SilverStep, GoldStep]): Step to execute
- `input_df` (DataFrame, optional): Input DataFrame

**Returns:**
- `DataFrame`: Processed DataFrame

**Example:**
```python
processed_df = engine.execute_step(bronze_step, input_df)
processed_df.show()
```

## Performance Monitoring

### PerformanceMonitor

Monitor and track pipeline performance.

```python
from sparkforge.performance import PerformanceMonitor

monitor = PerformanceMonitor()
```

#### Methods

##### `benchmark_function(func: Callable, name: str, iterations: int = 1000, *args, **kwargs) -> PerformanceResult`

Benchmark a function's performance.

**Parameters:**
- `func` (Callable): Function to benchmark
- `name` (str): Function name
- `iterations` (int): Number of iterations
- `*args, **kwargs`: Function arguments

**Returns:**
- `PerformanceResult`: Performance metrics

**Example:**
```python
def sample_function(data):
    return data * 2

result = monitor.benchmark_function(sample_function, "sample_function", 1000, [1, 2, 3, 4, 5])
print(f"Execution time: {result.execution_time}s")
print(f"Memory usage: {result.memory_usage_mb}MB")
```

##### `measure_memory_usage(func: Callable, *args, **kwargs) -> float`

Measure memory usage of a function.

**Parameters:**
- `func` (Callable): Function to measure
- `*args, **kwargs`: Function arguments

**Returns:**
- `float`: Memory usage in MB

**Example:**
```python
memory_usage = monitor.measure_memory_usage(sample_function, [1, 2, 3, 4, 5])
print(f"Memory usage: {memory_usage}MB")
```

## Error Handling

### Custom Exceptions

#### `ValidationError`

Raised when validation fails.

```python
from sparkforge.errors import ValidationError

try:
    bronze_step.validate()
except ValidationError as e:
    print(f"Validation error: {e}")
```

#### `PipelineError`

Raised when pipeline execution fails.

```python
from sparkforge.errors import PipelineError

try:
    runner.run_pipeline(builder)
except PipelineError as e:
    print(f"Pipeline error: {e}")
```

#### `ConfigurationError`

Raised when configuration is invalid.

```python
from sparkforge.errors import ConfigurationError

try:
    config = PipelineConfig(schema="", quality_thresholds=thresholds)
except ConfigurationError as e:
    print(f"Configuration error: {e}")
```

## Utility Functions

### `safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float`

Safely divide two numbers, handling division by zero.

**Parameters:**
- `numerator` (float): Numerator
- `denominator` (float): Denominator
- `default` (float): Default value if denominator is zero

**Returns:**
- `float`: Division result or default value

**Example:**
```python
from sparkforge.validation import safe_divide

result = safe_divide(10, 2)  # Returns 5.0
result = safe_divide(10, 0)  # Returns 0.0
result = safe_divide(10, 0, default=1.0)  # Returns 1.0
```

### `get_dataframe_info(df: DataFrame) -> Dict`

Get comprehensive information about a DataFrame.

**Parameters:**
- `df` (DataFrame): DataFrame to analyze

**Returns:**
- `Dict`: DataFrame information

**Example:**
```python
from sparkforge.validation import get_dataframe_info

info = get_dataframe_info(df)
print(f"Row count: {info['row_count']}")
print(f"Column count: {info['column_count']}")
print(f"Schema: {info['schema']}")
```

## Examples and Use Cases

### Complete Pipeline Example

```python
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig
from sparkforge.pipeline.runner import SimplePipelineRunner
from pyspark.sql import functions as F

# Configuration
config = PipelineConfig(
    schema="analytics",
    quality_thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
    parallel=ParallelConfig(enabled=True, max_workers=4),
    performance_monitoring=True
)

# Create builder
builder = PipelineBuilder(config)

# Bronze step
def bronze_transform(df):
    return df.withColumn("processed_at", F.current_timestamp())

bronze_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()],
    "amount": [F.col("amount") > 0]
}

builder.add_bronze_step("user_events", bronze_transform, bronze_rules)

# Silver step
def silver_transform(df):
    return df.withColumn("user_segment", 
                        F.when(F.col("amount") > 1000, "premium")
                         .when(F.col("amount") > 500, "standard")
                         .otherwise("basic"))

silver_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "amount": [F.col("amount") >= 0],
    "user_segment": [F.col("user_segment").isin(["premium", "standard", "basic"])]
}

builder.add_silver_step("user_profiles", silver_transform, silver_rules, source_bronze="user_events")

# Gold step
def gold_transform(df):
    return df.groupBy("user_segment").agg(
        F.count("*").alias("user_count"),
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount")
    )

gold_rules = {
    "user_segment": [F.col("user_segment").isNotNull()],
    "user_count": [F.col("user_count") > 0],
    "avg_amount": [F.col("avg_amount") >= 0],
    "total_amount": [F.col("total_amount") >= 0]
}

builder.add_gold_step("user_analytics", "user_analytics_table", gold_transform, gold_rules, source_silvers=["user_profiles"])

# Validate pipeline
validation_result = builder.validate_pipeline()
if not validation_result.is_valid:
    print("Pipeline validation failed:")
    for error in validation_result.errors:
        print(f"- {error}")
    exit(1)

# Run pipeline
runner = SimplePipelineRunner()
execution_result = runner.run_pipeline(builder)

print(f"Pipeline executed successfully in {execution_result.execution_time}s")
print(f"Total records processed: {execution_result.total_records}")

# Get performance summary
performance_summary = builder.get_performance_summary()
print(f"Memory peak: {performance_summary.peak_memory_mb}MB")
print(f"Throughput: {performance_summary.records_per_second} records/s")
```

### Error Handling Example

```python
from sparkforge.errors import ValidationError, PipelineError, ConfigurationError

try:
    # Validate pipeline
    validation_result = builder.validate_pipeline()
    if not validation_result.is_valid:
        raise ValidationError(f"Pipeline validation failed: {validation_result.errors}")
    
    # Run pipeline
    execution_result = runner.run_pipeline(builder)
    
except ValidationError as e:
    print(f"Validation error: {e}")
    # Handle validation errors
except PipelineError as e:
    print(f"Pipeline execution error: {e}")
    # Handle pipeline errors
except ConfigurationError as e:
    print(f"Configuration error: {e}")
    # Handle configuration errors
except Exception as e:
    print(f"Unexpected error: {e}")
    # Handle unexpected errors
```

### Performance Monitoring Example

```python
from sparkforge.performance import PerformanceMonitor

monitor = PerformanceMonitor()

# Benchmark transform function
def benchmark_transform(df):
    return df.withColumn("processed_at", F.current_timestamp())

result = monitor.benchmark_function(
    benchmark_transform, 
    "bronze_transform", 
    iterations=100,
    df=test_df
)

print(f"Execution time: {result.execution_time}s")
print(f"Memory usage: {result.memory_usage_mb}MB")
print(f"Throughput: {result.throughput} ops/sec")

# Measure memory usage
memory_usage = monitor.measure_memory_usage(benchmark_transform, test_df)
print(f"Memory usage: {memory_usage}MB")
```

This enhanced API reference provides comprehensive documentation for all SparkForge components with detailed examples and usage patterns. For additional information, refer to the specific class documentation or contact the development team.

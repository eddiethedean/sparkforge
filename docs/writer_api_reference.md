# SparkForge Writer Module API Reference

## Overview

The SparkForge Writer module provides comprehensive logging and analytics capabilities for pipeline execution results. It integrates seamlessly with the SparkForge ecosystem to provide data quality validation, table operations, and advanced reporting features.

## Table of Contents

- [Core Classes](#core-classes)
- [Configuration](#configuration)
- [Data Models](#data-models)
- [Exceptions](#exceptions)
- [Usage Examples](#usage-examples)
- [Advanced Features](#advanced-features)
- [Performance Tuning](#performance-tuning)

## Core Classes

### LogWriter

The main class for writing pipeline execution logs to Delta tables.

```python
from sparkforge.writer import LogWriter
from sparkforge.writer.models import WriterConfig

# Initialize with configuration
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND
)
writer = LogWriter(spark_session, config)
```

#### Constructor

```python
LogWriter(
    spark: SparkSession,
    config: WriterConfig,
    logger: PipelineLogger | None = None
)
```

**Parameters:**
- `spark`: SparkSession instance for DataFrame operations
- `config`: WriterConfig instance with configuration options
- `logger`: Optional PipelineLogger instance (defaults to new instance)

#### Methods

##### `write_execution_result(execution_result, run_id=None)`

Write an ExecutionResult to the log table.

```python
result = writer.write_execution_result(execution_result, run_id="run-123")
```

**Parameters:**
- `execution_result`: ExecutionResult instance to log
- `run_id`: Optional run identifier (defaults to UUID)

**Returns:**
- `dict`: Result containing success status, metrics, and metadata

##### `write_step_results(step_results, execution_context, run_id=None)`

Write individual step results to the log table.

```python
result = writer.write_step_results(
    step_results=[step1, step2],
    execution_context=context,
    run_id="run-123"
)
```

**Parameters:**
- `step_results`: List of StepResult instances
- `execution_context`: ExecutionContext for the pipeline run
- `run_id`: Optional run identifier

**Returns:**
- `dict`: Result containing success status and metrics

##### `write_log_rows(log_rows, run_id=None)`

Write log rows directly to the table.

```python
log_rows = [
    {
        "run_id": "test-run",
        "phase": "bronze",
        "step_name": "extract_data",
        "duration_secs": 10.0,
        "rows_processed": 1000,
        "validation_rate": 95.0
    }
]
result = writer.write_log_rows(log_rows, run_id="test-run")
```

**Parameters:**
- `log_rows`: List of log row dictionaries
- `run_id`: Optional run identifier

**Returns:**
- `dict`: Result containing success status, metrics, and quality results

##### `write_execution_result_batch(execution_results, run_id=None, batch_size=None)`

Write multiple execution results in batches for better performance.

```python
result = writer.write_execution_result_batch(
    execution_results=[exec1, exec2, exec3],
    run_id="batch-run",
    batch_size=1000
)
```

**Parameters:**
- `execution_results`: List of ExecutionResult instances
- `run_id`: Optional run identifier
- `batch_size`: Optional batch size (defaults to config.batch_size)

**Returns:**
- `dict`: Result containing batch processing statistics

##### `show_logs(limit=20)`

Display recent log entries.

```python
writer.show_logs(limit=50)
```

**Parameters:**
- `limit`: Maximum number of rows to display

##### `get_table_info()`

Get information about the log table.

```python
info = writer.get_table_info()
print(f"Table has {info['row_count']} rows")
```

**Returns:**
- `dict`: Table information including row count, columns, and schema

##### `get_metrics()`

Get writer performance metrics.

```python
metrics = writer.get_metrics()
print(f"Total writes: {metrics['total_writes']}")
print(f"Success rate: {metrics['success_rate_percent']}%")
```

**Returns:**
- `dict`: Performance metrics dictionary

##### `reset_metrics()`

Reset all performance metrics to zero.

```python
writer.reset_metrics()
```

##### `get_memory_usage()`

Get current memory usage statistics.

```python
memory = writer.get_memory_usage()
print(f"RSS: {memory['rss_mb']} MB")
print(f"Percentage: {memory['percent']}%")
```

**Returns:**
- `dict`: Memory usage statistics

### Advanced Methods

##### `validate_log_data_quality(log_rows, validation_rules=None)`

Validate data quality of log rows using SparkForge validation system.

```python
quality_result = writer.validate_log_data_quality(log_rows)
if quality_result['quality_passed']:
    print("Data quality validation passed")
else:
    print(f"Quality rate: {quality_result['validation_rate']}%")
```

**Parameters:**
- `log_rows`: List of log rows to validate
- `validation_rules`: Optional custom validation rules

**Returns:**
- `dict`: Quality validation results

##### `detect_anomalies(log_rows)`

Detect anomalies in log data patterns.

```python
anomaly_result = writer.detect_anomalies(log_rows)
if anomaly_result['anomalies_detected']:
    print(f"Found {anomaly_result['anomaly_count']} anomalies")
```

**Parameters:**
- `log_rows`: List of log rows to analyze

**Returns:**
- `dict`: Anomaly detection results

##### `optimize_table(**options)`

Optimize the log table for better performance.

```python
result = writer.optimize_table(
    enable_partitioning=True,
    enable_compression=True,
    enable_zordering=True,
    enable_vacuum=True
)
```

**Parameters:**
- `**options`: Optimization options

**Returns:**
- `dict`: Optimization results

##### `maintain_table(maintenance_options=None)`

Perform table maintenance operations.

```python
result = writer.maintain_table({
    'vacuum': True,
    'analyze': True,
    'validate_schema': True
})
```

**Parameters:**
- `maintenance_options`: Dictionary of maintenance operations

**Returns:**
- `dict`: Maintenance results

##### `get_table_history(limit=10)`

Get table version history and metadata.

```python
history = writer.get_table_history(limit=20)
```

**Parameters:**
- `limit`: Maximum number of history entries

**Returns:**
- `dict`: Table history information

##### `generate_summary_report(days=7)`

Generate summary statistics for the log table.

```python
report = writer.generate_summary_report(days=30)
print(f"Success rate: {report['success_rate_percent']}%")
```

**Parameters:**
- `days`: Number of days to include in report

**Returns:**
- `dict`: Summary report data

##### `analyze_performance_trends(days=30)`

Analyze performance trends over time.

```python
trends = writer.analyze_performance_trends(days=90)
print(f"Average duration: {trends['duration_trend']['mean']} seconds")
```

**Parameters:**
- `days`: Number of days to analyze

**Returns:**
- `dict`: Performance trend analysis

##### `export_analytics_data(format="json", limit=1000, filters=None)`

Export analytics data in various formats.

```python
# Export as JSON
json_data = writer.export_analytics_data(format="json", limit=500)

# Export as CSV
csv_data = writer.export_analytics_data(format="csv", limit=1000)

# Export as Parquet
parquet_data = writer.export_analytics_data(format="parquet", limit=2000)
```

**Parameters:**
- `format`: Export format ("json", "csv", "parquet")
- `limit`: Maximum number of records to export
- `filters`: Optional filters to apply

**Returns:**
- `dict`: Export results

## Configuration

### WriterConfig

Configuration class for LogWriter behavior.

```python
from sparkforge.writer.models import WriterConfig, WriteMode

config = WriterConfig(
    # Basic settings
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND,
    
    # Performance settings
    batch_size=1000,
    max_file_size_mb=128,
    parallel_write_threads=4,
    memory_fraction=0.8,
    
    # Data quality settings
    log_data_quality_results=True,
    enable_anomaly_detection=True,
    min_validation_rate=95.0,
    max_invalid_rows_percent=5.0,
    
    # Schema settings
    enable_schema_evolution=True,
    auto_optimize_schema=True,
    schema_validation_mode="strict",
    
    # Optimization settings
    enable_optimization=True,
    partition_count=10,
    compression="snappy",
)
```

#### Configuration Options

##### Basic Settings
- `table_schema`: Target schema name
- `table_name`: Target table name
- `write_mode`: Write mode (APPEND, OVERWRITE)

##### Performance Settings
- `batch_size`: Batch size for large operations (default: 1000)
- `max_file_size_mb`: Maximum file size in MB (default: 128)
- `parallel_write_threads`: Number of parallel write threads (default: 4)
- `memory_fraction`: Memory fraction to use (default: 0.6)

##### Data Quality Settings
- `log_data_quality_results`: Enable data quality logging (default: False)
- `enable_anomaly_detection`: Enable anomaly detection (default: False)
- `min_validation_rate`: Minimum validation rate threshold (default: 95.0)
- `max_invalid_rows_percent`: Maximum invalid rows percentage (default: 5.0)

##### Schema Settings
- `enable_schema_evolution`: Enable schema evolution (default: True)
- `auto_optimize_schema`: Auto-optimize schema (default: True)
- `schema_validation_mode`: Schema validation mode (default: "strict")

##### Optimization Settings
- `enable_optimization`: Enable table optimization (default: True)
- `partition_count`: Number of partitions (default: None)
- `compression`: Compression algorithm (default: "snappy")

##### Custom Table Naming
- `table_name_pattern`: Custom table naming pattern
- `table_suffix_pattern`: Table suffix pattern

##### Error Handling
- `max_retries`: Maximum retry attempts (default: 3)
- `retry_delay_secs`: Retry delay in seconds (default: 1.0)
- `fail_fast`: Fail fast on errors (default: False)
- `retry_exponential_backoff`: Use exponential backoff (default: True)

## Data Models

### LogRow

TypedDict representing a single log row entry.

```python
from sparkforge.writer.models import LogRow

log_row: LogRow = {
    "run_id": "run-123",
    "run_mode": "initial",
    "run_started_at": datetime.now(),
    "run_ended_at": datetime.now(),
    "execution_id": "exec-456",
    "pipeline_id": "pipeline-789",
    "schema": "analytics",
    "phase": "bronze",
    "step_name": "extract_data",
    "step_type": "extraction",
    "start_time": datetime.now(),
    "end_time": datetime.now(),
    "duration_secs": 10.0,
    "table_fqn": "analytics.source_table",
    "rows_processed": 1000,
    "rows_written": 950,
    "validation_rate": 95.0,
    "success": True,
    "error_message": None,
    "metadata": {}
}
```

#### LogRow Fields

- `run_id`: Unique run identifier
- `run_mode`: Run mode (initial, incremental, full_refresh, validation_only)
- `run_started_at`: Run start timestamp
- `run_ended_at`: Run end timestamp
- `execution_id`: Execution identifier
- `pipeline_id`: Pipeline identifier
- `schema`: Target schema
- `phase`: Pipeline phase (bronze, silver, gold)
- `step_name`: Step name
- `step_type`: Step type
- `start_time`: Step start timestamp
- `end_time`: Step end timestamp
- `duration_secs`: Step duration in seconds
- `table_fqn`: Fully qualified table name
- `rows_processed`: Number of rows processed
- `rows_written`: Number of rows written
- `validation_rate`: Validation success rate percentage
- `success`: Success status
- `error_message`: Error message if failed
- `metadata`: Additional metadata dictionary

### WriteMode

Enum for write modes.

```python
from sparkforge.writer.models import WriteMode

# Available write modes
WriteMode.APPEND      # Append to existing table
WriteMode.OVERWRITE   # Overwrite existing table
```

## Exceptions

### WriterError

Base exception for all writer-related errors.

```python
from sparkforge.writer.exceptions import WriterError

try:
    writer.write_log_rows(invalid_data)
except WriterError as e:
    print(f"Writer error: {e}")
    print(f"Context: {e.context}")
    print(f"Suggestions: {e.suggestions}")
```

### WriterConfigurationError

Raised when configuration is invalid.

```python
from sparkforge.writer.exceptions import WriterConfigurationError

try:
    invalid_config = WriterConfig(table_schema="", table_name="test")
    writer = LogWriter(spark, invalid_config)
except WriterConfigurationError as e:
    print(f"Configuration error: {e}")
```

### WriterValidationError

Raised when data validation fails.

```python
from sparkforge.writer.exceptions import WriterValidationError

try:
    writer.write_log_rows(invalid_log_rows)
except WriterValidationError as e:
    print(f"Validation error: {e}")
```

### WriterTableError

Raised when table operations fail.

```python
from sparkforge.writer.exceptions import WriterTableError

try:
    writer.optimize_table()
except WriterTableError as e:
    print(f"Table error: {e}")
```

### WriterDataQualityError

Raised when data quality checks fail.

```python
from sparkforge.writer.exceptions import WriterDataQualityError

try:
    writer.validate_log_data_quality(poor_quality_data)
except WriterDataQualityError as e:
    print(f"Data quality error: {e}")
    print(f"Quality issues: {e.quality_issues}")
```

### WriterPerformanceError

Raised when performance thresholds are exceeded.

```python
from sparkforge.writer.exceptions import WriterPerformanceError

try:
    writer.write_log_rows(extremely_large_dataset)
except WriterPerformanceError as e:
    print(f"Performance error: {e}")
```

### WriterSchemaError

Raised when schema operations fail.

```python
from sparkforge.writer.exceptions import WriterSchemaError

try:
    writer.write_log_rows(schema_incompatible_data)
except WriterSchemaError as e:
    print(f"Schema error: {e}")
    print(f"Expected schema: {e.expected_schema}")
    print(f"Actual schema: {e.actual_schema}")
```

## Usage Examples

### Basic Usage

```python
from pyspark.sql import SparkSession
from sparkforge.writer import LogWriter
from sparkforge.writer.models import WriterConfig, WriteMode

# Initialize Spark session
spark = SparkSession.builder.appName("WriterExample").getOrCreate()

# Create writer configuration
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND
)

# Initialize writer
writer = LogWriter(spark, config)

# Write log rows
log_rows = [
    {
        "run_id": "example-run",
        "phase": "bronze",
        "step_name": "extract_data",
        "duration_secs": 10.0,
        "rows_processed": 1000,
        "validation_rate": 95.0
    }
]

result = writer.write_log_rows(log_rows)
print(f"Write successful: {result['success']}")
print(f"Rows written: {result['rows_written']}")
```

### Advanced Configuration

```python
# Advanced configuration with all features enabled
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND,
    
    # Performance optimization
    batch_size=2000,
    max_file_size_mb=256,
    parallel_write_threads=8,
    memory_fraction=0.8,
    enable_optimization=True,
    
    # Data quality features
    log_data_quality_results=True,
    enable_anomaly_detection=True,
    min_validation_rate=98.0,
    max_invalid_rows_percent=2.0,
    
    # Schema management
    enable_schema_evolution=True,
    auto_optimize_schema=True,
    schema_validation_mode="strict",
    
    # Custom table naming
    table_name_pattern="{schema}.{pipeline_id}_{run_mode}_{date}",
    
    # Error handling
    max_retries=5,
    retry_delay_secs=2.0,
    retry_exponential_backoff=True,
)

writer = LogWriter(spark, config)
```

### Batch Processing

```python
# Process multiple execution results in batches
execution_results = [exec1, exec2, exec3, exec4, exec5]

result = writer.write_execution_result_batch(
    execution_results=execution_results,
    run_id="batch-processing-run",
    batch_size=2
)

print(f"Total executions: {result['total_executions']}")
print(f"Successful writes: {result['successful_writes']}")
print(f"Failed writes: {result['failed_writes']}")
print(f"Rows written: {result['rows_written']}")
```

### Data Quality Validation

```python
# Validate data quality
quality_result = writer.validate_log_data_quality(log_rows)

if quality_result['quality_passed']:
    print("✅ Data quality validation passed")
    print(f"Validation rate: {quality_result['validation_rate']}%")
else:
    print("❌ Data quality validation failed")
    print(f"Validation rate: {quality_result['validation_rate']}%")
    print(f"Valid rows: {quality_result['valid_rows']}")
    print(f"Invalid rows: {quality_result['invalid_rows']}")
```

### Anomaly Detection

```python
# Detect anomalies in log data
anomaly_result = writer.detect_anomalies(log_rows)

if anomaly_result['anomalies_detected']:
    print(f"🚨 Found {anomaly_result['anomaly_count']} anomalies")
    for anomaly in anomaly_result['anomalies']:
        print(f"  - {anomaly['type']}: {anomaly['description']}")
else:
    print("✅ No anomalies detected")
```

### Table Operations

```python
# Optimize table for better performance
optimization_result = writer.optimize_table(
    enable_partitioning=True,
    enable_compression=True,
    enable_zordering=True,
    enable_vacuum=True
)

if optimization_result['optimized']:
    print("✅ Table optimization completed")
    print(f"Optimizations applied: {optimization_result['optimizations_applied']}")
else:
    print("❌ Table optimization failed")

# Perform table maintenance
maintenance_result = writer.maintain_table({
    'vacuum': True,
    'analyze': True,
    'validate_schema': True
})

if maintenance_result['maintained']:
    print("✅ Table maintenance completed")
    print(f"Operations performed: {maintenance_result['maintenance_operations']}")
```

### Reporting and Analytics

```python
# Generate summary report
summary = writer.generate_summary_report(days=30)
print(f"Success rate: {summary['success_rate_percent']}%")
print(f"Total executions: {summary['total_executions']}")
print(f"Average duration: {summary['average_duration_secs']} seconds")

# Analyze performance trends
trends = writer.analyze_performance_trends(days=90)
print(f"Duration trend - Mean: {trends['duration_trend']['mean']}")
print(f"Duration trend - StdDev: {trends['duration_trend']['stddev']}")
print(f"Success trend - Rate: {trends['success_trend']['success_rate_percent']}%")

# Export analytics data
export_result = writer.export_analytics_data(
    format="json",
    limit=1000,
    filters={"phase": "bronze"}
)

if export_result['export_successful']:
    print("✅ Data export completed")
    print(f"Records exported: {export_result['records_exported']}")
    print(f"Export size: {export_result['export_size_mb']} MB")
```

## Advanced Features

### Custom Validation Rules

```python
# Define custom validation rules
custom_rules = {
    "duration_secs": [Column("duration_secs >= 0")],
    "validation_rate": [Column("validation_rate >= 90")],
    "rows_processed": [Column("rows_processed > 0")]
}

quality_result = writer.validate_log_data_quality(
    log_rows, 
    validation_rules=custom_rules
)
```

### Dynamic Table Naming

```python
# Configure dynamic table naming
config = WriterConfig(
    table_schema="analytics",
    table_name="base_logs",
    table_name_pattern="{schema}.{pipeline_id}_{run_mode}_{date}",
    table_suffix_pattern="_{run_mode}_{timestamp}"
)

# Generate table names dynamically
table_name = config.generate_table_name(
    pipeline_id="my-pipeline",
    run_mode="incremental",
    timestamp="20240101_120000"
)
# Result: "analytics.my-pipeline_incremental_20240101_120000"
```

### Performance Monitoring

```python
# Monitor memory usage
memory_info = writer.get_memory_usage()
print(f"RSS Memory: {memory_info['rss_mb']} MB")
print(f"Virtual Memory: {memory_info['vms_mb']} MB")
print(f"Memory Percentage: {memory_info['percent']}%")
print(f"Available Memory: {memory_info['available_mb']} MB")

# Get performance metrics
metrics = writer.get_metrics()
print(f"Total Writes: {metrics['total_writes']}")
print(f"Success Rate: {metrics['success_rate_percent']}%")
print(f"Average Duration: {metrics['average_duration_secs']} seconds")
print(f"Peak Memory Usage: {metrics['memory_usage_peak_mb']} MB")
```

## Performance Tuning

### Batch Size Optimization

```python
# For high-volume data
config = WriterConfig(
    batch_size=5000,  # Larger batches for high volume
    max_file_size_mb=512,
    parallel_write_threads=8
)

# For low-latency requirements
config = WriterConfig(
    batch_size=100,   # Smaller batches for low latency
    max_file_size_mb=64,
    parallel_write_threads=2
)
```

### Memory Configuration

```python
# For memory-constrained environments
config = WriterConfig(
    memory_fraction=0.4,  # Use less memory
    batch_size=500,
    parallel_write_threads=2
)

# For memory-rich environments
config = WriterConfig(
    memory_fraction=0.8,  # Use more memory
    batch_size=2000,
    parallel_write_threads=8
)
```

### Table Optimization

```python
# Enable all optimization features
config = WriterConfig(
    enable_optimization=True,
    auto_optimize_schema=True,
    enable_schema_evolution=True,
    compression="snappy",
    partition_count=20
)

# Regular table maintenance
writer.optimize_table(
    enable_partitioning=True,
    enable_compression=True,
    enable_zordering=True,
    enable_vacuum=True
)
```

### Error Handling Configuration

```python
# Robust error handling for production
config = WriterConfig(
    max_retries=5,
    retry_delay_secs=2.0,
    retry_exponential_backoff=True,
    fail_fast=False  # Don't fail fast, retry instead
)

# Fast failure for development
config = WriterConfig(
    max_retries=1,
    retry_delay_secs=0.5,
    retry_exponential_backoff=False,
    fail_fast=True  # Fail fast for quick feedback
)
```

## Best Practices

### 1. Configuration Management

- Use environment-specific configurations
- Enable data quality features in production
- Set appropriate batch sizes based on data volume
- Configure proper error handling and retry policies

### 2. Performance Optimization

- Monitor memory usage and adjust `memory_fraction`
- Use appropriate batch sizes for your workload
- Enable table optimization for large datasets
- Use parallel write threads for better throughput

### 3. Data Quality

- Enable data quality validation in production
- Set appropriate quality thresholds
- Use anomaly detection for monitoring
- Regular quality reports and trend analysis

### 4. Error Handling

- Implement proper exception handling
- Use retry mechanisms for transient failures
- Log errors with appropriate context
- Monitor error rates and patterns

### 5. Monitoring and Alerting

- Monitor writer performance metrics
- Set up alerts for quality threshold breaches
- Track anomaly detection results
- Regular performance trend analysis

### 6. Table Management

- Regular table optimization and maintenance
- Monitor table growth and performance
- Use appropriate partitioning strategies
- Clean up old data as needed

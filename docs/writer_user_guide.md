# SparkForge Writer Module User Guide

## Introduction

The SparkForge Writer module is a powerful tool for logging and analyzing pipeline execution results. It provides comprehensive data quality validation, table operations, and advanced analytics capabilities integrated with the SparkForge ecosystem.

## Table of Contents

- [Getting Started](#getting-started)
- [Basic Usage](#basic-usage)
- [Configuration Guide](#configuration-guide)
- [Data Quality Features](#data-quality-features)
- [Table Operations](#table-operations)
- [Reporting and Analytics](#reporting-and-analytics)
- [Performance Optimization](#performance-optimization)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Getting Started

### Installation

The Writer module is included with SparkForge. No additional installation is required.

```python
from sparkforge.writer import LogWriter
from sparkforge.writer.models import WriterConfig, WriteMode
```

### Quick Start

Here's a simple example to get you started:

```python
from pyspark.sql import SparkSession
from sparkforge.writer import LogWriter
from sparkforge.writer.models import WriterConfig, WriteMode

# Initialize Spark session
spark = SparkSession.builder.appName("WriterExample").getOrCreate()

# Create basic configuration
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND
)

# Initialize writer
writer = LogWriter(spark, config)

# Write some log data
log_rows = [
    {
        "run_id": "my-first-run",
        "phase": "bronze",
        "step_name": "extract_data",
        "duration_secs": 15.0,
        "rows_processed": 1000,
        "validation_rate": 95.0
    }
]

result = writer.write_log_rows(log_rows)
print(f"Success: {result['success']}, Rows written: {result['rows_written']}")
```

## Basic Usage

### Writing Execution Results

The most common use case is writing complete pipeline execution results:

```python
from sparkforge.models import ExecutionResult, ExecutionContext, StepResult, ExecutionMode
from datetime import datetime

# Create execution context
context = ExecutionContext(
    mode=ExecutionMode.INITIAL,
    start_time=datetime.now(),
    execution_id="exec-123",
    pipeline_id="my-pipeline",
    schema="analytics"
)

# Create step results
step_results = [
    StepResult(
        step_name="bronze_extract",
        phase="bronze",
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration_secs=10.0,
        rows_processed=1000,
        rows_written=950,
        validation_rate=95.0,
        success=True,
        execution_context=context
    )
]

# Create execution result
execution_result = ExecutionResult(
    success=True,
    context=context,
    step_results=step_results,
    total_duration_secs=10.0
)

# Write to log table
result = writer.write_execution_result(execution_result, run_id="run-123")
```

### Writing Individual Step Results

For more granular logging, you can write individual step results:

```python
result = writer.write_step_results(
    step_results=step_results,
    execution_context=context,
    run_id="step-run-123"
)
```

### Writing Raw Log Data

For custom logging scenarios, you can write raw log data:

```python
log_rows = [
    {
        "run_id": "custom-run",
        "phase": "silver",
        "step_name": "transform_data",
        "duration_secs": 25.0,
        "rows_processed": 950,
        "rows_written": 900,
        "validation_rate": 94.7,
        "success": True,
        "error_message": None,
        "metadata": {"custom_field": "custom_value"}
    }
]

result = writer.write_log_rows(log_rows, run_id="custom-run")
```

### Viewing Log Data

To view your logged data:

```python
# Show recent logs (default 20 rows)
writer.show_logs()

# Show specific number of rows
writer.show_logs(limit=50)

# Get table information
info = writer.get_table_info()
print(f"Table has {info['row_count']} rows")
print(f"Columns: {info['columns']}")
```

## Configuration Guide

### Basic Configuration

```python
config = WriterConfig(
    table_schema="analytics",           # Target schema
    table_name="pipeline_logs",         # Target table name
    write_mode=WriteMode.APPEND         # Write mode
)
```

### Performance Configuration

```python
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND,
    
    # Performance settings
    batch_size=2000,                    # Batch size for large operations
    max_file_size_mb=256,               # Maximum file size
    parallel_write_threads=4,           # Parallel write threads
    memory_fraction=0.8,                # Memory fraction to use
    enable_optimization=True,           # Enable table optimization
)
```

### Data Quality Configuration

```python
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND,
    
    # Data quality features
    log_data_quality_results=True,      # Enable quality logging
    enable_anomaly_detection=True,      # Enable anomaly detection
    min_validation_rate=95.0,           # Minimum validation rate
    max_invalid_rows_percent=5.0,       # Max invalid rows percentage
)
```

### Schema Management Configuration

```python
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND,
    
    # Schema settings
    enable_schema_evolution=True,       # Allow schema changes
    auto_optimize_schema=True,          # Auto-optimize schema
    schema_validation_mode="strict",    # Validation mode
)
```

### Custom Table Naming

```python
config = WriterConfig(
    table_schema="analytics",
    table_name="base_logs",
    
    # Custom naming patterns
    table_name_pattern="{schema}.{pipeline_id}_{run_mode}_{date}",
    table_suffix_pattern="_{run_mode}_{timestamp}"
)

# Generate dynamic table names
table_name = config.generate_table_name(
    pipeline_id="my-pipeline",
    run_mode="incremental",
    timestamp="20240101_120000"
)
# Result: "analytics.my-pipeline_incremental_20240101_120000"
```

### Error Handling Configuration

```python
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND,
    
    # Error handling
    max_retries=3,                      # Maximum retry attempts
    retry_delay_secs=1.0,               # Retry delay
    fail_fast=False,                    # Don't fail fast
    retry_exponential_backoff=True,     # Use exponential backoff
)
```

## Data Quality Features

### Data Quality Validation

Enable data quality validation to ensure your log data meets quality standards:

```python
# Enable in configuration
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    log_data_quality_results=True,
    min_validation_rate=95.0
)

writer = LogWriter(spark, config)

# Validate data quality
quality_result = writer.validate_log_data_quality(log_rows)

if quality_result['quality_passed']:
    print("✅ Data quality validation passed")
    print(f"Validation rate: {quality_result['validation_rate']}%")
else:
    print("❌ Data quality validation failed")
    print(f"Validation rate: {quality_result['validation_rate']}%")
    print(f"Threshold: {quality_result['threshold_met']}")
```

### Custom Validation Rules

Define custom validation rules for your specific requirements:

```python
from pyspark.sql import Column

# Define custom validation rules
custom_rules = {
    "duration_secs": [Column("duration_secs >= 0 AND duration_secs <= 3600")],
    "validation_rate": [Column("validation_rate >= 90")],
    "rows_processed": [Column("rows_processed > 0")],
    "phase": [Column("phase IN ('bronze', 'silver', 'gold')")]
}

# Apply custom rules
quality_result = writer.validate_log_data_quality(
    log_rows, 
    validation_rules=custom_rules
)
```

### Anomaly Detection

Detect anomalies in your log data patterns:

```python
# Enable anomaly detection
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    enable_anomaly_detection=True
)

writer = LogWriter(spark, config)

# Detect anomalies
anomaly_result = writer.detect_anomalies(log_rows)

if anomaly_result['anomalies_detected']:
    print(f"🚨 Found {anomaly_result['anomaly_count']} anomalies")
    
    for anomaly in anomaly_result['anomalies']:
        print(f"  - Type: {anomaly['type']}")
        print(f"    Description: {anomaly['description']}")
        print(f"    Severity: {anomaly['severity']}")
        print(f"    Value: {anomaly['value']}")
else:
    print("✅ No anomalies detected")
```

### Quality Monitoring

Monitor data quality over time:

```python
# Get quality metrics
metrics = writer.get_metrics()
print(f"Total writes: {metrics['total_writes']}")
print(f"Success rate: {metrics['success_rate_percent']}%")
print(f"Average validation rate: {metrics['average_validation_rate']}%")

# Check if quality thresholds are met
if metrics['average_validation_rate'] < config.min_validation_rate:
    print("⚠️ Average validation rate below threshold")
```

## Table Operations

### Table Optimization

Optimize your log table for better performance:

```python
# Optimize table
optimization_result = writer.optimize_table(
    enable_partitioning=True,       # Enable partitioning
    enable_compression=True,        # Enable compression
    enable_zordering=True,          # Enable Z-ordering
    enable_vacuum=True              # Enable vacuum
)

if optimization_result['optimized']:
    print("✅ Table optimization completed")
    print(f"Optimizations applied: {optimization_result['optimizations_applied']}")
    print(f"Optimization time: {optimization_result['optimization_duration_secs']} seconds")
else:
    print("❌ Table optimization failed")
    print(f"Reason: {optimization_result['reason']}")
```

### Table Maintenance

Perform regular table maintenance:

```python
# Table maintenance
maintenance_result = writer.maintain_table({
    'vacuum': True,                 # Vacuum old files
    'analyze': True,                # Update statistics
    'validate_schema': True         # Validate schema
})

if maintenance_result['maintained']:
    print("✅ Table maintenance completed")
    print(f"Operations performed: {maintenance_result['maintenance_operations']}")
    print(f"Maintenance time: {maintenance_result['maintenance_duration_secs']} seconds")
```

### Table History

Get table version history and metadata:

```python
# Get table history
history = writer.get_table_history(limit=10)

if history['history_available']:
    print(f"Table: {history['table_fqn']}")
    print(f"Total versions: {history['total_versions']}")
    
    for version in history['versions']:
        print(f"  Version {version['version']}: {version['timestamp']}")
        print(f"    Rows: {version['row_count']}")
        print(f"    Size: {version['size_mb']} MB")
else:
    print("❌ Table history not available")
```

## Reporting and Analytics

### Summary Reports

Generate summary statistics for your log data:

```python
# Generate summary report
summary = writer.generate_summary_report(days=30)

if summary['report_available']:
    print("📊 Summary Report (Last 30 days)")
    print(f"Total executions: {summary['total_executions']}")
    print(f"Success rate: {summary['success_rate_percent']}%")
    print(f"Average duration: {summary['average_duration_secs']} seconds")
    print(f"Total rows processed: {summary['total_rows_processed']}")
    print(f"Total rows written: {summary['total_rows_written']}")
    
    # Phase breakdown
    print("\nPhase Distribution:")
    for phase, count in summary['phase_distribution'].items():
        print(f"  {phase}: {count} executions")
else:
    print("❌ Summary report not available")
```

### Performance Trend Analysis

Analyze performance trends over time:

```python
# Analyze performance trends
trends = writer.analyze_performance_trends(days=90)

if trends['trends_available']:
    print("📈 Performance Trends (Last 90 days)")
    
    # Duration trends
    duration_trend = trends['duration_trend']
    print(f"Duration - Mean: {duration_trend['mean']:.2f}s")
    print(f"Duration - StdDev: {duration_trend['stddev']:.2f}s")
    print(f"Duration - Min: {duration_trend['min']:.2f}s")
    print(f"Duration - Max: {duration_trend['max']:.2f}s")
    
    # Success trends
    success_trend = trends['success_trend']
    print(f"Success Rate: {success_trend['success_rate_percent']:.2f}%")
    print(f"Total Executions: {success_trend['total_executions']}")
    
    # Validation trends
    if 'validation_trend' in trends:
        validation_trend = trends['validation_trend']
        print(f"Validation Rate - Mean: {validation_trend['mean']:.2f}%")
        print(f"Validation Rate - StdDev: {validation_trend['stddev']:.2f}%")
else:
    print("❌ Performance trends not available")
```

### Data Export

Export analytics data in various formats:

```python
# Export as JSON
json_result = writer.export_analytics_data(
    format="json",
    limit=1000,
    filters={"phase": "bronze"}
)

if json_result['export_successful']:
    print("✅ JSON export completed")
    print(f"Records exported: {json_result['records_exported']}")
    print(f"Export size: {json_result['export_size_mb']} MB")
    print(f"Export path: {json_result['export_path']}")

# Export as CSV
csv_result = writer.export_analytics_data(
    format="csv",
    limit=5000,
    filters={"success": True}
)

# Export as Parquet
parquet_result = writer.export_analytics_data(
    format="parquet",
    limit=10000
)
```

## Performance Optimization

### Batch Processing

Use batch processing for better performance with large datasets:

```python
# Process multiple execution results in batches
execution_results = [exec1, exec2, exec3, exec4, exec5]

result = writer.write_execution_result_batch(
    execution_results=execution_results,
    run_id="batch-run",
    batch_size=1000  # Process 1000 records per batch
)

print(f"Total executions: {result['total_executions']}")
print(f"Successful writes: {result['successful_writes']}")
print(f"Failed writes: {result['failed_writes']}")
print(f"Rows written: {result['rows_written']}")
print(f"Batch processing time: {result['batch_duration_secs']} seconds")
```

### Memory Optimization

Monitor and optimize memory usage:

```python
# Check memory usage
memory_info = writer.get_memory_usage()
print(f"RSS Memory: {memory_info['rss_mb']} MB")
print(f"Virtual Memory: {memory_info['vms_mb']} MB")
print(f"Memory Percentage: {memory_info['percent']}%")
print(f"Available Memory: {memory_info['available_mb']} MB")

# Adjust memory configuration if needed
if memory_info['percent'] > 80:
    print("⚠️ High memory usage detected")
    # Consider reducing batch_size or memory_fraction
```

### Performance Monitoring

Monitor writer performance metrics:

```python
# Get performance metrics
metrics = writer.get_metrics()
print(f"Total writes: {metrics['total_writes']}")
print(f"Success rate: {metrics['success_rate_percent']}%")
print(f"Average duration: {metrics['average_duration_secs']} seconds")
print(f"Peak memory usage: {metrics['memory_usage_peak_mb']} MB")
print(f"Total rows written: {metrics['total_rows_written']}")

# Calculate throughput
if metrics['total_duration_secs'] > 0:
    throughput = metrics['total_rows_written'] / metrics['total_duration_secs']
    print(f"Throughput: {throughput:.2f} rows/second")
```

### Configuration Tuning

Tune configuration for your specific workload:

```python
# For high-volume, batch-oriented workloads
high_volume_config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND,
    batch_size=5000,                    # Large batches
    max_file_size_mb=512,               # Large files
    parallel_write_threads=8,           # More threads
    memory_fraction=0.8,                # More memory
    enable_optimization=True            # Enable optimization
)

# For low-latency, real-time workloads
low_latency_config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    write_mode=WriteMode.APPEND,
    batch_size=100,                     # Small batches
    max_file_size_mb=64,                # Small files
    parallel_write_threads=2,           # Fewer threads
    memory_fraction=0.4,                # Less memory
    enable_optimization=False           # Disable optimization
)
```

## Troubleshooting

### Common Issues

#### 1. Configuration Errors

**Problem**: `WriterConfigurationError` when initializing writer

```python
# ❌ Invalid configuration
config = WriterConfig(
    table_schema="",  # Empty schema
    table_name="test"
)

# ✅ Valid configuration
config = WriterConfig(
    table_schema="analytics",
    table_name="test"
)
```

**Solution**: Ensure all required configuration fields are properly set.

#### 2. Validation Errors

**Problem**: `WriterValidationError` when writing data

```python
# ❌ Invalid log data
invalid_log_rows = [
    {
        "run_id": "",  # Empty run_id
        "phase": "bronze",
        "duration_secs": -1.0  # Negative duration
    }
]

# ✅ Valid log data
valid_log_rows = [
    {
        "run_id": "valid-run",
        "phase": "bronze",
        "duration_secs": 10.0,
        "rows_processed": 1000,
        "validation_rate": 95.0
    }
]
```

**Solution**: Ensure log data meets validation requirements.

#### 3. Table Errors

**Problem**: `WriterTableError` during table operations

```python
# Check if table exists
info = writer.get_table_info()
if info['row_count'] == 0:
    print("Table is empty or doesn't exist")

# Try table maintenance
try:
    result = writer.maintain_table()
except WriterTableError as e:
    print(f"Table operation failed: {e}")
```

**Solution**: Check table existence and permissions.

#### 4. Performance Issues

**Problem**: Slow write operations

```python
# Check current configuration
config = writer.config
print(f"Batch size: {config.batch_size}")
print(f"Parallel threads: {config.parallel_write_threads}")
print(f"Memory fraction: {config.memory_fraction}")

# Monitor performance
metrics = writer.get_metrics()
print(f"Average duration: {metrics['average_duration_secs']} seconds")

# Adjust configuration for better performance
config.batch_size = 2000  # Increase batch size
config.parallel_write_threads = 4  # Increase threads
```

**Solution**: Tune configuration parameters based on your workload.

### Debugging

#### Enable Debug Logging

```python
from sparkforge.logging import PipelineLogger

# Create logger with debug level
logger = PipelineLogger("WriterDebug", level="DEBUG")
writer = LogWriter(spark, config, logger)

# Debug information will be logged
result = writer.write_log_rows(log_rows)
```

#### Check Memory Usage

```python
# Monitor memory usage during operations
memory_before = writer.get_memory_usage()
print(f"Memory before: {memory_before['rss_mb']} MB")

result = writer.write_log_rows(large_dataset)

memory_after = writer.get_memory_usage()
print(f"Memory after: {memory_after['rss_mb']} MB")
print(f"Memory increase: {memory_after['rss_mb'] - memory_before['rss_mb']} MB")
```

#### Validate Data Quality

```python
# Check data quality before writing
quality_result = writer.validate_log_data_quality(log_rows)

if not quality_result['quality_passed']:
    print("Data quality issues detected:")
    print(f"Validation rate: {quality_result['validation_rate']}%")
    print(f"Valid rows: {quality_result['valid_rows']}")
    print(f"Invalid rows: {quality_result['invalid_rows']}")
    
    # Check specific validation rules
    for rule in quality_result['validation_rules_applied']:
        print(f"Rule applied: {rule}")
```

### Error Recovery

#### Retry Mechanisms

```python
# Configure retry settings
config = WriterConfig(
    table_schema="analytics",
    table_name="pipeline_logs",
    max_retries=5,
    retry_delay_secs=2.0,
    retry_exponential_backoff=True,
    fail_fast=False
)

# The writer will automatically retry failed operations
try:
    result = writer.write_log_rows(log_rows)
except WriterError as e:
    print(f"Operation failed after retries: {e}")
    print(f"Context: {e.context}")
    print(f"Suggestions: {e.suggestions}")
```

#### Partial Failure Handling

```python
# Handle partial failures in batch operations
result = writer.write_execution_result_batch(
    execution_results=large_batch,
    run_id="batch-run"
)

if result['failed_writes'] > 0:
    print(f"Partial failure: {result['failed_writes']} writes failed")
    print(f"Success rate: {result['successful_writes'] / result['total_executions'] * 100}%")
    
    # You can retry failed operations
    if result['failed_executions']:
        print("Retrying failed executions...")
        retry_result = writer.write_execution_result_batch(
            execution_results=result['failed_executions'],
            run_id="retry-run"
        )
```

## Best Practices

### 1. Configuration Management

- **Environment-specific configs**: Use different configurations for development, staging, and production
- **Enable data quality**: Always enable data quality features in production
- **Set appropriate thresholds**: Configure quality thresholds based on your requirements
- **Use retry mechanisms**: Configure proper retry policies for production environments

### 2. Performance Optimization

- **Batch size tuning**: Adjust batch size based on data volume and latency requirements
- **Memory management**: Monitor memory usage and adjust `memory_fraction` accordingly
- **Parallel processing**: Use appropriate number of parallel threads
- **Table optimization**: Regularly optimize tables for better performance

### 3. Data Quality

- **Validation rules**: Define appropriate validation rules for your data
- **Quality monitoring**: Monitor data quality metrics over time
- **Anomaly detection**: Use anomaly detection to identify issues early
- **Quality reporting**: Regular quality reports and trend analysis

### 4. Error Handling

- **Exception handling**: Implement proper exception handling in your code
- **Retry logic**: Use retry mechanisms for transient failures
- **Error logging**: Log errors with appropriate context and suggestions
- **Monitoring**: Monitor error rates and patterns

### 5. Monitoring and Alerting

- **Performance metrics**: Monitor writer performance metrics
- **Quality alerts**: Set up alerts for quality threshold breaches
- **Anomaly alerts**: Monitor anomaly detection results
- **Trend monitoring**: Regular performance trend analysis

### 6. Table Management

- **Regular maintenance**: Perform regular table optimization and maintenance
- **Growth monitoring**: Monitor table growth and performance
- **Partitioning**: Use appropriate partitioning strategies
- **Data lifecycle**: Implement data retention and cleanup policies

### 7. Security and Compliance

- **Access control**: Implement proper access controls for log tables
- **Data encryption**: Use encryption for sensitive log data
- **Audit trails**: Maintain audit trails for compliance
- **Data retention**: Implement appropriate data retention policies

### 8. Documentation and Maintenance

- **Documentation**: Keep configuration and usage documentation up to date
- **Version control**: Use version control for configuration changes
- **Testing**: Test configuration changes in non-production environments
- **Monitoring**: Monitor system health and performance continuously

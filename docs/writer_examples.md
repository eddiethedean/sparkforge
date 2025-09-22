# SparkForge Writer Module Examples

## Quick Start Examples

### Basic Logging
```python
from pyspark.sql import SparkSession
from sparkforge.writer import LogWriter
from sparkforge.writer.models import WriterConfig, WriteMode

# Initialize
spark = SparkSession.builder.appName("BasicExample").getOrCreate()
config = WriterConfig(table_schema="analytics", table_name="logs")
writer = LogWriter(spark, config)

# Write logs
log_rows = [{"run_id": "test", "phase": "bronze", "duration_secs": 10.0}]
result = writer.write_log_rows(log_rows)
print(f"Success: {result['success']}")
```

### Execution Result Logging
```python
from sparkforge.models import ExecutionResult, ExecutionContext, StepResult, ExecutionMode

# Create execution result
context = ExecutionContext(mode=ExecutionMode.INITIAL, start_time=datetime.now())
step = StepResult(step_name="extract", phase="bronze", duration_secs=15.0, 
                  execution_context=context)
execution = ExecutionResult(success=True, context=context, step_results=[step])

# Write to logs
result = writer.write_execution_result(execution)
```

## Advanced Examples

### Batch Processing
```python
# Process multiple executions
executions = [exec1, exec2, exec3]
result = writer.write_execution_result_batch(executions, batch_size=1000)
print(f"Processed {result['total_executions']} executions")
```

### Data Quality Validation
```python
# Enable quality validation
config = WriterConfig(table_schema="analytics", table_name="logs",
                     log_data_quality_results=True, min_validation_rate=95.0)
writer = LogWriter(spark, config)

# Validate quality
quality_result = writer.validate_log_data_quality(log_rows)
if quality_result['quality_passed']:
    print("✅ Quality validation passed")
```

### Anomaly Detection
```python
# Enable anomaly detection
config = WriterConfig(enable_anomaly_detection=True)
writer = LogWriter(spark, config)

# Detect anomalies
anomaly_result = writer.detect_anomalies(log_rows)
if anomaly_result['anomalies_detected']:
    print(f"🚨 Found {anomaly_result['anomaly_count']} anomalies")
```

### Table Operations
```python
# Optimize table
optimization = writer.optimize_table(enable_partitioning=True, enable_compression=True)
print(f"Optimized: {optimization['optimized']}")

# Table maintenance
maintenance = writer.maintain_table({'vacuum': True, 'analyze': True})
print(f"Maintained: {maintenance['maintained']}")
```

### Reporting and Analytics
```python
# Generate summary report
summary = writer.generate_summary_report(days=30)
print(f"Success rate: {summary['success_rate_percent']}%")

# Performance trends
trends = writer.analyze_performance_trends(days=90)
print(f"Average duration: {trends['duration_trend']['mean']}s")

# Export data
export = writer.export_analytics_data(format="json", limit=1000)
print(f"Exported {export['records_exported']} records")
```

## Configuration Examples

### High Performance Configuration
```python
config = WriterConfig(
    table_schema="analytics", table_name="logs",
    batch_size=5000, parallel_write_threads=8, memory_fraction=0.8,
    enable_optimization=True
)
```

### Data Quality Focused
```python
config = WriterConfig(
    table_schema="analytics", table_name="logs",
    log_data_quality_results=True, enable_anomaly_detection=True,
    min_validation_rate=98.0, max_invalid_rows_percent=2.0
)
```

### Production Ready
```python
config = WriterConfig(
    table_schema="analytics", table_name="logs",
    write_mode=WriteMode.APPEND, max_retries=5, retry_delay_secs=2.0,
    enable_schema_evolution=True, auto_optimize_schema=True
)
```

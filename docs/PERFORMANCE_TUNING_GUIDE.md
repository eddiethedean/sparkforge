# SparkForge Performance Tuning Guide

This guide provides comprehensive strategies for optimizing SparkForge pipeline performance in production environments.

## Table of Contents

1. [Performance Monitoring](#performance-monitoring)
2. [Pipeline Optimization](#pipeline-optimization)
3. [Data Processing Optimization](#data-processing-optimization)
4. [Memory Management](#memory-management)
5. [Parallel Processing](#parallel-processing)
6. [Caching Strategies](#caching-strategies)
7. [Resource Configuration](#resource-configuration)
8. [Best Practices](#best-practices)
9. [Performance Testing](#performance-testing)

## Performance Monitoring

### Built-in Performance Monitoring

SparkForge includes comprehensive performance monitoring capabilities:

```python
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.models import ValidationThresholds, ParallelConfig

# Enable performance monitoring
config = PipelineConfig(
    schema="analytics",
    quality_thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
    parallel=ParallelConfig(enabled=True, max_workers=4),  # Enable parallel processing
    performance_monitoring=True  # Enable performance tracking
)

builder = PipelineBuilder(config)
```

### Performance Metrics

The system tracks the following metrics:

- **Execution Time**: Total pipeline and step execution time
- **Memory Usage**: Peak and average memory consumption
- **Throughput**: Records processed per second
- **Resource Utilization**: CPU and memory usage patterns
- **Data Quality Metrics**: Validation success rates and failure patterns

### Accessing Performance Data

```python
# Get performance summary
performance_summary = builder.get_performance_summary()
print(f"Total execution time: {performance_summary.total_time}s")
print(f"Memory peak: {performance_summary.peak_memory_mb}MB")
print(f"Throughput: {performance_summary.records_per_second} records/s")

# Get detailed step performance
step_performance = builder.get_step_performance("bronze_events")
print(f"Step execution time: {step_performance.execution_time}s")
print(f"Step memory usage: {step_performance.memory_usage_mb}MB")
```

## Pipeline Optimization

### 1. Step Ordering and Dependencies

Optimize pipeline execution by carefully ordering steps:

```python
# ✅ Optimal: Minimize dependencies
builder.add_bronze_step("events", transform_func, validation_rules)
builder.add_bronze_step("users", transform_func, validation_rules)

# Silver steps can run in parallel
builder.add_silver_step("user_events", transform_func, validation_rules, source_bronze="events")
builder.add_silver_step("user_profiles", transform_func, validation_rules, source_bronze="users")

# ✅ Avoid: Unnecessary dependencies
# builder.add_silver_step("combined", transform_func, validation_rules, source_bronze="events,users")
```

### 2. Validation Rule Optimization

Optimize validation rules for performance:

```python
# ✅ Efficient: Combine related validations
validation_rules = {
    "user_id": [
        F.col("user_id").isNotNull(),
        F.col("user_id").rlike(r"^[0-9]+$")  # Combine null check and format validation
    ],
    "timestamp": [
        F.col("timestamp").isNotNull(),
        F.col("timestamp") > F.lit("2020-01-01")  # Combine null check and date range
    ]
}

# ❌ Inefficient: Separate rules that could be combined
validation_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "user_id_format": [F.col("user_id").rlike(r"^[0-9]+$")],
    "timestamp": [F.col("timestamp").isNotNull()],
    "timestamp_range": [F.col("timestamp") > F.lit("2020-01-01")]
}
```

### 3. Transform Function Optimization

Optimize transform functions for better performance:

```python
# ✅ Efficient: Use Spark SQL functions
def efficient_transform(df):
    return df.withColumn("processed_at", F.current_timestamp()) \
            .withColumn("user_segment", 
                       F.when(F.col("total_spent") > 1000, "premium")
                        .when(F.col("total_spent") > 500, "standard")
                        .otherwise("basic"))

# ❌ Inefficient: UDFs for simple operations
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def get_segment(spent):
    if spent > 1000:
        return "premium"
    elif spent > 500:
        return "standard"
    else:
        return "basic"

segment_udf = udf(get_segment, StringType())

def inefficient_transform(df):
    return df.withColumn("user_segment", segment_udf(F.col("total_spent")))
```

## Data Processing Optimization

### 1. Partitioning Strategy

Implement effective partitioning for large datasets:

```python
def optimized_bronze_transform(df):
    # Partition by date for time-series data
    return df.repartition(F.col("date"), 20)  # 20 partitions per date

def optimized_silver_transform(df):
    # Partition by user_id for user-centric data
    return df.repartition(F.col("user_id"), 50)  # 50 partitions per user_id
```

### 2. Data Type Optimization

Use appropriate data types to reduce memory usage:

```python
# ✅ Optimized: Use appropriate data types
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

def optimize_data_types(df):
    return df.withColumn("user_id", F.col("user_id").cast(IntegerType())) \
            .withColumn("amount", F.col("amount").cast(DoubleType())) \
            .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))

# ❌ Inefficient: String types for numeric data
def inefficient_data_types(df):
    return df.withColumn("user_id", F.col("user_id").cast("string")) \
            .withColumn("amount", F.col("amount").cast("string"))
```

### 3. Data Filtering

Filter data early in the pipeline:

```python
def early_filtering_transform(df):
    # Filter out invalid records early
    return df.filter(F.col("user_id").isNotNull()) \
            .filter(F.col("timestamp") > F.lit("2020-01-01")) \
            .filter(F.col("amount") > 0)
```

## Memory Management

### 1. Memory Configuration

Configure Spark memory settings appropriately:

```python
# In your Spark configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### 2. Memory-Efficient Operations

Use memory-efficient operations:

```python
# ✅ Memory efficient: Use select to reduce columns
def memory_efficient_transform(df):
    return df.select("user_id", "timestamp", "amount", "category") \
            .filter(F.col("amount") > 100)

# ❌ Memory inefficient: Keep all columns
def memory_inefficient_transform(df):
    return df.filter(F.col("amount") > 100)  # Keeps all columns in memory
```

### 3. Garbage Collection Tuning

Optimize garbage collection for long-running pipelines:

```python
# JVM options for better GC performance
# -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UnlockExperimentalVMOptions -XX:+UseZGC
```

## Parallel Processing

### 1. Parallel Configuration

Configure parallel processing for optimal performance:

```python
from sparkforge.models import ParallelConfig

# Optimal parallel configuration
parallel_config = ParallelConfig(
    enabled=True,
    max_workers=min(8, os.cpu_count())  # Don't exceed CPU cores
)

config = PipelineConfig(
    schema="analytics",
    parallel=parallel_config
)
```

### 2. Step Parallelization

Design steps for parallel execution:

```python
# ✅ Parallelizable: Independent silver steps
builder.add_silver_step("user_events", transform_func, validation_rules, source_bronze="events")
builder.add_silver_step("user_profiles", transform_func, validation_rules, source_bronze="users")
builder.add_silver_step("product_analytics", transform_func, validation_rules, source_bronze="products")

# These can run in parallel as they don't depend on each other
```

### 3. Data Parallelization

Implement data-level parallelization:

```python
def parallel_data_processing(df):
    # Process data in parallel chunks
    return df.repartition(20) \
            .withColumn("processed_chunk", F.spark_partition_id()) \
            .groupBy("processed_chunk") \
            .agg(F.count("*").alias("records_per_chunk"))
```

## Caching Strategies

### 1. Strategic Caching

Cache frequently accessed data:

```python
def cache_frequently_used_data(df):
    # Cache data that will be accessed multiple times
    df.cache()
    df.count()  # Trigger caching
    return df

# In your pipeline
builder.add_silver_step(
    "user_events", 
    cache_frequently_used_data, 
    validation_rules, 
    source_bronze="events"
)
```

### 2. Cache Management

Manage cache size and eviction:

```python
# Configure cache storage
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## Resource Configuration

### 1. Cluster Sizing

Size your cluster appropriately:

```python
# Recommended cluster sizing
# For small datasets (< 1GB): 2-4 cores, 8-16GB RAM
# For medium datasets (1-10GB): 4-8 cores, 16-32GB RAM  
# For large datasets (> 10GB): 8+ cores, 32+ GB RAM
```

### 2. Spark Configuration

Optimize Spark configuration:

```python
# Performance-optimized Spark configuration
spark_config = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB"
}
```

## Best Practices

### 1. Development Practices

- **Profile Early**: Use performance monitoring from the start
- **Test with Production Data**: Use realistic data volumes for testing
- **Monitor Continuously**: Set up alerts for performance degradation
- **Optimize Incrementally**: Make small, measurable improvements

### 2. Code Practices

- **Use Spark SQL**: Prefer Spark SQL functions over UDFs
- **Minimize Data Movement**: Reduce shuffles and data transfers
- **Optimize Joins**: Use broadcast joins for small tables
- **Filter Early**: Apply filters as early as possible in the pipeline

### 3. Operational Practices

- **Resource Monitoring**: Monitor CPU, memory, and disk usage
- **Error Handling**: Implement robust error handling and retry logic
- **Backup Strategies**: Maintain data backup and recovery procedures
- **Documentation**: Document performance characteristics and tuning decisions

## Performance Testing

### 1. Built-in Performance Tests

Use SparkForge's built-in performance testing:

```bash
# Run performance tests
python -m pytest tests/performance/ -v

# Generate performance report
python scripts/performance_report.py --summary
```

### 2. Custom Performance Tests

Create custom performance tests:

```python
import time
from sparkforge.pipeline.builder import PipelineBuilder

def performance_test():
    start_time = time.time()
    
    # Build and execute pipeline
    builder = PipelineBuilder(config)
    # ... add steps ...
    
    execution_time = time.time() - start_time
    print(f"Pipeline execution time: {execution_time:.2f}s")
    
    # Assert performance requirements
    assert execution_time < 300, "Pipeline execution time exceeds 5 minutes"
```

### 3. Load Testing

Implement load testing for production readiness:

```python
def load_test():
    # Test with large dataset
    large_df = spark.range(10000000)  # 10M records
    
    start_time = time.time()
    result = pipeline_process(large_df)
    execution_time = time.time() - start_time
    
    throughput = 10000000 / execution_time
    print(f"Throughput: {throughput:.0f} records/second")
    
    assert throughput > 10000, "Throughput below minimum requirement"
```

## Performance Monitoring Dashboard

### 1. Real-time Monitoring

Monitor pipeline performance in real-time:

```python
# Get real-time performance metrics
performance_metrics = builder.get_real_time_metrics()
print(f"Current memory usage: {performance_metrics.memory_usage_mb}MB")
print(f"Current CPU usage: {performance_metrics.cpu_usage_percent}%")
```

### 2. Historical Analysis

Analyze historical performance data:

```python
# Get performance history
history = builder.get_performance_history(days=30)
print(f"Average execution time: {history.avg_execution_time}s")
print(f"Peak memory usage: {history.peak_memory_mb}MB")
```

## Troubleshooting Performance Issues

### 1. Common Performance Problems

- **Slow Execution**: Check for inefficient transforms and validation rules
- **Memory Issues**: Optimize data types and partitioning
- **Skewed Data**: Implement data skew handling
- **Resource Contention**: Adjust parallel configuration

### 2. Performance Debugging

```python
# Enable detailed logging
import logging
logging.getLogger("sparkforge").setLevel(logging.DEBUG)

# Get detailed performance breakdown
performance_breakdown = builder.get_performance_breakdown()
for step, metrics in performance_breakdown.items():
    print(f"{step}: {metrics.execution_time}s, {metrics.memory_usage_mb}MB")
```

## Conclusion

This guide provides comprehensive strategies for optimizing SparkForge pipeline performance. Remember to:

1. **Monitor Continuously**: Use built-in performance monitoring
2. **Optimize Incrementally**: Make small, measurable improvements
3. **Test Thoroughly**: Use performance tests and load testing
4. **Document Changes**: Keep track of performance tuning decisions
5. **Monitor in Production**: Set up alerts for performance degradation

For additional support, refer to the troubleshooting guide or contact the development team.

# SparkForge Performance Tuning Guide

This guide provides comprehensive strategies for optimizing SparkForge pipeline performance in production environments, focusing on sequential execution, service optimization, and caching strategies.

## Table of Contents

1. [Performance Monitoring](#performance-monitoring)
2. [Pipeline Optimization](#pipeline-optimization)
3. [Service Optimization](#service-optimization)
4. [Data Processing Optimization](#data-processing-optimization)
5. [Memory Management](#memory-management)
6. [Caching Strategies](#caching-strategies)
7. [Resource Configuration](#resource-configuration)
8. [Best Practices](#best-practices)
9. [Performance Testing](#performance-testing)

## Performance Monitoring

### Built-in Performance Monitoring

SparkForge tracks execution metrics automatically:

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine

# Configure engine
spark = SparkSession.builder.getOrCreate()
configure_engine(spark=spark)

# Build pipeline with verbose logging
builder = PipelineBuilder(
    spark=spark,
    schema="analytics",
    min_bronze_rate=95.0,
    min_silver_rate=98.0,
    min_gold_rate=99.0,
    verbose=True  # Enable verbose logging
)
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
# Execute pipeline
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

# Access performance metrics
print(f"Status: {result.status.value}")
print(f"Duration: {result.duration_seconds:.2f}s")
print(f"Total rows written: {result.metrics.total_rows_written}")
print(f"Bronze duration: {result.metrics.bronze_duration:.2f}s")
print(f"Silver duration: {result.metrics.silver_duration:.2f}s")
print(f"Gold duration: {result.metrics.gold_duration:.2f}s")
```

## Pipeline Optimization

### 1. Step Ordering and Dependencies

Optimize pipeline execution by carefully ordering steps:

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions

configure_engine(spark=spark)
F = get_default_functions()

# ✅ Optimal: Minimize dependencies, execute in dependency order
builder = PipelineBuilder(spark=spark, schema="analytics")

# Bronze steps execute first
builder.with_bronze_rules(name="events", rules={...})
builder.with_bronze_rules(name="users", rules={...})

# Silver steps execute after their bronze dependencies
builder.add_silver_transform(
    name="user_events",
    source_bronze="events",
    transform=transform_func,
    rules={...},
    table_name="user_events"
)
builder.add_silver_transform(
    name="user_profiles",
    source_bronze="users",
    transform=transform_func,
    rules={...},
    table_name="user_profiles"
)

# Gold steps execute after all dependencies
builder.add_gold_transform(
    name="analytics",
    transform=analytics_func,
    rules={...},
    table_name="analytics",
    source_silvers=["user_events", "user_profiles"]
)
```

### 2. Validation Rule Optimization

Optimize validation rules for performance:

```python
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

# ✅ Efficient: Combine related validations
validation_rules = {
    "user_id": [
        F.col("user_id").isNotNull(),
        F.length(F.col("user_id")) > 0  # Combine null check and length validation
    ],
    "timestamp": [
        F.col("timestamp").isNotNull(),
        F.col("timestamp") > F.lit("2020-01-01")  # Combine null check and date range
    ]
}

# ❌ Inefficient: Separate rules that could be combined
validation_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "user_id_length": [F.length(F.col("user_id")) > 0],
    "timestamp": [F.col("timestamp").isNotNull()],
    "timestamp_range": [F.col("timestamp") > F.lit("2020-01-01")]
}
```

### 3. Transform Function Optimization

Optimize transform functions for better performance:

```python
from pipeline_builder.functions import get_default_functions

# ✅ Efficient: Use Spark SQL functions
def efficient_transform(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return bronze_df.withColumn("processed_at", F.current_timestamp()) \
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

def inefficient_transform(spark, bronze_df, prior_silvers):
    return bronze_df.withColumn("user_segment", segment_udf(F.col("total_spent")))
```

## Service Optimization

### 1. ExecutionValidator Optimization

Optimize validation service performance:

```python
from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models import PipelineConfig

# Use appropriate validation thresholds
config = PipelineConfig(
    schema="production",
    thresholds=ValidationThresholds(
        bronze=90.0,  # Lower threshold for bronze (faster)
        silver=95.0,  # Higher threshold for silver
        gold=98.0     # Highest threshold for gold
    )
)

engine = ExecutionEngine(spark=spark, config=config)

# Validation service caches validation results internally
# Reuse the same validator instance for multiple steps
```

### 2. TableService Optimization

Optimize table service for schema operations:

```python
# TableService caches schema information
# First call fetches schema, subsequent calls use cache
table_service = engine.table_service

# Check table existence (cached)
if table_service.table_exists("clean_events"):
    schema = table_service.get_table_schema("clean_events")  # Uses cache
```

### 3. WriteService Optimization

Optimize write service for Delta Lake operations:

```python
# WriteService handles Delta Lake optimizations automatically
write_service = engine.write_service

# Batch writes for better performance
result = write_service.write_step_output(
    df=output_df,
    step=silver_step,
    mode="overwrite"
)
```

### 4. TransformService Optimization

Optimize transform service:

```python
# TransformService applies transformations efficiently
transform_service = engine.transform_service

# Reuse transform functions
def silver_transform(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    # Efficient transformation logic
    return bronze_df.filter(F.col("status") == "active")

# Apply transform
transformed_df = transform_service.apply_silver_transform(
    step=silver_step,
    bronze_df=bronze_df,
    prior_silvers={}
)
```

## Data Processing Optimization

### 1. Partitioning Strategy

Implement effective partitioning for large datasets:

```python
from pipeline_builder.functions import get_default_functions

def optimized_silver_transform(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    # Partition by date for time-series data
    return bronze_df.repartition(F.col("date"), 20)  # 20 partitions per date

def optimized_gold_transform(spark, silvers):
    F = get_default_functions()
    # Partition by user_id for user-centric data
    return silvers["clean_events"].repartition(F.col("user_id"), 50)
```

### 2. Data Type Optimization

Use appropriate data types to reduce memory usage:

```python
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

def optimize_data_types(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return bronze_df \
        .withColumn("user_id", F.col("user_id").cast(IntegerType())) \
        .withColumn("amount", F.col("amount").cast(DoubleType())) \
        .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
```

### 3. Data Filtering

Filter data early in the pipeline:

```python
def early_filtering_transform(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    # Filter out invalid records early
    return bronze_df \
        .filter(F.col("user_id").isNotNull()) \
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
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

### 2. Memory-Efficient Operations

Use memory-efficient operations:

```python
# ✅ Memory efficient: Use select to reduce columns
def memory_efficient_transform(spark, bronze_df, prior_silvers):
    return bronze_df.select("user_id", "timestamp", "amount", "category") \
            .filter(F.col("amount") > 100)

# ❌ Memory inefficient: Keep all columns
def memory_inefficient_transform(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("amount") > 100)  # Keeps all columns in memory
```

### 3. Garbage Collection Tuning

Optimize garbage collection for long-running pipelines:

```bash
# JVM options for better GC performance
# -XX:+UseG1GC -XX:MaxGCPauseMillis=200
```

## Caching Strategies

### 1. Strategic Caching

Cache frequently accessed DataFrames:

```python
def cache_frequently_used_data(spark, bronze_df, prior_silvers):
    # Cache data that will be accessed multiple times
    bronze_df.cache()
    bronze_df.count()  # Trigger caching
    return bronze_df

# In your pipeline
builder.add_silver_transform(
    name="user_events",
    source_bronze="events",
    transform=cache_frequently_used_data,
    rules={...},
    table_name="user_events"
)
```

### 2. Service-Level Caching

Services cache metadata automatically:

```python
from pipeline_builder.execution import ExecutionEngine

engine = ExecutionEngine(spark=spark, config=config)

# TableService caches schema information
# First call fetches schema
schema1 = engine.table_service.get_table_schema("clean_events")

# Subsequent calls use cache (faster)
schema2 = engine.table_service.get_table_schema("clean_events")  # Uses cache
```

### 3. Cache Management

Manage cache size and eviction:

```python
# Configure cache storage
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Unpersist cached DataFrames when done
df.unpersist()
```

### 4. Incremental Processing Caching

Cache intermediate results for incremental processing:

```python
# Cache bronze data for incremental silver processing
def incremental_silver_transform(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    # Cache bronze data if it will be reused
    if bronze_df.is_cached:
        bronze_df.unpersist()
    bronze_df.cache()
    
    # Process incrementally
    return bronze_df.filter(
        F.col("timestamp") > F.lit("2024-01-01")
    )
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
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
    "spark.sql.shuffle.partitions": "200",
    "spark.default.parallelism": "200"
}

for key, value in spark_config.items():
    spark.conf.set(key, value)
```

### 3. Resource Allocation

Configure resource allocation:

```python
# Configure Spark resources
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.driver.maxResultSize", "2g")

# Enable dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "10")
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
- **Reuse Services**: Services are optimized and cache metadata

### 3. Service Practices

- **Reuse Service Instances**: Services cache metadata internally
- **Optimize Validation Rules**: Combine related validations
- **Use Appropriate Thresholds**: Set thresholds based on data quality requirements
- **Monitor Service Performance**: Track service-level metrics

### 4. Operational Practices

- **Resource Monitoring**: Monitor CPU, memory, and disk usage
- **Error Handling**: Implement robust error handling and retry logic
- **Backup Strategies**: Maintain data backup and recovery procedures
- **Documentation**: Document performance characteristics and tuning decisions

## Performance Testing

### 1. Built-in Performance Tests

Use execution results for performance analysis:

```python
# Execute pipeline and measure performance
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

# Analyze performance
print(f"Total duration: {result.duration_seconds:.2f}s")
print(f"Bronze duration: {result.metrics.bronze_duration:.2f}s")
print(f"Silver duration: {result.metrics.silver_duration:.2f}s")
print(f"Gold duration: {result.metrics.gold_duration:.2f}s")

# Assert performance requirements
assert result.duration_seconds < 300, "Pipeline execution time exceeds 5 minutes"
```

### 2. Custom Performance Tests

Create custom performance tests:

```python
import time
from pipeline_builder import PipelineBuilder

def performance_test():
    start_time = time.time()
    
    # Build and execute pipeline
    builder = PipelineBuilder(spark=spark, schema="test")
    # ... add steps ...
    
    pipeline = builder.to_pipeline()
    result = pipeline.run_initial_load(bronze_sources={"events": test_df})
    
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
    result = pipeline.run_initial_load(bronze_sources={"events": large_df})
    execution_time = time.time() - start_time
    
    throughput = 10000000 / execution_time
    print(f"Throughput: {throughput:.0f} records/second")
    
    assert throughput > 10000, "Throughput below minimum requirement"
```

## Troubleshooting Performance Issues

### 1. Common Performance Problems

- **Slow Execution**: Check for inefficient transforms and validation rules
- **Memory Issues**: Optimize data types and partitioning
- **Skewed Data**: Implement data skew handling
- **Service Initialization**: Services are initialized once and reused

### 2. Performance Debugging

```python
# Enable detailed logging
import logging
logging.getLogger("pipeline_builder").setLevel(logging.DEBUG)

# Get detailed performance breakdown
result = pipeline.run_initial_load(bronze_sources={"events": df})
print(f"Bronze duration: {result.metrics.bronze_duration:.2f}s")
print(f"Silver duration: {result.metrics.silver_duration:.2f}s")
print(f"Gold duration: {result.metrics.gold_duration:.2f}s")
```

## Conclusion

This guide provides comprehensive strategies for optimizing SparkForge pipeline performance. Remember to:

1. **Monitor Continuously**: Use built-in performance monitoring
2. **Optimize Incrementally**: Make small, measurable improvements
3. **Test Thoroughly**: Use performance tests and load testing
4. **Document Changes**: Keep track of performance tuning decisions
5. **Monitor in Production**: Set up alerts for performance degradation
6. **Leverage Services**: Services are optimized and cache metadata automatically

For additional support, refer to the troubleshooting guide or contact the development team.

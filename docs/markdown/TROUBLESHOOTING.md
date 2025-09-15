# Troubleshooting Guide - SparkForge

This comprehensive troubleshooting guide helps you diagnose and resolve common issues with SparkForge pipelines.

## Table of Contents

1. [Quick Diagnosis](#quick-diagnosis)
2. [Setup and Installation Issues](#setup-and-installation-issues)
3. [Pipeline Execution Issues](#pipeline-execution-issues)
4. [Data Quality Issues](#data-quality-issues)
5. [Performance Issues](#performance-issues)
6. [Memory and Resource Issues](#memory-and-resource-issues)
7. [Delta Lake Issues](#delta-lake-issues)
8. [Debugging Workflows](#debugging-workflows)
9. [Error Code Reference](#error-code-reference)
10. [Prevention Best Practices](#prevention-best-practices)

---

## Quick Diagnosis

### Pipeline Failed? Start Here

```python
# 1. Check basic pipeline status
result = pipeline.run_incremental(bronze_sources={"events": source_df})
print(f"Success: {result.success}")
print(f"Error: {result.error_message}")
print(f"Failed steps: {result.failed_steps}")

# 2. Check validation rates
if hasattr(result, 'stage_stats'):
    for stage, stats in result.stage_stats.items():
        print(f"{stage}: {stats.get('validation_rate', 0):.1f}%")

# 3. Debug individual steps
bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
print(f"Bronze validation: {bronze_result.validation_result.validation_rate:.1f}%")
```

### Common Quick Fixes

| Issue | Quick Fix |
|-------|-----------|
| **Java not found** | Install Java 8+ and set JAVA_HOME |
| **Memory errors** | Increase driver memory: `--driver-memory 4g` |
| **Validation failures** | Lower validation thresholds temporarily |
| **Import errors** | Check SparkForge installation: `pip install sparkforge` |
| **Permission errors** | Check write permissions for warehouse directory |

---

## Setup and Installation Issues

### Java Installation Issues

**Problem**: `java.lang.RuntimeException: No java executable found`

**Solution**:
```bash
# macOS
brew install openjdk@8
export JAVA_HOME=/opt/homebrew/opt/openjdk@8

# Ubuntu/Debian
sudo apt update
sudo apt install openjdk-8-jdk
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Windows
# Download and install Java 8+ from Oracle or OpenJDK
# Set JAVA_HOME environment variable
```

**Verification**:
```bash
java -version
echo $JAVA_HOME
```

### Python Environment Issues

**Problem**: `ModuleNotFoundError: No module named 'sparkforge'`

**Solution**:
```bash
# Check Python version (requires 3.8+)
python --version

# Install SparkForge
pip install sparkforge

# Verify installation
python -c "import sparkforge; print('SparkForge installed successfully')"

# If using virtual environment
python -m venv sparkforge_env
source sparkforge_env/bin/activate  # Linux/Mac
# or
sparkforge_env\Scripts\activate  # Windows
pip install sparkforge
```

### Spark Configuration Issues

**Problem**: Spark session fails to start

**Solution**:
```python
# Basic Spark configuration
spark = SparkSession.builder \
    .appName("SparkForge Pipeline") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# For production environments
spark = SparkSession.builder \
    .appName("SparkForge Pipeline") \
    .master("yarn") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

---

## Pipeline Execution Issues

### Validation Failures

**Problem**: Pipeline fails with validation errors

**Diagnosis**:
```python
# Check validation results
result = pipeline.run_incremental(bronze_sources={"events": source_df})
if not result.success:
    print(f"Validation failed: {result.validation_errors}")

    # Check specific stage validation
    bronze_stats = result.stage_stats.get('bronze', {})
    print(f"Bronze validation rate: {bronze_stats.get('validation_rate', 0):.1f}%")

    # Debug Bronze step
    bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
    print(f"Bronze validation details: {bronze_result.validation_result}")
```

**Solutions**:

1. **Lower Validation Thresholds**:
```python
builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    min_bronze_rate=90.0,  # Lower from 95.0
    min_silver_rate=95.0,  # Lower from 98.0
    min_gold_rate=98.0     # Lower from 99.0
)
```

2. **Fix Data Quality Issues**:
```python
# Check data quality
source_df.show()
source_df.printSchema()

# Check for null values
source_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in source_df.columns]).show()

# Check data ranges
source_df.describe().show()
```

3. **Adjust Validation Rules**:
```python
# More lenient validation rules
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        # Remove strict rules temporarily
        # "amount": [F.col("amount") > 0]  # Comment out problematic rules
    }
)
```

### Step Execution Failures

**Problem**: Individual steps fail during execution

**Diagnosis**:
```python
# Debug specific step
try:
    bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
    print(f"Bronze status: {bronze_result.status.value}")
    print(f"Bronze output: {bronze_result.output_count}")
except Exception as e:
    print(f"Bronze step failed: {e}")
    import traceback
    traceback.print_exc()

# Check step dependencies
step_info = pipeline.get_step_info("silver_events")
print(f"Step dependencies: {step_info['dependencies']}")
print(f"Step dependents: {step_info['dependents']}")
```

**Solutions**:

1. **Fix Transformation Logic**:
```python
# Test transformation function separately
def test_transform(spark, df):
    try:
        result = your_transform_function(spark, df, {})
        print(f"Transform successful: {result.count()} rows")
        return result
    except Exception as e:
        print(f"Transform failed: {e}")
        return None

# Test with sample data
test_result = test_transform(spark, source_df.limit(10))
```

2. **Check Data Schema**:
```python
# Verify schema compatibility
expected_schema = source_df.schema
actual_schema = your_df.schema
print(f"Expected: {expected_schema}")
print(f"Actual: {actual_schema}")

# Fix schema issues
fixed_df = source_df.select([F.col(c).cast("string").alias(c) for c in source_df.columns])
```

### Dependency Issues

**Problem**: Steps fail due to missing dependencies

**Diagnosis**:
```python
# Check pipeline structure
steps = pipeline.list_steps()
print(f"Bronze steps: {steps['bronze']}")
print(f"Silver steps: {steps['silver']}")
print(f"Gold steps: {steps['gold']}")

# Check specific step dependencies
for step_name in steps['silver']:
    step_info = pipeline.get_step_info(step_name)
    print(f"{step_name} depends on: {step_info['dependencies']}")
```

**Solutions**:

1. **Fix Missing Dependencies**:
```python
# Ensure Bronze step exists before Silver step
builder.with_bronze_rules(name="events", rules={...})

# Add Silver step with correct source
builder.add_silver_transform(
    name="silver_events",
    source_bronze="events",  # Must match Bronze step name
    transform=your_transform,
    rules={...},
    table_name="silver_events"
)
```

2. **Fix Silver-to-Silver Dependencies**:
```python
# Silver step depending on another Silver step
builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.join(silvers["user_profiles"], "user_id"),
    source_silvers=["user_profiles"],  # Specify Silver dependency
    rules={...},
    table_name="enriched_events"
)
```

---

## Data Quality Issues

### Schema Mismatches

**Problem**: Schema validation failures

**Diagnosis**:
```python
# Compare schemas
source_df.printSchema()
target_df.printSchema()

# Check for schema differences
source_columns = set(source_df.columns)
target_columns = set(target_df.columns)
missing_columns = target_columns - source_columns
extra_columns = source_columns - target_columns

print(f"Missing columns: {missing_columns}")
print(f"Extra columns: {extra_columns}")
```

**Solutions**:

1. **Schema Alignment**:
```python
# Align schemas
def align_schema(df, target_schema):
    for field in target_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df
```

2. **Dynamic Schema Handling**:
```python
# Handle schema evolution
def handle_schema_evolution(df, expected_columns):
    current_columns = df.columns
    for col in expected_columns:
        if col not in current_columns:
            df = df.withColumn(col, F.lit(None))
    return df.select(*expected_columns)
```

### Data Type Issues

**Problem**: Data type conversion failures

**Diagnosis**:
```python
# Check data types
source_df.dtypes

# Check for problematic values
source_df.select([F.col(c).cast("int").alias(f"{c}_int") for c in numeric_columns]).show()
```

**Solutions**:

1. **Safe Type Conversion**:
```python
# Safe conversion with error handling
def safe_cast(df, column, target_type):
    return df.withColumn(
        f"{column}_converted",
        F.when(F.col(column).isNotNull(), F.col(column).cast(target_type))
        .otherwise(None)
    )
```

2. **Data Cleaning**:
```python
# Clean problematic data
def clean_numeric_data(df, column):
    return df.withColumn(
        f"{column}_clean",
        F.when(
            F.col(column).rlike("^[0-9]+$"),
            F.col(column).cast("int")
        ).otherwise(None)
    )
```

---

## Performance Issues

### Slow Pipeline Execution

**Problem**: Pipeline takes too long to execute

**Diagnosis**:
```python
# Profile pipeline performance
from sparkforge.performance import performance_monitor
import time

start_time = time.time()
with performance_monitor("pipeline_execution"):
    result = pipeline.run_incremental(bronze_sources={"events": source_df})
end_time = time.time()

print(f"Total execution time: {end_time - start_time:.2f}s")
print(f"Rows processed: {result.totals['total_rows_written']}")
print(f"Rows per second: {result.totals['total_rows_written'] / (end_time - start_time):.0f}")
```

**Solutions**:

1. **Enable Parallel Execution**:
```python
builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    enable_parallel_silver=True,
    max_parallel_workers=4  # Increase for better performance
)

# Enable unified execution for maximum performance
pipeline = (builder
    .enable_unified_execution(
        max_workers=8,
        enable_parallel_execution=True,
        enable_dependency_optimization=True
    )
    .to_pipeline()
)
```

2. **Optimize Transformations**:
```python
# Use efficient Spark operations
def optimized_transform(spark, df, silvers):
    return (df
        .filter(F.col("status") == "active")  # Filter early
        .select("id", "name", "value")        # Select only needed columns
        .distinct()                           # Remove duplicates efficiently
    )
```

3. **Use Incremental Processing**:
```python
# Process only new/changed data
result = pipeline.run_incremental(bronze_sources={"events": new_data_df})
```

### Memory Issues

**Problem**: OutOfMemoryError or excessive memory usage

**Diagnosis**:
```python
# Check Spark UI for memory usage
# http://localhost:4040 (local mode)

# Monitor memory usage
import psutil
print(f"Memory usage: {psutil.virtual_memory().percent}%")
```

**Solutions**:

1. **Increase Driver Memory**:
```python
spark = SparkSession.builder \
    .appName("SparkForge Pipeline") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

2. **Optimize Data Processing**:
```python
# Use lazy evaluation
def memory_efficient_transform(spark, df, silvers):
    return (df
        .filter(F.col("date") >= "2024-01-01")  # Filter early
        .select("id", "value")                  # Select minimal columns
        .repartition(4)                         # Optimize partitioning
    )
```

3. **Use Caching Strategically**:
```python
# Cache frequently used DataFrames
frequently_used_df = df.filter(F.col("status") == "active").cache()
```

---

## Memory and Resource Issues

### Driver Memory Exhaustion

**Problem**: Driver runs out of memory

**Solutions**:
```python
# Increase driver memory
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "4g") \
    .getOrCreate()

# Use broadcast joins for small datasets
small_df = spark.table("small_table").broadcast()
result_df = large_df.join(small_df, "key")
```

### Executor Memory Issues

**Problem**: Executors run out of memory

**Solutions**:
```python
# Configure executor memory
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.memoryFraction", "0.8") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

# Optimize data partitioning
df = df.repartition(200)  # Increase partitions for better distribution
```

---

## Delta Lake Issues

### Transaction Conflicts

**Problem**: Concurrent write conflicts

**Solutions**:
```python
# Use appropriate write modes
df.write.mode("append").format("delta").save("path/to/table")

# For updates, use merge operations
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "path/to/table")
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Schema Evolution Issues

**Problem**: Schema changes cause failures

**Solutions**:
```python
# Enable automatic schema evolution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Handle schema evolution manually
def handle_schema_evolution(df, target_table):
    target_schema = spark.table(target_table).schema
    current_schema = df.schema

    # Add missing columns
    for field in target_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

    return df.select(*[field.name for field in target_schema.fields])
```

---

## Debugging Workflows

### Step-by-Step Debugging

```python
# 1. Start with Bronze layer
bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
print(f"Bronze validation rate: {bronze_result.validation_result.validation_rate:.1f}%")

if bronze_result.validation_result.validation_rate < 95:
    # Check data quality issues
    print("Data quality issues detected")
    # Fix data quality problems

# 2. Test Silver layer
silver_result = pipeline.execute_silver_step("clean_events")
print(f"Silver output rows: {silver_result.output_count}")

if silver_result.output_count == 0:
    # Check transformation logic
    print("Transformation produced no output")
    # Debug transformation function

# 3. Test Gold layer
gold_result = pipeline.execute_gold_step("analytics")
print(f"Gold output rows: {gold_result.output_count}")

if gold_result.output_count == 0:
    # Check aggregation logic
    print("Aggregation produced no output")
    # Debug aggregation function
```

### Interactive Debugging

```python
# Create step executor for detailed debugging
executor = pipeline.create_step_executor()

# Get step output for inspection
silver_output = executor.get_step_output("silver_events")
silver_output.show()
silver_output.printSchema()

# Check execution state
completed_steps = executor.list_completed_steps()
failed_steps = executor.list_failed_steps()

print(f"Completed: {completed_steps}")
print(f"Failed: {failed_steps}")

# Clear state for fresh start
executor.clear_execution_state()
```

### Performance Profiling

```python
# Profile individual steps
from sparkforge.performance import time_operation

@time_operation("bronze_validation")
def profile_bronze():
    return pipeline.execute_bronze_step("events", input_data=source_df)

@time_operation("silver_transform")
def profile_silver():
    return pipeline.execute_silver_step("clean_events")

# Run profiling
bronze_result = profile_bronze()
silver_result = profile_silver()

# Analyze results
print(f"Bronze duration: {bronze_result.duration_seconds:.2f}s")
print(f"Silver duration: {silver_result.duration_seconds:.2f}s")
```

---

## Error Code Reference

### Common Error Codes

| Error Code | Description | Solution |
|------------|-------------|----------|
| `VALIDATION_FAILED` | Data validation threshold not met | Lower validation thresholds or fix data quality |
| `DEPENDENCY_MISSING` | Required step dependency not found | Check step names and dependencies |
| `SCHEMA_MISMATCH` | Schema validation failed | Align schemas or handle schema evolution |
| `MEMORY_EXHAUSTED` | Out of memory error | Increase memory or optimize data processing |
| `JAVA_NOT_FOUND` | Java runtime not found | Install Java 8+ and set JAVA_HOME |
| `PERMISSION_DENIED` | File system permission error | Check write permissions for warehouse directory |

### Error Handling Patterns

```python
try:
    result = pipeline.run_incremental(bronze_sources={"events": source_df})
    if not result.success:
        print(f"Pipeline failed: {result.error_message}")
        # Handle specific error types
        if "VALIDATION_FAILED" in result.error_message:
            # Handle validation failure
            pass
        elif "MEMORY_EXHAUSTED" in result.error_message:
            # Handle memory issues
            pass
except Exception as e:
    print(f"Unexpected error: {e}")
    import traceback
    traceback.print_exc()
```

---

## Prevention Best Practices

### Data Quality Prevention

```python
# Implement data quality checks early
def validate_source_data(df):
    """Validate source data before processing"""
    total_rows = df.count()
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]

    quality_issues = []
    for col, null_count in null_counts.asDict().items():
        if null_count > total_rows * 0.1:  # More than 10% nulls
            quality_issues.append(f"Column {col} has {null_count} null values")

    if quality_issues:
        print("Data quality issues detected:")
        for issue in quality_issues:
            print(f"  - {issue}")

    return len(quality_issues) == 0

# Use in pipeline
if not validate_source_data(source_df):
    print("Skipping pipeline due to data quality issues")
    exit(1)
```

### Performance Prevention

```python
# Monitor pipeline performance
def monitor_performance(pipeline, source_data):
    """Monitor pipeline performance and alert on issues"""
    start_time = time.time()
    result = pipeline.run_incremental(bronze_sources={"events": source_data})
    end_time = time.time()

    execution_time = end_time - start_time
    rows_per_second = result.totals['total_rows_written'] / execution_time

    print(f"Execution time: {execution_time:.2f}s")
    print(f"Throughput: {rows_per_second:.0f} rows/second")

    # Alert on performance issues
    if execution_time > 300:  # More than 5 minutes
        print("⚠️  Pipeline execution took longer than expected")

    if rows_per_second < 1000:  # Less than 1000 rows/second
        print("⚠️  Pipeline throughput is lower than expected")

    return result
```

### Error Prevention

```python
# Implement comprehensive error handling
def safe_pipeline_execution(pipeline, source_data):
    """Execute pipeline with comprehensive error handling"""
    try:
        # Validate inputs
        if source_data is None or source_data.count() == 0:
            raise ValueError("Source data is empty or None")

        # Execute pipeline
        result = pipeline.run_incremental(bronze_sources={"events": source_data})

        # Validate results
        if not result.success:
            raise RuntimeError(f"Pipeline execution failed: {result.error_message}")

        # Log success
        print(f"✅ Pipeline completed successfully")
        print(f"📊 Rows processed: {result.totals['total_rows_written']}")

        return result

    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        # Log error details
        import logging
        logging.error(f"Pipeline error: {e}", exc_info=True)
        raise
```

---

## Getting Additional Help

### Documentation Resources
- **[User Guide](USER_GUIDE.md)** - Complete feature documentation
- **[API Reference](API_REFERENCE.md)** - Detailed API documentation
- **[Examples](examples/)** - Working code examples
- **[Visual Guide](VISUAL_GUIDE.md)** - Diagrams and flowcharts

### Community Support
- **GitHub Issues**: Report bugs and request features
- **GitHub Discussions**: Ask questions and share solutions
- **Documentation Issues**: Report documentation problems

### Professional Support
- **Enterprise Support**: Contact for commercial support
- **Training Services**: Professional training and consulting
- **Custom Development**: Custom pipeline development services

---

**💡 Remember**: Most issues can be resolved by following the debugging workflows and checking the common solutions above. When in doubt, start with step-by-step debugging to isolate the problem.

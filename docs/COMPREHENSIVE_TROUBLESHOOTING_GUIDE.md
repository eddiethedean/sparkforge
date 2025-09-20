# SparkForge Comprehensive Troubleshooting Guide

This guide provides detailed solutions for common issues encountered when using SparkForge in production environments.

## Table of Contents

1. [Installation Issues](#installation-issues)
2. [Configuration Problems](#configuration-problems)
3. [Pipeline Execution Errors](#pipeline-execution-errors)
4. [Data Validation Issues](#data-validation-issues)
5. [Performance Problems](#performance-problems)
6. [Memory and Resource Issues](#memory-and-resource-issues)
7. [Network and Connectivity Issues](#network-and-connectivity-issues)
8. [Deployment Issues](#deployment-issues)
9. [Monitoring and Logging Issues](#monitoring-and-logging-issues)
10. [Advanced Troubleshooting](#advanced-troubleshooting)

## Installation Issues

### Python Version Compatibility

**Problem**: SparkForge requires Python 3.8 or higher.

**Symptoms**:
- ImportError when importing sparkforge
- SyntaxError in Python 3.7 or earlier

**Solutions**:

1. **Check Python Version**:
```bash
python --version
# Should be 3.8 or higher
```

2. **Upgrade Python**:
```bash
# Using pyenv
pyenv install 3.8.18
pyenv global 3.8.18

# Using conda
conda create -n sparkforge python=3.8
conda activate sparkforge
```

3. **Verify Installation**:
```python
import sys
print(sys.version)
# Should show Python 3.8+
```

### Java Dependencies

**Problem**: PySpark requires Java 11 or higher.

**Symptoms**:
- `Java gateway process exited before sending its port number`
- `JAVA_HOME is not set`

**Solutions**:

1. **Install Java 11**:
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install openjdk-11-jdk

# macOS
brew install openjdk@11

# Windows
# Download from Oracle or use Chocolatey
choco install openjdk11
```

2. **Set JAVA_HOME**:
```bash
# Linux/macOS
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Windows
set JAVA_HOME=C:\Program Files\Java\jdk-11
set PATH=%JAVA_HOME%\bin;%PATH%
```

3. **Verify Java Installation**:
```bash
java -version
# Should show Java 11 or higher
```

### Package Dependencies

**Problem**: Missing or incompatible dependencies.

**Symptoms**:
- ImportError for specific modules
- Version conflicts

**Solutions**:

1. **Clean Installation**:
```bash
pip uninstall sparkforge
pip install --no-cache-dir sparkforge
```

2. **Check Dependencies**:
```bash
pip list | grep -E "(pyspark|pandas|numpy)"
```

3. **Install Specific Versions**:
```bash
pip install pyspark>=3.4.0 pandas>=1.3.0 numpy>=1.21.0
```

## Configuration Problems

### Invalid Pipeline Configuration

**Problem**: PipelineConfig validation fails.

**Symptoms**:
- `ConfigurationError: Invalid configuration`
- `ValidationError: Schema name cannot be empty`

**Solutions**:

1. **Check Schema Name**:
```python
# ❌ Invalid
config = PipelineConfig(schema="", quality_thresholds=thresholds)

# ✅ Valid
config = PipelineConfig(schema="analytics", quality_thresholds=thresholds)
```

2. **Validate Quality Thresholds**:
```python
# ❌ Invalid - thresholds must be between 0 and 100
thresholds = ValidationThresholds(bronze=150.0, silver=85.0, gold=90.0)

# ✅ Valid
thresholds = ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0)
```

3. **Check Parallel Configuration**:
```python
# ❌ Invalid - max_workers must be positive
parallel = ParallelConfig(enabled=True, max_workers=0)

# ✅ Valid
parallel = ParallelConfig(enabled=True, max_workers=4)
```

### Environment Variables

**Problem**: Missing or incorrect environment variables.

**Symptoms**:
- `KeyError: 'SPARKFORGE_ENV'`
- Configuration not loading properly

**Solutions**:

1. **Set Required Environment Variables**:
```bash
export SPARKFORGE_ENV=production
export SPARKFORGE_LOG_LEVEL=INFO
export SPARKFORGE_DATA_PATH=/data/sparkforge
export SPARKFORGE_CONFIG_PATH=/config/sparkforge
```

2. **Create Configuration File**:
```python
# config.py
import os

class Config:
    SPARKFORGE_ENV = os.getenv('SPARKFORGE_ENV', 'development')
    LOG_LEVEL = os.getenv('SPARKFORGE_LOG_LEVEL', 'INFO')
    DATA_PATH = os.getenv('SPARKFORGE_DATA_PATH', './data')
    CONFIG_PATH = os.getenv('SPARKFORGE_CONFIG_PATH', './config')
```

## Pipeline Execution Errors

### Step Validation Failures

**Problem**: Pipeline steps fail validation.

**Symptoms**:
- `ValidationError: Rules must be a non-empty dictionary`
- `ValidationError: Transform function must be callable`

**Solutions**:

1. **Fix Validation Rules**:
```python
# ❌ Invalid - empty rules
bronze_step = BronzeStep(
    name="events",
    transform=transform_func,
    rules={}  # Empty rules
)

# ✅ Valid - non-empty rules
bronze_step = BronzeStep(
    name="events",
    transform=transform_func,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    }
)
```

2. **Check Transform Functions**:
```python
# ❌ Invalid - not callable
bronze_step = BronzeStep(
    name="events",
    transform="not_a_function",  # String instead of function
    rules=validation_rules
)

# ✅ Valid - callable function
def transform_func(df):
    return df.withColumn("processed_at", F.current_timestamp())

bronze_step = BronzeStep(
    name="events",
    transform=transform_func,  # Callable function
    rules=validation_rules
)
```

### Step Dependencies

**Problem**: Steps have incorrect dependencies.

**Symptoms**:
- `PipelineError: Circular dependency detected`
- `PipelineError: Missing source step`

**Solutions**:

1. **Check Step Dependencies**:
```python
# ❌ Invalid - circular dependency
builder.add_silver_step("step1", transform_func, rules, source_bronze="step2")
builder.add_silver_step("step2", transform_func, rules, source_bronze="step1")

# ✅ Valid - no circular dependency
builder.add_bronze_step("bronze_step", transform_func, rules)
builder.add_silver_step("silver_step", transform_func, rules, source_bronze="bronze_step")
```

2. **Verify Source Steps**:
```python
# ❌ Invalid - source step doesn't exist
builder.add_silver_step("silver_step", transform_func, rules, source_bronze="nonexistent_step")

# ✅ Valid - source step exists
builder.add_bronze_step("bronze_step", transform_func, rules)
builder.add_silver_step("silver_step", transform_func, rules, source_bronze="bronze_step")
```

### Execution Timeouts

**Problem**: Pipeline execution times out.

**Symptoms**:
- `TimeoutError: Pipeline execution timed out`
- Long-running steps never complete

**Solutions**:

1. **Increase Timeout**:
```python
# Set longer timeout
runner = SimplePipelineRunner(timeout=3600)  # 1 hour timeout
```

2. **Optimize Transform Functions**:
```python
# ❌ Inefficient - processing all data
def inefficient_transform(df):
    return df.collect()  # Collects all data to driver

# ✅ Efficient - processing in parallel
def efficient_transform(df):
    return df.repartition(20).withColumn("processed", F.current_timestamp())
```

3. **Check Data Volume**:
```python
# Check data size before processing
row_count = df.count()
print(f"Processing {row_count:,} records")

if row_count > 1000000:  # 1M records
    print("Large dataset detected - consider partitioning")
    df = df.repartition(100)
```

## Data Validation Issues

### Low Quality Scores

**Problem**: Data validation fails with low quality scores.

**Symptoms**:
- `ValidationError: Quality score 45% below threshold 80%`
- High number of invalid records

**Solutions**:

1. **Investigate Data Quality**:
```python
# Check data quality
quality_result = assess_data_quality(df, validation_rules)
print(f"Quality score: {quality_result['quality_score']}%")
print(f"Invalid records: {quality_result['invalid_records']}")

# Analyze invalid records
invalid_df = df.filter(~validation_predicate)
invalid_df.show(10)
```

2. **Adjust Quality Thresholds**:
```python
# Lower thresholds for development
thresholds = ValidationThresholds(
    bronze=70.0,  # Lower threshold
    silver=75.0,
    gold=80.0
)
```

3. **Fix Data Issues**:
```python
# Clean data before validation
def clean_data(df):
    return df.filter(F.col("user_id").isNotNull()) \
            .filter(F.col("timestamp") > F.lit("2020-01-01")) \
            .filter(F.col("amount") > 0)
```

### Schema Validation Failures

**Problem**: DataFrame schema doesn't match expected schema.

**Symptoms**:
- `ValidationError: Schema validation failed`
- Missing or extra columns

**Solutions**:

1. **Check DataFrame Schema**:
```python
# Print current schema
df.printSchema()

# Check column names
print("Current columns:", df.columns)
print("Expected columns:", expected_columns)
```

2. **Fix Schema Issues**:
```python
# Add missing columns
if "missing_column" not in df.columns:
    df = df.withColumn("missing_column", F.lit(None))

# Remove extra columns
df = df.select(*expected_columns)

# Rename columns
df = df.withColumnRenamed("old_name", "new_name")
```

3. **Validate Schema**:
```python
# Validate schema
is_valid = validate_dataframe_schema(df, expected_columns)
if not is_valid:
    print("Schema validation failed")
    # Handle schema mismatch
```

## Performance Problems

### Slow Pipeline Execution

**Problem**: Pipeline execution is slower than expected.

**Symptoms**:
- Long execution times
- High resource usage
- Timeout errors

**Solutions**:

1. **Enable Parallel Processing**:
```python
# Enable parallel processing
parallel_config = ParallelConfig(
    enabled=True,
    max_workers=4  # Adjust based on available cores
)

config = PipelineConfig(
    schema="analytics",
    parallel=parallel_config
)
```

2. **Optimize Transform Functions**:
```python
# ❌ Inefficient - UDF for simple operations
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def get_category(amount):
    if amount > 1000:
        return "premium"
    elif amount > 500:
        return "standard"
    else:
        return "basic"

category_udf = udf(get_category, StringType())

# ✅ Efficient - use Spark SQL functions
def efficient_transform(df):
    return df.withColumn("category",
                        F.when(F.col("amount") > 1000, "premium")
                         .when(F.col("amount") > 500, "standard")
                         .otherwise("basic"))
```

3. **Optimize Data Partitioning**:
```python
# Partition data for better performance
df = df.repartition(F.col("date"), 20)  # 20 partitions per date

# Coalesce small partitions
df = df.coalesce(10)  # Reduce to 10 partitions
```

### Memory Issues

**Problem**: Out of memory errors during execution.

**Symptoms**:
- `OutOfMemoryError`
- High memory usage
- Slow performance

**Solutions**:

1. **Increase Memory Allocation**:
```python
# Configure Spark memory
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.maxResultSize", "2g")
```

2. **Optimize Data Types**:
```python
# Use appropriate data types
df = df.withColumn("user_id", F.col("user_id").cast("integer")) \
        .withColumn("amount", F.col("amount").cast("double")) \
        .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
```

3. **Implement Data Filtering**:
```python
# Filter data early to reduce memory usage
df = df.filter(F.col("date") >= F.lit("2023-01-01")) \
        .filter(F.col("amount") > 0) \
        .select("user_id", "amount", "date")  # Select only needed columns
```

## Memory and Resource Issues

### Driver Memory Issues

**Problem**: Driver runs out of memory.

**Symptoms**:
- `OutOfMemoryError` in driver
- Slow response times
- Application crashes

**Solutions**:

1. **Increase Driver Memory**:
```python
# Set driver memory
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("spark.driver.maxResultSize", "4g")
```

2. **Avoid Collecting Large Datasets**:
```python
# ❌ Dangerous - collects all data to driver
large_data = df.collect()

# ✅ Safe - process data in parallel
result = df.groupBy("category").count()
result.show()
```

3. **Use Broadcast Variables**:
```python
# Broadcast small datasets
small_df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
broadcast_df = broadcast(small_df)

# Join with broadcast
result = large_df.join(broadcast_df, "id")
```

### Executor Memory Issues

**Problem**: Executors run out of memory.

**Symptoms**:
- Executor failures
- Task retries
- Slow performance

**Solutions**:

1. **Increase Executor Memory**:
```python
# Set executor memory
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryFraction", "0.8")
```

2. **Optimize Data Partitioning**:
```python
# Increase number of partitions
df = df.repartition(200)  # More partitions = less data per partition

# Or use coalesce to reduce partitions
df = df.coalesce(50)  # Fewer partitions = more data per partition
```

3. **Enable Adaptive Query Execution**:
```python
# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## Network and Connectivity Issues

### Database Connection Issues

**Problem**: Cannot connect to database.

**Symptoms**:
- `ConnectionError: Unable to connect to database`
- Timeout errors
- Authentication failures

**Solutions**:

1. **Check Connection Parameters**:
```python
# Verify connection parameters
database_url = "postgresql://user:password@host:port/database"
print(f"Connecting to: {database_url}")

# Test connection
try:
    connection = psycopg2.connect(database_url)
    print("Database connection successful")
except Exception as e:
    print(f"Database connection failed: {e}")
```

2. **Check Network Connectivity**:
```bash
# Test network connectivity
telnet database_host 5432
ping database_host
```

3. **Verify Credentials**:
```python
# Check environment variables
import os
print(f"Database URL: {os.getenv('DATABASE_URL')}")
print(f"Database User: {os.getenv('DATABASE_USER')}")
```

### Spark Cluster Connectivity

**Problem**: Cannot connect to Spark cluster.

**Symptoms**:
- `SparkException: Failed to connect to cluster`
- Connection timeout
- Authentication failures

**Solutions**:

1. **Check Spark Configuration**:
```python
# Verify Spark configuration
print(f"Spark Master: {spark.conf.get('spark.master')}")
print(f"Spark App Name: {spark.conf.get('spark.app.name')}")
```

2. **Test Cluster Connectivity**:
```bash
# Test cluster connectivity
telnet spark_master_host 7077
```

3. **Check Authentication**:
```python
# Verify authentication
if spark.conf.get('spark.authenticate') == 'true':
    print("Authentication enabled")
    # Check authentication configuration
```

## Deployment Issues

### Container Deployment Issues

**Problem**: SparkForge fails to start in container.

**Symptoms**:
- Container exits immediately
- Port binding failures
- Volume mount issues

**Solutions**:

1. **Check Container Logs**:
```bash
# View container logs
docker logs sparkforge-container

# Follow logs in real-time
docker logs -f sparkforge-container
```

2. **Verify Port Binding**:
```bash
# Check port binding
docker port sparkforge-container

# Test port connectivity
telnet localhost 8080
```

3. **Check Volume Mounts**:
```bash
# Verify volume mounts
docker inspect sparkforge-container | grep -A 10 "Mounts"

# Test volume access
docker exec sparkforge-container ls -la /data
```

### Kubernetes Deployment Issues

**Problem**: SparkForge pods fail to start in Kubernetes.

**Symptoms**:
- Pods in CrashLoopBackOff state
- Image pull failures
- Resource quota exceeded

**Solutions**:

1. **Check Pod Status**:
```bash
# Check pod status
kubectl get pods -n sparkforge

# Describe pod for details
kubectl describe pod <pod-name> -n sparkforge
```

2. **Check Pod Logs**:
```bash
# View pod logs
kubectl logs <pod-name> -n sparkforge

# Follow logs
kubectl logs -f <pod-name> -n sparkforge
```

3. **Check Resource Quotas**:
```bash
# Check resource quotas
kubectl describe quota -n sparkforge

# Check resource usage
kubectl top pods -n sparkforge
```

## Monitoring and Logging Issues

### Logging Configuration Issues

**Problem**: Logs are not being generated or are incomplete.

**Symptoms**:
- No log files created
- Incomplete log information
- Wrong log levels

**Solutions**:

1. **Check Logging Configuration**:
```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/logs/sparkforge.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('sparkforge')
logger.info("Logging configured successfully")
```

2. **Verify Log Directory**:
```bash
# Check log directory permissions
ls -la /logs/
chmod 755 /logs/
chown sparkforge:sparkforge /logs/
```

3. **Test Logging**:
```python
# Test logging
logger.debug("Debug message")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
```

### Performance Monitoring Issues

**Problem**: Performance monitoring is not working.

**Symptoms**:
- No performance metrics collected
- Performance data is inaccurate
- Monitoring overhead is too high

**Solutions**:

1. **Enable Performance Monitoring**:
```python
# Enable performance monitoring
config = PipelineConfig(
    schema="analytics",
    performance_monitoring=True
)
```

2. **Check Performance Data**:
```python
# Get performance summary
performance_summary = builder.get_performance_summary()
print(f"Performance data: {performance_summary}")

# Check if monitoring is active
if performance_summary.total_tests == 0:
    print("Performance monitoring not active")
```

3. **Optimize Monitoring Overhead**:
```python
# Reduce monitoring frequency
monitor = PerformanceMonitor(sample_rate=0.1)  # 10% sampling
```

## Advanced Troubleshooting

### Debug Mode

Enable debug mode for detailed troubleshooting:

```python
# Enable debug mode
config = PipelineConfig(
    schema="analytics",
    debug_mode=True
)

# Set debug logging
import logging
logging.getLogger("sparkforge").setLevel(logging.DEBUG)
```

### Performance Profiling

Profile pipeline performance:

```python
import cProfile
import pstats

# Profile pipeline execution
profiler = cProfile.Profile()
profiler.enable()

# Run pipeline
runner.run_pipeline(builder)

profiler.disable()

# Analyze results
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(10)  # Top 10 functions
```

### Memory Profiling

Profile memory usage:

```python
import tracemalloc

# Start memory tracing
tracemalloc.start()

# Run pipeline
runner.run_pipeline(builder)

# Get memory snapshot
snapshot = tracemalloc.take_snapshot()
top_stats = snapshot.statistics('lineno')

# Print top memory usage
for stat in top_stats[:10]:
    print(stat)
```

### Network Troubleshooting

Debug network issues:

```python
import socket
import urllib.parse

def test_connection(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

# Test database connection
db_url = "postgresql://user:pass@host:port/db"
parsed = urllib.parse.urlparse(db_url)
if test_connection(parsed.hostname, parsed.port):
    print("Database connection successful")
else:
    print("Database connection failed")
```

## Getting Help

### Log Collection

Collect comprehensive logs for troubleshooting:

```bash
# Collect system logs
journalctl -u sparkforge > system.log

# Collect application logs
kubectl logs -l app=sparkforge > application.log

# Collect configuration
kubectl get configmap sparkforge-config -o yaml > config.yaml
```

### Support Information

When seeking help, provide:

1. **System Information**:
   - Python version
   - Java version
   - Operating system
   - Spark version

2. **Configuration**:
   - Pipeline configuration
   - Environment variables
   - Resource allocation

3. **Error Information**:
   - Complete error messages
   - Stack traces
   - Log files

4. **Reproduction Steps**:
   - Steps to reproduce the issue
   - Sample data (if applicable)
   - Expected vs actual behavior

### Contact Information

- **GitHub Issues**: [SparkForge Issues](https://github.com/eddiethedean/sparkforge/issues)
- **Documentation**: [SparkForge Docs](https://sparkforge.readthedocs.io/)
- **Community**: [SparkForge Discussions](https://github.com/eddiethedean/sparkforge/discussions)

This comprehensive troubleshooting guide should help resolve most issues encountered when using SparkForge. For additional support, refer to the specific error messages and contact the development team with detailed information about your issue.

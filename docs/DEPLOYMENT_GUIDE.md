# SparkForge Deployment Guide

This guide explains how to deploy SparkForge pipelines in production environments, including Spark/Delta Lake setup, service configuration, and troubleshooting.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Spark and Delta Lake Setup](#spark-and-delta-lake-setup)
3. [Service Configuration](#service-configuration)
4. [Pipeline Deployment](#pipeline-deployment)
5. [Production Considerations](#production-considerations)
6. [Troubleshooting](#troubleshooting)
7. [Documentation Deployment](#documentation-deployment)

## Prerequisites

### System Requirements

- **Java 17**: Required for Spark 3.5
- **Python 3.8+**: Python runtime
- **Apache Spark 3.5**: Core processing engine
- **Delta Lake 3.0.0**: Table storage format
- **PySpark 3.5**: Python API for Spark

### Installation

```bash
# Install Java 17
# macOS
brew install openjdk@17

# Linux (Ubuntu/Debian)
sudo apt-get install openjdk-17-jdk

# Verify Java installation
java -version  # Should show version 17

# Install Python dependencies
pip install pyspark==3.5.0 delta-spark==3.0.0
```

## Spark and Delta Lake Setup

### Local Development Setup

```python
from pyspark.sql import SparkSession
from pipeline_builder.engine_config import configure_engine

# Create Spark session with Delta Lake
spark = SparkSession.builder \
    .appName("SparkForgePipeline") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Configure engine (required!)
configure_engine(spark=spark)
```

### Production Spark Configuration

```python
from pyspark.sql import SparkSession
from pipeline_builder.engine_config import configure_engine

# Production Spark session
spark = SparkSession.builder \
    .appName("ProductionPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "200") \
    .getOrCreate()

configure_engine(spark=spark)
```

### Databricks Setup

```python
# Databricks automatically configures Delta Lake
from pyspark.sql import SparkSession
from pipeline_builder.engine_config import configure_engine

spark = SparkSession.builder.getOrCreate()
configure_engine(spark=spark)
```

### Delta Lake Storage Configuration

```python
# Configure Delta Lake storage location
spark.conf.set("spark.sql.warehouse.dir", "s3://your-bucket/warehouse/")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "your-access-key")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "your-secret-key")
```

## Service Configuration

### Execution Engine Configuration

The ExecutionEngine automatically initializes all services:

```python
from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models import PipelineConfig

# Create pipeline configuration
config = PipelineConfig.create_default(schema="production")

# ExecutionEngine initializes services internally:
# - ExecutionValidator: Data quality validation
# - TableService: Table operations and schema management
# - WriteService: Write operations to Delta Lake
# - TransformService: Transformation logic
# - ExecutionReporter: Execution reporting
# - ErrorHandler: Centralized error handling

engine = ExecutionEngine(spark=spark, config=config)

# Services are available as attributes:
# engine.validator
# engine.table_service
# engine.write_service
# engine.transform_service
# engine.reporter
# engine.error_handler
```

### Custom Service Configuration

```python
from pipeline_builder.models import PipelineConfig, ValidationThresholds

# Custom validation thresholds
thresholds = ValidationThresholds(
    bronze=90.0,  # 90% validation rate required
    silver=95.0,  # 95% validation rate required
    gold=98.0     # 98% validation rate required
)

config = PipelineConfig(
    schema="production",
    thresholds=thresholds,
    verbose=True  # Enable verbose logging
)

engine = ExecutionEngine(spark=spark, config=config)
```

### LogWriter Configuration

```python
from pipeline_builder.writer import LogWriter

# Simple configuration (recommended)
log_writer = LogWriter(
    spark=spark,
    schema="monitoring",
    table_name="pipeline_logs"
)

# Log execution results
result = pipeline.run_initial_load(bronze_sources={"events": source_df})
log_writer.append(result, run_id="run_123")
```

## Pipeline Deployment

### Basic Pipeline Deployment

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions

# 1. Configure engine
configure_engine(spark=spark)
F = get_default_functions()

# 2. Build pipeline
builder = PipelineBuilder(
    spark=spark,
    schema="production",
    min_bronze_rate=95.0,
    min_silver_rate=98.0,
    min_gold_rate=99.0
)

# 3. Define pipeline steps
builder.with_bronze_rules(name="events", rules={...})
builder.add_silver_transform(name="clean_events", ...)
builder.add_gold_transform(name="metrics", ...)

# 4. Create and execute pipeline
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": source_df})
```

### Incremental Pipeline Deployment

```python
# Initial load
result = pipeline.run_initial_load(bronze_sources={"events": historical_data})

# Incremental updates
result = pipeline.run_incremental(bronze_sources={"events": new_data})
```

### Scheduled Execution

```python
# Example: Airflow DAG
from airflow import DAG
from airflow.operators.python import PythonOperator

def run_pipeline():
    from pipeline_builder import PipelineBuilder
    from pipeline_builder.engine_config import configure_engine
    
    spark = SparkSession.builder.getOrCreate()
    configure_engine(spark=spark)
    
    builder = PipelineBuilder(spark=spark, schema="production")
    # ... build pipeline ...
    
    pipeline = builder.to_pipeline()
    result = pipeline.run_incremental(bronze_sources={"events": get_new_data()})
    
    return result.status.value == "completed"

dag = DAG('sparkforge_pipeline', schedule_interval='@daily')
task = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_pipeline,
    dag=dag
)
```

## Production Considerations

### Error Handling

```python
from pipeline_builder.models.exceptions import (
    PipelineConfigurationError,
    PipelineExecutionError
)

try:
    # Validate pipeline
    errors = builder.validate_pipeline()
    if errors:
        raise PipelineConfigurationError(f"Validation errors: {errors}")
    
    # Execute pipeline
    result = pipeline.run_initial_load(bronze_sources={"events": df})
    
    if result.status.value != "completed":
        raise PipelineExecutionError(f"Pipeline failed: {result.errors}")
        
except PipelineConfigurationError as e:
    # Log and notify
    logger.error(f"Configuration error: {e}")
    notify_team("Pipeline configuration error")
    
except PipelineExecutionError as e:
    # Log and handle gracefully
    logger.error(f"Execution error: {e}")
    handle_pipeline_failure(result)
```

### Monitoring and Logging

```python
from pipeline_builder.writer import LogWriter

# Set up logging
log_writer = LogWriter(
    spark=spark,
    schema="monitoring",
    table_name="pipeline_logs"
)

# Execute and log
result = pipeline.run_initial_load(bronze_sources={"events": source_df})
log_writer.append(result, run_id="run_123")

# Monitor performance
if result.duration_seconds > 300:  # 5 minutes
    alert("Pipeline execution exceeded threshold")

# Check validation rates
if result.metrics.total_rows_written < expected_rows:
    alert("Fewer rows written than expected")
```

### Resource Management

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

## Troubleshooting

### Common Issues

#### Engine Not Configured

**Error**: `Engine not configured`

**Solution**:
```python
from pipeline_builder.engine_config import configure_engine
configure_engine(spark=spark)  # Must be called before using pipeline components
```

#### Delta Lake Not Available

**Error**: `Delta Lake extensions not configured`

**Solution**:
```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

#### Validation Failures

**Error**: `Validation rate below threshold`

**Solution**:
```python
# Check validation errors
errors = builder.validate_pipeline()
if errors:
    print(f"Validation errors: {errors}")

# Adjust thresholds if needed
builder = PipelineBuilder(
    spark=spark,
    schema="production",
    min_bronze_rate=90.0,  # Lower threshold
    min_silver_rate=95.0,
    min_gold_rate=97.0
)
```

#### Memory Issues

**Error**: `OutOfMemoryError`

**Solution**:
```python
# Increase Spark memory
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.driver.memory", "8g")

# Enable spill to disk
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

#### Service Initialization Errors

**Error**: `Service initialization failed`

**Solution**:
```python
# Check Spark session is valid
if spark is None:
    raise ValueError("Spark session is None")

# Verify Delta Lake is configured
from pipeline_builder.table_operations import is_delta_lake_available
if not is_delta_lake_available(spark):
    raise ValueError("Delta Lake is not available")

# Configure engine before creating services
configure_engine(spark=spark)
```

### Debugging Tips

1. **Enable verbose logging**:
```python
builder = PipelineBuilder(spark=spark, schema="production", verbose=True)
```

2. **Check pipeline validation**:
```python
errors = builder.validate_pipeline()
if errors:
    for error in errors:
        print(f"Error: {error}")
```

3. **Inspect execution results**:
```python
result = pipeline.run_initial_load(bronze_sources={"events": df})
print(f"Status: {result.status.value}")
print(f"Errors: {result.errors}")
print(f"Metrics: {result.metrics}")
```

4. **Test individual steps**:
```python
# Test bronze step
bronze_result = engine.execute_step(
    step=bronze_step,
    sources={"events": source_df},
    mode=ExecutionMode.INITIAL
)
print(f"Bronze result: {bronze_result}")
```

## Documentation Deployment

### Prerequisites

1. **GitHub Repository**: Your SparkForge code must be in a GitHub repository
2. **Read the Docs Account**: Sign up at [readthedocs.org](https://readthedocs.org)
3. **Documentation Structure**: The docs are already set up in the `docs/` directory

### Deployment Steps

1. **Connect Repository to Read the Docs**:
   - Go to [readthedocs.org](https://readthedocs.org) and sign in
   - Click "Import a Project"
   - Connect your GitHub account
   - Select your SparkForge repository

2. **Configure Build Settings**:
   Read the Docs will automatically detect the configuration from `.readthedocs.yml`

3. **Trigger First Build**:
   - Click "Build version" on your project page
   - Monitor the build logs for any errors
   - Documentation will be available at: `https://your-project.readthedocs.io/`

### Local Testing

Test the documentation locally before deploying:

```bash
cd docs
pip install -r requirements.txt
sphinx-build -b html . _build/html
```

### Configuration Files

- **`.readthedocs.yml`**: Main configuration file for Read the Docs
- **`docs/conf.py`**: Sphinx configuration
- **`docs/requirements.txt`**: Documentation dependencies

---

**🎉 Your SparkForge pipeline is ready for production deployment!**

For more information, see:
- [User Guide](USER_GUIDE.md) for usage examples
- [Architecture](Architecture.md) for design patterns
- [API Reference](api_reference.rst) for detailed API documentation

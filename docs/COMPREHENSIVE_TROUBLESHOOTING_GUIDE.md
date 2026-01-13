# SparkForge Comprehensive Troubleshooting Guide

This guide provides detailed solutions for common issues encountered when using SparkForge in production environments, including service-specific troubleshooting.

## Table of Contents

1. [Installation Issues](#installation-issues)
2. [Engine Configuration Issues](#engine-configuration-issues)
3. [Service Initialization Issues](#service-initialization-issues)
4. [Configuration Problems](#configuration-problems)
5. [Pipeline Execution Errors](#pipeline-execution-errors)
6. [Data Validation Issues](#data-validation-issues)
7. [Service-Specific Issues](#service-specific-issues)
8. [Performance Problems](#performance-problems)
9. [Memory and Resource Issues](#memory-and-resource-issues)
10. [Delta Lake Issues](#delta-lake-issues)
11. [Advanced Troubleshooting](#advanced-troubleshooting)

## Installation Issues

### Python Version Compatibility

**Problem**: SparkForge requires Python 3.8 or higher.

**Symptoms**:
- `ImportError` when importing pipeline_builder
- `SyntaxError` in Python 3.7 or earlier

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
conda create -n pipeline_builder python=3.8
conda activate pipeline_builder
```

### Java Dependencies

**Problem**: PySpark requires Java 17 for Spark 3.5.

**Symptoms**:
- `Java gateway process exited before sending its port number`
- `JAVA_HOME is not set`

**Solutions**:

1. **Install Java 17**:
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install openjdk-17-jdk

# macOS
brew install openjdk@17

# Windows
# Download from Oracle or use Chocolatey
choco install openjdk17
```

2. **Set JAVA_HOME**:
```bash
# Linux/macOS
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Windows
set JAVA_HOME=C:\Program Files\Java\jdk-17
set PATH=%JAVA_HOME%\bin;%PATH%
```

3. **Verify Java Installation**:
```bash
java -version
# Should show Java 17
```

### Package Dependencies

**Problem**: Missing or incompatible dependencies.

**Symptoms**:
- `ImportError` for specific modules
- Version conflicts

**Solutions**:

1. **Install Required Packages**:
```bash
pip install pyspark==3.5.0 delta-spark==3.0.0
```

2. **Check Dependencies**:
```bash
pip list | grep -E "(pyspark|delta-spark)"
```

## Engine Configuration Issues

### Engine Not Configured

**Problem**: Engine not configured before using pipeline components.

**Symptoms**:
- `RuntimeError: Engine not configured`
- `AttributeError: 'NoneType' object has no attribute 'functions'`

**Solutions**:

1. **Configure Engine First**:
```python
from pipeline_builder.engine_config import configure_engine
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Configure engine (required!)
configure_engine(spark=spark)

# Now you can use pipeline components
from pipeline_builder import PipelineBuilder
builder = PipelineBuilder(spark=spark, schema="analytics")
```

2. **Check Configuration**:
```python
from pipeline_builder.compat import is_mock_spark

if is_mock_spark():
    print("Using mock Spark")
else:
    print("Using real PySpark")
```

### Functions Not Available

**Problem**: Functions not available after engine configuration.

**Symptoms**:
- `AttributeError: 'NoneType' object has no attribute 'col'`
- Functions return None

**Solutions**:

1. **Use get_default_functions()**:
```python
from pipeline_builder.functions import get_default_functions

# Get functions after engine configuration
F = get_default_functions()

# Use functions
df = df.withColumn("new_col", F.col("old_col"))
```

2. **Check Engine Configuration**:
```python
from pipeline_builder.engine_config import get_engine

engine = get_engine()
if engine is None:
    raise RuntimeError("Engine not configured")
```

## Service Initialization Issues

### ExecutionEngine Service Initialization

**Problem**: Services fail to initialize.

**Symptoms**:
- `AttributeError: 'ExecutionEngine' object has no attribute 'validator'`
- Services are None

**Solutions**:

1. **Check Spark Session**:
```python
from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models import PipelineConfig

# Ensure Spark session is valid
if spark is None:
    raise ValueError("Spark session is None")

# Create config
config = PipelineConfig.create_default(schema="production")

# Create engine (services initialized automatically)
engine = ExecutionEngine(spark=spark, config=config)

# Verify services
assert engine.validator is not None
assert engine.table_service is not None
assert engine.write_service is not None
```

2. **Check Delta Lake Availability**:
```python
from pipeline_builder.table_operations import is_delta_lake_available

if not is_delta_lake_available(spark):
    raise ValueError("Delta Lake is not available. Configure Spark with Delta extensions.")
```

### TableService Issues

**Problem**: TableService fails to access tables.

**Symptoms**:
- `TableOperationError: Table does not exist`
- Schema operations fail

**Solutions**:

1. **Ensure Schema Exists**:
```python
from pipeline_builder.execution import ExecutionEngine

engine = ExecutionEngine(spark=spark, config=config)

# Ensure schema exists
engine.table_service.ensure_schema_exists()

# Check if table exists
if not engine.table_service.table_exists("my_table"):
    print("Table does not exist")
```

2. **Check Permissions**:
```python
# Verify Spark has permissions to create schemas
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    spark.sql("DROP SCHEMA IF EXISTS test_schema")
except Exception as e:
    print(f"Permission error: {e}")
```

## Configuration Problems

### Invalid Pipeline Configuration

**Problem**: PipelineConfig validation fails.

**Symptoms**:
- `PipelineConfigurationError: Invalid configuration`
- `ValidationError: Schema name cannot be empty`

**Solutions**:

1. **Check Schema Name**:
```python
from pipeline_builder.models import PipelineConfig

# ❌ Invalid
config = PipelineConfig(schema="", thresholds=thresholds)

# ✅ Valid
config = PipelineConfig(schema="analytics", thresholds=thresholds)
```

2. **Validate Quality Thresholds**:
```python
from pipeline_builder.models import ValidationThresholds

# ❌ Invalid - thresholds must be between 0 and 100
thresholds = ValidationThresholds(bronze=150.0, silver=85.0, gold=90.0)

# ✅ Valid
thresholds = ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0)
thresholds.validate()  # Validates thresholds
```

### Pipeline Builder Configuration

**Problem**: PipelineBuilder configuration errors.

**Symptoms**:
- `ValueError: min_bronze_rate must be between 0 and 100`
- Invalid validation thresholds

**Solutions**:

1. **Use Valid Thresholds**:
```python
from pipeline_builder import PipelineBuilder

# ✅ Valid configuration
builder = PipelineBuilder(
    spark=spark,
    schema="analytics",
    min_bronze_rate=90.0,  # Between 0 and 100
    min_silver_rate=95.0,
    min_gold_rate=98.0,
    verbose=True
)
```

2. **Validate Pipeline**:
```python
# Validate pipeline before execution
errors = builder.validate_pipeline()
if errors:
    print(f"Validation errors: {errors}")
    for error in errors:
        print(f"  - {error}")
```

## Pipeline Execution Errors

### Step Validation Failures

**Problem**: Pipeline steps fail validation.

**Symptoms**:
- `PipelineConfigurationError: Rules must be a non-empty dictionary`
- `PipelineConfigurationError: Transform function must be callable`

**Solutions**:

1. **Fix Validation Rules**:
```python
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

# ❌ Invalid - empty rules
builder.with_bronze_rules(name="events", rules={})

# ✅ Valid - non-empty rules
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    }
)
```

2. **Check Transform Functions**:
```python
# ❌ Invalid - not callable
builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform="not_a_function",  # String instead of function
    rules={...},
    table_name="clean_events"
)

# ✅ Valid - callable function
def clean_events(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return bronze_df.filter(F.col("status") == "active")

builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_events,
    rules={...},
    table_name="clean_events"
)
```

### Step Dependencies

**Problem**: Steps have incorrect dependencies.

**Symptoms**:
- `PipelineConfigurationError: Circular dependency detected`
- `PipelineConfigurationError: Missing source step`

**Solutions**:

1. **Check Step Dependencies**:
```python
# ❌ Invalid - circular dependency
builder.add_silver_transform(
    name="step1",
    source_bronze="events",
    transform=transform1,
    rules={...},
    table_name="step1",
    source_silvers=["step2"]  # Circular!
)
builder.add_silver_transform(
    name="step2",
    source_bronze="events",
    transform=transform2,
    rules={...},
    table_name="step2",
    source_silvers=["step1"]  # Circular!
)

# ✅ Valid - no circular dependency
builder.with_bronze_rules(name="events", rules={...})
builder.add_silver_transform(
    name="step1",
    source_bronze="events",
    transform=transform1,
    rules={...},
    table_name="step1"
)
builder.add_silver_transform(
    name="step2",
    source_bronze="events",
    transform=transform2,
    rules={...},
    table_name="step2",
    source_silvers=["step1"]  # Valid dependency
)
```

2. **Verify Source Steps**:
```python
# ❌ Invalid - source step doesn't exist
builder.add_silver_transform(
    name="silver_step",
    source_bronze="nonexistent_step",  # Doesn't exist!
    transform=transform_func,
    rules={...},
    table_name="silver_step"
)

# ✅ Valid - source step exists
builder.with_bronze_rules(name="events", rules={...})
builder.add_silver_transform(
    name="silver_step",
    source_bronze="events",  # Exists!
    transform=transform_func,
    rules={...},
    table_name="silver_step"
)
```

### Execution Errors

**Problem**: Pipeline execution fails.

**Symptoms**:
- `PipelineExecutionError: Step execution failed`
- `RuntimeError: Transform function raised exception`

**Solutions**:

1. **Check Transform Functions**:
```python
# Add error handling in transform functions
def safe_transform(spark, bronze_df, prior_silvers):
    try:
        F = get_default_functions()
        return bronze_df.filter(F.col("status") == "active")
    except Exception as e:
        print(f"Transform error: {e}")
        # Return empty DataFrame or handle error
        return spark.createDataFrame([], bronze_df.schema)
```

2. **Check Execution Results**:
```python
result = pipeline.run_initial_load(bronze_sources={"events": df})

if result.status.value != "completed":
    print(f"Pipeline failed: {result.errors}")
    if hasattr(result, 'step_results'):
        for step_result in result.step_results:
            if not step_result.success:
                print(f"Step {step_result.step_name} failed: {step_result.error_message}")
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
from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models import PipelineConfig

config = PipelineConfig.create_default(schema="production")
engine = ExecutionEngine(spark=spark, config=config)

# Check validation results
validation_result = engine.validator.validate_step_output(
    df=output_df,
    step=bronze_step,
    stage="bronze"
)

print(f"Validation rate: {validation_result.validation_rate:.2f}%")
print(f"Valid rows: {validation_result.valid_rows}")
print(f"Invalid rows: {validation_result.invalid_rows}")

if not validation_result.validation_passed:
    print(f"Validation errors: {validation_result.errors}")
```

2. **Adjust Quality Thresholds**:
```python
from pipeline_builder.models import ValidationThresholds

# Lower thresholds for development
thresholds = ValidationThresholds(
    bronze=70.0,  # Lower threshold
    silver=75.0,
    gold=80.0
)

config = PipelineConfig(schema="development", thresholds=thresholds)
```

### Schema Validation Failures

**Problem**: DataFrame schema doesn't match expected schema.

**Symptoms**:
- `SchemaError: Schema validation failed`
- Missing or extra columns

**Solutions**:

1. **Check DataFrame Schema**:
```python
# Print current schema
df.printSchema()

# Check column names
print("Current columns:", df.columns)
```

2. **Use SchemaManager**:
```python
from pipeline_builder.execution import ExecutionEngine

engine = ExecutionEngine(spark=spark, config=config)

# Get table schema
schema = engine.schema_manager.get_table_schema("my_table")

# Validate schema match
is_valid = engine.schema_manager.validate_schema_match(
    df_schema=df.schema,
    table_schema=schema
)

if not is_valid:
    print("Schema mismatch detected")
```

## Service-Specific Issues

### ExecutionValidator Issues

**Problem**: Validation service fails.

**Symptoms**:
- `ValidationError: Validation service error`
- Validation results are incorrect

**Solutions**:

1. **Check Validation Rules**:
```python
from pipeline_builder.execution import ExecutionEngine

engine = ExecutionEngine(spark=spark, config=config)

# Ensure validation rules are valid
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "amount": [F.col("amount") > 0]
}

# Validate step output
result = engine.validator.validate_step_output(
    df=df,
    step=bronze_step,
    stage="bronze"
)
```

### TableService Issues

**Problem**: Table operations fail.

**Symptoms**:
- `TableOperationError: Table does not exist`
- Schema operations fail

**Solutions**:

1. **Ensure Schema Exists**:
```python
engine.table_service.ensure_schema_exists()

# Check table existence
if not engine.table_service.table_exists("my_table"):
    print("Table does not exist - will be created on first write")
```

2. **Check Table Schema**:
```python
# Get table schema
schema = engine.table_service.get_table_schema("my_table")
print(f"Table schema: {schema}")
```

### WriteService Issues

**Problem**: Write operations fail.

**Symptoms**:
- `WriteError: Failed to write DataFrame`
- Delta Lake write errors

**Solutions**:

1. **Check Delta Lake Configuration**:
```python
from pipeline_builder.table_operations import is_delta_lake_available

if not is_delta_lake_available(spark):
    raise ValueError("Delta Lake is not available")
```

2. **Check Write Permissions**:
```python
# Test write permissions
try:
    test_df = spark.createDataFrame([(1, "test")], ["id", "value"])
    test_df.write.format("delta").mode("overwrite").saveAsTable("test_schema.test_table")
    spark.sql("DROP TABLE IF EXISTS test_schema.test_table")
except Exception as e:
    print(f"Write permission error: {e}")
```

## Performance Problems

### Slow Pipeline Execution

**Problem**: Pipeline execution is slower than expected.

**Symptoms**:
- Long execution times
- High resource usage

**Solutions**:

1. **Optimize Transform Functions**:
```python
# ❌ Inefficient - UDF for simple operations
from pyspark.sql.functions import udf

def get_category(amount):
    if amount > 1000:
        return "premium"
    elif amount > 500:
        return "standard"
    else:
        return "basic"

category_udf = udf(get_category, StringType())

# ✅ Efficient - use Spark SQL functions
def efficient_transform(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return bronze_df.withColumn("category",
                        F.when(F.col("amount") > 1000, "premium")
                         .when(F.col("amount") > 500, "standard")
                         .otherwise("basic"))
```

2. **Check Execution Metrics**:
```python
result = pipeline.run_initial_load(bronze_sources={"events": df})

print(f"Total duration: {result.duration_seconds:.2f}s")
print(f"Bronze duration: {result.metrics.bronze_duration:.2f}s")
print(f"Silver duration: {result.metrics.silver_duration:.2f}s")
print(f"Gold duration: {result.metrics.gold_duration:.2f}s")

# Identify slow steps
if result.metrics.silver_duration > 300:
    print("Silver step is slow - consider optimization")
```

## Memory and Resource Issues

### Out of Memory Errors

**Problem**: Pipeline runs out of memory.

**Symptoms**:
- `OutOfMemoryError`
- Spark executor failures

**Solutions**:

1. **Increase Spark Memory**:
```python
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.driver.maxResultSize", "2g")
```

2. **Optimize Data Processing**:
```python
# Filter data early
def optimized_transform(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return bronze_df \
        .filter(F.col("status") == "active") \
        .select("user_id", "timestamp", "amount")  # Select only needed columns
```

## Delta Lake Issues

### Delta Lake Not Available

**Problem**: Delta Lake extensions not configured.

**Symptoms**:
- `DeltaLakeError: Delta Lake extensions not configured`
- Delta operations fail

**Solutions**:

1. **Configure Delta Lake**:
```python
spark = SparkSession.builder \
    .appName("Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

2. **Verify Delta Lake**:
```python
from pipeline_builder.table_operations import is_delta_lake_available

if not is_delta_lake_available(spark):
    raise ValueError("Delta Lake is not available. Install delta-spark package.")
```

## Advanced Troubleshooting

### Debug Mode

Enable debug logging:

```python
import logging

# Enable debug logging
logging.getLogger("pipeline_builder").setLevel(logging.DEBUG)

# Build pipeline with verbose mode
builder = PipelineBuilder(spark=spark, schema="debug", verbose=True)
```

### Step-by-Step Debugging

Debug individual steps:

```python
from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models.enums import ExecutionMode

engine = ExecutionEngine(spark=spark, config=config)

# Execute single step
result = engine.execute_step(
    step=bronze_step,
    sources={"events": source_df},
    mode=ExecutionMode.INITIAL
)

print(f"Step result: {result}")
```

### Common Error Patterns

1. **Engine not configured**: Always call `configure_engine(spark=spark)` first
2. **Functions not available**: Use `get_default_functions()` after engine configuration
3. **Service initialization fails**: Check Spark session and Delta Lake configuration
4. **Validation failures**: Check validation rules and thresholds
5. **Schema mismatches**: Use SchemaManager to validate schemas

---

For more help, see:
- [User Guide](USER_GUIDE.md) for usage examples
- [Deployment Guide](DEPLOYMENT_GUIDE.md) for deployment issues
- [Performance Tuning Guide](PERFORMANCE_TUNING_GUIDE.md) for performance optimization

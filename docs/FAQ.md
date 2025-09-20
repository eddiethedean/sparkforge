# SparkForge Frequently Asked Questions (FAQ)

This document answers common questions about SparkForge, its usage, and troubleshooting.

## Table of Contents

1. [General Questions](#general-questions)
2. [Installation and Setup](#installation-and-setup)
3. [Configuration](#configuration)
4. [Pipeline Development](#pipeline-development)
5. [Data Validation](#data-validation)
6. [Performance](#performance)
7. [Deployment](#deployment)
8. [Troubleshooting](#troubleshooting)
9. [Best Practices](#best-practices)
10. [Contributing](#contributing)

## General Questions

### What is SparkForge?

SparkForge is a Python framework for building data pipelines using the Medallion Architecture (Bronze, Silver, Gold tiers) with Apache Spark. It provides a simplified interface for creating, validating, and executing data processing pipelines with built-in quality assurance and performance monitoring.

### What are the main benefits of using SparkForge?

- **Simplified Pipeline Development**: Easy-to-use API for building complex data pipelines
- **Built-in Data Quality**: Automatic validation and quality assurance
- **Performance Monitoring**: Comprehensive performance tracking and optimization
- **Medallion Architecture**: Structured approach to data processing with Bronze, Silver, and Gold tiers
- **Production Ready**: Robust error handling, logging, and monitoring capabilities

### What is the Medallion Architecture?

The Medallion Architecture is a data processing pattern that organizes data into three tiers:

- **Bronze Tier**: Raw data ingestion and initial validation
- **Silver Tier**: Cleaned and enriched data with business logic applied
- **Gold Tier**: Aggregated and summarized data for analytics and reporting

### Is SparkForge free to use?

Yes, SparkForge is open-source and free to use. It's released under the MIT License.

### What Python versions are supported?

SparkForge requires Python 3.8 or higher. It has been tested with Python 3.8, 3.9, 3.10, and 3.11.

## Installation and Setup

### How do I install SparkForge?

```bash
pip install sparkforge
```

### What are the system requirements?

- **Python**: 3.8 or higher
- **Java**: 11 or higher (required for PySpark)
- **Memory**: Minimum 8GB RAM (16GB+ recommended for production)
- **Storage**: Minimum 50GB free space
- **Network**: Access to data sources and external services

### Do I need to install Apache Spark separately?

No, SparkForge includes PySpark as a dependency and will install it automatically. However, you need Java 11 or higher installed on your system.

### How do I verify my installation?

```python
import sparkforge
print(sparkforge.__version__)

# Test basic functionality
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.models import PipelineConfig
print("SparkForge installation successful!")
```

### Can I use SparkForge with existing Spark clusters?

Yes, SparkForge can connect to existing Spark clusters. You just need to configure the Spark session appropriately:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://your-cluster:7077") \
    .appName("SparkForge") \
    .getOrCreate()
```

## Configuration

### How do I configure SparkForge for production?

```python
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig

config = PipelineConfig(
    schema="production_analytics",
    quality_thresholds=ValidationThresholds(
        bronze=85.0,  # Higher thresholds for production
        silver=90.0,
        gold=95.0
    ),
    parallel=ParallelConfig(
        enabled=True,
        max_workers=8  # Adjust based on your cluster
    ),
    performance_monitoring=True,
    debug_mode=False
)
```

### What environment variables should I set?

```bash
export SPARKFORGE_ENV=production
export SPARKFORGE_LOG_LEVEL=INFO
export SPARKFORGE_DATA_PATH=/data/sparkforge
export SPARKFORGE_CONFIG_PATH=/config/sparkforge
export SPARKFORGE_LOG_PATH=/logs/sparkforge
```

### How do I configure Spark memory settings?

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()
```

### Can I use different configurations for different environments?

Yes, you can create environment-specific configurations:

```python
import os

def get_config():
    env = os.getenv('SPARKFORGE_ENV', 'development')
    
    if env == 'production':
        return PipelineConfig(
            schema="prod_analytics",
            quality_thresholds=ValidationThresholds(85.0, 90.0, 95.0),
            parallel=ParallelConfig(enabled=True, max_workers=8)
        )
    else:
        return PipelineConfig(
            schema="dev_analytics",
            quality_thresholds=ValidationThresholds(70.0, 75.0, 80.0),
            parallel=ParallelConfig(enabled=True, max_workers=2)
        )
```

## Pipeline Development

### How do I create a simple pipeline?

```python
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig
from pyspark.sql import functions as F

# Configuration
config = PipelineConfig(
    schema="analytics",
    quality_thresholds=ValidationThresholds(80.0, 85.0, 90.0),
    parallel=ParallelConfig(enabled=True, max_workers=4)
)

# Create builder
builder = PipelineBuilder(config)

# Add bronze step
def bronze_transform(df):
    return df.withColumn("processed_at", F.current_timestamp())

bronze_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()]
}

builder.add_bronze_step("user_events", bronze_transform, bronze_rules)

# Add silver step
def silver_transform(df):
    return df.withColumn("user_segment", 
                        F.when(F.col("amount") > 1000, "premium")
                         .otherwise("standard"))

silver_rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "user_segment": [F.col("user_segment").isin(["premium", "standard"])]
}

builder.add_silver_step("user_profiles", silver_transform, silver_rules, source_bronze="user_events")

# Add gold step
def gold_transform(df):
    return df.groupBy("user_segment").agg(
        F.count("*").alias("user_count"),
        F.avg("amount").alias("avg_amount")
    )

gold_rules = {
    "user_segment": [F.col("user_segment").isNotNull()],
    "user_count": [F.col("user_count") > 0],
    "avg_amount": [F.col("avg_amount") >= 0]
}

builder.add_gold_step("user_analytics", "analytics_table", gold_transform, gold_rules, source_silvers=["user_profiles"])
```

### How do I handle incremental data processing?

```python
# Bronze step with incremental processing
bronze_step = BronzeStep(
    name="user_events",
    transform=bronze_transform,
    rules=validation_rules,
    incremental_col="timestamp"  # Column for incremental processing
)
```

### Can I add custom validation logic?

Yes, you can add custom validation functions:

```python
from sparkforge.validation import UnifiedValidator

def custom_validator(data):
    # Custom validation logic
    if data.get("user_id") and len(str(data["user_id"])) < 5:
        return False, "User ID too short"
    return True, None

validator = UnifiedValidator()
validator.add_validator("user_id_length", custom_validator)
```

### How do I handle data dependencies between steps?

```python
# Silver step with dependencies
silver_step = SilverStep(
    name="combined_analytics",
    transform=transform_func,
    rules=validation_rules,
    source_bronze="user_events",
    depends_on_silvers=["user_profiles", "product_analytics"],
    can_run_parallel=False,  # Must run after dependencies
    execution_group="analytics"
)
```

## Data Validation

### How do I set up validation rules?

```python
from pyspark.sql import functions as F

validation_rules = {
    "user_id": [
        F.col("user_id").isNotNull(),
        F.col("user_id").rlike(r"^[0-9]+$")
    ],
    "timestamp": [
        F.col("timestamp").isNotNull(),
        F.col("timestamp") > F.lit("2020-01-01")
    ],
    "amount": [
        F.col("amount").isNotNull(),
        F.col("amount") > 0,
        F.col("amount") < 1000000
    ]
}
```

### What happens when validation fails?

When validation fails, SparkForge will:

1. Log the validation errors
2. Create separate DataFrames for valid and invalid data
3. Provide detailed statistics about the validation results
4. Optionally raise exceptions based on your configuration

### How do I adjust quality thresholds?

```python
# Lower thresholds for development
thresholds = ValidationThresholds(
    bronze=70.0,  # 70% quality threshold
    silver=75.0,  # 75% quality threshold
    gold=80.0     # 80% quality threshold
)

# Higher thresholds for production
thresholds = ValidationThresholds(
    bronze=85.0,  # 85% quality threshold
    silver=90.0,  # 90% quality threshold
    gold=95.0     # 95% quality threshold
)
```

### Can I validate data against external schemas?

Yes, you can validate against external schemas:

```python
from sparkforge.validation import validate_dataframe_schema

# Validate against expected schema
expected_columns = ["user_id", "timestamp", "amount", "category"]
is_valid = validate_dataframe_schema(df, expected_columns)

if not is_valid:
    print("Schema validation failed")
```

## Performance

### How do I optimize pipeline performance?

1. **Enable Parallel Processing**:
```python
parallel_config = ParallelConfig(
    enabled=True,
    max_workers=4  # Adjust based on available cores
)
```

2. **Optimize Transform Functions**:
```python
# Use Spark SQL functions instead of UDFs
def efficient_transform(df):
    return df.withColumn("category",
                        F.when(F.col("amount") > 1000, "premium")
                         .when(F.col("amount") > 500, "standard")
                         .otherwise("basic"))
```

3. **Optimize Data Partitioning**:
```python
# Partition data for better performance
df = df.repartition(F.col("date"), 20)
```

### How do I monitor pipeline performance?

```python
# Enable performance monitoring
config = PipelineConfig(
    schema="analytics",
    performance_monitoring=True
)

# Get performance summary
performance_summary = builder.get_performance_summary()
print(f"Execution time: {performance_summary.total_time}s")
print(f"Memory peak: {performance_summary.peak_memory_mb}MB")
print(f"Throughput: {performance_summary.records_per_second} records/s")
```

### What are the memory requirements?

- **Development**: Minimum 8GB RAM
- **Production**: 16GB+ RAM recommended
- **Large Datasets**: 32GB+ RAM for datasets > 10GB

### How do I handle large datasets?

1. **Increase Memory Allocation**:
```python
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.driver.memory", "8g")
```

2. **Optimize Partitioning**:
```python
df = df.repartition(200)  # More partitions for large datasets
```

3. **Use Caching Strategically**:
```python
df.cache()
df.count()  # Trigger caching
```

## Deployment

### How do I deploy SparkForge to production?

1. **Use Docker**:
```dockerfile
FROM openjdk:11-jre-slim
RUN pip install sparkforge
COPY . /app
WORKDIR /app
CMD ["python", "main.py"]
```

2. **Use Kubernetes**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sparkforge
spec:
  replicas: 3
  selector:
    matchLabels:
      app: sparkforge
  template:
    metadata:
      labels:
        app: sparkforge
    spec:
      containers:
      - name: sparkforge
        image: sparkforge:latest
        ports:
        - containerPort: 8080
```

### Can I use SparkForge with cloud providers?

Yes, SparkForge works with:

- **AWS EMR**: Deploy on Amazon EMR clusters
- **Azure Databricks**: Use with Azure Databricks
- **Google Cloud Dataproc**: Deploy on Google Cloud Dataproc
- **Kubernetes**: Deploy on any Kubernetes cluster

### How do I set up CI/CD for SparkForge?

Use GitHub Actions:

```yaml
name: Deploy SparkForge
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - name: Deploy to production
      run: |
        docker build -t sparkforge .
        docker push sparkforge:latest
```

## Troubleshooting

### My pipeline is running slowly. What should I do?

1. **Check Resource Usage**:
```python
# Monitor memory and CPU usage
performance_summary = builder.get_performance_summary()
print(f"Memory usage: {performance_summary.peak_memory_mb}MB")
```

2. **Enable Parallel Processing**:
```python
parallel_config = ParallelConfig(enabled=True, max_workers=4)
```

3. **Optimize Transform Functions**:
```python
# Use Spark SQL functions instead of UDFs
# Avoid collecting large datasets to driver
```

### I'm getting memory errors. How do I fix them?

1. **Increase Memory Allocation**:
```python
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.driver.memory", "8g")
```

2. **Optimize Data Processing**:
```python
# Filter data early
df = df.filter(F.col("date") >= F.lit("2023-01-01"))

# Use appropriate data types
df = df.withColumn("user_id", F.col("user_id").cast("integer"))
```

3. **Check Data Volume**:
```python
row_count = df.count()
if row_count > 1000000:  # 1M records
    df = df.repartition(100)
```

### My validation is failing. What should I check?

1. **Check Data Quality**:
```python
quality_result = assess_data_quality(df, validation_rules)
print(f"Quality score: {quality_result['quality_score']}%")
```

2. **Lower Quality Thresholds**:
```python
thresholds = ValidationThresholds(70.0, 75.0, 80.0)  # Lower thresholds
```

3. **Fix Data Issues**:
```python
# Clean data before validation
df = df.filter(F.col("user_id").isNotNull())
```

### How do I debug pipeline issues?

1. **Enable Debug Mode**:
```python
config = PipelineConfig(
    schema="analytics",
    debug_mode=True
)
```

2. **Check Logs**:
```python
import logging
logging.getLogger("sparkforge").setLevel(logging.DEBUG)
```

3. **Validate Pipeline**:
```python
validation_result = builder.validate_pipeline()
if not validation_result.is_valid:
    print("Validation errors:", validation_result.errors)
```

## Best Practices

### What are the best practices for pipeline development?

1. **Start Simple**: Begin with basic pipelines and add complexity gradually
2. **Use Validation**: Always include validation rules for data quality
3. **Monitor Performance**: Enable performance monitoring from the start
4. **Test Thoroughly**: Test with realistic data volumes
5. **Document Changes**: Keep track of pipeline modifications

### How should I structure my pipeline code?

```python
# 1. Configuration
config = get_pipeline_config()

# 2. Transform Functions
def bronze_transform(df):
    return df.withColumn("processed_at", F.current_timestamp())

def silver_transform(df):
    return df.withColumn("user_segment", get_segment_logic())

def gold_transform(df):
    return df.groupBy("user_segment").agg(get_aggregations())

# 3. Validation Rules
bronze_rules = get_bronze_validation_rules()
silver_rules = get_silver_validation_rules()
gold_rules = get_gold_validation_rules()

# 4. Pipeline Building
builder = PipelineBuilder(config)
builder.add_bronze_step("events", bronze_transform, bronze_rules)
builder.add_silver_step("profiles", silver_transform, silver_rules, source_bronze="events")
builder.add_gold_step("analytics", "analytics_table", gold_transform, gold_rules, source_silvers=["profiles"])

# 5. Execution
runner = SimplePipelineRunner()
result = runner.run_pipeline(builder)
```

### How do I handle errors gracefully?

```python
try:
    # Validate pipeline
    validation_result = builder.validate_pipeline()
    if not validation_result.is_valid:
        raise ValidationError(f"Pipeline validation failed: {validation_result.errors}")
    
    # Run pipeline
    execution_result = runner.run_pipeline(builder)
    
except ValidationError as e:
    logger.error(f"Validation error: {e}")
    # Handle validation errors
except PipelineError as e:
    logger.error(f"Pipeline error: {e}")
    # Handle pipeline errors
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    # Handle unexpected errors
```

### How do I optimize for production?

1. **Resource Configuration**:
```python
# Optimize Spark configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

2. **Quality Thresholds**:
```python
# Higher thresholds for production
thresholds = ValidationThresholds(85.0, 90.0, 95.0)
```

3. **Monitoring**:
```python
# Enable comprehensive monitoring
config = PipelineConfig(
    schema="production",
    performance_monitoring=True,
    debug_mode=False
)
```

## Contributing

### How can I contribute to SparkForge?

1. **Fork the Repository**: Fork the SparkForge repository on GitHub
2. **Create a Branch**: Create a feature branch for your changes
3. **Make Changes**: Implement your changes with tests
4. **Submit Pull Request**: Submit a pull request with a clear description

### What should I include in my pull request?

- Clear description of changes
- Tests for new functionality
- Documentation updates
- Examples if applicable

### How do I run the tests?

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test categories
python -m pytest tests/unit/ -v
python -m pytest tests/integration/ -v
python -m pytest tests/performance/ -v
```

### How do I build the documentation?

```bash
# Install documentation dependencies
pip install -r docs/requirements.txt

# Build documentation
cd docs
make html

# View documentation
open _build/html/index.html
```

---

For additional questions or support, please:

- Check the [troubleshooting guide](COMPREHENSIVE_TROUBLESHOOTING_GUIDE.md)
- Review the [API reference](ENHANCED_API_REFERENCE.md)
- Submit an issue on [GitHub](https://github.com/eddiethedean/sparkforge/issues)
- Join the [community discussions](https://github.com/eddiethedean/sparkforge/discussions)

# PipelineBuilder Quick Start Guide

Get up and running with PipelineBuilder in 5 minutes!

## Installation

```bash
# Install the package
pip install pipeline-builder

# Or install with extras
pip install pipeline-builder[pyspark]  # For PySpark support
pip install pipeline-builder[dev,test]  # For development
```

## Quick Start (5 Minutes)

### Step 1: Configure Engine

**Important**: You must configure the engine before using pipeline components.

```python
from pipeline_builder.engine_config import configure_engine
from pyspark.sql import SparkSession

# Create Spark session with Delta Lake
spark = SparkSession.builder \
    .appName("QuickStart") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configure engine (required!)
configure_engine(spark=spark)
```

### Step 2: Build Your First Pipeline

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

# Create sample data
data = [
    ("user1", "click", "2024-01-01 10:00:00"),
    ("user2", "purchase", "2024-01-01 11:00:00"),
    ("user3", "view", "2024-01-01 12:00:00"),
]
source_df = spark.createDataFrame(data, ["user_id", "action", "timestamp"])

# Build pipeline
builder = PipelineBuilder(spark=spark, schema="analytics")

# Bronze: Validate raw events
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
    },
    incremental_col="timestamp"
)

# Silver: Clean events
def clean_events(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("action").isin(["click", "view", "purchase"]))

builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_events,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
    },
    table_name="clean_events"
)

# Gold: Daily summary
def daily_summary(spark, silvers):
    return silvers["clean_events"].groupBy("action").count()

builder.add_gold_transform(
    name="daily_summary",
    transform=daily_summary,
    rules={"action": [F.col("action").isNotNull()], "count": [F.col("count") > 0]},
    table_name="daily_summary",
    source_silvers=["clean_events"]
)
```

### Step 3: Execute Pipeline

```python
# Build and execute
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

# Check results
print(f"✅ Pipeline completed: {result.status.value}")
print(f"📊 Rows written: {result.metrics.total_rows_written}")
```

## Engine Configuration

### Real PySpark (Production)

```python
from pipeline_builder.engine_config import configure_engine
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Production") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configure for real PySpark
configure_engine(spark=spark)
```

### Mock Spark (Testing)

```python
from pipeline_builder.engine_config import configure_engine
from mock_spark import MockSparkSession

# Create mock Spark for testing
mock_spark = MockSparkSession()

# Configure for mock Spark
configure_engine(spark=mock_spark)
```

## Common Patterns

### Pattern 1: Initial Load

```python
# First-time data load
result = pipeline.run_initial_load(
    bronze_sources={"events": historical_data}
)
```

### Pattern 2: Incremental Load

```python
# Daily/hourly updates
result = pipeline.run_incremental(
    bronze_sources={"events": new_data}
)
```

### Pattern 3: Full Refresh

```python
# Reprocess all data
result = pipeline.run_full_refresh(
    bronze_sources={"events": all_data}
)
```

### Pattern 4: Validation Only

```python
# Test validation without writing
result = pipeline.run_validation_only(
    bronze_sources={"events": test_data}
)
```

## String Rules (Simplified Syntax)

You can use human-readable string rules:

```python
# Instead of PySpark expressions
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": ["not_null"],           # F.col("user_id").isNotNull()
        "value": ["gt", 0],                # F.col("value") > 0
        "status": ["in", ["active", "inactive"]],  # F.col("status").isin([...])
    },
    incremental_col="timestamp"
)
```

## Logging Pipeline Executions

```python
from pipeline_builder.writer import LogWriter

# Create LogWriter (new simplified API)
log_writer = LogWriter(
    spark=spark,
    schema="monitoring",
    table_name="pipeline_logs"
)

# Execute pipeline
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

# Log execution
log_writer.append(result, run_id="run_123")
```

## Next Steps

- **Full Guide**: See [USER_GUIDE.md](USER_GUIDE.md) for comprehensive documentation
- **API Reference**: See [api_reference.rst](api_reference.rst) for detailed API docs
- **Examples**: See `examples/` directory for more examples
- **Architecture**: See [Architecture.md](Architecture.md) for architecture details

## Troubleshooting

### Engine Not Configured

```python
# Error: Engine not configured
# Solution: Always configure engine first
from pipeline_builder.engine_config import configure_engine
configure_engine(spark=spark)
```

### Delta Lake Not Available

```python
# Error: Delta Lake extensions not configured
# Solution: Add Delta Lake configs to Spark session
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

### Validation Errors

```python
# Check validation errors
errors = builder.validate_pipeline()
if errors:
    print(f"Validation errors: {errors}")
```

---

**Happy Pipeline Building! 🚀**

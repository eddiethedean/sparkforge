# Pipeline Builder

A production-ready data pipeline framework for Apache Spark & Delta Lake that transforms complex Spark development into clean, maintainable code using the proven Medallion Architecture (Bronze → Silver → Gold).

## Overview

Pipeline Builder provides a fluent API for constructing robust data pipelines with comprehensive validation, automatic dependency management, and enterprise-grade features. It eliminates 70% of boilerplate code while providing built-in data quality enforcement, parallel execution, and seamless incremental processing.

## Key Features

- **70% Less Boilerplate**: Transform 200+ lines of complex Spark code into 20-30 lines of clean, readable code
- **Automatic Dependency Management**: Pipeline Builder automatically detects and validates dependencies between steps
- **Built-in Validation**: Progressive quality gates at each layer (Bronze: 90%, Silver: 95%, Gold: 98%)
- **Parallel Execution**: Automatic parallelization of independent steps (3-5x faster execution)
- **Incremental Processing**: Seamless support for incremental updates with timestamp-based filtering
- **String Rules Support**: Human-readable validation rules (`"not_null"`, `"gt", 0`, `"in", [...]`)
- **Multi-Schema Support**: Enterprise-ready cross-schema data flows
- **Comprehensive Error Handling**: Detailed error messages with actionable suggestions
- **Delta Lake Integration**: Full support for ACID transactions, time travel, and schema evolution
- **Flexible Engine Support**: Works with PySpark or mock-spark for testing

## Installation

### From Source

```bash
# From the repository root directory

# Install with PySpark support
pip install -e ".[pyspark]"

# Install with mock-spark for testing
pip install -e ".[mock]"

# Install in development mode with all dependencies
pip install -e ".[pyspark,dev,test]"
```

**Prerequisites:**
- Python 3.8 or higher
- Java 11 (for PySpark 3.5)
- Git

**Note:** The `-e` flag installs the package in editable mode, so changes to the source code are immediately reflected.

## Quick Start

### Basic Pipeline

```python
from pipeline_builder import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

# Initialize Spark
spark = SparkSession.builder.appName("MyPipeline").getOrCreate()

# Create sample data
data = [("user1", "click", 100), ("user2", "purchase", 200)]
df = spark.createDataFrame(data, ["user_id", "action", "value"])

# Build pipeline
builder = PipelineBuilder(spark=spark, schema="analytics")

# Bronze: Raw data validation
builder.with_bronze_rules(
    name="events",
    rules={"user_id": ["not_null"], "value": ["gt", 0]},
    incremental_col="timestamp"
)

# Silver: Data transformation
builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.filter(F.col("value") > 50),
    rules={"value": [F.col("value") > 50]},
    table_name="clean_events"
)

# Gold: Business analytics
builder.add_gold_transform(
    name="daily_metrics",
    source_silvers=["clean_events"],
    transform=lambda spark, silvers: silvers["clean_events"]
        .groupBy("action")
        .agg(F.count("*").alias("count")),
    rules={"count": [F.col("count") > 0]},
    table_name="daily_metrics"
)

# Execute pipeline
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": df})

print(f"✅ Pipeline completed: {result.status}")
print(f"Rows written: {result.metrics.total_rows_written}")
```

### Incremental Processing

```python
# First run: Initial load
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

# Subsequent runs: Incremental updates
new_data_df = spark.read.parquet("/mnt/raw/user_events/")
result = pipeline.run_incremental(bronze_sources={"events": new_data_df})
```

## Package Structure

```
pipeline_builder/
├── __init__.py              # Main package exports
├── pipeline/                # Core pipeline components
│   ├── builder.py          # PipelineBuilder class
│   ├── runner.py           # PipelineRunner class
│   ├── models.py           # Pipeline models
│   └── monitor.py          # Pipeline monitoring
├── models/                  # Data models
│   ├── pipeline.py         # Pipeline configuration
│   ├── steps.py            # Step definitions
│   ├── execution.py        # Execution results
│   └── ...
├── validation/              # Validation system
│   ├── data_validation.py  # Data quality validation
│   ├── pipeline_validation.py  # Pipeline validation
│   └── utils.py            # Validation utilities
├── writer/                  # LogWriter for execution tracking
│   ├── core.py             # LogWriter class
│   ├── analytics.py        # Analytics functions
│   └── ...
├── dependencies/            # Dependency management
│   ├── analyzer.py          # Dependency analyzer
│   ├── graph.py            # Dependency graph
│   └── ...
├── engine/                  # Execution engine
│   └── spark_engine.py     # Spark engine implementation
├── compat.py                # Compatibility layer
├── execution.py             # Execution logic
├── functions.py             # Function utilities
├── logging.py               # Logging utilities
└── ...
```

## Core Components

### PipelineBuilder

The main class for constructing data pipelines. Provides a fluent API for defining Bronze, Silver, and Gold layers.

```python
from pipeline_builder import PipelineBuilder

builder = PipelineBuilder(
    spark=spark,
    schema="analytics",
    min_bronze_rate=90.0,
    min_silver_rate=95.0,
    min_gold_rate=98.0
)
```

**Key Methods:**
- `with_bronze_rules()` - Define Bronze layer validation rules
- `with_silver_rules()` - Add existing Silver table for validation and monitoring
- `add_silver_transform()` - Add Silver layer transformation
- `add_gold_transform()` - Add Gold layer aggregation
- `to_pipeline()` - Build the pipeline

### PipelineRunner

Executes pipelines with different modes:
- `run_initial_load()` - Process all data from scratch
- `run_incremental()` - Process only new/changed data
- `run_validation_only()` - Check data quality without writing

```python
from pipeline_builder import PipelineRunner

pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": source_df})
```

### LogWriter

Tracks and analyzes pipeline executions:

```python
from pipeline_builder import LogWriter

writer = LogWriter(
    spark=spark,
    schema="monitoring",
    table_name="pipeline_logs"
)

# Log execution
writer.create_table(result_initial)
writer.append(result_incremental)

# Query logs
logs = spark.table("monitoring.pipeline_logs")
```

## Validation System

Pipeline Builder enforces data quality through progressive validation thresholds:

- **Bronze Layer**: 90% (basic validation - required fields, data types)
- **Silver Layer**: 95% (business rules - value ranges, business logic)
- **Gold Layer**: 98% (final quality - aggregations must be correct)

### Validation Rules

You can use either **string rules** (human-readable) or **PySpark expressions**:

```python
# String rules (automatically converted to PySpark)
rules = {
    "user_id": ["not_null"],                    # F.col("user_id").isNotNull()
    "age": ["gt", 0],                          # F.col("age") > 0
    "status": ["in", ["active", "inactive"]],  # F.col("status").isin([...])
    "score": ["between", 0, 100],              # F.col("score").between(0, 100)
}

# Or PySpark expressions directly
rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "age": [F.col("age") > 0],
}
```

**Supported String Rules:**
- `"not_null"` → `F.col("column").isNotNull()`
- `"gt", value` → `F.col("column") > value`
- `"gte", value` → `F.col("column") >= value`
- `"lt", value` → `F.col("column") < value`
- `"lte", value` → `F.col("column") <= value`
- `"eq", value` → `F.col("column") == value`
- `"in", [values]` → `F.col("column").isin(values)`
- `"between", min, max` → `F.col("column").between(min, max)`
- `"like", pattern` → `F.col("column").like(pattern)`

## Parallel Execution

Pipeline Builder automatically analyzes dependencies and executes independent steps in parallel:

```python
# These 3 bronze steps will run in parallel
builder.with_bronze_rules(name="events_a", ...)
builder.with_bronze_rules(name="events_b", ...)
builder.with_bronze_rules(name="events_c", ...)

# These 3 silver steps will also run in parallel (after bronze completes)
builder.add_silver_transform(name="clean_a", source_bronze="events_a", ...)
builder.add_silver_transform(name="clean_b", source_bronze="events_b", ...)
builder.add_silver_transform(name="clean_c", source_bronze="events_c", ...)
```

**Benefits:**
- 3-5x faster execution for typical pipelines
- Automatic dependency analysis
- Thread-safe execution
- Configurable worker count (1-16+)

## Incremental Processing

Enable incremental processing by specifying an `incremental_col` in your Bronze layer:

```python
builder.with_bronze_rules(
    name="events",
    rules={"user_id": ["not_null"]},
    incremental_col="timestamp"  # Enable incremental processing
)

# First run: Process all data
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

# Subsequent runs: Process only new data
new_data_df = spark.read.parquet("/mnt/raw/user_events/")
result = pipeline.run_incremental(bronze_sources={"events": new_data_df})
```

**How it works:**
1. Pipeline Builder checks the last processed timestamp
2. Filters new data to only include rows after that timestamp
3. Processes only the new/changed data
4. Updates Silver and Gold tables incrementally

## Schema Override

Pipeline Builder supports schema override for explicit control over table schemas using Delta Lake's `overwriteSchema` option. This is useful for ensuring consistent schemas across environments, handling schema evolution, and enforcing specific data types and nullability constraints.

### Usage

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define schema override
custom_schema = StructType([
    StructField("user_id", StringType(), False),  # Not nullable
    StructField("event_type", StringType(), False),
    StructField("timestamp", TimestampType(), True),  # Nullable
    StructField("value", IntegerType(), True)
])

# Silver step with schema override
builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_transform,
    rules={"user_id": ["not_null"]},
    table_name="clean_events",
    schema_override=custom_schema  # Applied during initial runs
)

# Gold step with schema override
metrics_schema = StructType([
    StructField("date", StringType(), False),
    StructField("total_events", IntegerType(), False),
    StructField("unique_users", IntegerType(), False)
])

builder.add_gold_transform(
    name="daily_metrics",
    transform=metrics_transform,
    rules={"date": ["not_null"]},
    table_name="daily_metrics",
    schema_override=metrics_schema  # Always applied for gold writes
)
```

### When Schema Override is Applied

- **Gold tables**: Always applied (gold tables always use overwrite mode)
- **Silver initial runs**: Always applied during initial and full refresh runs
- **Silver incremental**: Only applied if the table doesn't exist (first write to new table)

The schema override uses Delta Lake's `overwriteSchema` option, which allows schema changes when writing to tables.

## Medallion Architecture

Pipeline Builder implements the Medallion Architecture pattern:

### Bronze Layer
- **Purpose**: Raw data ingestion and validation
- **Quality Standard**: 90% validation rate
- **Storage**: Delta Lake tables
- **Features**: Incremental processing, metadata tracking

### Silver Layer
- **Purpose**: Cleaned and enriched data
- **Quality Standard**: 95% validation rate
- **Storage**: Delta Lake tables
- **Features**: Business rule application, data enrichment, incremental updates

### Gold Layer
- **Purpose**: Business-ready analytics and metrics
- **Quality Standard**: 98% validation rate
- **Storage**: Delta Lake tables
- **Features**: Aggregations, KPIs, optimized for queries

## Examples

### E-Commerce Analytics Pipeline

```python
from pipeline_builder import PipelineBuilder, LogWriter
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
builder = PipelineBuilder(spark=spark, schema="ecommerce_analytics")

# Bronze: Raw customer events
builder.with_bronze_rules(
    name="customer_events",
    rules={
        "user_id": ["not_null"],
        "price": ["gt", 0],
        "timestamp": ["not_null"]
    },
    incremental_col="timestamp"
)

# Silver: Enriched events
def enrich_events(spark, bronze_df, prior_silvers):
    return bronze_df.withColumn("event_date", F.to_date("timestamp"))

builder.add_silver_transform(
    name="enriched_events",
    source_bronze="customer_events",
    transform=enrich_events,
    rules={"event_date": ["not_null"]},
    table_name="enriched_events"
)

# Gold: Daily revenue metrics
def daily_revenue(spark, silvers):
    return silvers["enriched_events"].groupBy("event_date").agg(
        F.sum("price").alias("total_revenue"),
        F.countDistinct("user_id").alias("unique_customers")
    )

builder.add_gold_transform(
    name="daily_revenue",
    source_silvers=["enriched_events"],
    transform=daily_revenue,
    rules={"total_revenue": ["gte", 0]},
    table_name="daily_revenue"
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"customer_events": source_df})

# Log execution
writer = LogWriter(spark=spark, schema="monitoring", table_name="pipeline_logs")
writer.create_table(result)
```

### Using Existing Silver Tables

When you have pre-existing Silver tables that you want to include in your pipeline for validation and monitoring (without transforming them), use `with_silver_rules()`:

```python
from pipeline_builder import PipelineBuilder, LogWriter
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
builder = PipelineBuilder(spark=spark, schema="analytics")

# Add an existing Silver table for validation and monitoring
builder.with_silver_rules(
    name="existing_clean_events",
    table_name="clean_events",
    rules={
        "user_id": ["not_null"],
        "event_type": ["in", ["click", "purchase", "view"]],
        "timestamp": ["not_null"]
    }
)

# You can still add Gold transforms that depend on this existing Silver table
builder.add_gold_transform(
    name="event_summary",
    source_silvers=["existing_clean_events"],
    transform=lambda spark, silvers: silvers["existing_clean_events"]
        .groupBy("event_type")
        .agg(F.count("*").alias("total_events")),
    rules={"total_events": ["gt", 0]},
    table_name="event_summary"
)

# Execute pipeline
pipeline = builder.to_pipeline()
result = pipeline.run_validation_only()  # Validate existing Silver table

# Log execution
writer = LogWriter(spark=spark, schema="monitoring", table_name="pipeline_logs")
writer.create_table(result)
```

**Use Cases:**
- Include legacy Silver tables in new pipelines
- Validate existing tables without reprocessing
- Build Gold aggregations on pre-existing Silver data
- Monitor data quality across multiple schemas

## Configuration

### Validation Thresholds

```python
builder = PipelineBuilder(
    spark=spark,
    schema="analytics",
    min_bronze_rate=90.0,   # 90% of Bronze data must pass validation
    min_silver_rate=95.0,   # 95% of Silver data must pass validation
    min_gold_rate=98.0      # 98% of Gold data must pass validation
)
```

### Parallel Execution

```python
from pipeline_builder.models import ParallelConfig

config = ParallelConfig(
    enabled=True,
    max_workers=4,      # Number of parallel workers
    timeout_secs=600    # Timeout for parallel execution
)
```

## Error Handling

Pipeline Builder provides comprehensive error handling:

```python
try:
    result = pipeline.run_incremental(bronze_sources={"events": source_df})
    if result.status != "completed":
        print(f"Pipeline failed: {result.errors}")
except Exception as e:
    print(f"Error: {e}")
    # Error messages include actionable suggestions
```

## Testing

Pipeline Builder supports both PySpark and mock-spark for testing:

```python
# With PySpark (production)
pip install pipeline_builder[pyspark]

# With mock-spark (testing)
pip install pipeline_builder[mock]
```

The framework automatically detects which engine is available.

## Documentation

- **Full Documentation**: [Documentation Directory](../../docs/)
- **API Reference**: [Enhanced API Reference](../../docs/ENHANCED_API_REFERENCE.md) or [API Reference (Markdown)](../../docs/markdown/API_REFERENCE.md)
- **User Guide**: [Comprehensive User Guide](../../docs/COMPREHENSIVE_USER_GUIDE.md) or [User Guide (Markdown)](../../docs/markdown/USER_GUIDE.md)
- **Examples**: [Examples Directory](../../examples/)

## Contributing

Contributions are welcome! Please see the main repository README for contribution guidelines.

## License

MIT License - see LICENSE file for details.

## Version

Current version: **2.3.0**

## Support

- **Documentation**: [Documentation Directory](../../docs/)
- **Examples**: [Examples Directory](../../examples/)
- **Troubleshooting**: [Comprehensive Troubleshooting Guide](../../docs/COMPREHENSIVE_TROUBLESHOOTING_GUIDE.md)

## Related Files

- **[Architecture.md](Architecture.md)** - Detailed architecture documentation for PipelineBuilder
- **[PRESENTATION_README.md](PRESENTATION_README.md)** - Presentation materials for Pipeline Builder on Databricks
- **[Main README](../../README.md)** - Project overview

---

**Made with ❤️ for the data engineering community**


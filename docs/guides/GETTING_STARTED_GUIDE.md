# Getting Started Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Engine Configuration](#engine-configuration)
4. [Your First Pipeline](#your-first-pipeline)
5. [Next Steps](#next-steps)

---

## Introduction

This guide gets you from zero to a running pipeline. You will install SparkForge, configure the engine, and run a small Bronze → Silver → Gold pipeline with no references to docs outside this guides directory.

## Installation

Install the package and optional extras:

```bash
# Core package
pip install pipeline-builder

# With PySpark (production)
pip install pipeline-builder[pyspark]

# With mock Spark (testing/development)
pip install pipeline-builder[mock]

# Development and testing
pip install pipeline-builder[dev,test]
```

**Requirements:** Python 3.8+. For PySpark you also need Java (e.g. Java 17 for Spark 3.5) and Delta Lake support in your Spark session.

## Engine Configuration

You **must** configure the engine before using any pipeline components. This lets the framework work with both real PySpark and mock Spark (e.g. for tests).

1. Create your Spark session (with Delta extensions if you use Delta Lake).
2. Call `configure_engine(spark=spark)`.
3. Then import and use `PipelineBuilder` and other pipeline components.

Minimal setup:

```python
from pipeline_builder.engine_config import configure_engine
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

configure_engine(spark=spark)

# Now safe to use pipeline components
from pipeline_builder import PipelineBuilder
```

## Your First Pipeline

This example defines one bronze step, one silver transform, and one gold transform, then runs an initial load.

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions
from pyspark.sql import SparkSession

# 1. Spark and engine
spark = SparkSession.builder \
    .appName("FirstPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
configure_engine(spark=spark)
F = get_default_functions()

# 2. Sample data (your bronze source)
data = [
    ("user1", "click", "2024-01-01 10:00:00", 1.0),
    ("user2", "view", "2024-01-01 10:01:00", 2.0),
    ("user1", "purchase", "2024-01-01 10:02:00", 29.99),
]
source_df = spark.createDataFrame(data, ["user_id", "action", "timestamp", "value"])

# 3. Build pipeline
builder = PipelineBuilder(spark=spark, schema="my_schema")

# Bronze: validate raw data
builder.with_bronze_rules(
    name="raw_events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "value": [F.col("value") > 0],
    },
    incremental_col="timestamp",
)

# Silver: filter to purchases only
def silver_transform(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("action") == "purchase")

builder.add_silver_transform(
    name="purchases",
    source_bronze="raw_events",
    transform=silver_transform,
    rules={"user_id": [F.col("user_id").isNotNull()], "value": [F.col("value") > 0]},
    table_name="purchases",
)

# Gold: aggregate by user
def gold_transform(spark, silvers):
    return silvers["purchases"].groupBy("user_id").agg(
        F.count("*").alias("count"),
        F.sum("value").alias("total"),
    )

builder.add_gold_transform(
    name="user_totals",
    transform=gold_transform,
    rules={"user_id": [F.col("user_id").isNotNull()], "count": [F.col("count") > 0]},
    table_name="user_totals",
    source_silvers=["purchases"],
)

# 4. Validate and build
errors = builder.validate_pipeline()
if errors:
    raise RuntimeError(f"Validation failed: {errors}")
pipeline = builder.to_pipeline()

# 5. Run initial load
result = pipeline.run_initial_load(bronze_sources={"raw_events": source_df})

print(f"Status: {result.status.value}")
print(f"Bronze rows processed: {result.bronze_results['raw_events']['rows_processed']}")
print(f"Silver rows written: {result.silver_results['purchases']['rows_written']}")
print(f"Gold rows written: {result.gold_results['user_totals']['rows_written']}")
```

**What happened:** Bronze validated the raw DataFrame, silver kept only purchase rows, gold aggregated by `user_id`. Tables were written to Delta in the schema `my_schema` (silver and gold). The keys in `bronze_sources` must match bronze step names (here `"raw_events"`).

## Next Steps

- **Define more steps and understand Medallion layers:** [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md)
- **Run incremental or validation-only:** [Execution Modes](./EXECUTION_MODES_GUIDE.md)
- **Read from a SQL database:** [SQL Source Steps](./SQL_SOURCE_STEPS_GUIDE.md)
- **Debug one step at a time:** [Stepwise Execution](./STEPWISE_EXECUTION_GUIDE.md)
- **Browse all guides:** [Guides index](./README.md)

# Use Case: Business Intelligence

**Version**: 2.8.0  
**Last Updated**: February 2025

This guide builds a **Business Intelligence** pipeline: sales analytics, customer intelligence, employee performance, and executive dashboards using the Medallion architecture. All content is self-contained; for pipeline basics see [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) and [Getting Started](./GETTING_STARTED_GUIDE.md).

## What You'll Build

- **Sales analytics:** Revenue, transactions, discounts, time dimensions
- **Customer intelligence:** Lifecycle stage, premium classification
- **Employee performance:** Tenure, performance category, salary bands
- **Gold aggregates:** Executive dashboard, sales performance, customer analytics, operational metrics

## Prerequisites

- Python 3.8+ with SparkForge installed
- Engine configured before building (see [Getting Started](./GETTING_STARTED_GUIDE.md))

## Step 1: Setup and Data

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions
from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta
import random

spark = SparkSession.builder \
    .appName("BI") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
configure_engine(spark=spark)
F = get_default_functions()

# Sample sales data
base_date = datetime(2024, 1, 1)
sales_data = [
    (
        f"TXN_{i:06d}", f"CUST_{random.randint(1, 500):04d}", f"PROD_{random.randint(1, 100):03d}",
        random.choice(["Electronics", "Clothing", "Home"]),
        round(random.uniform(10, 500), 2), random.randint(1, 5),
        (base_date + timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d"),
        random.choice(["North", "South", "East", "West"])
    )
    for i in range(500)
]
sales_df = spark.createDataFrame(
    sales_data,
    ["transaction_id", "customer_id", "product_id", "category", "sales_amount", "quantity", "transaction_date", "region"]
)
sales_df = sales_df.withColumn("transaction_date", F.to_date("transaction_date"))
```

## Step 2: Build the Pipeline

```python
builder = PipelineBuilder(
    spark=spark,
    schema="bi_schema",
    min_bronze_rate=90.0,
    min_silver_rate=95.0,
    min_gold_rate=98.0,
)

# Bronze: sales
builder.with_bronze_rules(
    name="sales",
    rules={
        "transaction_id": [F.col("transaction_id").isNotNull()],
        "customer_id": [F.col("customer_id").isNotNull()],
        "sales_amount": [F.col("sales_amount") > 0],
        "transaction_date": [F.col("transaction_date").isNotNull()],
    },
    incremental_col="transaction_date",
)

# Silver: enhanced sales
def enhance_sales(spark, bronze_df, prior_silvers):
    return (bronze_df
        .withColumn("net_sales", F.col("sales_amount") * F.col("quantity"))
        .withColumn("transaction_month", F.date_trunc("month", "transaction_date"))
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="enhanced_sales",
    source_bronze="sales",
    transform=enhance_sales,
    rules={
        "net_sales": [F.col("net_sales") > 0],
        "transaction_month": [F.col("transaction_month").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()],
    },
    table_name="enhanced_sales",
)

# Gold: sales performance by region and month
def sales_performance(spark, silvers):
    return (silvers["enhanced_sales"]
        .groupBy("region", "transaction_month")
        .agg(
            F.sum("net_sales").alias("revenue"),
            F.count("*").alias("transactions"),
            F.countDistinct("customer_id").alias("customers"),
        )
    )

builder.add_gold_transform(
    name="sales_performance",
    transform=sales_performance,
    rules={
        "revenue": [F.col("revenue") > 0],
        "transactions": [F.col("transactions") > 0],
        "customers": [F.col("customers") > 0],
    },
    table_name="sales_performance",
    source_silvers=["enhanced_sales"],
)
```

## Step 3: Validate and Run

```python
errors = builder.validate_pipeline()
if errors:
    raise RuntimeError(f"Validation failed: {errors}")

pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"sales": sales_df})

print(f"Status: {result.status.value}")
print(f"Silver rows written: {result.silver_results['enhanced_sales']['rows_written']}")
print(f"Gold rows written: {result.gold_results['sales_performance']['rows_written']}")
```

## Extending the Pipeline

You can add more bronze steps (e.g. customers, employees), more silver transforms (customer intelligence, employee performance), and more gold tables (executive dashboard, customer analytics, operational metrics). Follow the same pattern: `with_bronze_rules`, `add_silver_transform`, `add_gold_transform`, with rules for every column you want to keep. See [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) and [Validation Rules](./VALIDATION_RULES_GUIDE.md).

## Related Guides

- [Use Cases Index](./USE_CASES_INDEX.md)
- [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md)
- [Execution Modes](./EXECUTION_MODES_GUIDE.md)

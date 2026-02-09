# Use Case: E-commerce

**Version**: 2.8.0  
**Last Updated**: February 2025

This guide builds an **E-commerce** pipeline: order ingestion, enrichment, customer profiles, and gold tables for daily sales, product performance, and customer segmentation. All content is self-contained; for pipeline basics see [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) and [Getting Started](./GETTING_STARTED_GUIDE.md).

## What You'll Build

- **Order processing:** Ingest and validate orders
- **Enriched orders:** Time dimensions, order size category
- **Customer profiles:** Lifetime value, order frequency
- **Gold:** Daily sales, product performance, customer segmentation

## Prerequisites

- Python 3.8+ with SparkForge installed
- Engine configured (see [Getting Started](./GETTING_STARTED_GUIDE.md))

## Step 1: Setup and Data

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions
from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta
import random

spark = SparkSession.builder \
    .appName("Ecommerce") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
configure_engine(spark=spark)
F = get_default_functions()

base_date = datetime(2024, 1, 1)
orders = [
    (
        f"ORD_{i:05d}", f"CUST_{random.randint(1, 200):04d}", f"PROD_{random.randint(1, 50):03d}",
        random.choice(["Electronics", "Accessories", "Computers"]),
        random.randint(1, 5), round(random.uniform(20, 500), 2),
        (base_date + timedelta(days=random.randint(0, 60))).strftime("%Y-%m-%d"),
        random.choice(["North", "South", "East", "West"])
    )
    for i in range(300)
]
orders_df = spark.createDataFrame(
    orders,
    ["order_id", "customer_id", "product_id", "category", "quantity", "unit_price", "order_date", "region"]
)
orders_df = orders_df.withColumn("order_date", F.to_date("order_date")) \
    .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
```

## Step 2: Build the Pipeline

```python
builder = PipelineBuilder(spark=spark, schema="ecommerce_schema", min_bronze_rate=90.0, min_silver_rate=95.0, min_gold_rate=98.0)

# Bronze: orders
builder.with_bronze_rules(
    name="orders",
    rules={
        "order_id": [F.col("order_id").isNotNull()],
        "customer_id": [F.col("customer_id").isNotNull()],
        "total_amount": [F.col("total_amount") > 0],
        "order_date": [F.col("order_date").isNotNull()],
        "region": [F.col("region").isNotNull()],
    },
    incremental_col="order_date",
)

# Silver: enriched orders
def enrich_orders(spark, bronze_df, prior_silvers):
    return (bronze_df
        .withColumn("order_month", F.date_trunc("month", "order_date"))
        .withColumn("order_size", F.when(F.col("total_amount") > 500, "large").when(F.col("total_amount") > 100, "medium").otherwise("small"))
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="enriched_orders",
    source_bronze="orders",
    transform=enrich_orders,
    rules={
        "order_month": [F.col("order_month").isNotNull()],
        "order_size": [F.col("order_size").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()],
        "total_amount": [F.col("total_amount") > 0],
    },
    table_name="enriched_orders",
)

# Gold: daily sales
def daily_sales(spark, silvers):
    return (silvers["enriched_orders"]
        .groupBy("order_date", "region")
        .agg(
            F.count("*").alias("orders"),
            F.sum("total_amount").alias("revenue"),
            F.countDistinct("customer_id").alias("customers"),
        )
    )

builder.add_gold_transform(
    name="daily_sales",
    transform=daily_sales,
    rules={
        "order_date": [F.col("order_date").isNotNull()],
        "revenue": [F.col("revenue") > 0],
        "orders": [F.col("orders") > 0],
    },
    table_name="daily_sales",
    source_silvers=["enriched_orders"],
)
```

## Step 3: Run

```python
errors = builder.validate_pipeline()
if errors:
    raise RuntimeError(f"Validation failed: {errors}")

pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"orders": orders_df})

print(f"Status: {result.status.value}")
print(f"Gold rows written: {result.gold_results['daily_sales']['rows_written']}")
```

You can add more gold steps (e.g. product performance by category/region, customer segmentation by total_spent and region) using the same pattern. See [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md).

## Related Guides

- [Use Cases Index](./USE_CASES_INDEX.md)
- [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md)
- [Execution Modes](./EXECUTION_MODES_GUIDE.md)

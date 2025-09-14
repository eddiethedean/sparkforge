# SparkForge 5-Minute Quick Start

Get up and running with SparkForge in under 5 minutes! This guide will have you processing data through Bronze → Silver → Gold layers quickly.

## Prerequisites

Before you start, make sure you have:
- Python 3.8+ installed
- Java 8+ installed (for PySpark)
- Basic understanding of Python

## Installation

```bash
pip install sparkforge pyspark
```

## Your First Pipeline (2 minutes)

Let's create the simplest possible pipeline:

```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

# 1. Start Spark
spark = SparkSession.builder.appName("My First Pipeline").getOrCreate()

# 2. Create some sample data
data = [("user1", "click", 100), ("user2", "purchase", 200)]
df = spark.createDataFrame(data, ["user_id", "action", "value"])

# 3. Build your pipeline
builder = PipelineBuilder(spark=spark, schema="my_first_schema")

# Bronze: Raw data (just validate it exists)
builder.with_bronze_rules(name="events", rules={"user_id": [F.col("user_id").isNotNull()]})

# Silver: Clean the data
builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.filter(F.col("value") > 50),
    rules={"value": [F.col("value") > 50]},
    table_name="clean_events"
)

# Gold: Count actions
builder.add_gold_transform(
    name="action_counts",
    transform=lambda spark, silvers: silvers["clean_events"].groupBy("action").count(),
    rules={"action": [F.col("action").isNotNull()]},
    table_name="action_counts",
    source_silvers=["clean_events"]
)

# 4. Run it!
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"events": df})

print(f"Success: {result.success}")
print(f"Rows processed: {result.totals['total_rows_written']}")
```

## What Just Happened?

1. **Bronze Layer**: We took raw data and validated that `user_id` exists
2. **Silver Layer**: We filtered out low-value events (value ≤ 50)
3. **Gold Layer**: We counted how many of each action type we had
4. **Result**: We processed data through all three layers automatically!

## See Your Results

```python
# Check what was created
spark.sql("SHOW TABLES IN my_first_schema").show()

# Look at the final results
spark.table("my_first_schema.action_counts").show()
```

## Next Steps (3 minutes)

### Try Incremental Processing

```python
# Add new data
new_data = [("user3", "view", 150), ("user4", "purchase", 300)]
new_df = spark.createDataFrame(new_data, ["user_id", "action", "value"])

# Process incrementally (only new data)
result = pipeline.run_incremental(bronze_sources={"events": new_df})
```

### Debug Individual Steps

```python
# Test just the Bronze step
bronze_result = pipeline.execute_bronze_step("events", input_data=df)
print(f"Bronze validation passed: {bronze_result.validation_result.validation_passed}")

# Test just the Silver step
silver_result = pipeline.execute_silver_step("clean_events")
print(f"Silver output rows: {silver_result.output_count}")
```

### Add More Validation

```python
# More strict validation
builder = PipelineBuilder(
    spark=spark,
    schema="strict_schema",
    min_bronze_rate=95.0,  # 95% of data must pass validation
    min_silver_rate=98.0   # 98% of Silver data must pass validation
)
```

## Common Issues & Solutions

### "Java not found" Error
- Install Java 8+ and set JAVA_HOME environment variable
- On Mac: `brew install openjdk@8`
- On Ubuntu: `sudo apt install openjdk-8-jdk`

### "Module not found" Error
- Make sure you installed with: `pip install sparkforge pyspark`
- Check your Python environment

### Pipeline Fails
- Check validation rates in the result object
- Use step-by-step debugging to isolate issues
- Lower validation thresholds if needed

## What's Next?

You're ready to build real pipelines! Choose your next step:

- **[E-commerce Quick Start](USECASE_ECOMMERCE.md)** - Build an e-commerce analytics pipeline
- **[IoT Quick Start](USECASE_IOT.md)** - Process IoT sensor data
- **[Business Intelligence Quick Start](USECASE_BI.md)** - Create business dashboards
- **[User Guide](USER_GUIDE.md)** - Learn advanced features and patterns

## Need Help?

- Check the [troubleshooting section](TROUBLESHOOTING.md)
- Look at [working examples](examples/)
- Review the [API Reference](API_REFERENCE.md)

---

**🎉 Congratulations! You've built your first SparkForge pipeline in under 5 minutes!**

Ready for more? Pick a use case guide above and build something real!

# Progressive Examples - Learn SparkForge Step by Step

Learn SparkForge by building complexity gradually. Each example builds on the previous one, teaching you the concepts step by step.

## Learning Path

1. **[Step 1: Bronze Only](#step-1-bronze-only)** - Just data ingestion and validation
2. **[Step 2: Bronze + Silver](#step-2-bronze--silver)** - Add data transformation
3. **[Step 3: Complete Pipeline](#step-3-complete-pipeline)** - Add Gold analytics
4. **[Step 4: Add Validation](#step-4-add-validation)** - Stricter data quality
5. **[Step 5: Add Parallel Execution](#step-5-add-parallel-execution)** - Performance optimization

---

## Step 1: Bronze Only

**Goal**: Learn data ingestion and basic validation

**What you'll learn**: How to ingest raw data and validate it meets basic requirements

```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

# Start Spark
spark = SparkSession.builder.appName("Step 1 - Bronze Only").getOrCreate()

# Create simple data
data = [("user1", "click", 100), ("user2", "purchase", 200), (None, "view", 50)]
df = spark.createDataFrame(data, ["user_id", "action", "value"])

print("📊 Input Data:")
df.show()

# Build Bronze-only pipeline
builder = PipelineBuilder(spark=spark, schema="progressive_demo")

# Bronze: Just validate the data
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],  # User ID must exist
        "action": [F.col("action").isNotNull()],    # Action must exist
        "value": [F.col("value") > 0]               # Value must be positive
    }
)

# Create and run pipeline
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"events": df})

print(f"\n✅ Bronze validation completed: {result.success}")
print(f"📈 Rows processed: {result.totals['total_rows_written']}")

# Check what passed validation
spark.table("progressive_demo.events").show()
```

**Key Concepts Learned:**
- Data ingestion with SparkForge
- Basic validation rules
- Bronze layer purpose

---

## Step 2: Bronze + Silver

**Goal**: Add data transformation and cleaning

**What you'll learn**: How to transform raw data into clean, business-ready data

```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

# Start Spark
spark = SparkSession.builder.appName("Step 2 - Bronze + Silver").getOrCreate()

# Create data with some quality issues
data = [
    ("user1", "click", 100, "2024-01-01"),
    ("user2", "purchase", 200, "2024-01-01"),
    ("user3", "invalid_action", 150, "2024-01-01"),
    ("user4", "view", -50, "2024-01-01"),  # Negative value
    ("user5", "click", 75, "2024-01-01")
]
df = spark.createDataFrame(data, ["user_id", "action", "value", "date"])

print("📊 Input Data (with quality issues):")
df.show()

# Build Bronze + Silver pipeline
builder = PipelineBuilder(spark=spark, schema="progressive_demo")

# Bronze: Basic validation
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "value": [F.col("value").isNotNull()],
        "date": [F.col("date").isNotNull()]
    }
)

# Silver: Clean and transform the data
def clean_events(spark, bronze_df, prior_silvers):
    """Clean and transform event data."""
    return (bronze_df
        .filter(F.col("value") > 0)  # Remove negative values
        .filter(F.col("action").isin(["click", "view", "purchase"]))  # Valid actions only
        .withColumn("processed_date", F.to_date("date"))
        .withColumn("value_category",
            F.when(F.col("value") > 150, "high_value")
            .when(F.col("value") > 100, "medium_value")
            .otherwise("low_value")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_events,
    rules={
        "value_category": [F.col("value_category").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="clean_events"
)

# Create and run pipeline
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"events": df})

print(f"\n✅ Bronze + Silver completed: {result.success}")
print(f"📈 Rows processed: {result.totals['total_rows_written']}")

# Compare Bronze vs Silver
print("\n🥉 Bronze Layer (all data):")
spark.table("progressive_demo.events").show()

print("\n🥈 Silver Layer (cleaned data):")
spark.table("progressive_demo.clean_events").show()
```

**Key Concepts Learned:**
- Data transformation in Silver layer
- Filtering and cleaning data
- Adding business logic and categories

---

## Step 3: Complete Pipeline

**Goal**: Add Gold analytics layer

**What you'll learn**: How to create business analytics and aggregations

```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

# Start Spark
spark = SparkSession.builder.appName("Step 3 - Complete Pipeline").getOrCreate()

# Create more comprehensive data
data = [
    ("user1", "click", 100, "2024-01-01"),
    ("user1", "purchase", 200, "2024-01-01"),
    ("user2", "view", 50, "2024-01-01"),
    ("user2", "click", 150, "2024-01-02"),
    ("user3", "purchase", 300, "2024-01-02"),
    ("user3", "view", 75, "2024-01-02")
]
df = spark.createDataFrame(data, ["user_id", "action", "value", "date"])

print("📊 Input Data:")
df.show()

# Build complete Bronze → Silver → Gold pipeline
builder = PipelineBuilder(spark=spark, schema="progressive_demo")

# Bronze: Data ingestion
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "value": [F.col("value") > 0],
        "date": [F.col("date").isNotNull()]
    }
)

# Silver: Clean and enrich data
def clean_and_enrich_events(spark, bronze_df, prior_silvers):
    """Clean and enrich event data."""
    return (bronze_df
        .filter(F.col("value") > 0)
        .filter(F.col("action").isin(["click", "view", "purchase"]))
        .withColumn("processed_date", F.to_date("date"))
        .withColumn("value_category",
            F.when(F.col("value") > 150, "high_value")
            .when(F.col("value") > 100, "medium_value")
            .otherwise("low_value")
        )
        .withColumn("is_purchase", F.col("action") == "purchase")
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_and_enrich_events,
    rules={
        "value_category": [F.col("value_category").isNotNull()],
        "is_purchase": [F.col("is_purchase").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="clean_events"
)

# Gold: Business analytics
def create_analytics(spark, silvers):
    """Create business analytics."""
    events_df = silvers["clean_events"]

    # Daily analytics
    daily_analytics = (events_df
        .groupBy("processed_date")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.sum("value").alias("total_value"),
            F.sum(F.when(F.col("is_purchase"), 1).otherwise(0)).alias("purchases"),
            F.avg("value").alias("avg_value")
        )
    )

    # User analytics
    user_analytics = (events_df
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum("value").alias("total_spent"),
            F.sum(F.when(F.col("is_purchase"), 1).otherwise(0)).alias("purchases"),
            F.avg("value").alias("avg_value")
        )
        .withColumn("user_segment",
            F.when(F.col("total_spent") > 200, "high_value")
            .when(F.col("total_spent") > 100, "medium_value")
            .otherwise("low_value")
        )
    )

    return daily_analytics

builder.add_gold_transform(
    name="daily_analytics",
    transform=create_analytics,
    rules={
        "total_events": [F.col("total_events") > 0],
        "unique_users": [F.col("unique_users") > 0],
        "total_value": [F.col("total_value") > 0]
    },
    table_name="daily_analytics",
    source_silvers=["clean_events"]
)

# Create and run pipeline
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"events": df})

print(f"\n✅ Complete pipeline completed: {result.success}")
print(f"📈 Rows processed: {result.totals['total_rows_written']}")

# Show all layers
print("\n🥉 Bronze Layer (raw data):")
spark.table("progressive_demo.events").show()

print("\n🥈 Silver Layer (cleaned data):")
spark.table("progressive_demo.clean_events").show()

print("\n🥇 Gold Layer (analytics):")
spark.table("progressive_demo.daily_analytics").show()
```

**Key Concepts Learned:**
- Complete Bronze → Silver → Gold flow
- Business analytics and aggregations
- Data transformation across layers

---

## Step 4: Add Validation

**Goal**: Implement strict data quality controls

**What you'll learn**: How to set validation thresholds and handle data quality issues

```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

# Start Spark
spark = SparkSession.builder.appName("Step 4 - Add Validation").getOrCreate()

# Create data with quality issues to test validation
data = [
    ("user1", "click", 100, "2024-01-01"),
    ("user2", "purchase", 200, "2024-01-01"),
    ("user3", "view", 50, "2024-01-01"),
    (None, "click", 150, "2024-01-01"),      # Missing user_id
    ("user4", None, 75, "2024-01-01"),       # Missing action
    ("user5", "click", -25, "2024-01-01"),   # Negative value
    ("user6", "click", 125, None),           # Missing date
    ("user7", "click", 175, "2024-01-01")    # Good record
]
df = spark.createDataFrame(data, ["user_id", "action", "value", "date"])

print("📊 Input Data (with quality issues):")
df.show()

# Build pipeline with strict validation
builder = PipelineBuilder(
    spark=spark,
    schema="progressive_demo",
    min_bronze_rate=80.0,  # 80% of Bronze data must pass validation
    min_silver_rate=95.0,  # 95% of Silver data must pass validation
    min_gold_rate=98.0,    # 98% of Gold data must pass validation
    verbose=True
)

# Bronze: Strict validation
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "value": [F.col("value").isNotNull(), F.col("value") > 0],
        "date": [F.col("date").isNotNull()]
    }
)

# Silver: Enhanced cleaning with validation
def clean_events_with_validation(spark, bronze_df, prior_silvers):
    """Clean events with additional validation."""
    return (bronze_df
        .filter(F.col("value") > 0)
        .filter(F.col("action").isin(["click", "view", "purchase"]))
        .filter(F.col("user_id").isNotNull())
        .withColumn("processed_date", F.to_date("date"))
        .withColumn("value_category",
            F.when(F.col("value") > 150, "high_value")
            .when(F.col("value") > 100, "medium_value")
            .otherwise("low_value")
        )
        .withColumn("is_purchase", F.col("action") == "purchase")
        .withColumn("processed_at", F.current_timestamp())
        .filter(F.col("processed_date").isNotNull())  # Additional validation
    )

builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_events_with_validation,
    rules={
        "value_category": [F.col("value_category").isNotNull()],
        "is_purchase": [F.col("is_purchase").isNotNull()],
        "processed_date": [F.col("processed_date").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="clean_events"
)

# Gold: Analytics with validation
def create_validated_analytics(spark, silvers):
    """Create analytics with validation."""
    events_df = silvers["clean_events"]

    return (events_df
        .groupBy("processed_date")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.sum("value").alias("total_value"),
            F.sum(F.when(F.col("is_purchase"), 1).otherwise(0)).alias("purchases"),
            F.avg("value").alias("avg_value")
        )
        .filter(F.col("total_events") > 0)  # Additional validation
        .filter(F.col("unique_users") > 0)
    )

builder.add_gold_transform(
    name="daily_analytics",
    transform=create_validated_analytics,
    rules={
        "total_events": [F.col("total_events") > 0],
        "unique_users": [F.col("unique_users") > 0],
        "total_value": [F.col("total_value") > 0],
        "avg_value": [F.col("avg_value") > 0]
    },
    table_name="daily_analytics",
    source_silvers=["clean_events"]
)

# Create and run pipeline
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"events": df})

print(f"\n✅ Validation pipeline completed: {result.success}")
print(f"📈 Rows processed: {result.totals['total_rows_written']}")
print(f"🎯 Overall validation rate: {result.totals.get('overall_validation_rate', 0):.1f}%")

# Show validation results
if hasattr(result, 'stage_stats'):
    print(f"\n📊 Validation Results:")
    for stage, stats in result.stage_stats.items():
        print(f"   {stage.title()}: {stats.get('validation_rate', 0):.1f}% validation rate")

# Show processed data
print("\n🥉 Bronze Layer (validated data):")
spark.table("progressive_demo.events").show()

print("\n🥈 Silver Layer (cleaned data):")
spark.table("progressive_demo.clean_events").show()

print("\n🥇 Gold Layer (analytics):")
spark.table("progressive_demo.daily_analytics").show()
```

**Key Concepts Learned:**
- Validation thresholds and data quality control
- Handling data quality issues
- Monitoring validation rates

---

## Step 5: Add Parallel Execution

**Goal**: Optimize performance with parallel processing

**What you'll learn**: How to enable parallel execution for better performance

```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F
import time

# Start Spark
spark = SparkSession.builder.appName("Step 5 - Parallel Execution").getOrCreate()

# Create larger dataset for performance testing
def create_large_dataset(spark, num_records=10000):
    """Create a larger dataset for performance testing."""
    data = []
    for i in range(num_records):
        data.append({
            "user_id": f"user_{i % 1000:04d}",
            "action": random.choice(["click", "view", "purchase"]),
            "value": random.randint(10, 500),
            "date": f"2024-01-{(i % 30) + 1:02d}",
            "category": random.choice(["electronics", "clothing", "books", "home"])
        })
    return spark.createDataFrame(data)

# Create test data
print("📊 Creating large dataset for performance testing...")
df = create_large_dataset(spark, 10000)
print(f"Created {df.count()} records")

# Build pipeline with parallel execution
builder = PipelineBuilder(
    spark=spark,
    schema="progressive_demo",
    min_bronze_rate=95.0,
    min_silver_rate=98.0,
    min_gold_rate=99.0,
    enable_parallel_silver=True,      # Enable parallel Silver execution
    max_parallel_workers=4,           # Maximum 4 parallel workers
    verbose=True
)

# Bronze: Data ingestion
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "value": [F.col("value") > 0],
        "date": [F.col("date").isNotNull()]
    }
)

# Silver 1: Clean events
def clean_events(spark, bronze_df, prior_silvers):
    """Clean event data."""
    return (bronze_df
        .filter(F.col("value") > 0)
        .filter(F.col("action").isin(["click", "view", "purchase"]))
        .withColumn("processed_date", F.to_date("date"))
        .withColumn("value_category",
            F.when(F.col("value") > 200, "high_value")
            .when(F.col("value") > 100, "medium_value")
            .otherwise("low_value")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_events,
    rules={
        "value_category": [F.col("value_category").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="clean_events"
)

# Silver 2: User analytics (runs in parallel with clean_events)
def create_user_analytics(spark, bronze_df, prior_silvers):
    """Create user analytics."""
    return (bronze_df
        .filter(F.col("value") > 0)
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum("value").alias("total_spent"),
            F.avg("value").alias("avg_value"),
            F.countDistinct("action").alias("action_types")
        )
        .withColumn("user_segment",
            F.when(F.col("total_spent") > 500, "high_value")
            .when(F.col("total_spent") > 200, "medium_value")
            .otherwise("low_value")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="user_analytics",
    source_bronze="events",
    transform=create_user_analytics,
    rules={
        "user_segment": [F.col("user_segment").isNotNull()],
        "total_events": [F.col("total_events") > 0],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="user_analytics"
)

# Silver 3: Category analytics (runs in parallel)
def create_category_analytics(spark, bronze_df, prior_silvers):
    """Create category analytics."""
    return (bronze_df
        .filter(F.col("value") > 0)
        .groupBy("category")
        .agg(
            F.count("*").alias("total_events"),
            F.sum("value").alias("total_revenue"),
            F.avg("value").alias("avg_value"),
            F.countDistinct("user_id").alias("unique_users")
        )
        .withColumn("category_performance",
            F.when(F.col("total_revenue") > 10000, "high_performing")
            .when(F.col("total_revenue") > 5000, "medium_performing")
            .otherwise("low_performing")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="category_analytics",
    source_bronze="events",
    transform=create_category_analytics,
    rules={
        "category_performance": [F.col("category_performance").isNotNull()],
        "total_events": [F.col("total_events") > 0],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="category_analytics"
)

# Gold: Combined analytics (depends on all Silver steps)
def create_combined_analytics(spark, silvers):
    """Create combined analytics from all Silver steps."""
    clean_events_df = silvers["clean_events"]
    user_analytics_df = silvers["user_analytics"]
    category_analytics_df = silvers["category_analytics"]

    # Daily analytics
    daily_analytics = (clean_events_df
        .groupBy("processed_date")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.sum("value").alias("total_revenue"),
            F.avg("value").alias("avg_value")
        )
    )

    return daily_analytics

builder.add_gold_transform(
    name="combined_analytics",
    transform=create_combined_analytics,
    rules={
        "total_events": [F.col("total_events") > 0],
        "unique_users": [F.col("unique_users") > 0],
        "total_revenue": [F.col("total_revenue") > 0]
    },
    table_name="combined_analytics",
    source_silvers=["clean_events", "user_analytics", "category_analytics"]
)

# Create and run pipeline with timing
print("🚀 Building pipeline with parallel execution...")
pipeline = builder.to_pipeline()

print("⏱️  Executing pipeline (timing performance)...")
start_time = time.time()
result = pipeline.initial_load(bronze_sources={"events": df})
end_time = time.time()

execution_time = end_time - start_time

print(f"\n✅ Parallel pipeline completed: {result.success}")
print(f"📈 Rows processed: {result.totals['total_rows_written']}")
print(f"⏱️  Execution time: {execution_time:.2f} seconds")
print(f"🎯 Overall validation rate: {result.totals.get('overall_validation_rate', 0):.1f}%")

# Show results from all layers
print("\n📊 Results from all layers:")
print("\n🥉 Bronze Layer (sample):")
spark.table("progressive_demo.events").show(5)

print("\n🥈 Silver Layer - Clean Events (sample):")
spark.table("progressive_demo.clean_events").show(5)

print("\n🥈 Silver Layer - User Analytics (sample):")
spark.table("progressive_demo.user_analytics").show(5)

print("\n🥈 Silver Layer - Category Analytics:")
spark.table("progressive_demo.category_analytics").show()

print("\n🥇 Gold Layer - Combined Analytics:")
spark.table("progressive_demo.combined_analytics").show()
```

**Key Concepts Learned:**
- Parallel execution configuration
- Performance optimization
- Dependency management across layers

---

## Summary

🎉 **Congratulations!** You've learned SparkForge progressively:

1. **Bronze Layer**: Data ingestion and validation
2. **Silver Layer**: Data transformation and cleaning
3. **Gold Layer**: Business analytics and aggregations
4. **Validation**: Data quality control and monitoring
5. **Parallel Execution**: Performance optimization

## Next Steps

Now that you understand the fundamentals:

- **[E-commerce Analytics](USECASE_ECOMMERCE.md)** - Build a real business pipeline
- **[IoT Data Processing](USECASE_IOT.md)** - Process sensor data and detect anomalies
- **[Business Intelligence](USECASE_BI.md)** - Create executive dashboards and KPIs
- **[User Guide](USER_GUIDE.md)** - Learn advanced features and patterns

## Key Takeaways

- **Start Simple**: Begin with Bronze-only pipelines
- **Build Gradually**: Add Silver transformations, then Gold analytics
- **Validate Early**: Set appropriate validation thresholds
- **Optimize Later**: Enable parallel execution for performance
- **Monitor Quality**: Track validation rates and data quality metrics

---

**🚀 You're ready to build production pipelines!** Choose a use case guide and start building something real.

# E-commerce Analytics Quick Start

Build a complete e-commerce analytics pipeline in 10 minutes! This guide shows you how to process order data, create customer profiles, and generate business insights.

## What You'll Build

- **Order Processing**: Ingest and validate order data
- **Customer Analytics**: Create customer profiles and segmentation
- **Sales Analytics**: Daily revenue, product performance, regional analysis
- **Business Intelligence**: KPIs and actionable insights

## Prerequisites

- Python 3.8+ with SparkForge installed
- Basic understanding of e-commerce data

## Step 1: Setup (1 minute)

```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime, timedelta
import random

# Initialize Spark
spark = SparkSession.builder \
    .appName("E-commerce Analytics") \
    .master("local[*]") \
    .getOrCreate()

# Create sample e-commerce data
def create_sample_orders(spark, num_orders=1000):
    """Create realistic e-commerce order data."""
    orders = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_orders):
        orders.append({
            "order_id": f"ORD_{i:06d}",
            "customer_id": f"CUST_{random.randint(1, 500):04d}",
            "product_id": f"PROD_{random.randint(1, 100):03d}",
            "product_name": random.choice(["Laptop", "Phone", "Tablet", "Headphones", "Mouse", "Keyboard"]),
            "category": random.choice(["Electronics", "Accessories", "Computers"]),
            "quantity": random.randint(1, 5),
            "unit_price": round(random.uniform(20, 2000), 2),
            "total_amount": 0,  # Will calculate
            "order_date": base_date + timedelta(days=random.randint(0, 30)),
            "region": random.choice(["North", "South", "East", "West"]),
            "customer_tier": random.choice(["Bronze", "Silver", "Gold", "Platinum"])
        })

    df = spark.createDataFrame(orders)
    return df.withColumn("total_amount", F.col("quantity") * F.col("unit_price"))

# Create the data
print("🛒 Creating sample e-commerce data...")
orders_df = create_sample_orders(spark, 1000)
print(f"Created {orders_df.count()} orders")
orders_df.show(5)
```

## Step 2: Build the Pipeline (5 minutes)

```python
# Configure pipeline for e-commerce
builder = PipelineBuilder(
    spark=spark,
    schema="ecommerce_analytics",
    min_bronze_rate=95.0,  # 95% of orders must be valid
    min_silver_rate=98.0,  # 98% of processed orders must be valid
    min_gold_rate=99.0,    # 99% of analytics must be valid
    enable_parallel_silver=True,
    max_parallel_workers=4,
    verbose=True
)

# Bronze Layer: Raw Order Data
print("🥉 Building Bronze Layer - Order Ingestion...")
builder.with_bronze_rules(
    name="orders",
    rules={
        "order_id": [F.col("order_id").isNotNull()],
        "customer_id": [F.col("customer_id").isNotNull()],
        "total_amount": [F.col("total_amount") > 0],
        "order_date": [F.col("order_date").isNotNull()],
        "region": [F.col("region").isNotNull()]
    },
    incremental_col="order_date",
    description="Raw order data ingestion with validation"
)

# Silver Layer 1: Enriched Orders
print("🥈 Building Silver Layer - Order Enrichment...")

def enrich_orders(spark, bronze_df, prior_silvers):
    """Enrich orders with business logic."""
    return (bronze_df
        .withColumn("order_month", F.date_trunc("month", "order_date"))
        .withColumn("order_week", F.date_trunc("week", "order_date"))
        .withColumn("is_weekend", F.dayofweek("order_date").isin([1, 7]))
        .withColumn("order_size_category",
            F.when(F.col("total_amount") > 1000, "large")
            .when(F.col("total_amount") > 500, "medium")
            .otherwise("small")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="enriched_orders",
    source_bronze="orders",
    transform=enrich_orders,
    rules={
        "order_month": [F.col("order_month").isNotNull()],
        "order_size_category": [F.col("order_size_category").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="enriched_orders",
    watermark_col="order_date",
    description="Orders enriched with business logic and time dimensions"
)

# Silver Layer 2: Customer Profiles
print("🥈 Building Silver Layer - Customer Profiles...")

def create_customer_profiles(spark, bronze_df, prior_silvers):
    """Create customer profiles from order data."""
    return (bronze_df
        .groupBy("customer_id", "customer_tier", "region")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("total_amount").alias("total_spent"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("product_category").alias("categories_purchased")
        )
        .withColumn("customer_lifetime_days",
            F.datediff("last_order_date", "first_order_date"))
        .withColumn("orders_per_month",
            F.when(F.col("customer_lifetime_days") > 0,
                F.col("total_orders") / (F.col("customer_lifetime_days") / 30))
            .otherwise(0))
    )

builder.add_silver_transform(
    name="customer_profiles",
    source_bronze="orders",
    transform=create_customer_profiles,
    rules={
        "total_orders": [F.col("total_orders") > 0],
        "total_spent": [F.col("total_spent") > 0],
        "customer_lifetime_days": [F.col("customer_lifetime_days") >= 0]
    },
    table_name="customer_profiles",
    description="Customer profiles with lifetime value and behavior metrics"
)

# Gold Layer 1: Daily Sales Analytics
print("🥇 Building Gold Layer - Daily Sales Analytics...")

def daily_sales_analytics(spark, silvers):
    """Daily sales analytics and KPIs."""
    orders_df = silvers["enriched_orders"]

    return (orders_df
        .groupBy("order_date", "region")
        .agg(
            F.count("*").alias("daily_orders"),
            F.sum("total_amount").alias("daily_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("is_weekend"), 1).otherwise(0)).alias("weekend_orders")
        )
        .withColumn("week_over_week_growth", F.lit(0))  # Would calculate in real pipeline
    )

builder.add_gold_transform(
    name="daily_sales_analytics",
    transform=daily_sales_analytics,
    rules={
        "order_date": [F.col("order_date").isNotNull()],
        "daily_revenue": [F.col("daily_revenue") > 0],
        "unique_customers": [F.col("unique_customers") > 0]
    },
    table_name="daily_sales_analytics",
    source_silvers=["enriched_orders"],
    description="Daily sales analytics with regional breakdown"
)

# Gold Layer 2: Product Performance
print("🥇 Building Gold Layer - Product Performance...")

def product_performance(spark, silvers):
    """Product performance analytics."""
    orders_df = silvers["enriched_orders"]

    return (orders_df
        .groupBy("product_name", "category", "region")
        .agg(
            F.count("*").alias("units_sold"),
            F.sum("total_amount").alias("revenue"),
            F.avg("unit_price").alias("avg_price"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("quantity").alias("total_quantity")
        )
        .withColumn("revenue_per_unit", F.col("revenue") / F.col("units_sold"))
        .orderBy(F.desc("revenue"))
    )

builder.add_gold_transform(
    name="product_performance",
    transform=product_performance,
    rules={
        "product_name": [F.col("product_name").isNotNull()],
        "revenue": [F.col("revenue") > 0],
        "units_sold": [F.col("units_sold") > 0]
    },
    table_name="product_performance",
    source_silvers=["enriched_orders"],
    description="Product performance analytics by region and category"
)

# Gold Layer 3: Customer Segmentation
print("🥇 Building Gold Layer - Customer Segmentation...")

def customer_segmentation(spark, silvers):
    """Customer segmentation based on behavior and value."""
    customers_df = silvers["customer_profiles"]

    return (customers_df
        .withColumn("customer_segment",
            F.when(F.col("total_spent") > 2000, "High Value")
            .when(F.col("total_spent") > 1000, "Medium Value")
            .when(F.col("orders_per_month") > 2, "Frequent Buyer")
            .otherwise("Standard")
        )
        .groupBy("customer_segment", "region")
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("total_spent").alias("avg_lifetime_value"),
            F.avg("orders_per_month").alias("avg_orders_per_month"),
            F.sum("total_spent").alias("total_segment_value")
        )
        .orderBy(F.desc("total_segment_value"))
    )

builder.add_gold_transform(
    name="customer_segmentation",
    transform=customer_segmentation,
    rules={
        "customer_segment": [F.col("customer_segment").isNotNull()],
        "customer_count": [F.col("customer_count") > 0],
        "avg_lifetime_value": [F.col("avg_lifetime_value") > 0]
    },
    table_name="customer_segmentation",
    source_silvers=["customer_profiles"],
    description="Customer segmentation with lifetime value analysis"
)
```

## Step 3: Execute the Pipeline (2 minutes)

```python
# Build and run the pipeline
print("🚀 Building complete e-commerce pipeline...")
pipeline = builder.to_pipeline()

print("📊 Executing pipeline...")
result = pipeline.initial_load(bronze_sources={"orders": orders_df})

# Check results
print(f"\n✅ Pipeline completed: {result.success}")
print(f"📈 Total rows processed: {result.totals['total_rows_written']}")
print(f"⏱️  Execution time: {result.totals['total_duration_secs']:.2f}s")
print(f"🎯 Overall validation rate: {result.totals.get('overall_validation_rate', 0):.1f}%")
```

## Step 4: Explore Your Analytics (2 minutes)

```python
# Show all created tables
print("\n📋 Created Analytics Tables:")
spark.sql("SHOW TABLES IN ecommerce_analytics").show()

# Daily Sales Analytics
print("\n📊 Daily Sales Analytics:")
spark.table("ecommerce_analytics.daily_sales_analytics").show(10)

# Product Performance
print("\n🏆 Top Products by Revenue:")
spark.table("ecommerce_analytics.product_performance").show(10)

# Customer Segmentation
print("\n👥 Customer Segmentation:")
spark.table("ecommerce_analytics.customer_segmentation").show()

# Customer Profiles (sample)
print("\n👤 Customer Profiles (sample):")
spark.table("ecommerce_analytics.customer_profiles").show(5)
```

## Step 5: Business Insights (Bonus)

```python
# Calculate key business metrics
print("\n💡 Key Business Insights:")

# Total revenue
total_revenue = spark.table("ecommerce_analytics.daily_sales_analytics").agg(F.sum("daily_revenue")).collect()[0][0]
print(f"💰 Total Revenue: ${total_revenue:,.2f}")

# Average order value
avg_order_value = spark.table("ecommerce_analytics.daily_sales_analytics").agg(F.avg("avg_order_value")).collect()[0][0]
print(f"🛒 Average Order Value: ${avg_order_value:.2f}")

# Top customer segment
top_segment = spark.table("ecommerce_analytics.customer_segmentation").orderBy(F.desc("total_segment_value")).first()
print(f"👑 Top Customer Segment: {top_segment.customer_segment} (${top_segment.total_segment_value:,.2f} value)")

# Weekend vs weekday performance
weekend_revenue = spark.table("ecommerce_analytics.daily_sales_analytics").agg(F.sum("weekend_orders")).collect()[0][0]
total_orders = spark.table("ecommerce_analytics.daily_sales_analytics").agg(F.sum("daily_orders")).collect()[0][0]
weekend_percentage = (weekend_revenue / total_orders) * 100
print(f"📅 Weekend Orders: {weekend_percentage:.1f}% of total orders")
```

## What You've Built

🎉 **Congratulations!** You've created a complete e-commerce analytics pipeline with:

- **Data Ingestion**: Raw order data with validation
- **Order Enrichment**: Business logic and time dimensions
- **Customer Analytics**: Profiles, lifetime value, and segmentation
- **Sales Analytics**: Daily metrics and regional analysis
- **Product Analytics**: Performance by category and region
- **Business Intelligence**: KPIs and actionable insights

## Next Steps

### Try Incremental Processing
```python
# Add new orders and process incrementally
new_orders = create_sample_orders(spark, 100)  # 100 new orders
result = pipeline.run_incremental(bronze_sources={"orders": new_orders})
```

### Debug Individual Components
```python
# Test just the customer profiling step
customer_result = pipeline.execute_silver_step("customer_profiles")
print(f"Customer profiles created: {customer_result.output_count}")

# Test product performance analytics
product_result = pipeline.execute_gold_step("product_performance")
print(f"Products analyzed: {product_result.output_count}")
```

### Add More Analytics
- Seasonal trend analysis
- Customer churn prediction
- Inventory optimization
- A/B testing results
- Marketing campaign effectiveness

## Customization Ideas

1. **Add Real Data**: Replace sample data with your actual order data
2. **Custom Metrics**: Add your specific KPIs and business metrics
3. **Advanced Segmentation**: Use ML models for customer segmentation
4. **Real-time Processing**: Add streaming data sources
5. **External Integrations**: Connect to your CRM, marketing tools, or BI platforms

## Need Help?

- **[User Guide](USER_GUIDE.md)** - Learn advanced features
- **[API Reference](API_REFERENCE.md)** - Complete API documentation
- **[Examples](examples/)** - More working examples
- **[Troubleshooting](TROUBLESHOOTING.md)** - Common issues and solutions

---

**🚀 You're ready to build production e-commerce analytics!** Start with this foundation and customize it for your specific business needs.

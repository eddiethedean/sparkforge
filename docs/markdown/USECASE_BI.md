# Business Intelligence Quick Start

Build a complete business intelligence pipeline in 10 minutes! This guide shows you how to create KPIs, dashboards, and business analytics using SparkForge's medallion architecture.

## What You'll Build

- **Sales Analytics**: Revenue tracking, performance metrics, trend analysis
- **Customer Intelligence**: Customer segmentation, lifetime value, behavior analysis
- **Operational Metrics**: Efficiency KPIs, process optimization, resource utilization
- **Executive Dashboards**: High-level business metrics and strategic insights
- **Predictive Analytics**: Forecasting, trend prediction, business planning

## Prerequisites

- Python 3.8+ with SparkForge installed
- Basic understanding of business metrics and KPIs

## Step 1: Setup (1 minute)

```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime, timedelta
import random
import math

# Initialize Spark
spark = SparkSession.builder \
    .appName("Business Intelligence") \
    .master("local[*]") \
    .getOrCreate()

# Create comprehensive business data
def create_business_data(spark, num_records=10000):
    """Create realistic business data for BI analytics."""

    # Sales data
    sales_data = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_records):
        # Sales transactions
        sales_data.append({
            "transaction_id": f"TXN_{i:08d}",
            "customer_id": f"CUST_{random.randint(1, 2000):06d}",
            "product_id": f"PROD_{random.randint(1, 500):04d}",
            "product_category": random.choice(["Electronics", "Clothing", "Home", "Books", "Sports", "Beauty"]),
            "product_subcategory": random.choice(["Laptops", "Shirts", "Furniture", "Novels", "Equipment", "Skincare"]),
            "sales_amount": round(random.uniform(10, 2000), 2),
            "quantity": random.randint(1, 5),
            "discount_percent": round(random.uniform(0, 30), 2),
            "transaction_date": base_date + timedelta(days=random.randint(0, 365)),
            "sales_region": random.choice(["North", "South", "East", "West", "Central"]),
            "sales_rep": f"REP_{random.randint(1, 50):03d}",
            "channel": random.choice(["Online", "Store", "Phone", "Mobile"]),
            "payment_method": random.choice(["Credit Card", "Debit Card", "Cash", "Digital Wallet"]),
            "customer_segment": random.choice(["Premium", "Standard", "Value", "Enterprise"])
        })

    sales_df = spark.createDataFrame(sales_data)

    # Customer data
    customer_data = []
    for i in range(2000):
        customer_data.append({
            "customer_id": f"CUST_{i:06d}",
            "customer_name": f"Customer {i}",
            "customer_type": random.choice(["Individual", "Business", "Enterprise"]),
            "registration_date": base_date + timedelta(days=random.randint(-730, 0)),
            "customer_tier": random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
            "region": random.choice(["North", "South", "East", "West", "Central"]),
            "industry": random.choice(["Technology", "Healthcare", "Finance", "Retail", "Manufacturing", "Education"]),
            "company_size": random.choice(["Small", "Medium", "Large", "Enterprise"]),
            "annual_revenue": round(random.uniform(1000000, 100000000), 2),
            "credit_score": random.randint(300, 850)
        })

    customer_df = spark.createDataFrame(customer_data)

    # Employee data
    employee_data = []
    for i in range(200):
        employee_data.append({
            "employee_id": f"EMP_{i:04d}",
            "employee_name": f"Employee {i}",
            "department": random.choice(["Sales", "Marketing", "Finance", "Operations", "IT", "HR"]),
            "position": random.choice(["Manager", "Senior", "Junior", "Director", "VP"]),
            "hire_date": base_date + timedelta(days=random.randint(-1825, 0)),
            "salary": round(random.uniform(40000, 150000), 2),
            "performance_rating": round(random.uniform(1, 5), 1),
            "region": random.choice(["North", "South", "East", "West", "Central"])
        })

    employee_df = spark.createDataFrame(employee_data)

    return sales_df, customer_df, employee_df

# Create the data
print("📊 Creating comprehensive business data...")
sales_df, customer_df, employee_df = create_business_data(spark, 10000)
print(f"Created {sales_df.count()} sales transactions")
print(f"Created {customer_df.count()} customers")
print(f"Created {employee_df.count()} employees")

sales_df.show(5)
```

## Step 2: Build the BI Pipeline (5 minutes)

```python
# Configure pipeline for business intelligence
builder = PipelineBuilder(
    spark=spark,
    schema="business_intelligence",
    min_bronze_rate=95.0,  # High quality required for business data
    min_silver_rate=98.0,  # Very high quality for processed data
    min_gold_rate=99.0,    # Near perfect for business analytics
    enable_parallel_silver=True,
    max_parallel_workers=4,
    verbose=True
)

# Bronze Layer: Raw Business Data
print("🥉 Building Bronze Layer - Business Data Ingestion...")

# Sales data
builder.with_bronze_rules(
    name="sales_transactions",
    rules={
        "transaction_id": [F.col("transaction_id").isNotNull()],
        "customer_id": [F.col("customer_id").isNotNull()],
        "sales_amount": [F.col("sales_amount") > 0],
        "transaction_date": [F.col("transaction_date").isNotNull()],
        "product_category": [F.col("product_category").isNotNull()]
    },
    incremental_col="transaction_date",
    description="Raw sales transaction data ingestion"
)

# Customer data
builder.with_bronze_rules(
    name="customers",
    rules={
        "customer_id": [F.col("customer_id").isNotNull()],
        "customer_name": [F.col("customer_name").isNotNull()],
        "customer_tier": [F.col("customer_tier").isNotNull()],
        "registration_date": [F.col("registration_date").isNotNull()]
    },
    description="Raw customer data ingestion"
)

# Employee data
builder.with_bronze_rules(
    name="employees",
    rules={
        "employee_id": [F.col("employee_id").isNotNull()],
        "employee_name": [F.col("employee_name").isNotNull()],
        "department": [F.col("department").isNotNull()],
        "salary": [F.col("salary") > 0]
    },
    description="Raw employee data ingestion"
)

# Silver Layer 1: Enhanced Sales Analytics
print("🥈 Building Silver Layer - Enhanced Sales Analytics...")

def enhance_sales_data(spark, bronze_df, prior_silvers):
    """Enhance sales data with business calculations."""
    return (bronze_df
        .withColumn("discount_amount", F.col("sales_amount") * F.col("discount_percent") / 100)
        .withColumn("net_sales_amount", F.col("sales_amount") - F.col("discount_amount"))
        .withColumn("transaction_month", F.date_trunc("month", "transaction_date"))
        .withColumn("transaction_quarter", F.quarter("transaction_date"))
        .withColumn("transaction_year", F.year("transaction_date"))
        .withColumn("day_of_week", F.dayofweek("transaction_date"))
        .withColumn("is_weekend", F.dayofweek("transaction_date").isin([1, 7]))
        .withColumn("is_holiday_season",
            F.month("transaction_date").isin([11, 12]))  # Nov-Dec
        .withColumn("sales_tier",
            F.when(F.col("net_sales_amount") > 1000, "High Value")
            .when(F.col("net_sales_amount") > 500, "Medium Value")
            .otherwise("Standard")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="enhanced_sales",
    source_bronze="sales_transactions",
    transform=enhance_sales_data,
    rules={
        "net_sales_amount": [F.col("net_sales_amount") > 0],
        "transaction_month": [F.col("transaction_month").isNotNull()],
        "sales_tier": [F.col("sales_tier").isNotNull()]
    },
    table_name="enhanced_sales",
    watermark_col="transaction_date",
    description="Enhanced sales data with business calculations and time dimensions"
)

# Silver Layer 2: Customer Intelligence
print("🥈 Building Silver Layer - Customer Intelligence...")

def create_customer_intelligence(spark, bronze_df, prior_silvers):
    """Create customer intelligence and segmentation."""
    return (bronze_df
        .withColumn("customer_age_days",
            F.datediff(F.current_date(), "registration_date"))
        .withColumn("customer_age_months", F.col("customer_age_days") / 30)
        .withColumn("customer_age_years", F.col("customer_age_days") / 365)
        .withColumn("customer_lifecycle_stage",
            F.when(F.col("customer_age_days") < 30, "New")
            .when(F.col("customer_age_days") < 90, "Recent")
            .when(F.col("customer_age_days") < 365, "Established")
            .otherwise("Mature")
        )
        .withColumn("is_premium_customer",
            F.col("customer_tier").isin(["Gold", "Platinum"])
        )
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="customer_intelligence",
    source_bronze="customers",
    transform=create_customer_intelligence,
    rules={
        "customer_lifecycle_stage": [F.col("customer_lifecycle_stage").isNotNull()],
        "is_premium_customer": [F.col("is_premium_customer").isNotNull()]
    },
    table_name="customer_intelligence",
    description="Customer intelligence with lifecycle stages and premium classification"
)

# Silver Layer 3: Employee Performance
print("🥈 Building Silver Layer - Employee Performance...")

def create_employee_performance(spark, bronze_df, prior_silvers):
    """Create employee performance analytics."""
    return (bronze_df
        .withColumn("tenure_days", F.datediff(F.current_date(), "hire_date"))
        .withColumn("tenure_years", F.col("tenure_days") / 365)
        .withColumn("performance_category",
            F.when(F.col("performance_rating") >= 4.5, "Excellent")
            .when(F.col("performance_rating") >= 4.0, "Very Good")
            .when(F.col("performance_rating") >= 3.5, "Good")
            .when(F.col("performance_rating") >= 3.0, "Satisfactory")
            .otherwise("Needs Improvement")
        )
        .withColumn("salary_band",
            F.when(F.col("salary") >= 100000, "High")
            .when(F.col("salary") >= 70000, "Medium")
            .otherwise("Entry")
        )
        .withColumn("is_management", F.col("position").isin(["Manager", "Director", "VP"]))
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="employee_performance",
    source_bronze="employees",
    transform=create_employee_performance,
    rules={
        "performance_category": [F.col("performance_category").isNotNull()],
        "salary_band": [F.col("salary_band").isNotNull()],
        "is_management": [F.col("is_management").isNotNull()]
    },
    table_name="employee_performance",
    description="Employee performance analytics with tenure and compensation analysis"
)

# Gold Layer 1: Executive Dashboard
print("🥇 Building Gold Layer - Executive Dashboard...")

def create_executive_dashboard(spark, silvers):
    """Create high-level executive dashboard metrics."""
    sales_df = silvers["enhanced_sales"]
    customers_df = silvers["customer_intelligence"]
    employees_df = silvers["employee_performance"]

    # Sales metrics
    sales_metrics = (sales_df
        .agg(
            F.sum("net_sales_amount").alias("total_revenue"),
            F.count("*").alias("total_transactions"),
            F.avg("net_sales_amount").alias("avg_transaction_value"),
            F.sum("discount_amount").alias("total_discounts"),
            F.countDistinct("customer_id").alias("active_customers"),
            F.countDistinct("product_id").alias("products_sold")
        )
    )

    # Customer metrics
    customer_metrics = (customers_df
        .agg(
            F.count("*").alias("total_customers"),
            F.sum(F.when(F.col("is_premium_customer"), 1).otherwise(0)).alias("premium_customers"),
            F.avg("customer_age_days").alias("avg_customer_age_days")
        )
    )

    # Employee metrics
    employee_metrics = (employees_df
        .agg(
            F.count("*").alias("total_employees"),
            F.avg("performance_rating").alias("avg_performance_rating"),
            F.avg("salary").alias("avg_salary"),
            F.sum(F.when(F.col("is_management"), 1).otherwise(0)).alias("management_count")
        )
    )

    # Combine all metrics
    return (sales_metrics
        .crossJoin(customer_metrics)
        .crossJoin(employee_metrics)
        .withColumn("revenue_per_customer", F.col("total_revenue") / F.col("total_customers"))
        .withColumn("premium_customer_rate", (F.col("premium_customers") / F.col("total_customers")) * 100)
        .withColumn("management_ratio", (F.col("management_count") / F.col("total_employees")) * 100)
        .withColumn("report_date", F.current_date())
    )

builder.add_gold_transform(
    name="executive_dashboard",
    transform=create_executive_dashboard,
    rules={
        "total_revenue": [F.col("total_revenue") > 0],
        "total_customers": [F.col("total_customers") > 0],
        "total_employees": [F.col("total_employees") > 0]
    },
    table_name="executive_dashboard",
    source_silvers=["enhanced_sales", "customer_intelligence", "employee_performance"],
    description="Executive dashboard with high-level business KPIs and metrics"
)

# Gold Layer 2: Sales Performance Analytics
print("🥇 Building Gold Layer - Sales Performance Analytics...")

def create_sales_performance(spark, silvers):
    """Create detailed sales performance analytics."""
    sales_df = silvers["enhanced_sales"]

    return (sales_df
        .groupBy("sales_region", "product_category", "transaction_month")
        .agg(
            F.sum("net_sales_amount").alias("monthly_revenue"),
            F.count("*").alias("transaction_count"),
            F.avg("net_sales_amount").alias("avg_transaction_value"),
            F.sum("discount_amount").alias("total_discounts"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("is_weekend"), 1).otherwise(0)).alias("weekend_transactions")
        )
        .withColumn("weekend_percentage",
            (F.col("weekend_transactions") / F.col("transaction_count")) * 100)
        .withColumn("discount_rate",
            (F.col("total_discounts") / (F.col("monthly_revenue") + F.col("total_discounts"))) * 100)
        .withColumn("revenue_per_customer",
            F.col("monthly_revenue") / F.col("unique_customers"))
    )

builder.add_gold_transform(
    name="sales_performance",
    transform=create_sales_performance,
    rules={
        "monthly_revenue": [F.col("monthly_revenue") > 0],
        "transaction_count": [F.col("transaction_count") > 0],
        "unique_customers": [F.col("unique_customers") > 0]
    },
    table_name="sales_performance",
    source_silvers=["enhanced_sales"],
    description="Sales performance analytics by region, category, and time"
)

# Gold Layer 3: Customer Analytics
print("🥇 Building Gold Layer - Customer Analytics...")

def create_customer_analytics(spark, silvers):
    """Create customer analytics and segmentation."""
    customers_df = silvers["customer_intelligence"]

    return (customers_df
        .groupBy("customer_lifecycle_stage", "customer_tier", "region", "industry")
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("customer_age_days").alias("avg_customer_age"),
            F.avg("credit_score").alias("avg_credit_score"),
            F.sum(F.when(F.col("is_premium_customer"), 1).otherwise(0)).alias("premium_count")
        )
        .withColumn("premium_rate",
            (F.col("premium_count") / F.col("customer_count")) * 100)
        .withColumn("customer_segment_score",
            F.when(F.col("customer_tier") == "Platinum", 100)
            .when(F.col("customer_tier") == "Gold", 80)
            .when(F.col("customer_tier") == "Silver", 60)
            .otherwise(40)
        )
        .orderBy(F.desc("customer_segment_score"), F.desc("customer_count"))
    )

builder.add_gold_transform(
    name="customer_analytics",
    transform=create_customer_analytics,
    rules={
        "customer_count": [F.col("customer_count") > 0],
        "avg_customer_age": [F.col("avg_customer_age") > 0],
        "premium_rate": [F.col("premium_rate") >= 0]
    },
    table_name="customer_analytics",
    source_silvers=["customer_intelligence"],
    description="Customer analytics with segmentation and lifecycle analysis"
)

# Gold Layer 4: Operational Metrics
print("🥇 Building Gold Layer - Operational Metrics...")

def create_operational_metrics(spark, silvers):
    """Create operational efficiency metrics."""
    employees_df = silvers["employee_performance"]

    return (employees_df
        .groupBy("department", "region", "position")
        .agg(
            F.count("*").alias("employee_count"),
            F.avg("performance_rating").alias("avg_performance"),
            F.avg("salary").alias("avg_salary"),
            F.avg("tenure_years").alias("avg_tenure"),
            F.sum(F.when(F.col("is_management"), 1).otherwise(0)).alias("managers")
        )
        .withColumn("management_ratio",
            (F.col("managers") / F.col("employee_count")) * 100)
        .withColumn("salary_efficiency_score",
            F.col("avg_performance") / (F.col("avg_salary") / 1000)
        )
        .withColumn("department_health_score",
            (F.col("avg_performance") * 0.4) +
            (F.col("salary_efficiency_score") * 0.3) +
            ((F.col("avg_tenure") / 10) * 0.3)
        )
        .orderBy(F.desc("department_health_score"))
    )

builder.add_gold_transform(
    name="operational_metrics",
    transform=create_operational_metrics,
    rules={
        "employee_count": [F.col("employee_count") > 0],
        "avg_performance": [F.col("avg_performance") > 0],
        "avg_salary": [F.col("avg_salary") > 0]
    },
    table_name="operational_metrics",
    source_silvers=["employee_performance"],
    description="Operational metrics with department efficiency and performance analysis"
)
```

## Step 3: Execute the BI Pipeline (2 minutes)

```python
# Build and run the pipeline
print("🚀 Building complete business intelligence pipeline...")
pipeline = builder.to_pipeline()

print("📊 Executing pipeline...")
result = pipeline.initial_load(bronze_sources={
    "sales_transactions": sales_df,
    "customers": customer_df,
    "employees": employee_df
})

# Check results
print(f"\n✅ Pipeline completed: {result.success}")
print(f"📈 Total rows processed: {result.totals['total_rows_written']}")
print(f"⏱️  Execution time: {result.totals['total_duration_secs']:.2f}s")
print(f"🎯 Overall validation rate: {result.totals.get('overall_validation_rate', 0):.1f}%")
```

## Step 4: Explore Your BI Analytics (2 minutes)

```python
# Show all created tables
print("\n📋 Created Business Intelligence Tables:")
spark.sql("SHOW TABLES IN business_intelligence").show()

# Executive Dashboard
print("\n👔 Executive Dashboard - Key Business Metrics:")
executive_dashboard = spark.table("business_intelligence.executive_dashboard")
executive_dashboard.show(truncate=False)

# Sales Performance
print("\n📈 Sales Performance by Region and Category:")
spark.table("business_intelligence.sales_performance").show(10)

# Customer Analytics
print("\n👥 Customer Analytics and Segmentation:")
spark.table("business_intelligence.customer_analytics").show(10)

# Operational Metrics
print("\n🏢 Operational Metrics by Department:")
spark.table("business_intelligence.operational_metrics").show(10)
```

## Step 5: Business Intelligence Insights (Bonus)

```python
# Calculate key business insights
print("\n💡 Key Business Intelligence Insights:")

# Financial metrics
total_revenue = executive_dashboard.select("total_revenue").collect()[0][0]
avg_transaction = executive_dashboard.select("avg_transaction_value").collect()[0][0]
print(f"💰 Total Revenue: ${total_revenue:,.2f}")
print(f"🛒 Average Transaction Value: ${avg_transaction:.2f}")

# Customer insights
total_customers = executive_dashboard.select("total_customers").collect()[0][0]
premium_rate = executive_dashboard.select("premium_customer_rate").collect()[0][0]
print(f"👥 Total Customers: {total_customers:,}")
print(f"👑 Premium Customer Rate: {premium_rate:.1f}%")

# Operational insights
total_employees = executive_dashboard.select("total_employees").collect()[0][0]
avg_performance = executive_dashboard.select("avg_performance_rating").collect()[0][0]
print(f"👨‍💼 Total Employees: {total_employees}")
print(f"⭐ Average Performance Rating: {avg_performance:.1f}/5.0")

# Sales insights
sales_performance = spark.table("business_intelligence.sales_performance")
top_region = sales_performance.orderBy(F.desc("monthly_revenue")).first()
print(f"🏆 Top Performing Region: {top_region.sales_region} (${top_region.monthly_revenue:,.2f})")

# Department insights
operational_metrics = spark.table("business_intelligence.operational_metrics")
top_department = operational_metrics.orderBy(F.desc("department_health_score")).first()
print(f"🎯 Healthiest Department: {top_department.department} (Score: {top_department.department_health_score:.2f})")

# Customer lifecycle insights
customer_analytics = spark.table("business_intelligence.customer_analytics")
lifecycle_distribution = customer_analytics.groupBy("customer_lifecycle_stage").agg(
    F.sum("customer_count").alias("total_customers")
).collect()
print("\n🔄 Customer Lifecycle Distribution:")
for stage in lifecycle_distribution:
    percentage = (stage.total_customers / total_customers) * 100
    print(f"   {stage.customer_lifecycle_stage}: {stage.total_customers} customers ({percentage:.1f}%)")
```

## What You've Built

🎉 **Congratulations!** You've created a complete business intelligence pipeline with:

- **Executive Dashboard**: High-level KPIs and business metrics
- **Sales Analytics**: Revenue tracking, performance metrics, regional analysis
- **Customer Intelligence**: Segmentation, lifecycle analysis, premium classification
- **Operational Metrics**: Department efficiency, employee performance, resource utilization
- **Business Insights**: Actionable analytics for strategic decision-making

## Next Steps

### Try Incremental Updates
```python
# Add new business data and process incrementally
new_sales = create_business_data(spark, 1000)[0]  # New sales data
result = pipeline.run_incremental(bronze_sources={"sales_transactions": new_sales})
```

### Debug Individual Components
```python
# Test sales analytics
sales_result = pipeline.execute_silver_step("enhanced_sales")
print(f"Enhanced sales records: {sales_result.output_count}")

# Test customer intelligence
customer_result = pipeline.execute_silver_step("customer_intelligence")
print(f"Customer intelligence records: {customer_result.output_count}")
```

### Add Advanced Analytics
- **Predictive Analytics**: Revenue forecasting and trend prediction
- **Customer Lifetime Value**: CLV calculation and prediction models
- **Churn Analysis**: Customer retention and churn prediction
- **A/B Testing**: Marketing campaign effectiveness analysis
- **Financial Modeling**: Budget planning and variance analysis

## Customization Ideas

1. **Real Business Data**: Connect to your actual CRM, ERP, and sales systems
2. **Custom KPIs**: Add your specific business metrics and calculations
3. **Dashboard Integration**: Connect to Tableau, Power BI, or custom dashboards
4. **Alerting**: Set up automated alerts for key metric thresholds
5. **Scheduled Reports**: Automated daily/weekly/monthly business reports
6. **Mobile Analytics**: Mobile-friendly dashboards and reports

## BI-Specific Features

### Financial Analytics
```python
# Add financial modeling and analysis
def financial_analytics(spark, silvers):
    # Budget vs actual, variance analysis, financial forecasting
    pass
```

### Marketing Analytics
```python
# Add marketing campaign analysis
def marketing_analytics(spark, silvers):
    # Campaign ROI, customer acquisition cost, marketing attribution
    pass
```

### Competitive Analysis
```python
# Add competitive benchmarking
def competitive_analysis(spark, silvers):
    # Market share, competitive positioning, benchmark comparisons
    pass
```

## Dashboard Integration Examples

### Tableau Integration
```python
# Export data for Tableau
spark.table("business_intelligence.executive_dashboard").write.mode("overwrite").parquet("tableau_data/executive_dashboard")
```

### Power BI Integration
```python
# Export data for Power BI
spark.table("business_intelligence.sales_performance").write.mode("overwrite").csv("powerbi_data/sales_performance")
```

### Custom Dashboard
```python
# Create JSON for custom dashboard
import json
executive_data = spark.table("business_intelligence.executive_dashboard").toJSON().collect()
dashboard_json = json.dumps(executive_data, indent=2)
```

## Need Help?

- **[User Guide](USER_GUIDE.md)** - Learn advanced features
- **[API Reference](API_REFERENCE.md)** - Complete API documentation
- **[Examples](examples/)** - More working examples
- **[Troubleshooting](TROUBLESHOOTING.md)** - Common issues and solutions

---

**🚀 You're ready to build production business intelligence!** Start with this foundation and customize it for your specific business needs and KPIs.

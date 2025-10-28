#!/usr/bin/env python3
"""
E-commerce Analytics Pipeline Example

This example demonstrates a complete e-commerce analytics pipeline using SparkForge.
It processes order data through Bronze → Silver → Gold layers to create business insights.
"""

import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline_builder import PipelineBuilder


def create_sample_data(spark):
    """Create sample e-commerce data for demonstration."""

    # Sample order data
    orders_data = []
    base_time = datetime(2024, 1, 1, 10, 0, 0)

    for i in range(1000):
        orders_data.append(
            {
                "order_id": f"ORD_{i:06d}",
                "customer_id": f"CUST_{i % 100:03d}",
                "product_id": f"PROD_{i % 50:03d}",
                "product_category": random.choice(
                    ["Electronics", "Clothing", "Books", "Home", "Sports"]
                ),
                "quantity": random.randint(1, 5),
                "unit_price": round(random.uniform(10.0, 500.0), 2),
                "discount": round(random.uniform(0.0, 0.3), 2),
                "order_date": base_time + timedelta(hours=i),
                "customer_segment": random.choice(["Premium", "Standard", "Basic"]),
                "payment_method": random.choice(
                    ["Credit Card", "PayPal", "Bank Transfer"]
                ),
                "shipping_region": random.choice(
                    ["North", "South", "East", "West", "Central"]
                ),
            }
        )

    return spark.createDataFrame(orders_data)


def main():
    """Main function to run the e-commerce analytics pipeline."""

    print("🛒 E-commerce Analytics Pipeline Example")
    print("=" * 50)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("E-commerce Analytics Pipeline")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # Create sample data
        print("📊 Creating sample e-commerce data...")
        orders_df = create_sample_data(spark)
        print(f"Created {orders_df.count()} sample orders")

        # Build pipeline
        print("\n🏗️ Building pipeline...")
        builder = PipelineBuilder(
            spark=spark,
            schema="ecommerce_analytics",
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=True,
        )

        # Bronze Layer: Raw orders
        print("🥉 Setting up Bronze layer...")
        builder.with_bronze_rules(
            name="orders",
            rules={
                "order_id": [F.col("order_id").isNotNull()],
                "customer_id": [F.col("customer_id").isNotNull()],
                "product_id": [F.col("product_id").isNotNull()],
                "quantity": [F.col("quantity") > 0],
                "unit_price": [F.col("unit_price") > 0],
                "order_date": [F.col("order_date").isNotNull()],
            },
            incremental_col="order_date",
        )

        # Silver Layer: Enriched orders
        print("🥈 Setting up Silver layer...")

        def enrich_orders(spark, bronze_df, prior_silvers):
            return (
                bronze_df.withColumn(
                    "total_amount", F.col("quantity") * F.col("unit_price")
                )
                .withColumn(
                    "discount_amount", F.col("total_amount") * F.col("discount")
                )
                .withColumn(
                    "final_amount", F.col("total_amount") - F.col("discount_amount")
                )
                .withColumn("order_date", F.date_trunc("day", "order_date"))
                .withColumn("is_weekend", F.dayofweek("order_date").isin([1, 7]))
                .withColumn("is_high_value", F.col("final_amount") > 200)
                .withColumn("order_hour", F.hour("order_date"))
                .withColumn("is_peak_hour", F.col("order_hour").between(10, 16))
            )

        builder.add_silver_transform(
            name="enriched_orders",
            source_bronze="orders",
            transform=enrich_orders,
            rules={
                "total_amount": [F.col("total_amount") > 0],
                "final_amount": [F.col("final_amount") > 0],
                "order_date": [F.col("order_date").isNotNull()],
                "is_high_value": [F.col("is_high_value").isNotNull()],
            },
            table_name="enriched_orders",
            watermark_col="order_date",
        )

        # Silver Layer: Customer profiles
        def create_customer_profiles(spark, bronze_df, prior_silvers):
            enriched_orders = prior_silvers["enriched_orders"]
            return (
                enriched_orders.groupBy("customer_id")
                .agg(
                    F.count("*").alias("total_orders"),
                    F.sum("final_amount").alias("total_spent"),
                    F.avg("final_amount").alias("avg_order_value"),
                    F.max("order_date").alias("last_order_date"),
                    F.first("customer_segment").alias("customer_segment"),
                    F.first("shipping_region").alias("preferred_region"),
                )
                .withColumn(
                    "customer_tier",
                    F.when(F.col("total_spent") > 1000, "VIP")
                    .when(F.col("total_spent") > 500, "Gold")
                    .when(F.col("total_spent") > 100, "Silver")
                    .otherwise("Bronze"),
                )
            )

        builder.add_silver_transform(
            name="customer_profiles",
            source_bronze="orders",
            transform=create_customer_profiles,
            rules={
                "customer_id": [F.col("customer_id").isNotNull()],
                "total_orders": [F.col("total_orders") > 0],
                "total_spent": [F.col("total_spent") > 0],
                "customer_tier": [F.col("customer_tier").isNotNull()],
            },
            table_name="customer_profiles",
            source_silvers=["enriched_orders"],
        )

        # Gold Layer: Daily sales summary
        print("🥇 Setting up Gold layer...")

        def daily_sales_summary(spark, silvers):
            enriched_orders = silvers["enriched_orders"]
            return (
                enriched_orders.groupBy("order_date", "product_category")
                .agg(
                    F.count("*").alias("order_count"),
                    F.sum("final_amount").alias("total_revenue"),
                    F.avg("final_amount").alias("avg_order_value"),
                    F.countDistinct("customer_id").alias("unique_customers"),
                    F.sum("is_high_value").alias("high_value_orders"),
                )
                .withColumn(
                    "revenue_per_customer",
                    F.col("total_revenue") / F.col("unique_customers"),
                )
            )

        builder.add_gold_transform(
            name="daily_sales_summary",
            transform=daily_sales_summary,
            rules={
                "order_date": [F.col("order_date").isNotNull()],
                "product_category": [F.col("product_category").isNotNull()],
                "total_revenue": [F.col("total_revenue") > 0],
            },
            table_name="daily_sales_summary",
            source_silvers=["enriched_orders"],
        )

        # Gold Layer: Customer analytics
        def customer_analytics(spark, silvers):
            customer_profiles = silvers["customer_profiles"]
            return (
                customer_profiles.groupBy("customer_tier", "preferred_region")
                .agg(
                    F.count("*").alias("customer_count"),
                    F.avg("total_spent").alias("avg_spent"),
                    F.avg("total_orders").alias("avg_orders"),
                    F.sum("total_spent").alias("total_revenue"),
                )
                .withColumn(
                    "revenue_share",
                    F.col("total_revenue") / F.sum("total_revenue").over(),
                )
            )

        builder.add_gold_transform(
            name="customer_analytics",
            transform=customer_analytics,
            rules={
                "customer_tier": [F.col("customer_tier").isNotNull()],
                "preferred_region": [F.col("preferred_region").isNotNull()],
                "customer_count": [F.col("customer_count") > 0],
            },
            table_name="customer_analytics",
            source_silvers=["customer_profiles"],
        )

        # Build and execute pipeline
        print("\n🚀 Building pipeline...")
        pipeline = builder.to_pipeline()

        print("\n📋 Pipeline steps:")
        steps = pipeline.list_steps()
        print(f"Bronze steps: {steps['bronze']}")
        print(f"Silver steps: {steps['silver']}")
        print(f"Gold steps: {steps['gold']}")

        # Execute pipeline
        print("\n⚡ Executing pipeline...")
        result = pipeline.initial_load(bronze_sources={"orders": orders_df})

        # Display results
        print("\n📊 Pipeline Results:")
        print(f"Success: {result.success}")
        print(f"Total rows written: {result.totals['total_rows_written']}")
        print(f"Execution time: {result.totals['total_duration_secs']:.2f}s")

        if result.success:
            print("\n✅ Pipeline completed successfully!")

            # Show sample results
            print("\n📈 Sample Results:")

            # Daily sales summary
            print("\nDaily Sales Summary (top 10):")
            spark.table("ecommerce_analytics.daily_sales_summary").show(10)

            # Customer analytics
            print("\nCustomer Analytics:")
            spark.table("ecommerce_analytics.customer_analytics").show()

            # Customer profiles sample
            print("\nCustomer Profiles (top 10):")
            spark.table("ecommerce_analytics.customer_profiles").show(10)

        else:
            print(f"\n❌ Pipeline failed: {result.error_message}")
            if result.failed_steps:
                print(f"Failed steps: {result.failed_steps}")

    except Exception as e:
        print(f"\n💥 Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        try:
            spark.sql("DROP DATABASE IF EXISTS ecommerce_analytics CASCADE")
            spark.stop()
            print("\n🧹 Cleanup completed")
        except Exception:
            pass


if __name__ == "__main__":
    main()

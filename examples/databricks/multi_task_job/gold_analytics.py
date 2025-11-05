#!/usr/bin/env python3
"""
Databricks Multi-Task Job: Gold Analytics

Task 3 of a multi-task Databricks job for production ETL pipelines.
This task handles business analytics and aggregations.

Depends on: Task 2 (Silver Processing)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    import dbutils
except ImportError:
    dbutils = None


def main():
    """
    Gold analytics task - creates business metrics.
    """

    print("=" * 80)
    print("DATABRICKS MULTI-TASK JOB - TASK 3: GOLD ANALYTICS")
    print("=" * 80)

    spark = SparkSession.builder.getOrCreate()

    try:
        from pipeline_builder import PipelineBuilder

        # ========================================================================
        # CONFIGURATION
        # ========================================================================

        if dbutils:
            dbutils.widgets.text("target_schema", "analytics")
            dbutils.widgets.text("run_date", "")

            target_schema = dbutils.widgets.get("target_schema")
            run_date = dbutils.widgets.get("run_date")
        else:
            target_schema = "analytics"
            run_date = ""

        print("Configuration:")
        print(f"  Target Schema: {target_schema}")
        print(f"  Run Date: {run_date}")

        # Check that Silver tables exist (from Task 2)
        print("\n" + "=" * 80)
        print("CHECKING SILVER TABLES")
        print("=" * 80)

        try:
            silver_count = spark.table(f"{target_schema}.enriched_events").count()
            print(f"✅ Found Silver table with {silver_count} rows")
        except Exception as e:
            print(f"❌ Silver table not found: {e}")
            print("⚠️  Run Task 2 (Silver Processing) first!")
            raise

        # ========================================================================
        # BUILD GOLD PIPELINE
        # ========================================================================

        print("\n" + "=" * 80)
        print("BUILDING GOLD ANALYTICS")
        print("=" * 80)

        builder = PipelineBuilder(
            spark=spark,
            schema=target_schema,
            min_silver_rate=95.0,
            min_gold_rate=98.0,
            verbose=True,
        )

        # Note: We're reading from existing Silver tables
        # So we need to define them using with_silver_rules

        builder.with_silver_rules(
            name="enriched_events",
            table_name="enriched_events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value").isNotNull(), F.col("value") > 0],
                "event_date": [F.col("event_date").isNotNull()],
            },
        )

        # Gold: Create daily analytics
        def daily_analytics(spark, silvers):
            return (
                silvers["enriched_events"]
                .groupBy("event_date")
                .agg(
                    F.count("*").alias("total_events"),
                    F.countDistinct("user_id").alias("unique_users"),
                    F.sum(F.when(F.col("action") == "purchase", 1).otherwise(0)).alias(
                        "purchases"
                    ),
                    F.sum(F.when(F.col("action") == "click", 1).otherwise(0)).alias(
                        "clicks"
                    ),
                    F.sum(F.when(F.col("action") == "view", 1).otherwise(0)).alias(
                        "views"
                    ),
                    F.avg("value").alias("avg_value"),
                )
                .orderBy("event_date")
            )

        builder.add_gold_transform(
            name="daily_analytics",
            source_silvers=["enriched_events"],
            transform=daily_analytics,
            rules={
                "event_date": [F.col("event_date").isNotNull()],
                "total_events": [F.col("total_events") > 0],
                "unique_users": [F.col("unique_users") > 0],
                "purchases": [F.col("purchases") >= 0],
                "clicks": [F.col("clicks") >= 0],
                "views": [F.col("views") >= 0],
                "avg_value": [F.col("avg_value") > 0],
            },
            table_name="daily_analytics",
            description="Daily analytics and business metrics",
        )

        pipeline = builder.to_pipeline()

        # ========================================================================
        # EXECUTE GOLD ANALYTICS
        # ========================================================================

        print("\n" + "=" * 80)
        print("EXECUTING GOLD ANALYTICS")
        print("=" * 80)

        # For Gold transforms, we use a dummy bronze source since we're reading
        # from existing Silver tables
        from pyspark.sql import Row

        dummy_bronze = spark.createDataFrame([Row(dummy=1)], ["dummy"])

        result = pipeline.run_initial_load(bronze_sources={"dummy": dummy_bronze})

        print(f"✅ Gold analytics completed: {result.status}")
        print(f"   Rows written: {result.metrics.total_rows_written}")
        print(f"   Duration: {result.metrics.total_duration_secs:.2f}s")

        # Show results
        print("\n📊 Daily Analytics (Gold Layer):")
        gold_table = spark.table(f"{target_schema}.daily_analytics")
        gold_table.show(truncate=False)

        print("\n" + "=" * 80)
        print("✅ GOLD ANALYTICS COMPLETED")
        print("=" * 80)

        return result

    except Exception as e:
        print(f"\n❌ Gold analytics failed: {e}")
        raise


if __name__ == "__main__":
    result = main()

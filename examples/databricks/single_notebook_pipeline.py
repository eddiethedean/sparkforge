#!/usr/bin/env python3
"""
Databricks Single Notebook Pipeline Example

This example demonstrates how to run a complete PipelineBuilder pipeline
in a single Databricks notebook. This is the simplest deployment pattern.

Use this when:
- You want a single, self-contained pipeline
- All steps run sequentially in one job
- Simple workflows with minimal dependencies

Not suitable when:
- You need different cluster types per step
- Steps have very different resource requirements
- You want to parallelize across different clusters
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    # Databricks context is available in notebooks
    from pyspark.sql import SparkSession
    import dbutils
except ImportError:
    # For local testing
    dbutils = None


def main():
    """
    Main pipeline execution function for Databricks.
    """
    
    print("=" * 80)
    print("PIPELINEBUILDER DATABRICKS PIPELINE - SINGLE NOTEBOOK")
    print("=" * 80)
    
    # Initialize Spark (Databricks provides this)
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Import PipelineBuilder and LogWriter
        from pipeline_builder import PipelineBuilder, LogWriter
        
        # ========================================================================
        # CONFIGURATION
        # ========================================================================
        
        # Use Databricks widgets for parameterization
        if dbutils:
            dbutils.widgets.text("source_path", "/mnt/raw/user_events/")
            dbutils.widgets.text("target_schema", "analytics")
            dbutils.widgets.text("monitoring_schema", "monitoring")
            
            source_path = dbutils.widgets.get("source_path")
            target_schema = dbutils.widgets.get("target_schema")
            monitoring_schema = dbutils.widgets.get("monitoring_schema")
        else:
            # Defaults for local testing
            source_path = "/tmp/test_data/"
            target_schema = "analytics"
            monitoring_schema = "monitoring"
        
        print(f"Configuration:")
        print(f"  Source Path: {source_path}")
        print(f"  Target Schema: {target_schema}")
        print(f"  Monitoring Schema: {monitoring_schema}")
        
        # Create schemas if they don't exist
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {monitoring_schema}")
        
        # ========================================================================
        # LOAD SOURCE DATA
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("STEP 1: LOADING BRONZE DATA")
        print("=" * 80)
        
        # In production, load from your data source
        # For this example, we'll create sample data
        sample_data = [
            ("user1", "click", "2024-01-01 10:00:00", 100),
            ("user2", "view", "2024-01-01 11:00:00", 200),
            ("user3", "purchase", "2024-01-01 12:00:00", 300),
            ("user1", "click", "2024-01-01 13:00:00", 150),
            ("user2", "purchase", "2024-01-01 14:00:00", 250),
        ]
        
        source_df = spark.createDataFrame(
            sample_data,
            ["user_id", "action", "timestamp", "value"]
        )
        
        print(f"✅ Loaded {source_df.count()} rows")
        
        # ========================================================================
        # BUILD PIPELINE
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("STEP 2: BUILDING PIPELINE")
        print("=" * 80)
        
        builder = PipelineBuilder(
            spark=spark,
            schema=target_schema,
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=98.0,
            verbose=True
        )
        
        # Bronze: Raw data validation
        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
                "value": [F.col("value").isNotNull(), F.col("value") > 0]
            },
            incremental_col="timestamp",
            description="Raw event data ingestion"
        )
        
        # Silver: Cleaned and enriched data
        def enrich_events(spark, bronze_df, prior_silvers):
            return (
                bronze_df
                .withColumn("processed_at", F.current_timestamp())
                .withColumn("event_date", F.to_date("timestamp"))
                .withColumn("hour", F.hour("timestamp"))
                .filter(F.col("user_id").isNotNull())
                .select(
                    "user_id", "action", "value", 
                    "event_date", "processed_at", "hour"
                )
            )
        
        builder.add_silver_transform(
            name="enriched_events",
            source_bronze="events",
            transform=enrich_events,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value").isNotNull(), F.col("value") > 0],
                "event_date": [F.col("event_date").isNotNull()],
                "processed_at": [F.col("processed_at").isNotNull()],
                "hour": [F.col("hour").isNotNull()]
            },
            table_name="enriched_events",
            watermark_col="timestamp",
            description="Enriched event data with processing metadata"
        )
        
        # Gold: Business analytics
        def daily_analytics(spark, silvers):
            return (
                silvers["enriched_events"]
                .groupBy("event_date")
                .agg(
                    F.count("*").alias("total_events"),
                    F.countDistinct("user_id").alias("unique_users"),
                    F.sum(F.when(F.col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
                    F.sum(F.when(F.col("action") == "click", 1).otherwise(0)).alias("clicks"),
                    F.sum(F.when(F.col("action") == "view", 1).otherwise(0)).alias("views"),
                    F.avg("value").alias("avg_value")
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
                "avg_value": [F.col("avg_value") > 0]
            },
            table_name="daily_analytics",
            description="Daily analytics and business metrics"
        )
        
        # Build the pipeline
        pipeline = builder.to_pipeline()
        print("✅ Pipeline built successfully")
        
        # ========================================================================
        # EXECUTE PIPELINE
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("STEP 3: EXECUTING PIPELINE")
        print("=" * 80)
        
        result = pipeline.run_initial_load(bronze_sources={"events": source_df})
        
        # ========================================================================
        # LOG RESULTS
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("STEP 4: LOGGING EXECUTION RESULTS")
        print("=" * 80)
        
        writer = LogWriter(
            spark=spark,
            schema=monitoring_schema,
            table_name="pipeline_logs"
        )
        
        log_result = writer.append(result)
        print(f"✅ Logged {log_result['rows_written']} log entries")
        
        # ========================================================================
        # DISPLAY RESULTS
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Status: {result.status}")
        print(f"Total Steps: {result.metrics.total_steps}")
        print(f"Successful: {result.metrics.successful_steps}")
        print(f"Failed: {result.metrics.failed_steps}")
        print(f"Rows Written: {result.metrics.total_rows_written}")
        print(f"Duration: {result.metrics.total_duration_secs:.2f}s")
        
        # Show sample results
        print("\n📊 Daily Analytics (Gold Layer):")
        gold_table = spark.table(f"{target_schema}.daily_analytics")
        gold_table.show(truncate=False)
        
        print("\n🔍 Enriched Events (Silver Layer):")
        silver_table = spark.table(f"{target_schema}.enriched_events")
        silver_table.show(5, truncate=False)
        
        # ========================================================================
        # ERROR HANDLING
        # ========================================================================
        
        if result.status != "completed":
            error_msg = f"Pipeline failed: {result.errors}"
            print(f"\n❌ {error_msg}")
            
            # In production, you might want to send alerts here
            if dbutils:
                dbutils.notebook.exit(error_msg)
            else:
                raise Exception(error_msg)
        
        print("\n" + "=" * 80)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 80)
        
        return result
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        raise


if __name__ == "__main__":
    result = main()


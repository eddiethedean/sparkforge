#!/usr/bin/env python3
"""
Databricks Multi-Task Job: Silver Processing

Task 2 of a multi-task Databricks job for production ETL pipelines.
This task handles data cleaning and enrichment.

Depends on: Task 1 (Bronze Ingestion)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    import dbutils
except ImportError:
    dbutils = None


def main():
    """
    Silver processing task - cleans and enriches data.
    """
    
    print("=" * 80)
    print("DATABRICKS MULTI-TASK JOB - TASK 2: SILVER PROCESSING")
    print("=" * 80)
    
    spark = SparkSession.builder.getOrCreate()
    
    try:
        from pipeline_builder import PipelineBuilder
        
        # ========================================================================
        # CONFIGURATION
        # ========================================================================
        
        if dbutils:
            dbutils.widgets.text("source_path", "/mnt/raw/user_events/")
            dbutils.widgets.text("target_schema", "analytics")
            dbutils.widgets.text("run_date", "")
            
            source_path = dbutils.widgets.get("source_path")
            target_schema = dbutils.widgets.get("target_schema")
            run_date = dbutils.widgets.get("run_date")
        else:
            source_path = "/tmp/test_data/"
            target_schema = "analytics"
            run_date = ""
        
        print(f"Configuration:")
        print(f"  Source Path: {source_path}")
        print(f"  Target Schema: {target_schema}")
        print(f"  Run Date: {run_date}")
        
        # Create schema
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
        
        # ========================================================================
        # LOAD DATA (from previous task or source)
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("LOADING BRONZE DATA")
        print("=" * 80)
        
        # In production, this would load from where Task 1 stored the data
        # For this example, we'll recreate the data
        
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
        # BUILD SILVER PIPELINE
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("BUILDING SILVER TRANSFORMATION")
        print("=" * 80)
        
        builder = PipelineBuilder(
            spark=spark,
            schema=target_schema,
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            verbose=True
        )
        
        # Bronze: Define validation
        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
                "value": [F.col("value").isNotNull(), F.col("value") > 0]
            },
            incremental_col="timestamp"
        )
        
        # Silver: Clean and enrich
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
        
        pipeline = builder.to_pipeline()
        
        # ========================================================================
        # EXECUTE SILVER PROCESSING
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("EXECUTING SILVER TRANSFORMATION")
        print("=" * 80)
        
        result = pipeline.run_initial_load(bronze_sources={"events": source_df})
        
        print(f"✅ Silver processing completed: {result.status}")
        print(f"   Rows written: {result.metrics.total_rows_written}")
        print(f"   Duration: {result.metrics.total_duration_secs:.2f}s")
        
        # Show results
        print("\n📊 Enriched Events (Silver Layer):")
        silver_table = spark.table(f"{target_schema}.enriched_events")
        silver_table.show(5, truncate=False)
        
        print("\n" + "=" * 80)
        print("✅ SILVER PROCESSING COMPLETED")
        print("=" * 80)
        
        return result
        
    except Exception as e:
        print(f"\n❌ Silver processing failed: {e}")
        raise


if __name__ == "__main__":
    result = main()


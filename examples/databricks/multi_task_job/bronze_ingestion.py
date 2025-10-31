#!/usr/bin/env python3
"""
Databricks Multi-Task Job: Bronze Ingestion

Task 1 of a multi-task Databricks job for production ETL pipelines.
This task handles raw data ingestion and validation.

Job Structure:
- Task 1 (this): Bronze Ingestion
- Task 2: Silver Processing (depends on Task 1)
- Task 3: Gold Analytics (depends on Task 2)
- Task 4: Logging (depends on Task 3)

Use this pattern when:
- Steps have different resource requirements
- You want to use different cluster types per task
- You need fine-grained control over execution
- Steps can be scaled independently
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    # Databricks context is available in notebooks
    import dbutils
except ImportError:
    dbutils = None


def main():
    """
    Bronze ingestion task - loads and validates raw data.
    """
    
    print("=" * 80)
    print("DATABRICKS MULTI-TASK JOB - TASK 1: BRONZE INGESTION")
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
        # LOAD BRONZE DATA
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("LOADING RAW DATA")
        print("=" * 80)
        
        # In production, load from your actual data source
        # Example: source_df = spark.read.parquet(f"{source_path}/{run_date}")
        
        # For demonstration, create sample data
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
        # BUILD BRONZE PIPELINE
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("BUILDING BRONZE VALIDATION")
        print("=" * 80)
        
        builder = PipelineBuilder(
            spark=spark,
            schema=target_schema,
            min_bronze_rate=90.0,
            verbose=True
        )
        
        # Define bronze validation rules
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
        
        pipeline = builder.to_pipeline()
        
        # ========================================================================
        # EXECUTE BRONZE VALIDATION
        # ========================================================================
        
        print("\n" + "=" * 80)
        print("EXECUTING BRONZE VALIDATION")
        print("=" * 80)
        
        # Run validation only - this task doesn't write data
        result = pipeline.run_validation_only(bronze_sources={"events": source_df})
        
        print(f"✅ Bronze validation completed: {result.status}")
        print(f"   Validation rate: {result.metrics.total_rows_processed}")
        
        # Store bronze data for next task
        # In a real scenario, you might write to a temporary location
        # or pass through context variables
        
        print("\n" + "=" * 80)
        print("✅ BRONZE INGESTION COMPLETED")
        print("=" * 80)
        
        print("\n⚠️  Note: This task only validates data.")
        print("   Silver processing task will handle transformations.")
        
        return result
        
    except Exception as e:
        print(f"\n❌ Bronze ingestion failed: {e}")
        raise


if __name__ == "__main__":
    result = main()


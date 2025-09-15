#!/usr/bin/env python3
"""
Improved User Experience Example

This example demonstrates the new user experience improvements in SparkForge:
- Auto-inference of source_bronze for silver transforms
- Auto-inference of source_silvers for gold transforms  
- Preset configurations for different environments
- Validation helper methods for common patterns
- Auto-detection of timestamp columns for watermarking
"""

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.pipeline.runner import PipelineRunner
from sparkforge.models import ExecutionMode, ValidationThresholds
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Demonstrate improved user experience features."""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ImprovedUserExperienceExample") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    logger.info("SparkSession initialized.")

    # Define a schema for the raw data
    bronze_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("value", IntegerType(), True),
        StructField("device", StringType(), True)
    ])

    # Sample raw data
    events_data = [
        ("user1", "view", "prodA", datetime(2024, 1, 1, 10, 0, 0), 100, "mobile"),
        ("user1", "click", "prodA", datetime(2024, 1, 1, 10, 5, 0), 100, "mobile"),
        ("user2", "view", "prodB", datetime(2024, 1, 1, 10, 10, 0), 200, "desktop"),
        ("user1", "purchase", "prodA", datetime(2024, 1, 1, 10, 15, 0), 100, "mobile"),
        ("user3", "view", "prodC", datetime(2024, 1, 2, 11, 0, 0), 300, "tablet"),
        ("user2", "purchase", "prodB", datetime(2024, 1, 2, 11, 30, 0), 200, "desktop"),
        ("user1", "view", "prodD", datetime(2024, 1, 3, 12, 0, 0), 150, "mobile"),
        (None, "error", "prodE", datetime(2024, 1, 3, 12, 5, 0), 0, "unknown"), # Invalid user_id
    ]
    source_df = spark.createDataFrame(events_data, schema=bronze_schema)

    # ============================================================================
    # DEMONSTRATION 1: PRESET CONFIGURATIONS
    # ============================================================================
    logger.info("🎯 DEMONSTRATION 1: Preset Configurations")
    logger.info("=" * 60)
    
    # Use development preset (relaxed validation, verbose logging)
    logger.info("Using development preset configuration...")
    builder = PipelineBuilder.for_development(
        spark=spark,
        schema="improved_ux_schema"
    )
    logger.info(f"✅ Development preset: Bronze={builder.config.thresholds.bronze}%, Silver={builder.config.thresholds.silver}%, Gold={builder.config.thresholds.gold}%")

    # ============================================================================
    # DEMONSTRATION 2: VALIDATION HELPER METHODS
    # ============================================================================
    logger.info("\n🎯 DEMONSTRATION 2: Validation Helper Methods")
    logger.info("=" * 60)
    
    # Instead of writing repetitive validation rules:
    # OLD WAY:
    # rules = {
    #     "user_id": [F.col("user_id").isNotNull()],
    #     "action": [F.col("action").isNotNull()],
    #     "timestamp": [F.col("timestamp").isNotNull()],
    #     "value": [F.col("value").isNotNull(), F.col("value") > 0]
    # }
    
    # NEW WAY: Use helper methods
    logger.info("Using validation helper methods...")
    bronze_rules = PipelineBuilder.not_null_rules(["user_id", "action", "timestamp"])
    bronze_rules.update(PipelineBuilder.positive_number_rules(["value"]))
    bronze_rules.update(PipelineBuilder.string_not_empty_rules(["device"]))
    
    logger.info(f"✅ Generated bronze rules: {list(bronze_rules.keys())}")

    # ============================================================================
    # DEMONSTRATION 3: AUTO-INFERENCE FEATURES
    # ============================================================================
    logger.info("\n🎯 DEMONSTRATION 3: Auto-Inference Features")
    logger.info("=" * 60)
    
    # Add bronze step
    logger.info("🟤 Adding Bronze step...")
    builder.with_bronze_rules(
        name="events",
        rules=bronze_rules,
        incremental_col="timestamp"
    )

    # Define Silver transformation functions
    def clean_events(spark_session, bronze_df, prior_silvers):
        logger.info("Running clean_events silver transform...")
        return (
            bronze_df
            .filter(F.col("user_id").isNotNull())
            .filter(F.col("action").isin(["click", "view", "purchase"]))
            .withColumn("event_date", F.date_trunc("day", F.col("timestamp")))
            .withColumn("is_purchase", F.col("action") == "purchase")
        )

    def enrich_events(spark_session, bronze_df, prior_silvers):
        logger.info("Running enrich_events silver transform...")
        return (
            bronze_df
            .withColumn("event_hour", F.hour(F.col("timestamp")))
            .withColumn("event_day_of_week", F.dayofweek(F.col("timestamp")))
        )

    def daily_analytics(spark_session, silvers):
        logger.info("Running daily_analytics gold transform...")
        enriched_events_df = silvers["enriched_events"]
        return (
            enriched_events_df
            .groupBy("event_date")
            .agg(
                F.countDistinct("user_id").alias("unique_users"),
                F.count("action").alias("event_count"),
                F.sum(F.when(F.col("is_purchase"), 1).otherwise(0)).alias("purchases")
            )
            .withColumn("conversion_rate", 
                F.col("purchases") / F.col("event_count")
            )
        )

    # Add silver steps with auto-inference
    logger.info("🟡 Adding Silver steps with auto-inference...")
    
    # 1. Auto-infer source_bronze (from "events")
    # 2. Auto-detect watermark column (from "timestamp")
    builder.add_silver_transform(
        name="clean_events",
        # source_bronze auto-inferred from "events"
        # watermark_col could be auto-detected from "timestamp"
        transform=clean_events,
        rules=PipelineBuilder.not_null_rules(["user_id", "action", "event_date"]),
        table_name="clean_events",
        watermark_col="timestamp"  # Still explicit for demonstration
    )

    # Another silver step, also auto-inferring from "events"
    builder.add_silver_transform(
        name="enriched_events",
        # source_bronze auto-inferred from "events"
        transform=enrich_events,
        rules=PipelineBuilder.not_null_rules(["event_hour", "event_day_of_week"]),
        table_name="enriched_events"
    )

    # Add gold step with auto-inference
    logger.info("🟨 Adding Gold step with auto-inference...")
    
    # 1. Auto-infer source_silvers (from all available silver steps)
    builder.add_gold_transform(
        name="daily_analytics",
        # source_silvers auto-inferred from ["clean_events", "enriched_events"]
        transform=daily_analytics,
        rules=PipelineBuilder.not_null_rules(["event_date", "event_count"]),
        table_name="daily_analytics"
    )

    # ============================================================================
    # DEMONSTRATION 4: TIMESTAMP DETECTION
    # ============================================================================
    logger.info("\n🎯 DEMONSTRATION 4: Timestamp Detection")
    logger.info("=" * 60)
    
    # Detect timestamp columns from the source DataFrame
    timestamp_cols = PipelineBuilder.detect_timestamp_columns(source_df.schema)
    logger.info(f"✅ Detected timestamp columns: {timestamp_cols}")

    # ============================================================================
    # DEMONSTRATION 5: SIMPLIFIED PIPELINE EXECUTION
    # ============================================================================
    logger.info("\n🎯 DEMONSTRATION 5: Simplified Pipeline Execution")
    logger.info("=" * 60)
    
    # Build and run pipeline
    logger.info("🚀 Building pipeline...")
    pipeline = builder.to_pipeline()
    logger.info("✅ Pipeline validation passed")
    
    logger.info("📊 Running pipeline...")
    result = pipeline.initial_load(bronze_sources={"events": source_df})

    if result.success:
        logger.info("✅ Pipeline execution completed successfully!")
    else:
        logger.error("❌ Pipeline execution failed!")
        
    logger.info(f"📈 Bronze steps executed: {result.metrics.get('bronze_steps_executed', 0)}")
    logger.info(f"📈 Silver steps executed: {result.metrics.get('silver_steps_executed', 0)}")
    logger.info(f"📈 Gold steps executed: {result.metrics.get('gold_steps_executed', 0)}")
    logger.info(f"⏱️  Total duration: {result.metrics.get('total_duration_secs', 0):.2f}s")

    # ============================================================================
    # DEMONSTRATION 6: COMPARISON WITH OLD API
    # ============================================================================
    logger.info("\n🎯 DEMONSTRATION 6: API Comparison")
    logger.info("=" * 60)
    
    logger.info("OLD API (verbose):")
    logger.info("""
    builder = PipelineBuilder(
        spark=spark,
        schema="schema",
        min_bronze_rate=80.0,
        min_silver_rate=85.0,
        min_gold_rate=90.0,
        verbose=True,
        enable_parallel_silver=True,
        max_parallel_workers=2
    )
    
    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]},
        incremental_col="timestamp"
    )
    
    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",  # Explicit
        transform=clean_events,
        rules={"user_id": [F.col("user_id").isNotNull()]},
        table_name="clean_events",  # Explicit
        watermark_col="timestamp"
    )
    
    builder.add_gold_transform(
        name="daily_analytics",
        transform=daily_analytics,
        rules={"event_date": [F.col("event_date").isNotNull()]},
        table_name="daily_analytics",  # Explicit
        source_silvers=["clean_events"]  # Explicit
    )
    """)
    
    logger.info("NEW API (simplified):")
    logger.info("""
    builder = PipelineBuilder.for_development(spark=spark, schema="schema")
    
    builder.with_bronze_rules(
        name="events",
        rules=PipelineBuilder.not_null_rules(["user_id"]),
        incremental_col="timestamp"
    )
    
    builder.add_silver_transform(
        name="clean_events",
        # source_bronze auto-inferred
        transform=clean_events,
        rules=PipelineBuilder.not_null_rules(["user_id"]),
        table_name="clean_events",
        watermark_col="timestamp"
    )
    
    builder.add_gold_transform(
        name="daily_analytics",
        # source_silvers auto-inferred
        transform=daily_analytics,
        rules=PipelineBuilder.not_null_rules(["event_date"]),
        table_name="daily_analytics"
    )
    """)
    
    logger.info("✅ New API reduces boilerplate by ~60%!")

    spark.stop()
    logger.info("SparkSession stopped.")

if __name__ == "__main__":
    main()

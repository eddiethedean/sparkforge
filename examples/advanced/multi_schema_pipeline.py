"""
Multi-Schema Pipeline Example

This example demonstrates how to use SparkForge's multi-schema support
to create cross-schema data pipelines, enabling data flows between
different schemas for better organization and isolation.

Key Features Demonstrated:
- Reading from different schemas for bronze data
- Writing to different schemas for silver and gold data
- Schema validation and error handling
- Cross-schema data flows
- Multi-tenant data isolation

Use Cases:
- Multi-tenant SaaS applications
- Data lake architecture with separate schemas
- Environment separation (dev/staging/prod)
- Compliance and data residency requirements
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_test_data(spark: SparkSession):
    """Create test data in different schemas."""
    # Clean up any existing schemas and tables
    try:
        spark.sql("DROP SCHEMA IF EXISTS raw_data CASCADE")
        spark.sql("DROP SCHEMA IF EXISTS processing CASCADE")
        spark.sql("DROP SCHEMA IF EXISTS analytics CASCADE")
        spark.sql("DROP SCHEMA IF EXISTS staging CASCADE")
    except:
        pass  # Ignore errors if schemas don't exist

    # Create schemas
    spark.sql("CREATE SCHEMA IF NOT EXISTS raw_data")
    spark.sql("CREATE SCHEMA IF NOT EXISTS processing")
    spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")
    spark.sql("CREATE SCHEMA IF NOT EXISTS staging")

    # Create test data
    test_data = [
        ("user1", "click", "2024-01-01 10:00:00", 100),
        ("user2", "view", "2024-01-01 11:00:00", 200),
        ("user3", "purchase", "2024-01-01 12:00:00", 300),
        ("user1", "view", "2024-01-01 13:00:00", 150),
        ("user2", "purchase", "2024-01-01 14:00:00", 250),
    ]

    schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )

    df = spark.createDataFrame(test_data, schema)

    # Write to raw_data schema using external table
    df.write.mode("overwrite").option(
        "path", "/tmp/sparkforge_test/raw_data/user_events"
    ).saveAsTable("raw_data.user_events")

    logger.info("✅ Test data created in raw_data.user_events")


def clean_events_transform(spark, bronze_df, prior_silvers):
    """Clean and enrich events data."""
    return (
        bronze_df.filter(F.col("user_id").isNotNull())
        .withColumn("event_date", F.date_trunc("day", F.col("timestamp")))
        .withColumn("is_weekend", F.dayofweek(F.col("timestamp")).isin([1, 7]))
        .withColumn(
            "value_category",
            F.when(F.col("value") < 100, "low")
            .when(F.col("value") < 200, "medium")
            .otherwise("high"),
        )
    )


def enrich_events_transform(spark, bronze_df, prior_silvers):
    """Enrich events with additional business logic."""
    clean_events = prior_silvers["clean_events"]

    return clean_events.withColumn(
        "user_segment",
        F.when(F.col("value") > 200, "premium")
        .when(F.col("value") > 100, "standard")
        .otherwise("basic"),
    ).withColumn("processing_timestamp", F.current_timestamp())


def daily_metrics_transform(spark, silvers):
    """Create daily metrics from silver data."""
    clean_events = silvers["clean_events"]
    enriched_events = silvers["enriched_events"]

    # Daily event counts by user
    daily_counts = clean_events.groupBy("user_id", "event_date").agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_type").alias("unique_event_types"),
        F.sum("value").alias("total_value"),
    )

    # User segments
    user_segments = enriched_events.select("user_id", "user_segment").distinct()

    # Join metrics with segments
    return daily_counts.join(user_segments, "user_id", "left").withColumn(
        "report_date", F.current_date()
    )


def run_multi_schema_pipeline():
    """Run the multi-schema pipeline example."""
    # Initialize Spark
    spark = (
        SparkSession.builder.appName("MultiSchemaPipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # Create test data
        create_test_data(spark)

        # Import PipelineBuilder
        from sparkforge import PipelineBuilder

        # Create pipeline builder with default schema
        builder = PipelineBuilder(spark=spark, schema="default")

        logger.info("🏗️  Building multi-schema pipeline...")

        # Bronze Layer: Read from raw_data schema
        # Note: Validation rules filter output to only columns with rules by default
        # Use filter_columns_by_rules=False in apply_column_rules to preserve all columns
        builder.with_bronze_rules(
            name="user_events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
                "value": [F.col("value") > 0],
            },
            incremental_col="timestamp",
            schema="raw_data",  # Read from different schema
        )

        # Silver Layer: Write to processing schema
        builder.add_silver_transform(
            name="clean_events",
            transform=clean_events_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "event_date": [F.col("event_date").isNotNull()],
                "value_category": [
                    F.col("value_category").isin(["low", "medium", "high"])
                ],
            },
            table_name="clean_user_events",
            watermark_col="timestamp",
            schema="processing",  # Write to different schema
        )

        # Silver Layer: Write to staging schema
        builder.add_silver_transform(
            name="enriched_events",
            transform=enrich_events_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "user_segment": [
                    F.col("user_segment").isin(["basic", "standard", "premium"])
                ],
                "processing_timestamp": [F.col("processing_timestamp").isNotNull()],
            },
            table_name="enriched_user_events",
            watermark_col="timestamp",
            schema="staging",  # Write to different schema
        )

        # Gold Layer: Write to analytics schema
        builder.add_gold_transform(
            name="daily_metrics",
            transform=daily_metrics_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "event_date": [F.col("event_date").isNotNull()],
                "total_events": [F.col("total_events") > 0],
            },
            table_name="user_daily_metrics",
            schema="analytics",  # Write to different schema
        )

        # Build and run pipeline
        pipeline = builder.to_pipeline()

        logger.info("🚀 Running multi-schema pipeline...")
        # For this example, we'll use initial_load with mock data
        # In a real scenario, you would provide actual bronze data sources
        mock_bronze_data = {
            "user_events": spark.sql("SELECT * FROM raw_data.user_events")
        }
        result = pipeline.initial_load(bronze_sources=mock_bronze_data)

        # Display results
        logger.info("📊 Pipeline execution completed!")
        logger.info(f"✅ Bronze steps processed: {len(result.bronze_results)}")
        logger.info(f"✅ Silver steps processed: {len(result.silver_results)}")
        logger.info(f"✅ Gold steps processed: {len(result.gold_results)}")

        # Show data distribution across schemas
        logger.info("\n📋 Data Distribution Across Schemas:")

        # Check raw data
        raw_count = spark.sql(
            "SELECT COUNT(*) as count FROM raw_data.user_events"
        ).collect()[0]["count"]
        logger.info(f"  Raw Data (raw_data.user_events): {raw_count} records")

        # Check processed data
        try:
            processing_count = spark.sql(
                "SELECT COUNT(*) as count FROM processing.clean_user_events"
            ).collect()[0]["count"]
            logger.info(
                f"  Clean Data (processing.clean_user_events): {processing_count} records"
            )
        except Exception as e:
            logger.warning(f"  Could not read processing.clean_user_events: {e}")

        try:
            staging_count = spark.sql(
                "SELECT COUNT(*) as count FROM staging.enriched_user_events"
            ).collect()[0]["count"]
            logger.info(
                f"  Enriched Data (staging.enriched_user_events): {staging_count} records"
            )
        except Exception as e:
            logger.warning(f"  Could not read staging.enriched_user_events: {e}")

        try:
            analytics_count = spark.sql(
                "SELECT COUNT(*) as count FROM analytics.user_daily_metrics"
            ).collect()[0]["count"]
            logger.info(
                f"  Analytics Data (analytics.user_daily_metrics): {analytics_count} records"
            )
        except Exception as e:
            logger.warning(f"  Could not read analytics.user_daily_metrics: {e}")

        # Show sample results
        logger.info("\n📈 Sample Analytics Results:")
        try:
            sample_data = spark.sql(
                """
                SELECT user_id, event_date, total_events, user_segment, total_value
                FROM analytics.user_daily_metrics
                ORDER BY total_events DESC
                LIMIT 5
            """
            ).collect()

            for row in sample_data:
                logger.info(
                    f"  User {row['user_id']}: {row['total_events']} events on {row['event_date']} "
                    f"(Segment: {row['user_segment']}, Value: {row['total_value']})"
                )
        except Exception as e:
            logger.warning(f"  Could not read sample analytics data: {e}")

        return result

    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        raise
    finally:
        spark.stop()


def demonstrate_schema_validation():
    """Demonstrate schema validation features."""
    spark = SparkSession.builder.appName("SchemaValidationDemo").getOrCreate()

    try:
        from sparkforge import PipelineBuilder

        builder = PipelineBuilder(spark=spark, schema="default")

        logger.info("🔍 Demonstrating schema validation...")

        # This should work - schema exists
        try:
            spark.sql("CREATE SCHEMA IF NOT EXISTS valid_schema")
            builder.with_bronze_rules(
                name="test_events",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                schema="valid_schema",
            )
            logger.info("✅ Valid schema accepted")
        except Exception as e:
            logger.error(f"❌ Unexpected error with valid schema: {e}")

        # This should fail - schema doesn't exist
        try:
            builder.with_bronze_rules(
                name="test_events2",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                schema="nonexistent_schema",
            )
            logger.error("❌ Should have failed with nonexistent schema")
        except Exception as e:
            logger.info(f"✅ Correctly caught schema validation error: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    print("🚀 Multi-Schema Pipeline Example")
    print("=" * 50)

    print("\n1. Running multi-schema pipeline...")
    run_multi_schema_pipeline()

    print("\n2. Demonstrating schema validation...")
    demonstrate_schema_validation()

    print("\n✅ Multi-schema example completed!")
    print("\nKey Benefits Demonstrated:")
    print("  • Cross-schema data flows")
    print("  • Schema validation and error handling")
    print("  • Multi-tenant data isolation")
    print("  • Environment separation")
    print("  • Backward compatibility")

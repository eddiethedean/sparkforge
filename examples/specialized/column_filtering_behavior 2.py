"""
Column Filtering Behavior Example

This example demonstrates how SparkForge's validation system filters columns
based on validation rules, and how to control this behavior with the
filter_columns_by_rules parameter.

Key Concepts:
- By default, validation filters output to only columns with rules
- Use filter_columns_by_rules=False to preserve all columns
- This behavior affects Bronze, Silver, and Gold layer validation
- Important for downstream steps that need access to all columns
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


def demonstrate_column_filtering():
    """Demonstrate column filtering behavior in validation."""

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("ColumnFilteringBehavior")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        # Create sample data with multiple columns
        sample_data = [
            ("user1", "click", 100, "2024-01-01 10:00:00", "mobile"),
            ("user2", "view", 200, "2024-01-01 11:00:00", "desktop"),
            ("user3", "purchase", 50, "2024-01-01 12:00:00", "mobile"),
            ("user4", "click", 75, "2024-01-01 13:00:00", "tablet"),
        ]

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("timestamp", StringType(), True),
                StructField(
                    "device_type", StringType(), True
                ),  # This column won't have rules
            ]
        )

        df = spark.createDataFrame(sample_data, schema)

        logger.info("📊 Original DataFrame columns:")
        df.printSchema()
        df.show()

        # Import validation function
        import sys

        sys.path.insert(0, ".")
        from sparkforge.validation import apply_column_rules

        # Define validation rules for only some columns
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
            "value": [F.col("value") > 0]
            # Note: timestamp and device_type have no rules
        }

        logger.info("\n🔍 Validation Rules:")
        for col, rule_list in rules.items():
            logger.info(f"  {col}: {len(rule_list)} rules")

        # Test 1: Default behavior (filter_columns_by_rules=True)
        logger.info("\n✅ Test 1: Default behavior (filter_columns_by_rules=True)")
        logger.info("   This will only keep columns that have validation rules")

        valid_df1, invalid_df1, stats1 = apply_column_rules(
            df=df,
            rules=rules,
            stage="bronze",
            step="test_step",
            filter_columns_by_rules=True,  # DEFAULT BEHAVIOR
        )

        logger.info(f"   Validation rate: {stats1.validation_rate:.1f}%")
        logger.info(f"   Valid rows: {stats1.valid_rows}")
        logger.info(f"   Invalid rows: {stats1.invalid_rows}")
        logger.info("   Valid DataFrame columns:")
        valid_df1.printSchema()
        valid_df1.show()

        # Test 2: Preserve all columns (filter_columns_by_rules=False)
        logger.info("\n✅ Test 2: Preserve all columns (filter_columns_by_rules=False)")
        logger.info("   This will keep ALL columns from the original DataFrame")

        valid_df2, invalid_df2, stats2 = apply_column_rules(
            df=df,
            rules=rules,
            stage="bronze",
            step="test_step",
            filter_columns_by_rules=False,  # PRESERVE ALL COLUMNS
        )

        logger.info(f"   Validation rate: {stats2.validation_rate:.1f}%")
        logger.info(f"   Valid rows: {stats2.valid_rows}")
        logger.info(f"   Invalid rows: {stats2.invalid_rows}")
        logger.info("   Valid DataFrame columns:")
        valid_df2.printSchema()
        valid_df2.show()

        # Demonstrate the difference
        logger.info("\n📋 Summary:")
        logger.info(f"   Original columns: {df.columns}")
        logger.info(f"   With filtering: {valid_df1.columns}")
        logger.info(f"   Without filtering: {valid_df2.columns}")
        logger.info(f"   Filtered out: {set(df.columns) - set(valid_df1.columns)}")

        # Show practical impact
        logger.info("\n💡 Practical Impact:")
        logger.info("   - With filtering: Downstream steps only see columns with rules")
        logger.info("   - Without filtering: Downstream steps see all original columns")
        logger.info("   - Choose based on whether downstream steps need all columns")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        spark.stop()


def demonstrate_pipeline_impact():
    """Demonstrate how column filtering affects pipeline steps."""

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("PipelineColumnFiltering")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        import sys

        sys.path.insert(0, ".")
        from sparkforge import PipelineBuilder

        # Create sample data
        sample_data = [
            ("user1", "click", 100, "2024-01-01 10:00:00", "mobile", "premium"),
            ("user2", "view", 200, "2024-01-01 11:00:00", "desktop", "basic"),
            ("user3", "purchase", 50, "2024-01-01 12:00:00", "mobile", "standard"),
        ]

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("timestamp", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("user_tier", StringType(), True),  # This won't have rules
            ]
        )

        df = spark.createDataFrame(sample_data, schema)

        logger.info("🏗️  Building pipeline with column filtering...")

        # Create pipeline builder
        builder = PipelineBuilder(spark=spark, schema="default")

        # Bronze layer with limited rules
        builder.with_bronze_rules(
            name="user_events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
                "value": [F.col("value") > 0]
                # Note: timestamp, device_type, user_tier have no rules
            },
            incremental_col="timestamp",
        )

        # Silver transform that needs ALL columns
        def silver_transform(spark, bronze_df):
            """Transform that needs access to all columns."""
            logger.info(f"Silver transform input columns: {bronze_df.columns}")

            # This will fail if columns are filtered out
            try:
                result = (
                    bronze_df.withColumn("event_date", F.to_date("timestamp"))
                    .withColumn("is_mobile", F.col("device_type") == "mobile")
                    .withColumn("is_premium", F.col("user_tier") == "premium")
                    .select(
                        "user_id",
                        "event_type",
                        "value",
                        "event_date",
                        "is_mobile",
                        "is_premium",
                    )
                )
                logger.info(f"Silver transform output columns: {result.columns}")
                return result
            except Exception as e:
                logger.error(f"Silver transform failed: {e}")
                logger.error(
                    "This happens when columns are filtered out by validation rules"
                )
                raise

        builder.add_silver_transform(
            name="enriched_events",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "event_date": [F.col("event_date").isNotNull()],
                "is_mobile": [F.col("is_mobile").isNotNull()],
                "is_premium": [F.col("is_premium").isNotNull()],
            },
            table_name="enriched_user_events",
        )

        # Build pipeline
        pipeline = builder.to_pipeline()

        logger.info("🚀 Running pipeline...")
        logger.info(
            "   Note: This will show how column filtering affects downstream steps"
        )

        # Run pipeline
        pipeline.initial_load(bronze_sources={"user_events": df})

        logger.info("✅ Pipeline completed successfully!")
        logger.info("   The error above demonstrates exactly what happens when")
        logger.info("   columns are filtered out by validation rules and downstream")
        logger.info("   steps try to access them. This is the expected behavior!")

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    print("Column Filtering Behavior Example")
    print("=" * 50)

    print("\n1. Direct validation function usage:")
    demonstrate_column_filtering()

    print("\n2. Pipeline impact:")
    demonstrate_pipeline_impact()

    print("\n" + "=" * 50)
    print("Key Takeaways:")
    print("- filter_columns_by_rules=True (default): Only keeps columns with rules")
    print("- filter_columns_by_rules=False: Preserves all original columns")
    print("- Choose based on downstream step requirements")
    print("- Pipeline steps typically need access to all columns")

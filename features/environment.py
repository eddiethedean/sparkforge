"""
Behave environment configuration for SparkForge BDD tests.

This module sets up the test environment, manages Spark sessions,
and provides common fixtures for all BDD scenarios.
"""

import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def before_all(context):
    """Set up the test environment before all scenarios."""
    print("🚀 Setting up SparkForge BDD test environment...")

    # Set up Java environment for Spark
    os.environ["JAVA_HOME"] = (
        "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
    )
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    # Create temporary directory for test data
    context.test_dir = tempfile.mkdtemp(prefix="sparkforge_bdd_")
    context.test_schema = "test_schema"

    # Initialize Spark session
    context.spark = (
        SparkSession.builder.appName("SparkForge-BDD-Tests")
        .master("local[1]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    context.spark.conf.set("spark.sql.warehouse.dir", context.test_dir)

    print(f"✅ Spark session created: {context.spark.version}")
    print(f"✅ Test directory: {context.test_dir}")


def before_scenario(context, scenario):
    """Set up before each scenario."""
    context.scenario_name = scenario.name
    context.step_results = []
    context.pipeline_config = None
    context.execution_result = None
    context.writer_result = None

    print(f"\n📋 Starting scenario: {scenario.name}")


def after_scenario(context, scenario):
    """Clean up after each scenario."""
    if hasattr(context, "spark") and context.spark:
        # Clean up any temporary tables
        try:
            context.spark.catalog.clearCache()
        except Exception as e:
            print(f"⚠️ Warning: Could not clear cache: {e}")

    print(f"✅ Completed scenario: {scenario.name}")


def after_all(context):
    """Clean up after all scenarios."""
    print("\n🧹 Cleaning up test environment...")

    if hasattr(context, "spark") and context.spark:
        context.spark.stop()
        print("✅ Spark session stopped")

    if hasattr(context, "test_dir"):
        import shutil

        try:
            shutil.rmtree(context.test_dir)
            print(f"✅ Test directory cleaned: {context.test_dir}")
        except Exception as e:
            print(f"⚠️ Warning: Could not clean test directory: {e}")

    print("🎉 BDD test environment cleanup complete!")


def create_test_dataframe(context, data, schema=None):
    """Helper function to create test DataFrames."""
    if schema is None:
        # Default schema for basic test data
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("timestamp", TimestampType(), True),
            ]
        )

    return context.spark.createDataFrame(data, schema)


def create_bronze_step(context, name, rules=None):
    """Helper function to create bronze steps."""
    from pipeline_builder.models import BronzeStep

    if rules is None:
        rules = {"id": ["not_null"], "name": ["not_null"]}

    return BronzeStep(name=name, rules=rules, incremental_col="timestamp")


def create_silver_step(context, name, source_bronze, transform_func=None):
    """Helper function to create silver steps."""
    from pipeline_builder.models import SilverStep

    return SilverStep(
        name=name,
        source_bronze=source_bronze,
        transform=transform_func,
        rules={"id": ["not_null"]},
    )


def create_gold_step(context, name, transform_func=None):
    """Helper function to create gold steps."""
    from pipeline_builder.models import GoldStep

    return GoldStep(name=name, transform=transform_func, rules={"id": ["not_null"]})

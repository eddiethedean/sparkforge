#!/usr/bin/env python3
"""
Test runner for Pipeline Builder with proper Spark setup.

This script sets up Spark and runs the pipeline tests.
"""

import os
import sys

from pyspark.sql import SparkSession


def setup_spark():
    """Set up Spark session with Delta Lake for testing."""
    print("🔧 Setting up Spark session with Delta Lake...")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    jars_dir = os.path.join(project_root, "jars")
    delta_core_jar = os.path.join(jars_dir, "delta-core_2.12-2.0.2.jar")
    delta_storage_jar = os.path.join(jars_dir, "delta-storage-2.0.2.jar")

    spark = (
        SparkSession.builder.appName("PipelineBuilderTests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars", f"{delta_core_jar},{delta_storage_jar}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    print(f"✅ Spark session created with Delta Lake: {spark.version}")
    return spark


def run_tests():
    """Run the pipeline tests."""
    print("🚀 Running Pipeline Builder Tests")
    print("=" * 50)

    # Set up Spark
    spark = setup_spark()

    # Create test database
    spark.sql("CREATE DATABASE IF NOT EXISTS test_schema")

    # Clean up any existing test tables
    test_tables = [
        "test_schema.test_silver",
        "test_schema.test_gold",
        "test_schema.silver_events",
        "test_schema.silver_users",
    ]

    for table in test_tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass

    # Make spark available globally for the test module
    import builtins

    builtins.spark = spark

    # Import and run the test functions
    try:
        from pipeline_tests import run_all_tests

        # Run all tests
        passed, failed = run_all_tests()
        return failed == 0

    except Exception as e:
        print(f"❌ Test setup failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Clean up Spark
        try:
            spark.stop()
            print("\n✅ Spark session stopped")
        except Exception:
            pass


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

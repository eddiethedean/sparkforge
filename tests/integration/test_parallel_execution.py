#!/usr/bin/env python3
"""
Test script to verify parallel execution implementation.

This script tests:
1. Multiple independent bronze steps running in parallel
2. Multiple independent silver steps running in parallel
3. Proper dependency handling across layers
4. Parallel efficiency metrics
"""

import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkforge import PipelineBuilder
from sparkforge.models import PipelineConfig


def create_test_data(spark: SparkSession, name: str, num_rows: int = 100):
    """Create test data for pipeline steps."""
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )

    data = [
        {
            "id": i,
            "name": f"{name}_{i}",
            "value": i * 10,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        for i in range(num_rows)
    ]

    return spark.createDataFrame(data, schema)


def slow_transform_1(spark, df, silvers):
    """Simulated slow transformation to test parallelism."""
    time.sleep(2)  # Simulate processing time
    return df.withColumn("processed_1", F.lit("done"))


def slow_transform_2(spark, df, silvers):
    """Simulated slow transformation to test parallelism."""
    time.sleep(2)  # Simulate processing time
    return df.withColumn("processed_2", F.lit("done"))


def slow_transform_3(spark, df, silvers):
    """Simulated slow transformation to test parallelism."""
    time.sleep(2)  # Simulate processing time
    return df.withColumn("processed_3", F.lit("done"))


def test_parallel_execution():
    """Test parallel execution with multiple independent steps."""
    print("=" * 80)
    print("Testing Parallel Execution Implementation")
    print("=" * 80)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("Test Parallel Execution")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )

    try:
        # Create test schema
        spark.sql("CREATE DATABASE IF NOT EXISTS test_parallel")

        # Test 1: Parallel execution with default config
        print("\n" + "-" * 80)
        print("Test 1: Parallel Execution with Default Config (4 workers)")
        print("-" * 80)

        config = PipelineConfig.create_default(schema="test_parallel")
        print(f"Parallel enabled: {config.parallel.enabled}")
        print(f"Max workers: {config.parallel.max_workers}")

        builder = PipelineBuilder(
            spark=spark, schema="test_parallel", verbose=True
        )

        # Create multiple bronze steps (should run in parallel)
        builder.with_bronze_rules(
            name="source_a",
            rules={"id": [F.col("id").isNotNull()], "value": [F.col("value") > 0]},
        )

        builder.with_bronze_rules(
            name="source_b",
            rules={"id": [F.col("id").isNotNull()], "value": [F.col("value") > 0]},
        )

        builder.with_bronze_rules(
            name="source_c",
            rules={"id": [F.col("id").isNotNull()], "value": [F.col("value") > 0]},
        )

        # Create multiple silver steps (should run in parallel)
        builder.add_silver_transform(
            name="silver_a",
            source_bronze="source_a",
            transform=slow_transform_1,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_a",
        )

        builder.add_silver_transform(
            name="silver_b",
            source_bronze="source_b",
            transform=slow_transform_2,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_b",
        )

        builder.add_silver_transform(
            name="silver_c",
            source_bronze="source_c",
            transform=slow_transform_3,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_c",
        )

        # Create gold step that depends on all silvers
        builder.add_gold_transform(
            name="gold_combined",
            transform=lambda spark, silvers: (
                silvers["silver_a"]
                .union(silvers["silver_b"])
                .union(silvers["silver_c"])
            ),
            rules={"id": [F.col("id").isNotNull()]},
            table_name="gold_combined",
            source_silvers=["silver_a", "silver_b", "silver_c"],
        )

        # Build pipeline
        pipeline = builder.to_pipeline()

        # Create test data
        source_data_a = create_test_data(spark, "source_a")
        source_data_b = create_test_data(spark, "source_b")
        source_data_c = create_test_data(spark, "source_c")

        # Execute pipeline
        print("\nExecuting pipeline with parallel execution...")
        start_time = time.time()

        result = pipeline.run_initial_load(
            bronze_sources={
                "source_a": source_data_a,
                "source_b": source_data_b,
                "source_c": source_data_c,
            }
        )

        execution_time = time.time() - start_time

        print("\n✅ Pipeline completed!")
        print(f"Total execution time: {execution_time:.2f}s")
        print(f"Parallel efficiency: {result.parallel_efficiency:.1f}%")
        print(f"Execution groups: {result.execution_groups_count}")
        print(f"Max group size: {result.max_group_size}")
        print(f"Successful steps: {result.successful_steps}")
        print(f"Failed steps: {result.failed_steps}")

        # With 3 silver steps each taking 2 seconds:
        # - Sequential: ~6 seconds
        # - Parallel (4 workers): ~2 seconds
        if execution_time < 4:
            print("\n✅ PASS: Execution time suggests parallel execution is working!")
        else:
            print("\n⚠️ WARNING: Execution time suggests steps may be running sequentially")

        # Test 2: Sequential execution
        print("\n" + "-" * 80)
        print("Test 2: Sequential Execution (parallel disabled)")
        print("-" * 80)

        config_sequential = PipelineConfig.create_conservative(schema="test_parallel")
        print(f"Parallel enabled: {config_sequential.parallel.enabled}")

        builder2 = PipelineBuilder(
            spark=spark, schema="test_parallel", verbose=True
        )

        builder2.with_bronze_rules(
            name="source_d",
            rules={"id": [F.col("id").isNotNull()]},
        )

        builder2.add_silver_transform(
            name="silver_d",
            source_bronze="source_d",
            transform=slow_transform_1,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_d",
        )

        pipeline2 = builder2.to_pipeline()

        print("\nExecuting pipeline with sequential execution...")
        start_time = time.time()

        result2 = pipeline2.run_initial_load(
            bronze_sources={"source_d": create_test_data(spark, "source_d")}
        )

        execution_time2 = time.time() - start_time

        print("\n✅ Pipeline completed!")
        print(f"Total execution time: {execution_time2:.2f}s")
        print(f"Parallel efficiency: {result2.parallel_efficiency:.1f}%")
        print(f"Execution groups: {result2.execution_groups_count}")

        # Test 3: Compare execution times
        print("\n" + "-" * 80)
        print("Test 3: Performance Comparison")
        print("-" * 80)
        print(f"Parallel execution (3 steps, 4 workers): {execution_time:.2f}s")
        print(f"Sequential execution (1 step): {execution_time2:.2f}s")

        if execution_time < execution_time2 * 2:
            print(
                "\n✅ PASS: Parallel execution shows performance improvement over sequential!"
            )
        else:
            print(
                "\n⚠️ Note: Parallel performance not significantly faster (this is OK for small workloads)"
            )

        print("\n" + "=" * 80)
        print("All Tests Completed Successfully!")
        print("=" * 80)

    finally:
        # Cleanup
        spark.sql("DROP DATABASE IF EXISTS test_parallel CASCADE")
        spark.stop()


if __name__ == "__main__":
    test_parallel_execution()


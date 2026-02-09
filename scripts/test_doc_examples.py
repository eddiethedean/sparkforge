#!/usr/bin/env python3
"""
Test script to run examples from the documentation and capture real outputs.

This script tests the examples from:
- docs/guides/VALIDATION_ONLY_STEPS_GUIDE.md
- USER_GUIDE.md (new sections)

Run with: python scripts/test_doc_examples.py
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))
sys.path.insert(0, str(project_root))

# Set up environment
SPARK_MODE = os.environ.get("SPARK_MODE", "mock").lower()
os.environ["SPARK_MODE"] = SPARK_MODE

# Import and configure engine BEFORE importing pipeline_builder
from pipeline_builder.engine_config import configure_engine  # noqa: E402

if SPARK_MODE == "real":
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as pyspark_functions
    from pyspark.sql import types as pyspark_types
    from pyspark.sql.functions import desc as pyspark_desc
    from pyspark.sql.utils import AnalysisException as PySparkAnalysisException
    from pyspark.sql.window import Window as PySparkWindow

    spark = (
        SparkSession.builder.appName("DocExamples")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .master("local[1]")
        .getOrCreate()
    )

    configure_engine(
        functions=pyspark_functions,
        types=pyspark_types,
        analysis_exception=PySparkAnalysisException,
        window=PySparkWindow,
        desc=pyspark_desc,
        engine_name="pyspark",
    )
else:
    from sparkless import SparkSession  # type: ignore[import]
    from sparkless import functions as mock_functions
    from sparkless import spark_types as mock_types  # type: ignore[import]
    from sparkless.functions import desc as mock_desc  # type: ignore[import]
    from sparkless.window import Window as MockWindow  # type: ignore[import]

    # Create mock AnalysisException
    class MockAnalysisException(Exception):
        pass

    spark = SparkSession("DocExamples")

    configure_engine(
        functions=mock_functions,
        types=mock_types,
        analysis_exception=MockAnalysisException,
        window=MockWindow,
        desc=mock_desc,
        engine_name="sparkless",
    )

# Now import pipeline_builder components
from pipeline_builder import PipelineBuilder  # noqa: E402
from pipeline_builder.functions import get_default_functions  # noqa: E402
from pipeline_builder.pipeline.runner import SimplePipelineRunner  # noqa: E402
from pipeline_builder_base.models import PipelineConfig  # noqa: E402

F = get_default_functions()

print("=" * 80)
print("Testing Documentation Examples")
print("=" * 80)
print(f"SPARK_MODE: {SPARK_MODE}")
print()

# Test 1: Basic with_silver_rules
print("Test 1: Basic with_silver_rules")
print("-" * 80)
try:
    builder = PipelineBuilder(spark=spark, schema="test_schema")

    # Create a table first (for validation-only step to read from)
    test_data = [
        {"user_id": 1, "event_date": "2025-01-01", "value": 100},
        {"user_id": 2, "event_date": "2025-01-02", "value": 200},
    ]
    test_df = spark.createDataFrame(test_data)

    # Create schema and table
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    test_df.write.mode("overwrite").saveAsTable("test_schema.clean_events")
    print("✅ Created test table: test_schema.clean_events")

    # Add validation-only silver step
    builder.with_silver_rules(
        name="existing_clean_events",
        table_name="clean_events",
        rules={
            "user_id": [F.col("user_id").isNotNull()],
            "event_date": [F.col("event_date").isNotNull()],
            "value": [F.col("value") > 0],
        },
    )

    # Check the step properties
    pipeline = builder.to_pipeline()
    # Get step from builder's silver_steps dict
    silver_step = builder.silver_steps.get("existing_clean_events")

    if silver_step:
        print(f"✅ Step created: {silver_step.name}")
        print(f"   - transform is None: {silver_step.transform is None}")
        print(f"   - existing: {silver_step.existing}")
        print(f"   - source_bronze: '{silver_step.source_bronze}'")
        print(f"   - table_name: {silver_step.table_name}")
        print(f"   - rules: {list(silver_step.rules.keys())}")
    else:
        print("❌ Step not found")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback

    traceback.print_exc()

print()

# Test 2: Basic with_gold_rules
print("Test 2: Basic with_gold_rules")
print("-" * 80)
try:
    builder = PipelineBuilder(spark=spark, schema="test_schema")

    # Create a gold table
    gold_data = [
        {"user_id": 1, "total_events": 10, "last_activity": "2025-01-01"},
        {"user_id": 2, "total_events": 20, "last_activity": "2025-01-02"},
    ]
    gold_df = spark.createDataFrame(gold_data)
    gold_df.write.mode("overwrite").saveAsTable("test_schema.user_metrics")
    print("✅ Created test table: test_schema.user_metrics")

    # Add validation-only gold step
    builder.with_gold_rules(
        name="existing_user_metrics",
        table_name="user_metrics",
        rules={
            "user_id": [F.col("user_id").isNotNull()],
            "total_events": [F.col("total_events") > 0],
            "last_activity": [F.col("last_activity").isNotNull()],
        },
    )

    # Check the step properties
    pipeline = builder.to_pipeline()
    # Get step from builder's gold_steps dict
    gold_step = builder.gold_steps.get("existing_user_metrics")

    if gold_step:
        print(f"✅ Step created: {gold_step.name}")
        print(f"   - transform is None: {gold_step.transform is None}")
        print(f"   - existing: {gold_step.existing}")
        print(f"   - table_name: {gold_step.table_name}")
        print(f"   - rules: {list(gold_step.rules.keys())}")
    else:
        print("❌ Step not found")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback

    traceback.print_exc()

print()

# Test 3: Complete pipeline with validation-only steps
print("Test 3: Complete Pipeline with Validation-Only Steps")
print("-" * 80)
try:
    builder = PipelineBuilder(spark=spark, schema="test_schema")

    # Create existing tables
    existing_silver_data = [
        {"id": 1, "user_id": 1, "value": 100},
        {"id": 2, "user_id": 2, "value": 200},
    ]
    existing_silver_df = spark.createDataFrame(existing_silver_data)
    existing_silver_df.write.mode("overwrite").saveAsTable("test_schema.clean_events")

    existing_gold_data = [
        {"user_id": 1, "total_events": 10},
        {"user_id": 2, "total_events": 20},
    ]
    existing_gold_df = spark.createDataFrame(existing_gold_data)
    existing_gold_df.write.mode("overwrite").saveAsTable("test_schema.user_metrics")

    print("✅ Created existing tables")

    # 1. Add bronze step
    builder.with_bronze_rules(
        name="events",
        rules={"id": [F.col("id").isNotNull()]},
        incremental_col="timestamp",
    )
    print("✅ Added bronze step: events")

    # 2. Validate existing silver table
    builder.with_silver_rules(
        name="existing_clean_events",
        table_name="clean_events",
        rules={"id": [F.col("id").isNotNull()]},
        watermark_col="updated_at",
    )
    print("✅ Added validation-only silver step: existing_clean_events")

    # 3. Validate existing gold table
    builder.with_gold_rules(
        name="existing_user_metrics",
        table_name="user_metrics",
        rules={"user_id": [F.col("user_id").isNotNull()]},
    )
    print("✅ Added validation-only gold step: existing_user_metrics")

    # 4. Add new silver transform that uses prior_silvers and prior_golds
    def enriched_silver_transform(spark, bronze_df, prior_silvers, prior_golds=None):
        result = bronze_df

        # Use validated existing silver table
        if "existing_clean_events" in prior_silvers:
            existing = prior_silvers["existing_clean_events"]
            result = result.withColumn("has_existing", F.lit(True))
            print(f"   📊 Accessed existing_clean_events: {existing.count()} rows")

        # Use validated existing gold table
        if prior_golds and "existing_user_metrics" in prior_golds:
            metrics = prior_golds["existing_user_metrics"]
            result = result.withColumn("has_metrics", F.lit(True))
            print(f"   📊 Accessed existing_user_metrics: {metrics.count()} rows")

        return result

    builder.add_silver_transform(
        name="enriched_events",
        source_bronze="events",
        transform=enriched_silver_transform,
        rules={"id": [F.col("id").isNotNull()]},
        table_name="enriched_events",
    )
    print("✅ Added silver transform: enriched_events")

    # 5. Add new gold transform that uses prior_golds
    def enhanced_gold_transform(spark, silvers, prior_golds=None):
        result = silvers["enriched_events"]

        # Use previously validated gold table
        if prior_golds and "existing_user_metrics" in prior_golds:
            existing_metrics = prior_golds["existing_user_metrics"]
            # Join with existing metrics
            result = result.join(existing_metrics, "user_id", "left")
            print(
                f"   📊 Joined with existing_user_metrics: {existing_metrics.count()} rows"
            )

        return result

    builder.add_gold_transform(
        name="enhanced_metrics",
        transform=enhanced_gold_transform,
        rules={"user_id": [F.col("user_id").isNotNull()]},
        table_name="enhanced_metrics",
        source_silvers=["enriched_events"],
    )
    print("✅ Added gold transform: enhanced_metrics")

    # Build pipeline
    pipeline = builder.to_pipeline()
    print(f"✅ Pipeline built with {len(pipeline.steps)} steps")

    # Create bronze source data
    bronze_data = [
        {"id": 1, "user_id": 1, "timestamp": "2025-01-01 10:00:00"},
        {"id": 2, "user_id": 2, "timestamp": "2025-01-01 11:00:00"},
    ]
    bronze_df = spark.createDataFrame(bronze_data)

    # Run pipeline
    config = PipelineConfig.create_default(schema="test_schema")
    runner = SimplePipelineRunner(spark, config)

    print("\n🚀 Running pipeline...")
    report = runner.run_pipeline(pipeline.steps, bronze_sources={"events": bronze_df})

    print("\n✅ Pipeline execution completed")
    print(f"   Status: {report.status}")
    if hasattr(report, "step_results") and report.step_results:
        print(f"   Steps executed: {len(report.step_results)}")
        for step_name, step_result in report.step_results.items():
            status = getattr(step_result, "status", "unknown")
            print(f"   - {step_name}: {status}")
    elif hasattr(report, "steps") and report.steps:
        print(f"   Steps executed: {len(report.steps)}")
        for step_result in report.steps:
            step_name = getattr(step_result, "step_name", "unknown")
            status = getattr(step_result, "status", "unknown")
            print(f"   - {step_name}: {status}")
    else:
        print("   Steps executed: 0 (no step results available)")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback

    traceback.print_exc()

print()
print("=" * 80)
print("All tests completed!")
print("=" * 80)

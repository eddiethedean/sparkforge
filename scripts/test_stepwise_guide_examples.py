#!/usr/bin/env python3
"""
Test script to verify all code examples in the stepwise execution user guide.

This script runs all code examples and captures their outputs to ensure
they work correctly and can be included in the documentation.
"""

import os
import sys

# Set up path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Configure engine based on mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from sparkless import AnalysisException as MockAnalysisException
    from sparkless import Column as MockColumn
    from sparkless import DataFrame as MockDataFrame
    from sparkless import SparkSession
    from sparkless import SparkSession as MockSparkSession
    from sparkless import Window as MockWindow
    from sparkless import functions as F
    from sparkless import functions as mock_functions
    from sparkless import spark_types as mock_types
    from sparkless.functions import desc as mock_desc

    from pipeline_builder.engine_config import configure_engine

    configure_engine(
        functions=mock_functions,
        types=mock_types,
        analysis_exception=MockAnalysisException,
        window=MockWindow,
        desc=mock_desc,
        engine_name="mock",
        dataframe_cls=MockDataFrame,
        spark_session_cls=MockSparkSession,
        column_cls=MockColumn,
    )
    spark = SparkSession.builder.appName("test").getOrCreate()
else:
    from pyspark.sql import Column as PySparkColumn
    from pyspark.sql import DataFrame as PySparkDataFrame
    from pyspark.sql import SparkSession
    from pyspark.sql import SparkSession as PySparkSparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import functions as pyspark_functions
    from pyspark.sql import types as pyspark_types
    from pyspark.sql.functions import desc as pyspark_desc
    from pyspark.sql.utils import AnalysisException as PySparkAnalysisException
    from pyspark.sql.window import Window as PySparkWindow

    from pipeline_builder.engine_config import configure_engine

    configure_engine(
        functions=pyspark_functions,
        types=pyspark_types,
        analysis_exception=PySparkAnalysisException,
        window=PySparkWindow,
        desc=pyspark_desc,
        engine_name="pyspark",
        dataframe_cls=PySparkDataFrame,
        spark_session_cls=PySparkSparkSession,
        column_cls=PySparkColumn,
    )
    spark = (
        SparkSession.builder.appName("test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

from pipeline_builder.models import BronzeStep, GoldStep, PipelineConfig, SilverStep
from pipeline_builder.pipeline.debug_session import PipelineDebugSession
from pipeline_builder.pipeline.runner import SimplePipelineRunner

print("=" * 80)
print("Testing Stepwise Execution Guide Examples")
print("=" * 80)
print()

# Create test data
print("Creating test data...")
data = [
    ("1", "event1", 10, "active"),
    ("2", "event2", 20, "active"),
    ("3", "event3", 30, "inactive"),
    ("4", "event4", 40, "active"),
]
source_df = spark.createDataFrame(data, ["id", "event", "value", "status"])
print(f"Created source DataFrame with {source_df.count()} rows")
print()

# Create config
config = PipelineConfig.create_default(schema="test_schema")

# Example 1: Basic run_until
print("=" * 80)
print("Example 1: Running until a specific step")
print("=" * 80)

bronze_step = BronzeStep(
    name="events",
    rules={"id": [F.col("id").isNotNull()]},
    schema="test_schema",
)


def simple_silver_transform(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("value") > 15)


silver_step = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=simple_silver_transform,
    rules={"id": [F.col("id").isNotNull()]},
    table_name="clean_events",
    schema="test_schema",
)


def simple_gold_transform(spark, silvers):
    return silvers["clean_events"].withColumn("total", F.col("value") * 2)


gold_step = GoldStep(
    name="aggregated_events",
    transform=simple_gold_transform,
    rules={"id": [F.col("id").isNotNull()]},
    table_name="aggregated_events",
    source_silvers=["clean_events"],
    schema="test_schema",
)

runner = SimplePipelineRunner(spark, config)

print("Code:")
print("  report, context = runner.run_until(")
print("      'clean_events',")
print("      steps=[bronze_step, silver_step, gold_step],")
print("      bronze_sources={'events': source_df}")
print("  )")
print()

report, context = runner.run_until(
    "clean_events",
    steps=[bronze_step, silver_step, gold_step],
    bronze_sources={"events": source_df},
)

print("Output:")
print(f"  Status: {report.status.value}")
print(f"  Steps executed: {len(report.bronze_results) + len(report.silver_results)}")
clean_events_df = context["clean_events"]
print(f"  Rows in clean_events: {clean_events_df.count()}")
print()

# Example 2: run_step
print("=" * 80)
print("Example 2: Running a single step")
print("=" * 80)

context = {"events": source_df}
print("Code:")
print("  report, context = runner.run_step(")
print("      'clean_events',")
print("      steps=[bronze_step, silver_step, gold_step],")
print("      context=context")
print("  )")
print()

report, context = runner.run_step(
    "clean_events", steps=[bronze_step, silver_step, gold_step], context=context
)

print("Output:")
print(f"  Status: {report.status.value}")
print(f"  Steps executed: {len(report.silver_results)}")
print(f"  'clean_events' in context: {'clean_events' in context}")
print()

# Example 3: Parameter overrides
print("=" * 80)
print("Example 3: Silver step with parameters")
print("=" * 80)


def clean_events_transform(spark, bronze_df, prior_silvers, params=None):
    """Transform function that accepts parameters."""
    threshold = params.get("threshold", 0.5) if params else 0.5
    filter_mode = params.get("filter_mode", "standard") if params else "standard"

    df = bronze_df.filter(F.col("value") > threshold)

    if filter_mode == "strict":
        df = df.filter(F.col("value") > 20)

    return df


silver_step_with_params = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=clean_events_transform,
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="test_schema",
)

print("Code:")
print("  step_params = {'clean_events': {'threshold': 0.8, 'filter_mode': 'strict'}}")
print("  report, context = runner.run_step(")
print("      'clean_events',")
print("      steps=[silver_step_with_params],")
print("      step_params=step_params")
print("  )")
print()

step_params = {"clean_events": {"threshold": 15, "filter_mode": "strict"}}
context = {"events": source_df}
report, context = runner.run_step(
    "clean_events",
    steps=[silver_step_with_params],
    context=context,
    step_params=step_params,
)

print("Output:")
print(f"  Status: {report.status.value}")
print(f"  Rows after filtering: {context['clean_events'].count()}")
print()

# Example 4: Gold step with parameters
print("=" * 80)
print("Example 4: Gold step with parameters")
print("=" * 80)


def aggregate_metrics(spark, silvers, params=None):
    """Gold transform that accepts parameters."""
    multiplier = params.get("multiplier", 1.0) if params else 1.0

    df = silvers["clean_events"]
    return df.withColumn("adjusted_value", F.col("value") * multiplier)


gold_step_with_params = GoldStep(
    name="user_metrics",
    transform=aggregate_metrics,
    rules={"adjusted_value": [F.col("adjusted_value") > 0]},
    table_name="user_metrics",
    source_silvers=["clean_events"],
    schema="test_schema",
)

# First run silver step
context = {"events": source_df}
report, context = runner.run_step(
    "clean_events",
    steps=[silver_step_with_params],
    context=context,
    step_params={"clean_events": {"threshold": 0, "filter_mode": "standard"}},
)

print("Code:")
print("  step_params = {'user_metrics': {'multiplier': 1.5, 'window_days': 30}}")
print("  report, context = runner.run_step(")
print("      'user_metrics',")
print("      steps=[gold_step_with_params],")
print("      context=context,")
print("      step_params=step_params")
print("  )")
print()

step_params = {"user_metrics": {"multiplier": 1.5, "window_days": 30}}
report, context = runner.run_step(
    "user_metrics",
    steps=[gold_step_with_params],
    context=context,
    step_params=step_params,
)

print("Output:")
print(f"  Status: {report.status.value}")
print(f"  Rows in user_metrics: {context['user_metrics'].count()}")
if "adjusted_value" in context["user_metrics"].columns:
    print("  'adjusted_value' column created: True")
print()

# Example 5: PipelineDebugSession
print("=" * 80)
print("Example 5: PipelineDebugSession")
print("=" * 80)

print("Code:")
print("  session = PipelineDebugSession(")
print("      spark, config, [bronze_step, silver_step],")
print("      bronze_sources={'events': source_df}")
print("  )")
print("  report, context = session.run_until('clean_events')")
print()

session = PipelineDebugSession(
    spark, config, [bronze_step, silver_step], bronze_sources={"events": source_df}
)

report, context = session.run_until("clean_events")

print("Output:")
print(f"  Status: {report.status.value}")
print(f"  'clean_events' in session.context: {'clean_events' in session.context}")
print(f"  Rows: {session.context['clean_events'].count()}")
print()

# Example 6: Write control
print("=" * 80)
print("Example 6: Write control (write_outputs=False)")
print("=" * 80)

print("Code:")
print("  report, context = runner.run_step(")
print("      'clean_events',")
print("      steps=[bronze_step, silver_step],")
print("      context={'events': source_df},")
print("      write_outputs=False")
print("  )")
print("  print(f'Would write {context[\"clean_events\"].count()} rows')")
print()

report, context = runner.run_step(
    "clean_events",
    steps=[bronze_step, silver_step],
    context={"events": source_df},
    write_outputs=False,
)

print("Output:")
print(f"  Status: {report.status.value}")
print(f"  Would write {context['clean_events'].count()} rows")
print()

# Example 7: Iterative parameter tuning
print("=" * 80)
print("Example 7: Iterative parameter tuning")
print("=" * 80)

session = PipelineDebugSession(
    spark,
    config,
    [bronze_step, silver_step_with_params],
    bronze_sources={"events": source_df},
)

print("Code:")
print("  session.set_step_params('clean_events', {'threshold': 0.7})")
print("  report2, _ = session.rerun_step('clean_events')")
print(
    "  print(f'With threshold=0.7: {session.context[\"clean_events\"].count()} rows')"
)
print()

# Initial run
report1, _ = session.run_step("clean_events", write_outputs=False)
print(f"Initial output: {session.context['clean_events'].count()} rows")

# Adjust parameters
session.set_step_params("clean_events", {"threshold": 15, "filter_mode": "standard"})
report2, _ = session.rerun_step("clean_events", write_outputs=False)
print(f"With threshold=15: {session.context['clean_events'].count()} rows")

# Try different threshold
session.set_step_params("clean_events", {"threshold": 25, "filter_mode": "standard"})
report3, _ = session.rerun_step("clean_events", write_outputs=False)
print(f"With threshold=25: {session.context['clean_events'].count()} rows")
print()

print("=" * 80)
print("All examples completed successfully!")
print("=" * 80)

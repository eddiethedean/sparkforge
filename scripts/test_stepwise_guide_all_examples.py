#!/usr/bin/env python3
"""
Test script to verify all code examples in docs/guides/STEPWISE_EXECUTION_GUIDE.md.

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
print("Testing All Code Examples from docs/guides/STEPWISE_EXECUTION_GUIDE.md")
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

# Example 1: Quick Start - Minimal Example
print("=" * 80)
print("Example 1: Quick Start - Minimal Example")
print("=" * 80)

bronze_step = BronzeStep(
    name="events",
    rules={"id": [F.col("id").isNotNull()]},
    schema="test_schema",
)


def clean_transform(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("value") > 10)


silver_step = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=clean_transform,
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="test_schema",
)

runner = SimplePipelineRunner(spark, config)

source_df_simple = spark.createDataFrame(
    [("1", "event1", 10), ("2", "event2", 20), ("3", "event3", 30)],
    ["id", "event", "value"],
)

report, context = runner.run_until(
    "clean_events",
    steps=[bronze_step, silver_step],
    bronze_sources={"events": source_df_simple},
)

print(f"Status: {report.status.value}")
print(f"Rows: {context['clean_events'].count()}")
print()

# Example 2: run_until()
print("=" * 80)
print("Example 2: run_until()")
print("=" * 80)

bronze_step2 = BronzeStep(
    name="events", rules={"id": [F.col("id").isNotNull()]}, schema="test_schema"
)
silver_step2 = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.filter(F.col("value") > 15),
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="test_schema",
)
gold_step2 = GoldStep(
    name="aggregated",
    transform=lambda spark, silvers: silvers["clean_events"],
    rules={"id": [F.col("id").isNotNull()]},
    table_name="aggregated",
    source_silvers=["clean_events"],
    schema="test_schema",
)

source_df2 = spark.createDataFrame(
    [("1", "event1", 10), ("2", "event2", 20), ("3", "event3", 30)],
    ["id", "event", "value"],
)

report, context = runner.run_until(
    "clean_events",
    steps=[bronze_step2, silver_step2, gold_step2],
    bronze_sources={"events": source_df2},
)

clean_events_df = context["clean_events"]
print(f"Status: {report.status.value}")
print(f"Steps executed: {len(report.bronze_results) + len(report.silver_results)}")
print(f"Rows in clean_events: {clean_events_df.count()}")
print()

# Example 3: run_step()
print("=" * 80)
print("Example 3: run_step()")
print("=" * 80)

context = {"events": source_df2}
report, context = runner.run_step(
    "clean_events", steps=[bronze_step2, silver_step2, gold_step2], context=context
)

print(f"Status: {report.status.value}")
print(f"Steps executed: {len(report.silver_results)}")
print(f"'clean_events' in context: {'clean_events' in context}")
print()

# Example 4: rerun_step()
print("=" * 80)
print("Example 4: rerun_step()")
print("=" * 80)

report1, context = runner.run_step(
    "clean_events", steps=[bronze_step2, silver_step2], context={"events": source_df2}
)
step_params = {"clean_events": {"threshold": 20, "filter_mode": "strict"}}


# Need a step that accepts params
def clean_with_params(spark, bronze_df, prior_silvers, params=None):
    threshold = params.get("threshold", 0) if params else 0
    filter_mode = params.get("filter_mode", "standard") if params else "standard"
    df = bronze_df.filter(F.col("value") > threshold)
    if filter_mode == "strict":
        df = df.filter(F.col("value") > 20)
    return df


silver_step_params = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=clean_with_params,
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="test_schema",
)

report2, context = runner.rerun_step(
    "clean_events",
    steps=[bronze_step2, silver_step_params],
    context=context,
    step_params=step_params,
)

print(f"Status: {report2.status.value}")
print()

# Example 5: Silver Step with Parameters
print("=" * 80)
print("Example 5: Silver Step with Parameters")
print("=" * 80)


def clean_events_transform(spark, bronze_df, prior_silvers, params=None):
    threshold = params.get("threshold", 0) if params else 0
    filter_mode = params.get("filter_mode", "standard") if params else "standard"
    df = bronze_df.filter(F.col("value") > threshold)
    if filter_mode == "strict":
        df = df.filter(F.col("value") > 20)
    return df


silver_step_params2 = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=clean_events_transform,
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="test_schema",
)

step_params2 = {"clean_events": {"threshold": 15, "filter_mode": "strict"}}
context2 = {"events": source_df2}
report, context2 = runner.run_step(
    "clean_events",
    steps=[silver_step_params2],
    context=context2,
    step_params=step_params2,
)

print(f"Status: {report.status.value}")
print(f"Rows after filtering: {context2['clean_events'].count()}")
print()

# Example 6: Gold Step with Parameters
print("=" * 80)
print("Example 6: Gold Step with Parameters")
print("=" * 80)


def aggregate_metrics(spark, silvers, params=None):
    multiplier = params.get("multiplier", 1.0) if params else 1.0
    df = silvers["clean_events"]
    return df.withColumn("adjusted_value", F.col("value") * multiplier)


gold_step_params = GoldStep(
    name="user_metrics",
    transform=aggregate_metrics,
    rules={"adjusted_value": [F.col("adjusted_value") > 0]},
    table_name="user_metrics",
    source_silvers=["clean_events"],
    schema="test_schema",
)

context3 = {"events": source_df2}
report, context3 = runner.run_step(
    "clean_events",
    steps=[silver_step_params2],
    context=context3,
    step_params={"clean_events": {"threshold": 0, "filter_mode": "standard"}},
)

step_params3 = {"user_metrics": {"multiplier": 1.5}}
report, context3 = runner.run_step(
    "user_metrics", steps=[gold_step_params], context=context3, step_params=step_params3
)

print(f"Status: {report.status.value}")
print(f"Rows in user_metrics: {context3['user_metrics'].count()}")
if "adjusted_value" in context3["user_metrics"].columns:
    print("'adjusted_value' column created: True")
print()

# Example 7: PipelineDebugSession
print("=" * 80)
print("Example 7: PipelineDebugSession")
print("=" * 80)

session = PipelineDebugSession(
    spark,
    config,
    steps=[bronze_step2, silver_step2],
    bronze_sources={"events": source_df2},
)

report, context = session.run_until("clean_events")
print(f"Status: {report.status.value}")
print(f"'clean_events' in session.context: {'clean_events' in session.context}")
print(f"Rows: {session.context['clean_events'].count()}")
print()

# Example 8: Write Control
print("=" * 80)
print("Example 8: Write Control")
print("=" * 80)

report, context = runner.run_step(
    "clean_events",
    steps=[bronze_step2, silver_step2],
    context={"events": source_df2},
    write_outputs=False,
)

clean_df = context["clean_events"]
print(f"Status: {report.status.value}")
print(f"Would write {clean_df.count()} rows")
print()

# Example 9: Iterative Refining
print("=" * 80)
print("Example 9: Iterative Refining Transform Logic")
print("=" * 80)

session2 = PipelineDebugSession(
    spark,
    config,
    steps=[bronze_step2, silver_step_params2],
    bronze_sources={"events": source_df2},
)

report1, _ = session2.run_step("clean_events", write_outputs=False)
print(f"Initial output: {session2.context['clean_events'].count()} rows")

session2.set_step_params("clean_events", {"threshold": 15, "filter_mode": "standard"})
report2, _ = session2.rerun_step("clean_events", write_outputs=False)
print(f"With threshold=15: {session2.context['clean_events'].count()} rows")

session2.set_step_params("clean_events", {"threshold": 25, "filter_mode": "standard"})
report3, _ = session2.rerun_step("clean_events", write_outputs=False)
print(f"With threshold=25: {session2.context['clean_events'].count()} rows")
print()

# Example 10: Testing Parameter Changes
print("=" * 80)
print("Example 10: Testing Parameter Changes")
print("=" * 80)

test_params = [
    {"threshold": 10, "filter_mode": "standard"},
    {"threshold": 20, "filter_mode": "strict"},
    {"threshold": 30, "filter_mode": "strict"},
]

context_test = {"events": source_df2}
for params in test_params:
    step_params_test = {"clean_events": params}
    report, context_test = runner.run_step(
        "clean_events",
        steps=[silver_step_params2],
        context=context_test,
        step_params=step_params_test,
        write_outputs=False,
    )
    row_count = context_test["clean_events"].count()
    print(f"Params {params}: {row_count} rows")
print()

# Example 11: Complete Example 1
print("=" * 80)
print("Example 11: Complete Example 1 - Basic Stepwise Execution")
print("=" * 80)

bronze_complete = BronzeStep(
    name="events",
    rules={"id": [F.col("id").isNotNull()]},
    schema="test_schema",
)


def clean_transform_complete(spark, bronze_df, prior_silvers, params=None):
    threshold = params.get("threshold", 0) if params else 0
    return bronze_df.filter(F.col("value") > threshold)


silver_complete = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=clean_transform_complete,
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="test_schema",
)


def aggregate_transform_complete(spark, silvers, params=None):
    multiplier = params.get("multiplier", 1.0) if params else 1.0
    df = silvers["clean_events"]
    return df.withColumn("adjusted_value", F.col("value") * multiplier)


gold_complete = GoldStep(
    name="aggregated_events",
    transform=aggregate_transform_complete,
    rules={"adjusted_value": [F.col("adjusted_value") > 0]},
    table_name="aggregated_events",
    source_silvers=["clean_events"],
    schema="test_schema",
)

data_complete = [
    ("1", "event1", 10),
    ("2", "event2", 20),
    ("3", "event3", 30),
    ("4", "event4", 40),
]
source_df_complete = spark.createDataFrame(data_complete, ["id", "event", "value"])

# Example 1: Run until a step
report, context = runner.run_until(
    "clean_events",
    steps=[bronze_complete, silver_complete, gold_complete],
    bronze_sources={"events": source_df_complete},
)
print(f"Status: {report.status.value}")
print(f"Rows in clean_events: {context['clean_events'].count()}")

# Example 2: Run single step
context = {"events": source_df_complete}
report, context = runner.run_step(
    "clean_events",
    steps=[bronze_complete, silver_complete, gold_complete],
    context=context,
)
print(f"Status: {report.status.value}")

# Example 3: Parameter overrides
step_params = {"clean_events": {"threshold": 15}}
report, context = runner.run_step(
    "clean_events",
    steps=[silver_complete],
    context={"events": source_df_complete},
    step_params=step_params,
)
print(f"Rows with threshold=15: {context['clean_events'].count()}")

# Example 4: DebugSession
session_complete = PipelineDebugSession(
    spark,
    config,
    [bronze_complete, silver_complete],
    bronze_sources={"events": source_df_complete},
)
report, _ = session_complete.run_until("clean_events")
print(f"Session context has clean_events: {'clean_events' in session_complete.context}")

# Example 5: Iterative tuning
session_complete.set_step_params("clean_events", {"threshold": 20})
report, _ = session_complete.rerun_step("clean_events", write_outputs=False)
print(f"With threshold=20: {session_complete.context['clean_events'].count()} rows")
print()

print("=" * 80)
print("All examples completed successfully!")
print("=" * 80)

"""
Real Spark regression test to ensure overwrite to Delta tables does not use
truncate-in-batch (which Delta V2 disallows) and pipeline completes.
"""

import os

import pytest

from pipeline_builder import PipelineBuilder

# Run only in real Spark mode
pytestmark = pytest.mark.skipif(
    os.environ.get("SPARK_MODE", "mock").lower() != "real",
    reason="Requires real PySpark + Delta",
)


def test_initial_load_overwrite_delta(spark_session, unique_schema, unique_table_name):
    """Initial load should succeed writing Silver/Gold tables in Delta."""
    from pyspark.sql import functions as F

    schema = unique_schema
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    data = [
        ("u1", "click", "2024-01-01 10:00:00", 1.0),
        ("u2", "view", "2024-01-01 11:00:00", 2.0),
    ]
    df = spark_session.createDataFrame(data, ["user_id", "action", "timestamp", "value"])

    builder = PipelineBuilder(spark=spark_session, schema=schema)

    builder.with_bronze_rules(
        name="events",
        rules={
            "user_id": [F.col("user_id").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()],
        },
        incremental_col="timestamp",
    )

    def silver_transform(_spark, bronze_df, _silvers):
        return bronze_df.withColumn("event_date", F.to_date("timestamp"))

    builder.add_silver_transform(
        name="processed",
        source_bronze="events",
        transform=silver_transform,
        rules={
            "user_id": [F.col("user_id").isNotNull()],
            "event_date": [F.col("event_date").isNotNull()],
        },
        table_name=unique_table_name("processed"),
        schema=schema,
    )

    def gold_transform(_spark, silvers):
        return silvers["processed"].groupBy("event_date").agg(F.count("*").alias("cnt"))

    builder.add_gold_transform(
        name="summary",
        transform=gold_transform,
        rules={"cnt": [F.col("cnt") >= 0]},
        table_name=unique_table_name("summary"),
        source_silvers=["processed"],
        schema=schema,
    )

    pipeline = builder.to_pipeline()
    result = pipeline.run_initial_load(bronze_sources={"events": df})

    assert result.status.value == "completed"
    # Basic sanity: tables exist
    assert spark_session.catalog.tableExists(f"{schema}.{builder.silver_steps['processed'].table_name}")
    assert spark_session.catalog.tableExists(f"{schema}.{builder.gold_steps['summary'].table_name}")


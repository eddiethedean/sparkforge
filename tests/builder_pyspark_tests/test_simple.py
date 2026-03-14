"""
Simple test to verify basic functionality. Runs in both mock and real mode.
"""

import os
import sys

import pytest

from pipeline_builder.pipeline import PipelineBuilder

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test_helpers.isolation import get_unique_schema


def test_simple_pipeline_creation(spark):
    """Test basic pipeline creation with PySpark."""

    # Create unique schema for this test
    bronze_schema = get_unique_schema("bronze")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_schema}")

    # Create pipeline builder
    builder = PipelineBuilder(spark=spark, schema=bronze_schema, verbose=True)

    # Add bronze rules with string-based validation
    builder.with_bronze_rules(
        name="test_orders",
        rules={"order_id": ["not_null"], "customer_id": ["not_null"]},
    )

    # Build pipeline
    pipeline = builder.to_pipeline()

    # Verify pipeline was created
    assert pipeline is not None
    assert hasattr(pipeline, "run_initial_load")
    assert hasattr(pipeline, "run_incremental")

    print("✅ Simple pipeline test passed")

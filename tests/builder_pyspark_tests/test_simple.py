"""
Simple test to verify basic functionality.
"""

import os

import pytest

# Skip all tests in this module if SPARK_MODE is not "real"
if os.environ.get("SPARK_MODE", "mock").lower() != "real":
    pytestmark = pytest.mark.skip(
        reason="PySpark-specific tests require SPARK_MODE=real"
    )

from pipeline_builder.pipeline import PipelineBuilder


def test_simple_pipeline_creation(spark_session):
    """Test basic pipeline creation with PySpark."""

    # Create pipeline builder
    builder = PipelineBuilder(spark=spark_session, schema="bronze", verbose=True)

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

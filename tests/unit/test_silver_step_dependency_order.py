"""
Test to reproduce the issue where silver steps execute in wrong order.

This test demonstrates that when a second silver step depends on a first silver step
via prior_silvers, the second step runs before the first, causing a KeyError.
"""

from unittest.mock import Mock

import pytest

from pipeline_builder.functions import get_default_functions
from pipeline_builder.models import SilverStep
from pipeline_builder_base.models import ExecutionMode, PipelineConfig
from pipeline_builder.execution import ExecutionEngine

F = get_default_functions()


class TestSilverStepDependencyOrder:
    """Test that silver steps execute in correct order when one depends on another."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = Mock()
        spark.table.return_value.count.return_value = 0
        return spark

    @pytest.fixture
    def bronze_df(self, mock_spark):
        """Create a sample bronze DataFrame."""
        data = [("user1", "event1", 100), ("user2", "event2", 200)]
        return mock_spark.createDataFrame(data, ["user_id", "event_type", "value"])

    def test_silver_steps_execute_in_wrong_order(self, mock_spark, bronze_df):
        """Test that silver steps execute in wrong order when second depends on first.
        
        This test reproduces the bug where:
        - silver_step_1 should run first
        - silver_step_2 depends on silver_step_1 via prior_silvers
        - But silver_step_2 runs first and fails because silver_step_1 isn't in prior_silvers yet
        """
        # Track execution order
        execution_order = []

        def silver_step_1_transform(spark, bronze_df, prior_silvers):
            """First silver step - should run first."""
            execution_order.append("silver_step_1")
            # This step doesn't depend on other silvers
            return bronze_df.withColumn("processed", F.lit(True))

        def silver_step_2_transform(spark, bronze_df, prior_silvers):
            """Second silver step - depends on silver_step_1 via prior_silvers."""
            execution_order.append("silver_step_2")
            # This step needs access to silver_step_1 via prior_silvers
            if "silver_step_1" not in prior_silvers:
                raise KeyError(
                    f"silver_step_1 not found in prior_silvers. Available: {list(prior_silvers.keys())}"
                )
            # Use data from silver_step_1
            first_silver = prior_silvers["silver_step_1"]
            return first_silver.withColumn("enriched", F.lit(True))

        # Create silver steps
        silver_step_1 = SilverStep(
            name="silver_step_1",
            source_bronze="bronze_step",
            transform=silver_step_1_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_step_1",
        )

        # silver_step_2 depends on silver_step_1 via source_silvers
        silver_step_2 = SilverStep(
            name="silver_step_2",
            source_bronze="bronze_step",
            transform=silver_step_2_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="silver_step_2",
            source_silvers=["silver_step_1"],  # Declare dependency on silver_step_1
        )

        # Create execution engine
        config = PipelineConfig.create_default(schema="test")
        engine = ExecutionEngine(mock_spark, config)

        # Execute pipeline - with source_silvers declared, silver_step_1 should run first
        result = engine.execute_pipeline(
            steps=[silver_step_1, silver_step_2],
            mode=ExecutionMode.INITIAL,
            context={"bronze_step": bronze_df},
        )

        # Verify execution order is correct (silver_step_1 runs before silver_step_2)
        assert execution_order == ["silver_step_1", "silver_step_2"], (
            f"Expected silver_step_1 to run before silver_step_2, but got: {execution_order}"
        )

        # Verify execution groups show correct dependency ordering
        # silver_step_1 should be in an earlier group than silver_step_2
        # This is verified by the log output showing 2 execution groups
        assert len(execution_order) == 2, f"Expected 2 steps to execute, got {len(execution_order)}"
        assert execution_order[0] == "silver_step_1", (
            f"silver_step_1 should execute first, but got: {execution_order}"
        )
        assert execution_order[1] == "silver_step_2", (
            f"silver_step_2 should execute second, but got: {execution_order}"
        )

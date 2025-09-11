#!/usr/bin/env python3
"""
Tests for Bronze tables without datetime columns.

This module tests the functionality where Bronze tables don't have datetime columns
and therefore force full refresh of downstream Silver tables.
"""

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from sparkforge import PipelineBuilder, ExecutionMode


class TestBronzeNoDatetime:
    """Test Bronze tables without datetime columns."""

    @pytest.fixture
    def sample_data_no_datetime(self, spark_session):
        """Create sample data without datetime columns."""
        data = [
            ("user1", "click", 100),
            ("user2", "view", 200),
            ("user3", "purchase", 300),
            ("user4", "click", 150),
            ("user5", "view", 250),
        ]
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("value", IntegerType(), True)
        ])
        return spark_session.createDataFrame(data, schema)

    @pytest.mark.spark
    def test_bronze_without_incremental_col(self, spark_session, sample_data_no_datetime):
        """Test Bronze step without incremental column."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema"
        )
        
        # Add Bronze step without incremental_col
        builder.with_bronze_rules(
            name="events_no_datetime",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value").isNotNull(), F.col("value") > 0]
            }
            # Note: no incremental_col parameter
        )
        
        # Add Silver step
        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df.withColumn("processed_at", F.current_timestamp())
        
        builder.add_silver_transform(
            name="silver_events",
            source_bronze="events_no_datetime",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "processed_at": [F.col("processed_at").isNotNull()]
            },
            table_name="silver_events_no_datetime",
            # No watermark_col since Bronze has no datetime column
        )
        
        # Build and run pipeline
        runner = builder.to_pipeline()
        
        # Test initial run
        result = runner.initial_load(
            bronze_sources={"events_no_datetime": sample_data_no_datetime}
        )
        
        assert result.status.name == "COMPLETED"
        assert "events_no_datetime" in result.bronze_results
        assert "silver_events" in result.silver_results
        
        # Verify Bronze step has no incremental capability
        bronze_step = builder.bronze_steps["events_no_datetime"]
        assert not bronze_step.has_incremental_capability
        assert bronze_step.incremental_col is None

    @pytest.mark.spark
    def test_silver_uses_overwrite_mode_without_bronze_incremental(self, spark_session, sample_data_no_datetime):
        """Test that Silver steps use overwrite mode when Bronze has no incremental column."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema"
        )
        
        # Add Bronze step without incremental_col
        builder.with_bronze_rules(
            name="events_no_datetime",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()]
            }
        )
        
        # Add Silver step
        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df.withColumn("processed_at", F.current_timestamp())
        
        builder.add_silver_transform(
            name="silver_events",
            source_bronze="events_no_datetime",
            transform=silver_transform,
            rules={},
            table_name="silver_events_no_datetime",
            # No watermark_col since Bronze has no datetime column
        )
        
        runner = builder.to_pipeline()
        
        # Run in incremental mode - should still use overwrite for Silver
        result = runner.run_incremental(
            bronze_sources={"events_no_datetime": sample_data_no_datetime}
        )
        
        assert result.status.name == "COMPLETED"
        
        # Check that Silver step used overwrite mode
        silver_result = result.silver_results["silver_events"]
        assert silver_result["write"]["mode"] == "overwrite"

    @pytest.mark.spark
    def test_bronze_with_incremental_col_still_works(self, spark_session, sample_data_no_datetime):
        """Test that Bronze steps with incremental_col still work as before."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema"
        )
        
        # Add Bronze step with incremental_col (even though data doesn't have datetime)
        builder.with_bronze_rules(
            name="events_with_incremental",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()]
            },
            incremental_col="user_id"  # Using user_id as incremental column
        )
        
        # Add Silver step
        def silver_transform(spark, bronze_df, prior_silvers):
            return bronze_df.withColumn("processed_at", F.current_timestamp())
        
        builder.add_silver_transform(
            name="silver_events",
            source_bronze="events_with_incremental",
            transform=silver_transform,
            rules={},
            table_name="silver_events_with_incremental",
            watermark_col="processed_at"
        )
        
        runner = builder.to_pipeline()
        
        # Test initial run
        result = runner.initial_load(
            bronze_sources={"events_with_incremental": sample_data_no_datetime}
        )
        
        assert result.status.name == "COMPLETED"
        
        # Verify Bronze step has incremental capability
        bronze_step = builder.bronze_steps["events_with_incremental"]
        assert bronze_step.has_incremental_capability
        assert bronze_step.incremental_col == "user_id"

    @pytest.mark.spark
    def test_multiple_bronze_steps_mixed_incremental(self, spark_session, sample_data_no_datetime):
        """Test multiple Bronze steps with mixed incremental capabilities."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema"
        )
        
        # Bronze step with incremental column
        builder.with_bronze_rules(
            name="events_with_incremental",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="user_id"
        )
        
        # Bronze step without incremental column
        builder.with_bronze_rules(
            name="events_no_incremental",
            rules={"user_id": [F.col("user_id").isNotNull()]}
        )
        
        # Silver step from incremental Bronze
        def silver_transform_incremental(spark, bronze_df, prior_silvers):
            return bronze_df.withColumn("processed_at", F.current_timestamp())
        
        builder.add_silver_transform(
            name="silver_incremental",
            source_bronze="events_with_incremental",
            transform=silver_transform_incremental,
            rules={},
            table_name="silver_incremental",
            watermark_col="processed_at"
        )
        
        # Silver step from non-incremental Bronze
        def silver_transform_no_incremental(spark, bronze_df, prior_silvers):
            return bronze_df.withColumn("processed_at", F.current_timestamp())
        
        builder.add_silver_transform(
            name="silver_no_incremental",
            source_bronze="events_no_incremental",
            transform=silver_transform_no_incremental,
            rules={},
            table_name="silver_no_incremental",
            # No watermark_col since Bronze has no datetime column
        )
        
        runner = builder.to_pipeline()
        
        # Run in incremental mode
        result = runner.run_incremental(
            bronze_sources={
                "events_with_incremental": sample_data_no_datetime,
                "events_no_incremental": sample_data_no_datetime
            }
        )
        
        assert result.status.name == "COMPLETED"
        
        # Check that Silver from incremental Bronze uses append
        silver_incremental_result = result.silver_results["silver_incremental"]
        assert silver_incremental_result["write"]["mode"] == "append"
        
        # Check that Silver from non-incremental Bronze uses overwrite
        silver_no_incremental_result = result.silver_results["silver_no_incremental"]
        assert silver_no_incremental_result["write"]["mode"] == "overwrite"

    @pytest.mark.spark
    def test_bronze_step_validation(self, spark_session):
        """Test Bronze step validation with optional incremental_col."""
        from sparkforge.models import BronzeStep
        
        # Test Bronze step without incremental_col
        step_no_incremental = BronzeStep(
            name="test_step",
            rules={"user_id": [F.col("user_id").isNotNull()]}
        )
        
        assert step_no_incremental.incremental_col is None
        assert not step_no_incremental.has_incremental_capability
        
        # Test Bronze step with incremental_col
        step_with_incremental = BronzeStep(
            name="test_step",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="user_id"
        )
        
        assert step_with_incremental.incremental_col == "user_id"
        assert step_with_incremental.has_incremental_capability

    @pytest.mark.spark
    def test_pipeline_builder_with_bronze_rules_optional_incremental(self, spark_session):
        """Test PipelineBuilder.with_bronze_rules with optional incremental_col."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema"
        )
        
        # Test without incremental_col
        builder.with_bronze_rules(
            name="test_no_incremental",
            rules={"user_id": [F.col("user_id").isNotNull()]}
        )
        
        assert "test_no_incremental" in builder.bronze_steps
        bronze_step = builder.bronze_steps["test_no_incremental"]
        assert bronze_step.incremental_col is None
        assert not bronze_step.has_incremental_capability
        
        # Test with incremental_col
        builder.with_bronze_rules(
            name="test_with_incremental",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="user_id"
        )
        
        assert "test_with_incremental" in builder.bronze_steps
        bronze_step = builder.bronze_steps["test_with_incremental"]
        assert bronze_step.incremental_col == "user_id"
        assert bronze_step.has_incremental_capability

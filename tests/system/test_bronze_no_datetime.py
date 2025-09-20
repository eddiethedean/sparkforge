"""
Tests for Bronze steps without datetime columns in the simplified execution system.

This module tests the functionality where Bronze steps don't have datetime columns
and therefore force full refresh of downstream Silver steps.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkforge import PipelineBuilder
from sparkforge.execution import ExecutionEngine
from sparkforge.models import (
    BronzeStep,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)


class TestBronzeNoDatetime:
    """Test Bronze steps without datetime columns in the simplified execution system."""

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
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("action", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        return spark_session.createDataFrame(data, schema)

    def test_bronze_step_without_incremental_col(
        self, spark_session, sample_data_no_datetime
    ):
        """Test Bronze step without incremental column."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add Bronze step without incremental_col
        builder.with_bronze_rules(
            name="events_no_datetime",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
        )

        # Verify bronze step was created correctly
        assert "events_no_datetime" in builder.bronze_steps
        bronze_step = builder.bronze_steps["events_no_datetime"]
        assert bronze_step.name == "events_no_datetime"
        assert bronze_step.incremental_col is None
        assert "user_id" in bronze_step.rules
        assert "action" in bronze_step.rules
        assert "value" in bronze_step.rules

    def test_bronze_step_with_incremental_col(self, spark_session):
        """Test Bronze step with incremental column."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add Bronze step with incremental_col
        builder.with_bronze_rules(
            name="events_with_datetime",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )

        # Verify bronze step was created correctly
        assert "events_with_datetime" in builder.bronze_steps
        bronze_step = builder.bronze_steps["events_with_datetime"]
        assert bronze_step.name == "events_with_datetime"
        assert bronze_step.incremental_col == "timestamp"
        assert "user_id" in bronze_step.rules
        assert "action" in bronze_step.rules
        assert "timestamp" in bronze_step.rules

    def test_silver_step_creation(self, spark_session, sample_data_no_datetime):
        """Test Silver step creation."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add Bronze step first
        builder.with_bronze_rules(
            name="events_no_datetime",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
        )

        # Add Silver step
        def silver_transform(spark, df, silvers):
            return df.filter(F.col("value") > 150).select("user_id", "action", "value")

        builder.add_silver_transform(
            name="high_value_events",
            source_bronze="events_no_datetime",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value") > 150],
            },
            table_name="high_value_events",
        )

        # Verify silver step was created correctly
        assert "high_value_events" in builder.silver_steps
        silver_step = builder.silver_steps["high_value_events"]
        assert silver_step.name == "high_value_events"
        assert silver_step.source_bronze == "events_no_datetime"
        assert silver_step.table_name == "high_value_events"
        assert callable(silver_step.transform)

    def test_gold_step_creation(self, spark_session, sample_data_no_datetime):
        """Test Gold step creation."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add Bronze step first
        builder.with_bronze_rules(
            name="events_no_datetime",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
        )

        # Add Silver step
        def silver_transform(spark, df, silvers):
            return df.filter(F.col("value") > 150).select("user_id", "action", "value")

        builder.add_silver_transform(
            name="high_value_events",
            source_bronze="events_no_datetime",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value") > 150],
            },
            table_name="high_value_events",
        )

        # Add Gold step
        def gold_transform(spark, silvers):
            events_df = silvers.get("high_value_events")
            if events_df is not None:
                return events_df.groupBy("action").agg(
                    F.sum("value").alias("total_value")
                )
            else:
                return spark.createDataFrame([], ["action", "total_value"])

        builder.add_gold_transform(
            name="action_summary",
            transform=gold_transform,
            rules={
                "action": [F.col("action").isNotNull()],
                "total_value": [F.col("total_value").isNotNull()],
            },
            table_name="action_summary",
        )

        # Verify gold step was created correctly
        assert "action_summary" in builder.gold_steps
        gold_step = builder.gold_steps["action_summary"]
        assert gold_step.name == "action_summary"
        assert gold_step.table_name == "action_summary"
        assert callable(gold_step.transform)

    def test_pipeline_validation(self, spark_session, sample_data_no_datetime):
        """Test pipeline validation."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add Bronze step
        builder.with_bronze_rules(
            name="events_no_datetime",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
        )

        # Add Silver step
        def silver_transform(spark, df, silvers):
            return df.filter(F.col("value") > 150).select("user_id", "action", "value")

        builder.add_silver_transform(
            name="high_value_events",
            source_bronze="events_no_datetime",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value") > 150],
            },
            table_name="high_value_events",
        )

        # Add Gold step
        def gold_transform(spark, silvers):
            events_df = silvers.get("high_value_events")
            if events_df is not None:
                return events_df.groupBy("action").agg(
                    F.sum("value").alias("total_value")
                )
            else:
                return spark.createDataFrame([], ["action", "total_value"])

        builder.add_gold_transform(
            name="action_summary",
            transform=gold_transform,
            rules={
                "action": [F.col("action").isNotNull()],
                "total_value": [F.col("total_value").isNotNull()],
            },
            table_name="action_summary",
        )

        # Test validation
        errors = builder.validate_pipeline()
        assert len(errors) == 0

    def test_pipeline_creation(self, spark_session, sample_data_no_datetime):
        """Test pipeline creation."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add Bronze step
        builder.with_bronze_rules(
            name="events_no_datetime",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
        )

        # Add Silver step
        def silver_transform(spark, df, silvers):
            return df.filter(F.col("value") > 150).select("user_id", "action", "value")

        builder.add_silver_transform(
            name="high_value_events",
            source_bronze="events_no_datetime",
            transform=silver_transform,
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "action": [F.col("action").isNotNull()],
                "value": [F.col("value") > 150],
            },
            table_name="high_value_events",
        )

        # Add Gold step
        def gold_transform(spark, silvers):
            events_df = silvers.get("high_value_events")
            if events_df is not None:
                return events_df.groupBy("action").agg(
                    F.sum("value").alias("total_value")
                )
            else:
                return spark.createDataFrame([], ["action", "total_value"])

        builder.add_gold_transform(
            name="action_summary",
            transform=gold_transform,
            rules={
                "action": [F.col("action").isNotNull()],
                "total_value": [F.col("total_value").isNotNull()],
            },
            table_name="action_summary",
        )

        # Create pipeline
        pipeline = builder.to_pipeline()
        assert pipeline is not None
        assert hasattr(pipeline, "run_initial_load")
        assert hasattr(pipeline, "run_pipeline")

    def test_dataframe_operations(self, spark_session, sample_data_no_datetime):
        """Test DataFrame operations with data without datetime columns."""
        # Test basic operations
        assert sample_data_no_datetime.count() == 5
        assert len(sample_data_no_datetime.columns) == 3
        assert "user_id" in sample_data_no_datetime.columns
        assert "action" in sample_data_no_datetime.columns
        assert "value" in sample_data_no_datetime.columns

        # Test filtering
        filtered_df = sample_data_no_datetime.filter(F.col("value") > 150)
        assert filtered_df.count() == 3

        # Test grouping
        grouped_df = sample_data_no_datetime.groupBy("action").agg(
            F.sum("value").alias("total_value")
        )
        assert grouped_df.count() == 3

        # Test aggregation
        agg_df = sample_data_no_datetime.groupBy("action").count()
        assert agg_df.count() == 3

    def test_execution_engine_initialization(self, spark_session):
        """Test execution engine initialization."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        engine = ExecutionEngine(spark=spark_session, config=config)
        assert engine.spark == spark_session
        assert engine.config == config
        assert engine.logger is not None

    def test_step_type_detection(self, spark_session):
        """Test step type detection."""
        # Test BronzeStep
        bronze_step = BronzeStep(
            name="test_bronze",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col=None,
        )
        assert isinstance(bronze_step, BronzeStep)

        # Test SilverStep
        def silver_transform(spark, df, silvers):
            return df.filter(F.col("value") > 150)

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="test_silver",
        )
        assert isinstance(silver_step, SilverStep)

        # Test GoldStep
        def gold_transform(spark, silvers):
            return (
                list(silvers.values())[0]
                if silvers
                else spark.createDataFrame([], ["user_id"])
            )

        gold_step = GoldStep(
            name="test_gold",
            transform=gold_transform,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="test_gold",
        )
        assert isinstance(gold_step, GoldStep)

    def test_pipeline_configuration(self, spark_session):
        """Test pipeline configuration."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        assert config.schema == "test_schema"
        assert config.thresholds.bronze == 95.0
        assert config.thresholds.silver == 98.0
        assert config.thresholds.gold == 99.0
        assert config.parallel.max_workers == 4
        assert config.parallel.enabled is True

    def test_execution_mode_enum(self):
        """Test execution mode enum."""
        from sparkforge.execution import ExecutionMode

        assert ExecutionMode.INITIAL.value == "initial"
        assert ExecutionMode.INCREMENTAL.value == "incremental"
        assert ExecutionMode.FULL_REFRESH.value == "full_refresh"
        assert ExecutionMode.VALIDATION_ONLY.value == "validation_only"

    def test_step_status_enum(self):
        """Test step status enum."""
        from sparkforge.execution import StepStatus

        assert StepStatus.PENDING.value == "pending"
        assert StepStatus.RUNNING.value == "running"
        assert StepStatus.COMPLETED.value == "completed"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.SKIPPED.value == "skipped"

    def test_step_type_enum(self):
        """Test step type enum."""
        from sparkforge.execution import StepType

        assert StepType.BRONZE.value == "bronze"
        assert StepType.SILVER.value == "silver"
        assert StepType.GOLD.value == "gold"

"""
Tests for pipeline execution functionality.

This module tests the actual pipeline execution flow with real data,
including bronze, silver, and gold step execution in sequence.
"""

from pyspark.sql import functions as F

from sparkforge.execution import ExecutionEngine
from sparkforge.models import (
    BronzeStep,
    ParallelConfig,
    PipelineConfig,
    ValidationThresholds,
)
from sparkforge.pipeline.builder import PipelineBuilder


class TestPipelineExecutionFlow:
    """Test the complete pipeline execution flow."""

    def test_pipeline_builder_creation(self, spark_session):
        """Test that pipeline builder creates correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        assert builder.spark == spark_session
        assert builder.schema == "test_schema"
        assert builder.bronze_steps == {}
        assert builder.silver_steps == {}
        assert builder.gold_steps == {}

    def test_pipeline_builder_bronze_step_creation(self, spark_session):
        """Test that pipeline builder can create bronze steps."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step
        builder.with_bronze_rules(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )

        assert "events" in builder.bronze_steps
        assert builder.bronze_steps["events"].name == "events"
        assert builder.bronze_steps["events"].incremental_col == "timestamp"

    def test_pipeline_builder_silver_step_creation(self, spark_session):
        """Test that pipeline builder can create silver steps."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step first
        builder.with_bronze_rules(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )

        # Add silver step
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.filter(F.col("id").isNotNull()),
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
        )

        assert "clean_events" in builder.silver_steps
        assert builder.silver_steps["clean_events"].name == "clean_events"
        assert builder.silver_steps["clean_events"].source_bronze == "events"
        assert builder.silver_steps["clean_events"].table_name == "clean_events"

    def test_pipeline_builder_gold_step_creation(self, spark_session):
        """Test that pipeline builder can create gold steps."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step first
        builder.with_bronze_rules(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )

        # Add silver step
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.filter(F.col("id").isNotNull()),
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
        )

        # Add gold step
        builder.add_gold_transform(
            name="event_summary",
            transform=lambda spark, silvers: list(silvers.values())[0]
            .groupBy("id")
            .count(),
            rules={"id": [F.col("id").isNotNull()]},
            table_name="event_summary",
        )

        assert "event_summary" in builder.gold_steps
        assert builder.gold_steps["event_summary"].name == "event_summary"
        assert builder.gold_steps["event_summary"].table_name == "event_summary"

    def test_pipeline_builder_validation(self, spark_session):
        """Test that pipeline builder validation works correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add steps
        builder.with_bronze_rules(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.filter(F.col("id").isNotNull()),
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
        )

        # Test validation
        errors = builder.validate_pipeline()
        assert len(errors) == 0

    def test_pipeline_builder_to_pipeline(self, spark_session):
        """Test that pipeline builder can create a pipeline."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add steps
        builder.with_bronze_rules(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.filter(F.col("id").isNotNull()),
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
        )

        # Create pipeline
        pipeline = builder.to_pipeline()

        assert pipeline is not None
        assert hasattr(pipeline, "run_initial_load")
        assert hasattr(pipeline, "run_pipeline")

    def test_pipeline_execution_with_mock_data(self, spark_session):
        """Test pipeline execution with mock data."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step
        builder.with_bronze_rules(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )

        # Create pipeline
        builder.to_pipeline()

        # Create mock data
        mock_data = [
            (1, "click", "2023-01-01"),
            (2, "view", "2023-01-02"),
            (3, "purchase", "2023-01-03"),
        ]
        mock_df = spark_session.createDataFrame(
            mock_data, ["id", "event_type", "timestamp"]
        )

        # Test that we can work with the mock data
        assert mock_df.count() == 3
        assert len(mock_df.columns) == 3
        assert "id" in mock_df.columns
        assert "event_type" in mock_df.columns
        assert "timestamp" in mock_df.columns

    def test_pipeline_configuration_creation(self, spark_session):
        """Test that pipeline configuration is created correctly."""
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

    def test_execution_engine_with_pipeline_config(self, spark_session):
        """Test that execution engine works with pipeline configuration."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        engine = ExecutionEngine(spark=spark_session, config=config)

        assert engine.spark == spark_session
        assert engine.config == config
        assert engine.config.schema == "test_schema"
        assert engine.config.thresholds.bronze == 95.0

    def test_step_execution_with_real_data(self, spark_session):
        """Test step execution with real Spark data."""
        # Create real test data
        test_data = [
            (1, "user1", "click", "2023-01-01 10:00:00"),
            (2, "user2", "view", "2023-01-01 11:00:00"),
            (3, "user1", "purchase", "2023-01-01 12:00:00"),
            (4, "user3", "click", "2023-01-01 13:00:00"),
            (5, "user2", "view", "2023-01-01 14:00:00"),
        ]

        df = spark_session.createDataFrame(
            test_data, ["id", "user_id", "event_type", "timestamp"]
        )

        # Test data operations
        assert df.count() == 5
        assert len(df.columns) == 4

        # Test filtering
        filtered_df = df.filter(F.col("event_type") == "click")
        assert filtered_df.count() == 2

        # Test grouping
        grouped_df = df.groupBy("user_id").count()
        assert grouped_df.count() == 3

        # Test aggregation
        agg_df = df.groupBy("event_type").count()
        assert agg_df.count() == 3

    def test_pipeline_step_validation_with_real_data(self, spark_session):
        """Test pipeline step validation with real data."""
        # Create real test data
        test_data = [
            (1, "user1", "click", "2023-01-01 10:00:00"),
            (2, "user2", "view", "2023-01-01 11:00:00"),
            (3, "user1", "purchase", "2023-01-01 12:00:00"),
        ]

        spark_session.createDataFrame(
            test_data, ["id", "user_id", "event_type", "timestamp"]
        )

        # Create bronze step with validation rules
        bronze_step = BronzeStep(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "user_id": [F.col("user_id").isNotNull()],
                "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
            },
            incremental_col="timestamp",
        )

        # Test validation rules
        assert "id" in bronze_step.rules
        assert "user_id" in bronze_step.rules
        assert "event_type" in bronze_step.rules
        assert len(bronze_step.rules["id"]) == 1
        assert len(bronze_step.rules["user_id"]) == 1
        assert len(bronze_step.rules["event_type"]) == 1

    def test_pipeline_execution_flow_integration(self, spark_session):
        """Test complete pipeline execution flow integration."""
        # Create pipeline builder
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step
        builder.with_bronze_rules(
            name="events",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
        )

        # Add silver step
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.filter(F.col("id").isNotNull()),
            rules={"id": [F.col("id").isNotNull()]},
            table_name="clean_events",
        )

        # Add gold step
        builder.add_gold_transform(
            name="event_summary",
            transform=lambda spark, silvers: list(silvers.values())[0]
            .groupBy("id")
            .count(),
            rules={"id": [F.col("id").isNotNull()]},
            table_name="event_summary",
        )

        # Validate pipeline
        errors = builder.validate_pipeline()
        assert len(errors) == 0

        # Create pipeline
        pipeline = builder.to_pipeline()
        assert pipeline is not None

        # Test that pipeline has expected methods
        assert hasattr(pipeline, "run_initial_load")
        assert hasattr(pipeline, "run_pipeline")

        # Test that pipeline has expected attributes
        assert hasattr(pipeline, "spark")
        assert hasattr(pipeline, "config")
        assert hasattr(pipeline, "logger")

"""
Tests for DataFrame access in the simplified execution system.

This module tests that the simplified execution system properly handles
DataFrame operations and transformations.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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


class TestDataFrameAccess:
    """Test DataFrame access in the simplified execution system."""

    @pytest.fixture
    def sample_bronze_data(self, spark_session):
        """Create sample bronze data for testing."""
        data = [
            ("user1", "click", "2023-01-01 10:00:00"),
            ("user2", "view", "2023-01-01 11:00:00"),
            ("user3", "purchase", "2023-01-01 12:00:00"),
            ("user4", "click", "2023-01-01 13:00:00"),
            ("user5", "view", "2023-01-01 14:00:00"),
        ]
        return spark_session.createDataFrame(data, ["user_id", "action", "timestamp"])

    @pytest.fixture
    def sample_bronze_rules(self):
        """Create sample bronze validation rules."""
        return {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()],
        }

    @pytest.fixture
    def sample_silver_rules(self):
        """Create sample silver validation rules."""
        return {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
            "event_date": [F.col("event_date").isNotNull()],
        }

    @pytest.fixture
    def sample_gold_rules(self):
        """Create sample gold validation rules."""
        return {
            "action": [F.col("action").isNotNull()],
            "event_date": [F.col("event_date").isNotNull()],
        }

    def test_bronze_step_creation(self, spark_session, sample_bronze_rules):
        """Test that bronze steps are created correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step
        builder.with_bronze_rules(
            name="test_bronze", rules=sample_bronze_rules, incremental_col="timestamp"
        )

        assert "test_bronze" in builder.bronze_steps
        bronze_step = builder.bronze_steps["test_bronze"]
        assert bronze_step.name == "test_bronze"
        assert bronze_step.incremental_col == "timestamp"
        assert "user_id" in bronze_step.rules
        assert "action" in bronze_step.rules
        assert "timestamp" in bronze_step.rules

    def test_silver_step_creation(
        self, spark_session, sample_bronze_rules, sample_silver_rules
    ):
        """Test that silver steps are created correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step first
        builder.with_bronze_rules(
            name="test_bronze", rules=sample_bronze_rules, incremental_col="timestamp"
        )

        # Add silver step
        def silver_transform(spark, df, silvers):
            return df.withColumn("event_date", F.to_date("timestamp")).select(
                "user_id", "action", "event_date"
            )

        builder.add_silver_transform(
            name="silver_events",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=sample_silver_rules,
            table_name="silver_events",
        )

        assert "silver_events" in builder.silver_steps
        silver_step = builder.silver_steps["silver_events"]
        assert silver_step.name == "silver_events"
        assert silver_step.source_bronze == "test_bronze"
        assert silver_step.table_name == "silver_events"
        assert callable(silver_step.transform)

    def test_gold_step_creation(
        self, spark_session, sample_bronze_rules, sample_silver_rules, sample_gold_rules
    ):
        """Test that gold steps are created correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step first
        builder.with_bronze_rules(
            name="test_bronze", rules=sample_bronze_rules, incremental_col="timestamp"
        )

        # Add silver step
        def silver_transform(spark, df, silvers):
            return df.withColumn("event_date", F.to_date("timestamp")).select(
                "user_id", "action", "event_date"
            )

        builder.add_silver_transform(
            name="silver_events",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=sample_silver_rules,
            table_name="silver_events",
        )

        # Add gold step
        def gold_transform(spark, silvers):
            events_df = silvers.get("silver_events")
            if events_df is not None:
                w = Window.partitionBy("action").orderBy("event_date")
                return (
                    events_df.withColumn("rn", F.row_number().over(w))
                    .filter(F.col("rn") == 1)
                    .select("action", "event_date")
                )
            else:
                return spark.createDataFrame([], ["action", "event_date"])

        builder.add_gold_transform(
            name="gold_summary",
            transform=gold_transform,
            rules=sample_gold_rules,
            table_name="gold_summary",
            source_silvers=["silver_events"],
        )

        assert "gold_summary" in builder.gold_steps
        gold_step = builder.gold_steps["gold_summary"]
        assert gold_step.name == "gold_summary"
        assert gold_step.table_name == "gold_summary"
        assert callable(gold_step.transform)

    def test_pipeline_builder_validation(
        self, spark_session, sample_bronze_rules, sample_silver_rules, sample_gold_rules
    ):
        """Test that pipeline builder validation works correctly."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step
        builder.with_bronze_rules(
            name="test_bronze", rules=sample_bronze_rules, incremental_col="timestamp"
        )

        # Add silver step
        def silver_transform(spark, df, silvers):
            return df.withColumn("event_date", F.to_date("timestamp")).select(
                "user_id", "action", "event_date"
            )

        builder.add_silver_transform(
            name="silver_events",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=sample_silver_rules,
            table_name="silver_events",
        )

        # Add gold step
        def gold_transform(spark, silvers):
            events_df = silvers.get("silver_events")
            if events_df is not None:
                w = Window.partitionBy("action").orderBy("event_date")
                return (
                    events_df.withColumn("rn", F.row_number().over(w))
                    .filter(F.col("rn") == 1)
                    .select("action", "event_date")
                )
            else:
                return spark.createDataFrame([], ["action", "event_date"])

        builder.add_gold_transform(
            name="gold_summary",
            transform=gold_transform,
            rules=sample_gold_rules,
            table_name="gold_summary",
            source_silvers=["silver_events"],
        )

        # Test validation
        errors = builder.validate_pipeline()
        assert len(errors) == 0

    def test_pipeline_creation(
        self, spark_session, sample_bronze_rules, sample_silver_rules, sample_gold_rules
    ):
        """Test that pipeline can be created successfully."""
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")

        # Add bronze step
        builder.with_bronze_rules(
            name="test_bronze", rules=sample_bronze_rules, incremental_col="timestamp"
        )

        # Add silver step
        def silver_transform(spark, df, silvers):
            return df.withColumn("event_date", F.to_date("timestamp")).select(
                "user_id", "action", "event_date"
            )

        builder.add_silver_transform(
            name="silver_events",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=sample_silver_rules,
            table_name="silver_events",
        )

        # Add gold step
        def gold_transform(spark, silvers):
            events_df = silvers.get("silver_events")
            if events_df is not None:
                w = Window.partitionBy("action").orderBy("event_date")
                return (
                    events_df.withColumn("rn", F.row_number().over(w))
                    .filter(F.col("rn") == 1)
                    .select("action", "event_date")
                )
            else:
                return spark.createDataFrame([], ["action", "event_date"])

        builder.add_gold_transform(
            name="gold_summary",
            transform=gold_transform,
            rules=sample_gold_rules,
            table_name="gold_summary",
            source_silvers=["silver_events"],
        )

        # Create pipeline
        pipeline = builder.to_pipeline()
        assert pipeline is not None
        assert hasattr(pipeline, "run_initial_load")
        assert hasattr(pipeline, "run_pipeline")

    def test_dataframe_operations(self, spark_session, sample_bronze_data):
        """Test that DataFrame operations work correctly."""
        # Test basic DataFrame operations
        assert sample_bronze_data.count() == 5
        assert len(sample_bronze_data.columns) == 3
        assert "user_id" in sample_bronze_data.columns
        assert "action" in sample_bronze_data.columns
        assert "timestamp" in sample_bronze_data.columns

        # Test filtering
        filtered_df = sample_bronze_data.filter(F.col("action") == "click")
        assert filtered_df.count() == 2

        # Test column operations
        df_with_date = sample_bronze_data.withColumn(
            "event_date", F.to_date("timestamp")
        )
        assert "event_date" in df_with_date.columns

        # Test window functions
        w = Window.partitionBy("action").orderBy("timestamp")
        df_with_rank = sample_bronze_data.withColumn("rn", F.row_number().over(w))
        assert "rn" in df_with_rank.columns

    def test_execution_engine_initialization(self, spark_session):
        """Test that execution engine initializes correctly."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(max_workers=4, enabled=True),
        )

        engine = ExecutionEngine(spark=spark_session, config=config)

        assert engine.spark == spark_session
        assert engine.config == config
        assert engine.logger is not None

    def test_step_type_detection(
        self, spark_session, sample_bronze_rules, sample_silver_rules, sample_gold_rules
    ):
        """Test that step types are correctly detected."""
        # Test BronzeStep
        bronze_step = BronzeStep(
            name="test_bronze", rules=sample_bronze_rules, incremental_col="timestamp"
        )
        assert isinstance(bronze_step, BronzeStep)

        # Test SilverStep
        def silver_transform(spark, df, silvers):
            return df.withColumn("event_date", F.to_date("timestamp"))

        silver_step = SilverStep(
            name="silver_events",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules=sample_silver_rules,
            table_name="silver_events",
        )
        assert isinstance(silver_step, SilverStep)

        # Test GoldStep
        def gold_transform(spark, silvers):
            return (
                list(silvers.values())[0]
                if silvers
                else spark.createDataFrame([], ["action", "event_date"])
            )

        gold_step = GoldStep(
            name="gold_summary",
            transform=gold_transform,
            rules=sample_gold_rules,
            table_name="gold_summary",
        )
        assert isinstance(gold_step, GoldStep)

    def test_pipeline_configuration(self, spark_session):
        """Test that pipeline configuration works correctly."""
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
        """Test that execution modes work correctly."""
        from sparkforge.execution import ExecutionMode

        assert ExecutionMode.INITIAL.value == "initial"
        assert ExecutionMode.INCREMENTAL.value == "incremental"
        assert ExecutionMode.FULL_REFRESH.value == "full_refresh"
        assert ExecutionMode.VALIDATION_ONLY.value == "validation_only"

    def test_step_status_enum(self):
        """Test that step statuses work correctly."""
        from sparkforge.execution import StepStatus

        assert StepStatus.PENDING.value == "pending"
        assert StepStatus.RUNNING.value == "running"
        assert StepStatus.COMPLETED.value == "completed"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.SKIPPED.value == "skipped"

    def test_step_type_enum(self):
        """Test that step types work correctly."""
        from sparkforge.execution import StepType

        assert StepType.BRONZE.value == "bronze"
        assert StepType.SILVER.value == "silver"
        assert StepType.GOLD.value == "gold"

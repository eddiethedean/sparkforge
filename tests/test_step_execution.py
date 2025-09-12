#!/usr/bin/env python3
"""
Tests for step-by-step execution functionality.
"""

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from sparkforge import PipelineBuilder, StepExecutor, StepType, StepStatus


class TestStepExecution:
    """Test step-by-step execution functionality."""
    
    @pytest.fixture
    def sample_data(self, spark_session):
        """Create sample data for testing."""
        data = [
            ("user1", "click", 100),
            ("user2", "view", 200),
            ("user3", "purchase", 300),
        ]
        
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("value", IntegerType(), True)
        ])
        
        return spark_session.createDataFrame(data, schema)
    
    def test_step_executor_creation(self, spark_session, sample_data):
        """Test creating a StepExecutor."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]}
            )
            .to_pipeline()
        )
        
        executor = pipeline.create_step_executor()
        assert isinstance(executor, StepExecutor)
    
    def test_list_steps(self, spark_session, sample_data):
        """Test listing available steps."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]}
            )
            .add_silver_transform(
                name="silver_events",
                source_bronze="events",
                transform=lambda spark, df, prior_silvers: df,
                rules={"action": [F.col("action").isNotNull()]},
                table_name="silver_events",
                watermark_col="timestamp"
            )
            .to_pipeline()
        )
        
        steps = pipeline.list_steps()
        
        assert "bronze" in steps
        assert "silver" in steps
        assert "gold" in steps
        assert "events" in steps["bronze"]
        assert "silver_events" in steps["silver"]
        assert len(steps["gold"]) == 0
    
    def test_get_step_info(self, spark_session, sample_data):
        """Test getting step information."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                incremental_col="timestamp"
            )
            .to_pipeline()
        )
        
        step_info = pipeline.get_step_info("events")
        
        assert step_info is not None
        assert step_info["name"] == "events"
        assert step_info["type"] == "bronze"
        assert step_info["incremental_col"] == "timestamp"
        assert step_info["dependencies"] == []
    
    def test_execute_bronze_step(self, spark_session, sample_data):
        """Test executing a Bronze step."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]}
            )
            .to_pipeline()
        )
        
        result = pipeline.execute_bronze_step(
            step_name="events",
            input_data=sample_data,
            output_to_table=False
        )
        
        assert result.step_name == "events"
        assert result.step_type == StepType.BRONZE
        assert result.status == StepStatus.COMPLETED
        assert result.output_count == 3
        assert result.validation_result.validation_passed
        assert result.duration_seconds > 0
    
    def test_execute_silver_step(self, spark_session, sample_data):
        """Test executing a Silver step."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        def silver_transform(spark, bronze_df):
            return bronze_df.withColumn("processed_at", F.current_timestamp())
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]}
            )
            .add_silver_transform(
                name="silver_events",
                source_bronze="events",
                transform=silver_transform,
                rules={"action": [F.col("action").isNotNull()]},
                table_name="silver_events",
                watermark_col="processed_at"
            )
            .to_pipeline()
        )
        
        # First execute Bronze step
        bronze_result = pipeline.execute_bronze_step(
            step_name="events",
            input_data=sample_data,
            output_to_table=False
        )
        
        # Then execute Silver step
        result = pipeline.execute_silver_step(
            step_name="silver_events",
            output_to_table=False
        )
        
        assert result.step_name == "silver_events"
        assert result.step_type == StepType.SILVER
        assert result.status == StepStatus.COMPLETED
        assert result.output_count == 3
        assert result.validation_result.validation_passed
        assert result.duration_seconds > 0
    
    def test_execute_gold_step(self, spark_session, sample_data):
        """Test executing a Gold step."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        def silver_transform(spark, bronze_df):
            return bronze_df.withColumn("processed_at", F.current_timestamp())
        
        def gold_transform(spark, silvers):
            events_df = silvers["silver_events"]
            return events_df.groupBy("action").count()
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]}
            )
            .add_silver_transform(
                name="silver_events",
                source_bronze="events",
                transform=silver_transform,
                rules={"action": [F.col("action").isNotNull()]},
                table_name="silver_events",
                watermark_col="processed_at"
            )
            .add_gold_transform(
                name="gold_summary",
                transform=gold_transform,
                rules={"action": [F.col("action").isNotNull()]},
                table_name="gold_summary",
                source_silvers=["silver_events"]
            )
            .to_pipeline()
        )
        
        # Execute Bronze and Silver steps first
        pipeline.execute_bronze_step("events", sample_data, output_to_table=False)
        pipeline.execute_silver_step("silver_events", output_to_table=False)
        
        # Then execute Gold step
        result = pipeline.execute_gold_step(
            step_name="gold_summary",
            output_to_table=False
        )
        
        assert result.step_name == "gold_summary"
        assert result.step_type == StepType.GOLD
        assert result.status == StepStatus.COMPLETED
        assert result.output_count == 3  # 3 different actions
        assert result.validation_result.validation_passed
        assert result.duration_seconds > 0
    
    def test_step_execution_with_errors(self, spark_session, sample_data):
        """Test step execution with validation errors."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        # Create data that will fail validation
        bad_data = spark_session.createDataFrame([
            ("user1", "click", 100),
            (None, "view", 200),  # This will fail user_id validation
            ("user3", "purchase", 300),
        ], ["user_id", "action", "value"])
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]}
            )
            .to_pipeline()
        )
        
        result = pipeline.execute_bronze_step(
            step_name="events",
            input_data=bad_data,
            output_to_table=False
        )
        
        assert result.step_name == "events"
        assert result.step_type == StepType.BRONZE
        assert result.status == StepStatus.FAILED
        assert not result.validation_result.validation_passed
        assert result.validation_result.validation_rate < 100.0
    
    def test_get_step_output(self, spark_session, sample_data):
        """Test getting step output after execution."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]}
            )
            .to_pipeline()
        )
        
        # Execute Bronze step
        pipeline.execute_bronze_step("events", sample_data, output_to_table=False)
        
        # Get step output
        executor = pipeline.create_step_executor()
        output = executor.get_step_output("events")
        
        assert output is not None
        assert output.count() == 3
    
    def test_execution_state_tracking(self, spark_session, sample_data):
        """Test execution state tracking."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]}
            )
            .to_pipeline()
        )
        
        executor = pipeline.create_step_executor()
        
        # Initially no completed steps
        assert len(executor.list_completed_steps()) == 0
        assert len(executor.list_failed_steps()) == 0
        
        # Execute Bronze step
        pipeline.execute_bronze_step("events", sample_data, output_to_table=False)
        
        # Check execution state
        completed_steps = executor.list_completed_steps()
        assert "events" in completed_steps
        assert len(completed_steps) == 1
        
        execution_state = executor.get_execution_state()
        assert "events" in execution_state
        assert execution_state["events"].is_successful
    
    def test_clear_execution_state(self, spark_session, sample_data):
        """Test clearing execution state."""
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]}
            )
            .to_pipeline()
        )
        
        executor = pipeline.create_step_executor()
        
        # Execute step
        pipeline.execute_bronze_step("events", sample_data, output_to_table=False)
        assert len(executor.list_completed_steps()) == 1
        
        # Clear state
        executor.clear_execution_state()
        assert len(executor.list_completed_steps()) == 0
        assert len(executor.get_execution_state()) == 0

"""
Additional tests for sparkforge.execution module to achieve 100% coverage.

This module covers the remaining uncovered lines in execution.py.
"""

import os
from unittest.mock import patch

import pytest
from mock_spark import (
    IntegerType,
    MockStructField,
    MockStructType,
    StringType,
)

from sparkforge.errors import ExecutionError
from sparkforge.execution import (
    ExecutionEngine,
    ExecutionMode,
    StepStatus,
    StepType,
)
from sparkforge.models import (
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from mock_spark import functions as F
    MockF = F
else:
    from pyspark.sql import functions as F
    MockF = None


@pytest.fixture(scope="function", autouse=True)
def reset_test_environment():
    """Reset test environment before each test in this file."""
    import gc

    gc.collect()
    yield
    gc.collect()


class TestExecuteStepComplete:
    """Test execute_step method for complete coverage."""

    def test_execute_silver_step_success(self, spark_session):
        """Test successful silver step execution."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create silver step with transform function
        def transform_func(spark, bronze_df, silvers):
            return bronze_df.select("id", "name")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
            schema="test_schema",
        )

        # Execute step
        context = {"test_bronze": test_df}
        result = engine.execute_step(silver_step, context, ExecutionMode.INITIAL)

        assert result.step_name == "test_silver"
        assert result.step_type == StepType.SILVER
        assert result.status == StepStatus.COMPLETED
        assert result.start_time is not None
        assert result.end_time is not None
        assert result.duration is not None
        assert result.rows_processed == 2
        assert result.output_table == "test_schema.test_silver_table"

    def test_execute_gold_step_success(self, spark_session):
        """Test successful gold step execution."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create gold step with transform function
        def transform_func(spark, silvers):
            return silvers["test_silver"]

        gold_step = GoldStep(
            name="test_gold",
            source_silvers=["test_silver"],
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
            schema="test_schema",
        )

        # Execute step
        context = {"test_silver": test_df}
        result = engine.execute_step(gold_step, context, ExecutionMode.INITIAL)

        assert result.step_name == "test_gold"
        assert result.step_type == StepType.GOLD
        assert result.status == StepStatus.COMPLETED
        assert result.start_time is not None
        assert result.end_time is not None
        assert result.duration is not None
        assert result.rows_processed == 2
        assert result.output_table == "test_schema.test_gold_table"

    def test_execute_step_with_rules_validation(self, spark_session):
        """Test step execution with rules validation."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create silver step with rules
        def transform_func(spark, bronze_df, silvers):
            return bronze_df.select("id", "name")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
            schema="test_schema",
        )

        # Execute step with validation
        context = {"test_bronze": test_df}
        with patch("sparkforge.execution.apply_column_rules") as mock_apply_rules:
            mock_apply_rules.return_value = (test_df, {}, {})
            result = engine.execute_step(silver_step, context, ExecutionMode.INITIAL)

            # Verify validation was called
            mock_apply_rules.assert_called_once()
            assert result.status == StepStatus.COMPLETED

    def test_execute_step_validation_only_mode(self, spark_session):
        """Test step execution in validation-only mode."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create silver step
        def transform_func(spark, bronze_df, silvers):
            return bronze_df.select("id", "name")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
            schema="test_schema",
        )

        # Execute step in validation-only mode
        context = {"test_bronze": test_df}
        result = engine.execute_step(
            silver_step, context, ExecutionMode.VALIDATION_ONLY
        )

        assert result.status == StepStatus.COMPLETED
        assert result.output_table is None  # No table writing in validation-only mode
        # In validation-only mode, rows_processed may be None or the actual count
        assert result.rows_processed is None or result.rows_processed == 2

    def test_execute_step_silver_missing_schema(self, spark_session):
        """Test silver step execution with missing schema."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create silver step without schema
        def transform_func(spark, bronze_df, silvers):
            return bronze_df.select("id", "name")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
            # No schema provided
        )

        context = {"test_bronze": test_df}

        with pytest.raises(
            ExecutionError, match="Step 'test_silver' requires a schema"
        ):
            engine.execute_step(silver_step, context, ExecutionMode.INITIAL)

    def test_execute_step_gold_missing_schema(self, spark_session):
        """Test gold step execution with missing schema."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create gold step without schema
        def transform_func(spark, silvers):
            return silvers["test_silver"]

        gold_step = GoldStep(
            name="test_gold",
            source_silvers=["test_silver"],
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
            # No schema provided
        )

        context = {"test_silver": test_df}

        with pytest.raises(ExecutionError, match="Step 'test_gold' requires a schema"):
            engine.execute_step(gold_step, context, ExecutionMode.INITIAL)

    def test_execute_step_silver_missing_source(self, spark_session):
        """Test silver step execution with missing source bronze data."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        def transform_func(spark, bronze_df, silvers):
            return bronze_df.select("id", "name")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="missing_bronze",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
            schema="test_schema",
        )

        context = {}  # Missing source bronze data

        with pytest.raises(
            ExecutionError, match="Source bronze step missing_bronze not found"
        ):
            engine.execute_step(silver_step, context, ExecutionMode.INITIAL)

    def test_execute_step_gold_missing_source(self, spark_session):
        """Test gold step execution with missing source silver data."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        def transform_func(spark, silvers):
            return silvers["missing_silver"]

        gold_step = GoldStep(
            name="test_gold",
            source_silvers=["missing_silver"],
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
            schema="test_schema",
        )

        context = {}  # Missing source silver data

        with pytest.raises(
            ExecutionError, match="Source silver missing_silver not found"
        ):
            engine.execute_step(gold_step, context, ExecutionMode.INITIAL)

    def test_execute_step_transform_error(self, spark_session):
        """Test step execution with transform function error."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create silver step with failing transform
        def failing_transform(spark, bronze_df, silvers):
            raise ValueError("Transform failed")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=failing_transform,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
            schema="test_schema",
        )

        context = {"test_bronze": test_df}

        with pytest.raises(
            ExecutionError, match="Step execution failed: Transform failed"
        ):
            engine.execute_step(silver_step, context, ExecutionMode.INITIAL)


class TestExecutePipelineComplete:
    """Test execute_pipeline method for complete coverage."""

    def test_execute_pipeline_success_with_silver_steps(self, spark_session):
        """Test successful pipeline execution with silver steps."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create silver step
        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.select("id", "name")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
            schema="test_schema",
        )

        # Execute pipeline
        context = {"test_bronze": test_df}
        result = engine.execute_pipeline(
            steps=[silver_step], mode=ExecutionMode.INITIAL, context=context
        )

        assert result.execution_id is not None
        assert result.mode == ExecutionMode.INITIAL
        assert result.status == "completed"
        assert result.start_time is not None
        assert result.end_time is not None
        # Duration may be None or calculated from start/end times
        if result.duration is None:
            assert result.end_time > result.start_time
        else:
            assert result.duration > 0
        assert len(result.steps) == 1

        # Check step result
        silver_result = result.steps[0]
        assert silver_result.step_name == "test_silver"
        assert silver_result.status == StepStatus.COMPLETED

    def test_execute_pipeline_success_with_gold_steps(self, spark_session):
        """Test successful pipeline execution with gold steps."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create gold step
        def gold_transform(spark, silvers):
            return silvers["test_silver"]

        gold_step = GoldStep(
            name="test_gold",
            source_silvers=["test_silver"],
            transform=gold_transform,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
            schema="test_schema",
        )

        # Execute pipeline
        context = {"test_silver": test_df}
        result = engine.execute_pipeline(
            steps=[gold_step], mode=ExecutionMode.INITIAL, context=context
        )

        assert result.execution_id is not None
        assert result.mode == ExecutionMode.INITIAL
        assert result.status == "completed"
        assert result.start_time is not None
        assert result.end_time is not None
        # Duration may be None or calculated from start/end times
        if result.duration is None:
            assert result.end_time > result.start_time
        else:
            assert result.duration > 0
        assert len(result.steps) == 1

        # Check step result
        gold_result = result.steps[0]
        assert gold_result.step_name == "test_gold"
        assert gold_result.status == StepStatus.COMPLETED

    def test_execute_pipeline_with_failed_silver_step(self, spark_session):
        """Test pipeline execution with failed silver step."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create silver step that will fail
        def failing_transform(spark, bronze_df, silvers):
            raise ValueError("Transform failed")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="missing_bronze",  # This will cause failure
            transform=failing_transform,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
            schema="test_schema",
        )

        # Execute pipeline
        result = engine.execute_pipeline(
            steps=[silver_step], mode=ExecutionMode.INITIAL, context={}
        )

        assert result.status == "failed"
        assert len(result.steps) == 1
        assert result.steps[0].status == StepStatus.FAILED
        assert result.steps[0].error is not None

    def test_execute_pipeline_with_failed_gold_step(self, spark_session):
        """Test pipeline execution with failed gold step."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create gold step that will fail
        def failing_transform(spark, silvers):
            raise ValueError("Transform failed")

        gold_step = GoldStep(
            name="test_gold",
            source_silvers=["missing_silver"],  # This will cause failure
            transform=failing_transform,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
            schema="test_schema",
        )

        # Execute pipeline
        result = engine.execute_pipeline(
            steps=[gold_step], mode=ExecutionMode.INITIAL, context={}
        )

        assert result.status == "failed"
        assert len(result.steps) == 1
        assert result.steps[0].status == StepStatus.FAILED
        assert result.steps[0].error is not None

    def test_execute_pipeline_silver_step_without_schema(self, spark_session):
        """Test pipeline execution with silver step without schema."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create silver step without schema
        def silver_transform(spark, bronze_df, silvers):
            return bronze_df.select("id", "name")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=silver_transform,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
            # No schema provided
        )

        # Execute pipeline
        context = {"test_bronze": test_df}
        result = engine.execute_pipeline(
            steps=[silver_step], mode=ExecutionMode.INITIAL, context=context
        )

        assert result.status == "failed"
        assert len(result.steps) == 1
        assert result.steps[0].status == StepStatus.FAILED

    def test_execute_pipeline_gold_step_without_schema(self, spark_session):
        """Test pipeline execution with gold step without schema."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        # Create gold step without schema
        def gold_transform(spark, silvers):
            return silvers["test_silver"]

        gold_step = GoldStep(
            name="test_gold",
            source_silvers=["test_silver"],
            transform=gold_transform,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
            # No schema provided
        )

        # Execute pipeline
        context = {"test_silver": test_df}
        result = engine.execute_pipeline(
            steps=[gold_step], mode=ExecutionMode.INITIAL, context=context
        )

        assert result.status == "failed"
        assert len(result.steps) == 1
        assert result.steps[0].status == StepStatus.FAILED


class TestPrivateMethodsComplete:
    """Test private execution methods for complete coverage."""

    def test_execute_silver_step_success(self, spark_session):
        """Test silver step execution success."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        def transform_func(spark, bronze_df, silvers):
            return bronze_df.select("id", "name")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
        )

        context = {"test_bronze": test_df}
        result = engine._execute_silver_step(silver_step, context)

        assert result.count() == 2

    def test_execute_silver_step_missing_source(self, spark_session):
        """Test silver step execution with missing source."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        def transform_func(spark, bronze_df, silvers):
            return bronze_df.select("id", "name")

        silver_step = SilverStep(
            name="test_silver",
            source_bronze="missing_bronze",
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_silver_table",
        )

        context = {}

        with pytest.raises(
            ExecutionError, match="Source bronze step missing_bronze not found"
        ):
            engine._execute_silver_step(silver_step, context)

    def test_execute_gold_step_success(self, spark_session):
        """Test gold step execution success."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        test_df = spark_session.createDataFrame(test_data, schema)

        def transform_func(spark, silvers):
            return silvers["test_silver"]

        gold_step = GoldStep(
            name="test_gold",
            source_silvers=["test_silver"],
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
        )

        context = {"test_silver": test_df}
        result = engine._execute_gold_step(gold_step, context)

        assert result.count() == 2

    def test_execute_gold_step_missing_source(self, spark_session):
        """Test gold step execution with missing source."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        def transform_func(spark, silvers):
            return silvers["missing_silver"]

        gold_step = GoldStep(
            name="test_gold",
            source_silvers=["missing_silver"],
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
        )

        context = {}

        with pytest.raises(
            ExecutionError, match="Source silver missing_silver not found"
        ):
            engine._execute_gold_step(gold_step, context)

    def test_execute_gold_step_with_none_source_silvers(self, spark_session):
        """Test gold step execution with None source_silvers."""
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=False, max_workers=1),
        )
        engine = ExecutionEngine(spark=spark_session, config=config, functions=MockF)

        # Transform that works with empty silvers dict
        def transform_func(spark, silvers):
            # Create a simple DataFrame when no source silvers
            schema = MockStructType([MockStructField("id", IntegerType())])
            data = [{"id": 1}, {"id": 2}]
            return spark.createDataFrame(data, schema)

        gold_step = GoldStep(
            name="test_gold",
            source_silvers=None,  # None source_silvers
            transform=transform_func,
            rules={"id": ["not_null"]},
            table_name="test_gold_table",
        )

        context = {}
        result = engine._execute_gold_step(gold_step, context)

        # Should call transform with empty silvers dict
        assert result is not None

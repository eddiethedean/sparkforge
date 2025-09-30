#!/usr/bin/env python3
"""
Test for Trap 5: Default Schema Fallbacks fix.

This test verifies that the execution engine properly validates schema
requirements instead of using silent fallback values.
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from sparkforge.execution import ExecutionEngine
from sparkforge.errors import ExecutionError
from sparkforge.execution import ExecutionMode
from sparkforge.models.execution import PipelinePhase
from sparkforge.models.steps import SilverStep, GoldStep
from sparkforge.types import StepName


class TestTrap5DefaultSchemaFallbacks:
    """Test that schema validation prevents silent fallback to 'default'."""

    def test_silver_step_without_schema_raises_error(self, spark_session):
        """Test that SilverStep without schema raises ExecutionError."""
        # Create a SilverStep without schema
        def dummy_transform(spark, bronze_df, prior_silvers):
            return bronze_df
        
        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=dummy_transform,
            rules={"id": []},
            table_name="test_table",
            # schema is not provided
        )
        
        # Create ExecutionEngine
        engine = ExecutionEngine(
            spark=spark_session,
            config=Mock(),
            logger=Mock(),
        )
        
        # Create test data
        schema = StructType([StructField("id", StringType(), True)])
        test_df = spark_session.createDataFrame([("1",)], schema)
        context = {"test_bronze": test_df}
        
        # Should raise ExecutionError, not silently use "default" schema
        with pytest.raises(ExecutionError) as excinfo:
            engine.execute_step(silver_step, context, ExecutionMode.INITIAL)
        
        error_msg = str(excinfo.value)
        assert "requires a schema to be specified" in error_msg
        assert "Silver and Gold steps must have a valid schema" in error_msg

    def test_gold_step_without_schema_raises_error(self, spark_session):
        """Test that GoldStep without schema raises ExecutionError."""
        # Create a GoldStep without schema
        def dummy_gold_transform(spark, silver_dfs):
            return list(silver_dfs.values())[0] if silver_dfs else spark.createDataFrame([], "id STRING")
        
        gold_step = GoldStep(
            name="test_gold",
            transform=dummy_gold_transform,
            rules={"id": []},
            table_name="test_table",
            # schema is not provided
        )
        
        # Create ExecutionEngine
        engine = ExecutionEngine(
            spark=spark_session,
            config=Mock(),
            logger=Mock(),
        )
        
        # Create test data
        schema = StructType([StructField("id", StringType(), True)])
        test_df = spark_session.createDataFrame([("1",)], schema)
        context = {"test_silver": test_df}  # GoldStep needs source_silvers in context
        
        # Should raise ExecutionError, not silently use "default" schema
        with pytest.raises(ExecutionError) as excinfo:
            engine.execute_step(gold_step, context, ExecutionMode.INITIAL)
        
        error_msg = str(excinfo.value)
        assert "requires a schema to be specified" in error_msg
        assert "Silver and Gold steps must have a valid schema" in error_msg

    def test_silver_step_with_schema_works_correctly(self, spark_session):
        """Test that SilverStep with schema works correctly."""
        # Create a SilverStep with schema
        def dummy_transform(spark, bronze_df, prior_silvers):
            return bronze_df
        
        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=dummy_transform,
            rules={"id": []},
            table_name="test_table",
            schema="test_schema",
        )
        
        # Create ExecutionEngine
        engine = ExecutionEngine(
            spark=spark_session,
            config=Mock(),
            logger=Mock(),
        )
        
        # Create test data
        schema = StructType([StructField("id", StringType(), True)])
        test_df = spark_session.createDataFrame([("1",)], schema)
        context = {"test_bronze": test_df}
        
        # Should work without error
        result = engine.execute_step(silver_step, context, ExecutionMode.INITIAL)
        assert result.status.value == "completed"

    def test_gold_step_with_schema_works_correctly(self, spark_session):
        """Test that GoldStep with schema works correctly."""
        # Create a GoldStep with schema
        def dummy_gold_transform(spark, silver_dfs):
            return list(silver_dfs.values())[0] if silver_dfs else spark.createDataFrame([], "id STRING")
        
        gold_step = GoldStep(
            name="test_gold",
            transform=dummy_gold_transform,
            rules={"id": []},
            table_name="test_table",
            schema="test_schema",
        )
        
        # Create ExecutionEngine
        engine = ExecutionEngine(
            spark=spark_session,
            config=Mock(),
            logger=Mock(),
        )
        
        # Create test data
        schema = StructType([StructField("id", StringType(), True)])
        test_df = spark_session.createDataFrame([("1",)], schema)
        context = {"test_silver": test_df}  # GoldStep needs source_silvers in context
        
        # Should work without error
        result = engine.execute_step(gold_step, context, ExecutionMode.INITIAL)
        assert result.status.value == "completed"

    def test_pipeline_execution_logs_missing_schema_warnings(self, spark_session):
        """Test that pipeline execution logs warnings for steps without schema."""
        # Create steps without schema
        def dummy_transform(spark, bronze_df, prior_silvers):
            return bronze_df
        
        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=dummy_transform,
            rules={"id": []},
            table_name="test_table",
            # schema is not provided
        )
        
        # Create ExecutionEngine
        engine = ExecutionEngine(
            spark=spark_session,
            config=Mock(),
            logger=Mock(),
        )
        
        # Mock the logger to capture warnings
        with patch.object(engine.logger, 'error') as mock_logger:
            # Create test data
            schema = StructType([StructField("id", StringType(), True)])
            test_df = spark_session.createDataFrame([("1",)], schema)
            context = {"test_bronze": test_df}
            
            # Execute step (this will fail due to missing schema)
            with pytest.raises(ExecutionError):
                engine.execute_step(silver_step, context, ExecutionMode.INITIAL)
            
            # Verify error was logged
            mock_logger.assert_called()
            log_calls = [call[0][0] for call in mock_logger.call_args_list]
            assert any("requires a schema to be specified" in call for call in log_calls)

    def test_no_silent_fallback_to_default_schema(self, spark_session):
        """Test that no silent fallback to 'default' schema occurs."""
        # Create a SilverStep without schema
        def dummy_transform(spark, bronze_df, prior_silvers):
            return bronze_df
        
        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=dummy_transform,
            rules={"id": []},
            table_name="test_table",
            # schema is not provided
        )
        
        # Create ExecutionEngine
        engine = ExecutionEngine(
            spark=spark_session,
            config=Mock(),
            logger=Mock(),
        )
        
        # Create test data
        schema = StructType([StructField("id", StringType(), True)])
        test_df = spark_session.createDataFrame([("1",)], schema)
        context = {"test_bronze": test_df}
        
        # Mock fqn to detect if "default" schema is used
        with patch('sparkforge.execution.fqn') as mock_fqn:
            mock_fqn.return_value = "test_schema.test_table"
            
            # Should raise ExecutionError before fqn is called
            with pytest.raises(ExecutionError):
                engine.execute_step(silver_step, context, ExecutionMode.INITIAL)
            
            # Verify fqn was not called with "default" schema
            mock_fqn.assert_not_called()

    def test_validation_mode_skips_schema_validation(self, spark_session):
        """Test that validation mode skips schema validation for table operations."""
        # Create a SilverStep without schema
        def dummy_transform(spark, bronze_df, prior_silvers):
            return bronze_df
        
        silver_step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=dummy_transform,
            rules={"id": []},
            table_name="test_table",
            # schema is not provided
        )
        
        # Create ExecutionEngine
        engine = ExecutionEngine(
            spark=spark_session,
            config=Mock(),
            logger=Mock(),
        )
        
        # Create test data
        schema = StructType([StructField("id", StringType(), True)])
        test_df = spark_session.createDataFrame([("1",)], schema)
        context = {"test_bronze": test_df}
        
        # Should work in validation mode (no table operations)
        result = engine.execute_step(silver_step, context, ExecutionMode.VALIDATION_ONLY)
        assert result.status.value == "completed"

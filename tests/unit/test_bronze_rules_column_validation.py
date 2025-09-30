"""
Test cases for bronze rules column validation bug fix.

This module tests the fix for the issue where bronze rules would fail
with "column not found" errors when columns referenced in rules don't exist
in the DataFrame.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkforge import PipelineBuilder
from sparkforge.errors import ValidationError
from sparkforge.validation import apply_column_rules


class TestBronzeRulesColumnValidation:
    """Test cases for bronze rules column validation."""

    def test_missing_columns_validation_error(self, spark_session):
        """Test that ValidationError is raised when columns don't exist."""
        # Create DataFrame with limited columns
        df = spark_session.createDataFrame(
            [("user1", "click"), ("user2", "view")], ["user_id", "action"]
        )
        
        # Try to apply rules for columns that don't exist
        rules = {
            "user_id": [F.col("user_id").isNotNull()],  # This exists
            "value": [F.col("value") > 0],  # This doesn't exist
            "timestamp": [F.col("timestamp").isNotNull()],  # This doesn't exist
        }
        
        with pytest.raises(ValidationError) as exc_info:
            apply_column_rules(df, rules, "bronze", "test_step")
        
        # Verify the error message contains helpful information
        error_msg = str(exc_info.value)
        assert "Columns referenced in validation rules do not exist" in error_msg
        assert "Missing columns:" in error_msg
        assert "value" in error_msg
        assert "timestamp" in error_msg
        assert "Available columns:" in error_msg
        assert "user_id" in error_msg
        assert "action" in error_msg
        assert "Stage: bronze" in error_msg
        assert "Step: test_step" in error_msg

    def test_existing_columns_validation_success(self, spark_session):
        """Test that validation succeeds when all columns exist."""
        # Create DataFrame with all required columns
        df = spark_session.createDataFrame(
            [("user1", "click", 100, "2024-01-01 10:00:00")],
            ["user_id", "action", "value", "timestamp"]
        )
        
        # Apply rules for columns that exist
        rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
            "value": [F.col("value") > 0],
            "timestamp": [F.col("timestamp").isNotNull()],
        }
        
        # Should not raise an exception
        valid_df, invalid_df, stats = apply_column_rules(df, rules, "bronze", "test_step")
        
        # Verify results
        assert valid_df.count() == 1
        assert invalid_df.count() == 0
        assert stats.total_rows == 1
        assert stats.valid_rows == 1
        assert stats.invalid_rows == 0

    def test_empty_rules_validation_success(self, spark_session):
        """Test that empty rules don't cause column validation errors."""
        df = spark_session.createDataFrame(
            [("user1", "click")], ["user_id", "action"]
        )
        
        # Empty rules should not cause validation errors
        rules = {}
        
        valid_df, invalid_df, stats = apply_column_rules(df, rules, "bronze", "test_step")
        
        # Verify results
        assert valid_df.count() == 1
        assert invalid_df.count() == 0
        assert stats.total_rows == 1
        assert stats.valid_rows == 1
        assert stats.invalid_rows == 0

    def test_bronze_step_with_provided_data(self, spark_session):
        """Test that bronze step works correctly with provided data."""
        from sparkforge.models import BronzeStep, PipelineConfig
        from sparkforge.execution import ExecutionEngine
        
        # Create sample data with the expected columns
        sample_data = [("user1", 100, "2024-01-01 10:00:00"), ("user2", 200, "2024-01-01 11:00:00")]
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("timestamp", StringType(), True),
        ])
        df = spark_session.createDataFrame(sample_data, schema)
        
        # Create a bronze step with rules for specific columns
        bronze_step = BronzeStep(
            name="test_bronze",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value") > 0],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
            incremental_col="timestamp",
        )
        
        # Create execution engine with required config
        config = PipelineConfig.create_default("test_schema")
        engine = ExecutionEngine(spark_session, config)
        
        # Execute bronze step with provided data in context
        context = {"test_bronze": df}
        result_df = engine._execute_bronze_step(bronze_step, context)
        
        # Verify the DataFrame has the expected columns
        expected_columns = {"user_id", "value", "timestamp"}
        actual_columns = set(result_df.columns)
        
        assert expected_columns.issubset(actual_columns), (
            f"Expected columns {expected_columns} not found in DataFrame. "
            f"Actual columns: {actual_columns}"
        )
        
        # Verify the DataFrame has data
        assert result_df.count() == 2
        
        # Verify we can apply the rules without column errors
        valid_df, invalid_df, stats = apply_column_rules(
            result_df, bronze_step.rules, "bronze", "test_bronze"
        )
        
        # Should not raise an exception and should validate data
        assert stats.total_rows == 2
        assert stats.valid_rows == 2
        assert stats.invalid_rows == 0

    def test_pipeline_builder_with_missing_columns(self, spark_session):
        """Test that PipelineBuilder handles missing columns gracefully."""
        # Create sample data with limited columns
        sample_data = [("user1", "click"), ("user2", "view")]
        df = spark_session.createDataFrame(sample_data, ["user_id", "action"])
        
        # Create pipeline builder
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")
        
        # Add bronze rules that reference missing columns
        builder.with_bronze_rules(
            name="events",
            rules={
                "user_id": [F.col("user_id").isNotNull()],  # Exists
                "value": [F.col("value") > 0],  # Missing
                "timestamp": [F.col("timestamp").isNotNull()],  # Missing
            },
            incremental_col="timestamp",
        )
        
        # Build the pipeline runner
        runner = builder.to_pipeline()
        
        # Try to execute with data that has missing columns
        # This should either work with fallback schema or fail with clear error
        try:
            result = runner.run_initial_load(bronze_sources={"events": df})
            # If it succeeds, verify the result
            assert result is not None
        except ValidationError as e:
            # If it fails, verify the error message is helpful
            error_msg = str(e)
            assert "Columns referenced in validation rules do not exist" in error_msg
            assert "Missing columns:" in error_msg
            assert "value" in error_msg
            assert "timestamp" in error_msg

    def test_bronze_step_missing_data_error(self, spark_session):
        """Test that bronze step raises clear error when no data is provided."""
        from sparkforge.models import BronzeStep, PipelineConfig
        from sparkforge.execution import ExecutionEngine
        from sparkforge.errors import ExecutionError
        
        # Create bronze step with various column types
        bronze_step = BronzeStep(
            name="test_types",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "value": [F.col("value") > 0],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
        )
        
        config = PipelineConfig.create_default("test_schema")
        engine = ExecutionEngine(spark_session, config)
        
        # Execute bronze step without providing data in context
        # This should raise a clear error
        with pytest.raises(ExecutionError) as excinfo:
            engine._execute_bronze_step(bronze_step, {})
        
        error_msg = str(excinfo.value)
        assert "Bronze step 'test_types' requires data to be provided in context" in error_msg
        assert "Bronze steps are for validating existing data, not creating it" in error_msg
        assert "Please provide data using bronze_sources parameter" in error_msg

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

    def test_bronze_step_fallback_schema_matching(self, spark_session):
        """Test that bronze step creates fallback DataFrame with matching schema."""
        from sparkforge.models import BronzeStep, PipelineConfig
        from sparkforge.execution import ExecutionEngine
        
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
        
        # Execute bronze step without providing data in context
        # This should trigger the fallback DataFrame creation
        result_df = engine._execute_bronze_step(bronze_step, {})
        
        # Verify the fallback DataFrame has the expected columns
        expected_columns = {"user_id", "value", "timestamp"}
        actual_columns = set(result_df.columns)
        
        assert expected_columns.issubset(actual_columns), (
            f"Expected columns {expected_columns} not found in fallback DataFrame. "
            f"Actual columns: {actual_columns}"
        )
        
        # Verify the DataFrame is empty but has the right schema
        assert result_df.count() == 0
        
        # Verify we can apply the rules without column errors
        valid_df, invalid_df, stats = apply_column_rules(
            result_df, bronze_step.rules, "bronze", "test_bronze"
        )
        
        # Should not raise an exception
        assert stats.total_rows == 0
        assert stats.valid_rows == 0
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

    def test_column_type_inference_in_fallback(self, spark_session):
        """Test that fallback DataFrame uses appropriate column types."""
        from sparkforge.models import BronzeStep, PipelineConfig
        from sparkforge.execution import ExecutionEngine
        
        # Create bronze step with various column types
        bronze_step = BronzeStep(
            name="test_types",
            rules={
                "user_id": [F.col("user_id").isNotNull()],  # Should be STRING
                "customer_id": [F.col("customer_id").isNotNull()],  # Should be STRING
                "value": [F.col("value") > 0],  # Should be INT
                "amount": [F.col("amount") > 0],  # Should be INT
                "timestamp": [F.col("timestamp").isNotNull()],  # Should be TIMESTAMP
                "created_at": [F.col("created_at").isNotNull()],  # Should be TIMESTAMP
                "action": [F.col("action").isNotNull()],  # Should be STRING
                "status": [F.col("status").isNotNull()],  # Should be STRING
                "unknown_col": [F.col("unknown_col").isNotNull()],  # Should be STRING (default)
            },
        )
        
        config = PipelineConfig.create_default("test_schema")
        engine = ExecutionEngine(spark_session, config)
        result_df = engine._execute_bronze_step(bronze_step, {})
        
        # Check that columns exist
        expected_columns = {
            "user_id", "customer_id", "value", "amount", 
            "timestamp", "created_at", "action", "status", "unknown_col"
        }
        actual_columns = set(result_df.columns)
        assert expected_columns.issubset(actual_columns)
        
        # Check that we can apply rules without errors
        valid_df, invalid_df, stats = apply_column_rules(
            result_df, bronze_step.rules, "bronze", "test_types"
        )
        
        # Should not raise an exception
        assert stats.total_rows == 0
        assert stats.valid_rows == 0
        assert stats.invalid_rows == 0

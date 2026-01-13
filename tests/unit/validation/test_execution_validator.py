"""Tests for ExecutionValidator service."""

from unittest.mock import Mock

from pipeline_builder.validation.execution_validator import ExecutionValidator


class TestExecutionValidator:
    """Tests for ExecutionValidator."""

    def test_validate_step_output_with_rules(self, spark_session):
        """Test validating step output with rules."""
        validator = ExecutionValidator()

        # Create test data
        data = [("user1", "click"), ("user2", None)]
        df = spark_session.createDataFrame(data, ["user_id", "action"])

        # Create rules
        rules = {
            "user_id": [Mock(return_value=True)],
            "action": [Mock(return_value=True)],
        }

        # Validate
        valid_df, invalid_df, stats = validator.validate_step_output(
            df, "test_step", rules
        )

        # Assert
        assert valid_df is not None
        assert invalid_df is not None

    def test_validate_step_output_without_rules(self, spark_session):
        """Test validating step output without rules returns original DataFrame."""
        validator = ExecutionValidator()

        # Create test data
        data = [("user1", "click")]
        df = spark_session.createDataFrame(data, ["user_id", "action"])

        # Validate without rules
        valid_df, invalid_df, stats = validator.validate_step_output(
            df, "test_step", {}
        )

        # Assert - should return original DataFrame
        assert valid_df is not None

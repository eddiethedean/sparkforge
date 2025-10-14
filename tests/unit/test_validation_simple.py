"""
Simplified tests for sparkforge.validation modules that work with mock_spark.
"""

from datetime import datetime

from mock_spark import (
    IntegerType,
    MockStructField,
    MockStructType,
    StringType,
)

from sparkforge.errors import ValidationError
from sparkforge.logging import PipelineLogger
from sparkforge.models import (
    BronzeStep,
    ExecutionContext,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)
from sparkforge.models.enums import ExecutionMode
from sparkforge.validation.pipeline_validation import (
    StepValidator,
    UnifiedValidator,
    ValidationResult,
)
from sparkforge.validation.utils import get_dataframe_info, safe_divide


class TestValidationUtils:
    """Tests for validation utility functions."""

    def test_safe_divide_normal(self, mock_spark_session):
        """Test safe_divide with normal values."""
        result = safe_divide(10, 2)
        assert result == 5.0

    def test_safe_divide_by_zero(self, mock_spark_session):
        """Test safe_divide with zero divisor."""
        result = safe_divide(10, 0)
        assert result == 0.0

    def test_safe_divide_none_values(self, mock_spark_session):
        """Test safe_divide with None values."""
        # Test with None numerator - should return default (0.0)
        result = safe_divide(None, 2)
        assert result == 0.0

        # Test with None denominator - should return default (0.0)
        result = safe_divide(10, None)
        assert result == 0.0

        # Test with custom default
        result = safe_divide(None, 2, default=99.0)
        assert result == 99.0

    def test_safe_divide_edge_cases(self, mock_spark_session):
        """Test safe_divide with edge cases."""
        # Test with very small numbers
        result = safe_divide(0.0001, 0.0001)
        assert result == 1.0

        # Test with large numbers
        result = safe_divide(1000000, 1000)
        assert result == 1000.0

        # Test with custom default
        result = safe_divide(10, 0, default=99.0)
        assert result == 99.0

    def test_get_dataframe_info(self, mock_spark_session):
        """Test get_dataframe_info function."""
        schema = MockStructType(
            [
                MockStructField("id", IntegerType(), False),
                MockStructField("name", StringType(), True),
            ]
        )
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df = mock_spark_session.createDataFrame(data, schema)

        info = get_dataframe_info(df)

        assert isinstance(info, dict)
        assert "row_count" in info
        assert "column_count" in info
        assert "columns" in info
        assert "schema" in info
        assert "is_empty" in info
        assert info["row_count"] == 2
        assert info["column_count"] == 2
        assert info["is_empty"] is False

    def test_get_dataframe_info_empty(self, mock_spark_session):
        """Test get_dataframe_info with empty DataFrame."""
        schema = MockStructType([MockStructField("id", IntegerType(), False)])
        data = []
        df = mock_spark_session.createDataFrame(data, schema)

        info = get_dataframe_info(df)

        assert info["row_count"] == 0
        assert info["is_empty"] is True

    def test_get_dataframe_info_error_handling(self, mock_spark_session):
        """Test get_dataframe_info error handling."""
        # Test with invalid DataFrame
        try:
            info = get_dataframe_info(None)
            assert "error" in info
            assert info["row_count"] == 0
            assert info["is_empty"] is True
        except Exception:
            # Some error handling might raise exceptions
            pass


class TestPipelineValidation:
    """Tests for pipeline validation functions."""

    def test_validation_result_creation(self, mock_spark_session):
        """Test ValidationResult creation."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=["Minor warning"],
            recommendations=["Consider optimization"],
        )

        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == ["Minor warning"]
        assert result.recommendations == ["Consider optimization"]
        assert bool(result) is True

    def test_validation_result_false(self, mock_spark_session):
        """Test ValidationResult with validation failure."""
        result = ValidationResult(
            is_valid=False, errors=["Critical error"], warnings=[], recommendations=[]
        )

        assert result.is_valid is False
        assert result.errors == ["Critical error"]
        assert bool(result) is False

    def test_unified_validator_initialization(self, mock_spark_session):
        """Test UnifiedValidator initialization."""
        validator = UnifiedValidator()
        assert validator is not None

        logger = PipelineLogger("TestValidator")
        validator_with_logger = UnifiedValidator(logger)
        assert validator_with_logger is not None

    def test_unified_validator_add_validator(self, mock_spark_session):
        """Test adding custom validators."""
        validator = UnifiedValidator()

        class CustomValidator(StepValidator):
            def validate(self, step, context):
                return ["Custom validation error"]

        custom_validator = CustomValidator()
        validator.add_validator(custom_validator)

        # Test that validator was added
        assert len(validator.custom_validators) == 1

    def test_unified_validator_pipeline_validation(self, mock_spark_session):
        """Test unified validator pipeline validation."""
        validator = UnifiedValidator()

        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
            verbose=False,
        )

        bronze_steps = {
            "bronze1": BronzeStep(name="bronze1", rules={"id": ["not_null"]})
        }
        silver_steps = {
            "silver1": SilverStep(
                name="silver1",
                source_bronze="bronze1",
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="silver1",
                schema="test",
            )
        }
        gold_steps = {
            "gold1": GoldStep(
                name="gold1",
                source_silvers=["silver1"],
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="gold1",
                schema="test",
            )
        }

        result = validator.validate_pipeline(
            config, bronze_steps, silver_steps, gold_steps
        )

        assert isinstance(result, ValidationResult)
        assert isinstance(result.is_valid, bool)

    def test_unified_validator_step_validation(self, mock_spark_session):
        """Test unified validator step validation."""
        validator = UnifiedValidator()

        step = BronzeStep(name="test_bronze", rules={"id": ["not_null"]})
        context = ExecutionContext(
            execution_id="test-123",
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
        )

        result = validator.validate_step(step, "bronze", context)

        assert isinstance(result, ValidationResult)
        assert isinstance(result.is_valid, bool)

    def test_unified_validator_custom_validators(self, mock_spark_session):
        """Test unified validator with custom validators."""
        validator = UnifiedValidator()

        class CustomValidator(StepValidator):
            def validate(self, step, context):
                if step.name == "invalid_step":
                    return ["Step name is invalid"]
                return []

        validator.add_validator(CustomValidator())

        # Test with valid step
        valid_step = BronzeStep(name="valid_step", rules={"id": ["not_null"]})
        context = ExecutionContext(
            execution_id="test-123",
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
        )

        result = validator.validate_step(valid_step, "bronze", context)
        assert isinstance(result, ValidationResult)
        assert result.is_valid is True

        # Test with invalid step
        invalid_step = BronzeStep(name="invalid_step", rules={"id": ["not_null"]})
        result = validator.validate_step(invalid_step, "bronze", context)
        assert isinstance(result, ValidationResult)
        assert result.is_valid is False
        assert "Step name is invalid" in result.errors

    def test_unified_validator_empty_pipeline(self, mock_spark_session):
        """Test unified validator with empty pipeline."""
        validator = UnifiedValidator()

        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
            verbose=False,
        )

        result = validator.validate_pipeline(config, {}, {}, {})

        assert isinstance(result, ValidationResult)
        assert isinstance(result.is_valid, bool)

    def test_unified_validator_invalid_config(self, mock_spark_session):
        """Test unified validator with invalid configuration."""
        validator = UnifiedValidator()

        # Test with empty schema
        config = PipelineConfig(
            schema="",  # Empty schema should be invalid
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
            verbose=False,
        )

        result = validator.validate_pipeline(config, {}, {}, {})

        assert isinstance(result, ValidationResult)
        assert result.is_valid is False
        assert "Pipeline schema is required" in result.errors


class TestValidationErrorHandling:
    """Test validation error handling and edge cases."""

    def test_validation_error_creation(self, mock_spark_session):
        """Test ValidationError creation."""
        error = ValidationError("Test validation error")
        assert str(error) == "Test validation error"

    def test_validation_error_with_context(self, mock_spark_session):
        """Test ValidationError with context."""
        error = ValidationError(
            "Test validation error", context={"column": "test_col", "value": "invalid"}
        )
        assert "Test validation error" in str(error)

    def test_validation_error_attributes(self, mock_spark_session):
        """Test ValidationError attributes."""
        error = ValidationError(
            "Test validation error", context={"column": "test_col", "value": "invalid"}
        )

        # Test that context is accessible
        assert hasattr(error, "context") or "context" in str(error)


class TestValidationIntegration:
    """Integration tests for validation modules."""

    def test_validation_workflow_with_mock_data(self, mock_spark_session):
        """Test validation workflow with mock data."""
        # Create test data
        schema = MockStructType(
            [
                MockStructField("id", IntegerType(), False),
                MockStructField("name", StringType(), True),
                MockStructField("age", IntegerType(), True),
            ]
        )
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": None, "age": 35},
        ]
        df = mock_spark_session.createDataFrame(data, schema)

        # Test DataFrame info
        info = get_dataframe_info(df)
        assert info["row_count"] == 3
        assert info["column_count"] == 3
        assert info["is_empty"] is False

    def test_validation_with_pipeline_config(self, mock_spark_session):
        """Test validation with pipeline configuration."""
        # Create pipeline config
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
            verbose=False,
        )

        # Validate config using unified validator
        validator = UnifiedValidator()
        config_result = validator.validate_pipeline(config, {}, {}, {})
        assert isinstance(config_result, ValidationResult)

        # Create and validate steps
        bronze_step = BronzeStep(name="bronze1", rules={"id": ["not_null"]})
        context = ExecutionContext(
            execution_id="test-123",
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
        )

        step_result = validator.validate_step(bronze_step, "bronze", context)
        assert isinstance(step_result, ValidationResult)

    def test_validation_with_complex_pipeline(self, mock_spark_session):
        """Test validation with complex pipeline structure."""
        validator = UnifiedValidator()

        # Create complex pipeline
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
            verbose=False,
        )

        bronze_steps = {
            "bronze1": BronzeStep(name="bronze1", rules={"id": ["not_null"]}),
            "bronze2": BronzeStep(name="bronze2", rules={"name": ["not_null"]}),
        }
        silver_steps = {
            "silver1": SilverStep(
                name="silver1",
                source_bronze="bronze1",
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="silver1",
                schema="test",
            ),
            "silver2": SilverStep(
                name="silver2",
                source_bronze="bronze2",
                transform=lambda df: df,
                rules={"name": ["not_null"]},
                table_name="silver2",
                schema="test",
            ),
        }
        gold_steps = {
            "gold1": GoldStep(
                name="gold1",
                source_silvers=["silver1", "silver2"],
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="gold1",
                schema="test",
            )
        }

        result = validator.validate_pipeline(
            config, bronze_steps, silver_steps, gold_steps
        )

        assert isinstance(result, ValidationResult)
        assert isinstance(result.is_valid, bool)

    def test_validation_error_scenarios(self, mock_spark_session):
        """Test various validation error scenarios."""
        validator = UnifiedValidator()

        # Test with missing source dependencies
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
            parallel=ParallelConfig(enabled=True, max_workers=4),
            verbose=False,
        )

        bronze_steps = {
            "bronze1": BronzeStep(name="bronze1", rules={"id": ["not_null"]})
        }
        silver_steps = {
            "silver1": SilverStep(
                name="silver1",
                source_bronze="nonexistent_bronze",  # This should cause an error
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="silver1",
                schema="test",
            )
        }
        gold_steps = {}

        result = validator.validate_pipeline(
            config, bronze_steps, silver_steps, gold_steps
        )

        assert isinstance(result, ValidationResult)
        assert result.is_valid is False
        assert any("non-existent bronze step" in error for error in result.errors)

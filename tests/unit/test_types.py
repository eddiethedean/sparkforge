#!/usr/bin/env python3
"""
Unit tests for sparkforge/types.py module.

This module tests all type definitions, enums, protocols, and type aliases
defined in the types module.
"""

from typing import Any, Dict, List, Union
from unittest.mock import Mock

from sparkforge.pipeline.models import PipelineMode
from sparkforge.writer.models import WriteMode
from sparkforge.types import (
    Duration,
    ErrorCode,
    ErrorContext,
    ErrorSuggestions,
    ExecutionId,
    GenericDict,
    NumericDict,
    OptionalDict,
    OptionalList,
    PipelineConfigDict,
    PipelineId,
    QualityRate,
    RowCount,
    SchemaName,
    StepContext,
    StepName,
    StepResult,
    StepStatus,
    StepType,
    StringDict,
    TableName,
)


class TestTypeAliases:
    """Test basic type aliases."""

    def test_string_type_aliases(self) -> None:
        """Test string type aliases."""
        # Test that aliases are properly defined
        assert StepName is str
        assert PipelineId is str
        assert ExecutionId is str
        assert TableName is str
        assert SchemaName is str
        assert ErrorCode is str

    def test_numeric_type_aliases(self) -> None:
        """Test numeric type aliases."""
        assert QualityRate is float
        assert Duration is float
        assert RowCount is int

    def test_dictionary_type_aliases(self) -> None:
        """Test dictionary type aliases."""
        assert StringDict == Dict[str, str]
        assert NumericDict == Dict[str, Union[int, float]]
        assert GenericDict == Dict[str, Any]

    def test_optional_type_aliases(self) -> None:
        """Test optional type aliases."""
        assert OptionalDict == Union[Dict[str, Any], type(None)]
        assert OptionalList == Union[List[Any], type(None)]


class TestEnums:
    """Test enum definitions."""

    def test_step_type_enum(self) -> None:
        """Test StepType enum."""
        assert StepType.BRONZE.value == "bronze"
        assert StepType.SILVER.value == "silver"
        assert StepType.GOLD.value == "gold"

        # Test enum iteration
        step_types = list(StepType)
        assert len(step_types) == 3
        assert StepType.BRONZE in step_types
        assert StepType.SILVER in step_types
        assert StepType.GOLD in step_types

    def test_step_status_enum(self) -> None:
        """Test StepStatus enum."""
        assert StepStatus.PENDING.value == "pending"
        assert StepStatus.RUNNING.value == "running"
        assert StepStatus.COMPLETED.value == "completed"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.SKIPPED.value == "skipped"

        # Test enum iteration
        statuses = list(StepStatus)
        assert len(statuses) == 5
        assert StepStatus.PENDING in statuses
        assert StepStatus.RUNNING in statuses
        assert StepStatus.COMPLETED in statuses
        assert StepStatus.FAILED in statuses
        assert StepStatus.SKIPPED in statuses

    def test_pipeline_mode_enum(self) -> None:
        """Test PipelineMode enum."""
        assert PipelineMode.INITIAL.value == "initial"
        assert PipelineMode.INCREMENTAL.value == "incremental"
        assert PipelineMode.FULL_REFRESH.value == "full_refresh"

        # Test enum iteration
        modes = list(PipelineMode)
        assert len(modes) == 4  # Now includes VALIDATION_ONLY
        assert PipelineMode.INITIAL in modes
        assert PipelineMode.INCREMENTAL in modes
        assert PipelineMode.FULL_REFRESH in modes


class TestFunctionTypes:
    """Test function type definitions."""

    def test_transform_function_types(self) -> None:
        """Test transform function types."""

        # Test TransformFunction
        def test_transform(spark, df):
            return df

        # This should not raise an error
        assert callable(test_transform)

        # Test BronzeTransformFunction
        def test_bronze_transform(spark, df):
            return df

        assert callable(test_bronze_transform)

        # Test SilverTransformFunction
        def test_silver_transform(spark, df, bronze_dfs):
            return df

        assert callable(test_silver_transform)

        # Test GoldTransformFunction
        def test_gold_transform(spark, silver_dfs):
            return list(silver_dfs.values())[0] if silver_dfs else None

        assert callable(test_gold_transform)

    def test_filter_function_type(self) -> None:
        """Test filter function type."""

        def test_filter(df):
            return df

        assert callable(test_filter)


class TestDataTypeAliases:
    """Test data type aliases."""

    def test_column_rules_type(self) -> None:
        """Test ColumnRules type alias."""
        # Create mock column
        mock_column = Mock()

        # Test with string rules
        rules_str = {"col1": ["not_null", "positive"]}

        # Test with column rules
        rules_col = {"col1": [mock_column]}

        # Test with mixed rules
        rules_mixed = {"col1": ["not_null", mock_column]}

        # These should not raise errors
        assert isinstance(rules_str, dict)
        assert isinstance(rules_col, dict)
        assert isinstance(rules_mixed, dict)

    def test_result_type_aliases(self) -> None:
        """Test result type aliases."""
        # Test StepResult
        step_result = {"status": "completed", "rows": 100}
        assert isinstance(step_result, dict)

        # Test PipelineResult
        pipeline_result = {"steps": ["bronze", "silver"], "total_rows": 1000}
        assert isinstance(pipeline_result, dict)

        # Test ExecutionResult
        execution_result = {"execution_id": "exec_123", "duration": 30.5}
        assert isinstance(execution_result, dict)

        # Test ValidationResult
        validation_result = {"valid_rows": 950, "invalid_rows": 50}
        assert isinstance(validation_result, dict)

    def test_context_type_aliases(self) -> None:
        """Test context type aliases."""
        # Test StepContext
        step_context = {"step_name": "bronze_step", "config": {}}
        assert isinstance(step_context, dict)

        # Test ExecutionContext
        execution_context = {"pipeline_id": "pipeline_123", "start_time": "2023-01-01"}
        assert isinstance(execution_context, dict)

    def test_config_type_aliases(self) -> None:
        """Test configuration type aliases."""
        # Test PipelineConfigDict
        pipeline_config = {"name": "test_pipeline", "steps": []}
        assert isinstance(pipeline_config, dict)

        # Test ExecutionConfig
        execution_config = {"parallel": True, "timeout": 300}
        assert isinstance(execution_config, dict)

        # Test ValidationConfig
        validation_config = {"thresholds": {"bronze": 80.0}}
        assert isinstance(validation_config, dict)

        # Test MonitoringConfig
        monitoring_config = {"metrics": True, "logging": "debug"}
        assert isinstance(monitoring_config, dict)

    def test_quality_thresholds_type(self) -> None:
        """Test QualityThresholds type alias."""
        thresholds = {"bronze": 80.0, "silver": 85.0, "gold": 90.0}
        assert isinstance(thresholds, dict)
        assert all(isinstance(v, float) for v in thresholds.values())

    def test_error_type_aliases(self) -> None:
        """Test error type aliases."""
        # Test ErrorContext
        error_context = {"error_code": "VALIDATION_FAILED", "details": {}}
        assert isinstance(error_context, dict)

        # Test ErrorSuggestions
        error_suggestions = ["Check data quality", "Verify schema"]
        assert isinstance(error_suggestions, list)
        assert all(isinstance(s, str) for s in error_suggestions)


class TestProtocols:
    """Test protocol definitions."""

    def test_validatable_protocol(self) -> None:
        """Test Validatable protocol."""

        class MockValidatable:
            def validate(self) -> None:
                pass

        obj = MockValidatable()

        # Test that the object implements the protocol
        assert hasattr(obj, "validate")
        assert callable(obj.validate)

        # Test protocol validation
        obj.validate()  # Should not raise an error

    def test_serializable_protocol(self) -> None:
        """Test Serializable protocol."""

        class MockSerializable:
            def to_dict(self) -> Dict[str, Any]:
                return {"test": "value"}

        obj = MockSerializable()

        # Test that the object implements the protocol
        assert hasattr(obj, "to_dict")
        assert callable(obj.to_dict)

        # Test protocol usage
        result = obj.to_dict()
        assert isinstance(result, dict)
        assert result["test"] == "value"


class TestBackwardCompatibility:
    """Test backward compatibility aliases."""

    def test_backward_compatibility_aliases(self) -> None:
        """Test backward compatibility aliases."""
        # All backward compatibility aliases have been removed
        # PipelinePhase is now directly StepType
        assert StepType is not None

        # WriteMode is now only available from writer.models
        # WriteMode is a separate enum, not an alias
        assert WriteMode is not None


class TestTypeUsage:
    """Test type usage in practical scenarios."""

    def test_pipeline_configuration_usage(self) -> None:
        """Test type usage in pipeline configuration."""
        # Create a realistic pipeline configuration
        config: PipelineConfigDict = {
            "name": "test_pipeline",
            "steps": [
                {
                    "name": "bronze_step",
                    "type": StepType.BRONZE.value,
                    "status": StepStatus.PENDING.value,
                }
            ],
            "execution": {"mode": PipelineMode.INITIAL.value, "parallel": True},
        }

        assert isinstance(config, dict)
        assert config["name"] == "test_pipeline"

    def test_step_result_usage(self) -> None:
        """Test type usage in step results."""
        # Create a realistic step result
        result: StepResult = {
            "step_name": "bronze_step",
            "status": StepStatus.COMPLETED.value,
            "rows_processed": 1000,
            "quality_rate": 95.5,
            "duration": 30.2,
            "errors": [],
        }

        assert isinstance(result, dict)
        assert result["status"] == StepStatus.COMPLETED.value
        assert isinstance(result["rows_processed"], int)
        assert isinstance(result["quality_rate"], float)

    def test_validation_context_usage(self) -> None:
        """Test type usage in validation context."""
        # Create a realistic validation context
        context: StepContext = {
            "pipeline_id": "pipeline_123",
            "step_name": "silver_step",
            "execution_id": "exec_456",
            "config": {"validation_threshold": 85.0, "error_handling": "strict"},
        }

        assert isinstance(context, dict)
        assert isinstance(context["config"], dict)
        assert context["config"]["validation_threshold"] == 85.0

    def test_error_handling_usage(self) -> None:
        """Test type usage in error handling."""
        # Create a realistic error context
        error_context: ErrorContext = {
            "error_code": "VALIDATION_FAILED",
            "step_name": "bronze_step",
            "details": {"failed_columns": ["user_id", "email"], "quality_rate": 75.0},
        }

        suggestions: ErrorSuggestions = [
            "Check data quality for user_id column",
            "Verify email format validation",
            "Consider adjusting validation thresholds",
        ]

        assert isinstance(error_context, dict)
        assert isinstance(suggestions, list)
        assert all(isinstance(s, str) for s in suggestions)

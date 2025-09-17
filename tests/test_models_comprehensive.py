#!/usr/bin/env python3
"""
Comprehensive tests for models.py.

This module tests all the data models, validation methods, and utility functions
in the models.py file to achieve high test coverage.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from pyspark.sql import DataFrame, functions as F

from sparkforge.errors import ValidationError
from sparkforge.models import (
    # Exceptions
    PipelineConfigurationError,
    PipelineExecutionError,
    PipelineValidationError,
    
    # Enums
    PipelinePhase,
    ExecutionMode,
    WriteMode,
    ValidationResult,
    
    # Type definitions
    ModelValue,
    ColumnRule,
    ResourceValue,
    ColumnRules,
    TransformFunction,
    
    # Base classes
    BaseModel,
    
    # Step classes
    BronzeStep,
    SilverStep,
    GoldStep,
    
    # Result classes
    StepResult,
    PipelineMetrics,
    PipelineConfig,
    
    # Dependency classes
    SilverDependencyInfo,
    
    # Utility classes
    ParallelConfig,
    ValidationThresholds,
)

# Import transform function types from types.py
from sparkforge.types import (
    BronzeTransformFunction,
    SilverTransformFunction,
    GoldTransformFunction,
)


class TestExceptions:
    """Test custom exceptions."""

    def test_pipeline_configuration_error(self):
        """Test PipelineConfigurationError."""
        error = PipelineConfigurationError("Invalid configuration")
        assert str(error) == "Invalid configuration"
        assert isinstance(error, ValueError)

    def test_pipeline_execution_error(self):
        """Test PipelineExecutionError."""
        error = PipelineExecutionError("Execution failed")
        assert str(error) == "Execution failed"
        assert isinstance(error, RuntimeError)


class TestEnums:
    """Test enumeration classes."""

    def test_pipeline_phase_enum(self):
        """Test PipelinePhase enum."""
        assert PipelinePhase.BRONZE.value == "bronze"
        assert PipelinePhase.SILVER.value == "silver"
        assert PipelinePhase.GOLD.value == "gold"
        
        # Test enum iteration
        phases = list(PipelinePhase)
        assert len(phases) == 3
        assert PipelinePhase.BRONZE in phases

    def test_execution_mode_enum(self):
        """Test ExecutionMode enum."""
        assert ExecutionMode.INITIAL.value == "initial"
        assert ExecutionMode.INCREMENTAL.value == "incremental"
        
        modes = list(ExecutionMode)
        assert len(modes) == 2

    def test_write_mode_enum(self):
        """Test WriteMode enum."""
        assert WriteMode.OVERWRITE.value == "overwrite"
        assert WriteMode.APPEND.value == "append"
        
        modes = list(WriteMode)
        assert len(modes) == 2

    def test_validation_result_enum(self):
        """Test ValidationResult enum."""
        assert ValidationResult.PASSED.value == "passed"
        assert ValidationResult.FAILED.value == "failed"
        assert ValidationResult.WARNING.value == "warning"
        
        results = list(ValidationResult)
        assert len(results) == 3


class TestTypeDefinitions:
    """Test type definitions and aliases."""

    def test_model_value_types(self):
        """Test ModelValue type accepts correct values."""
        # These should all be valid ModelValue types
        valid_values = [
            "string",
            42,
            3.14,
            True,
            ["list", "of", "strings"],
            {"key": "value"},
            None
        ]
        
        for value in valid_values:
            # Just test that we can assign these to ModelValue variables
            model_value: ModelValue = value
            assert model_value == value

    def test_column_rule_types(self):
        """Test ColumnRule type accepts correct values."""
        # Mock DataFrame for testing
        mock_df = Mock(spec=DataFrame)
        
        valid_rules = [
            mock_df,  # DataFrame
            "column_name",  # string
            True,  # boolean
        ]
        
        for rule in valid_rules:
            column_rule: ColumnRule = rule
            assert column_rule == rule

    def test_resource_value_types(self):
        """Test ResourceValue type accepts correct values."""
        valid_values = [
            "string",
            42,
            3.14,
            True,
            ["list", "of", "strings"],
            {"key": "value"}
        ]
        
        for value in valid_values:
            resource_value: ResourceValue = value
            assert resource_value == value


class TestBaseModel:
    """Test BaseModel abstract base class."""

    def test_base_model_abstract(self):
        """Test that BaseModel cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseModel()

    def test_base_model_validation_abstract(self):
        """Test that validate method is abstract."""
        class ConcreteModel(BaseModel):
            def __init__(self, name: str):
                self.name = name
        
        with pytest.raises(TypeError):
            ConcreteModel("test")


class TestBronzeStep:
    """Test BronzeStep class."""

    def test_bronze_step_creation(self):
        """Test BronzeStep creation with valid parameters."""
        rules = {"id": [F.col("id").isNotNull()]}
        step = BronzeStep(
            name="test_bronze",
            rules=rules,
            incremental_col="timestamp",
            schema="test_schema"
        )
        
        assert step.name == "test_bronze"
        assert step.incremental_col == "timestamp"
        assert step.schema == "test_schema"
        assert step.rules is rules  # Check reference equality instead of value equality

    def test_bronze_step_creation_minimal(self):
        """Test BronzeStep creation with minimal valid parameters."""
        # A BronzeStep must have non-empty rules - this is the core purpose
        rules = {"id": [F.col("id").isNotNull()]}
        step = BronzeStep(
            name="minimal_bronze",
            rules=rules
        )
        
        assert step.name == "minimal_bronze"
        assert step.rules is rules
        assert step.incremental_col is None
        assert step.schema is None

    def test_bronze_step_validation_success(self):
        """Test BronzeStep validation with valid data."""
        step = BronzeStep(
            name="valid_bronze",
            rules={"id": [F.col("id").isNotNull()]},
            incremental_col="timestamp",
            schema="test_schema"
        )
        
        # Should not raise any exception
        step.validate()

    def test_bronze_step_validation_empty_name(self):
        """Test BronzeStep creation with empty name should fail."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        with pytest.raises(ValidationError, match="Step name must be a non-empty string"):
            BronzeStep(
                name="",
                rules=rules
            )

    def test_bronze_step_validation_none_name(self):
        """Test BronzeStep creation with None name should fail."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        with pytest.raises(ValidationError, match="Step name must be a non-empty string"):
            BronzeStep(
                name=None,
                rules=rules
            )

    def test_bronze_step_validation_invalid_rules_type(self):
        """Test BronzeStep creation with invalid rules type should fail."""
        with pytest.raises(ValidationError, match="Rules must be a non-empty dictionary"):
            BronzeStep(
                name="test",
                rules="invalid"  # Should be dict
            )

    def test_bronze_step_has_incremental_capability(self):
        """Test has_incremental_capability property."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        # With incremental column
        step_with_incremental = BronzeStep(
            name="test",
            rules=rules,
            incremental_col="timestamp"
        )
        assert step_with_incremental.has_incremental_capability is True
        
        # Without incremental column
        step_without_incremental = BronzeStep(
            name="test",
            rules=rules
        )
        assert step_without_incremental.has_incremental_capability is False


class TestSilverStep:
    """Test SilverStep class."""

    def test_silver_step_creation(self):
        """Test SilverStep creation with valid parameters."""
        step = SilverStep(
            name="test_silver",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="silver_table",
            schema="test_schema"
        )
        
        assert step.name == "test_silver"
        assert step.source_bronze == "bronze_step"
        assert step.table_name == "silver_table"
        assert step.schema == "test_schema"
        assert callable(step.transform)

    def test_silver_step_creation_minimal(self):
        """Test SilverStep creation with minimal parameters."""
        step = SilverStep(
            name="minimal_silver",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={},
            table_name="silver_table"
        )
        
        assert step.name == "minimal_silver"
        assert step.source_bronze == "bronze_step"
        assert step.table_name == "silver_table"
        assert step.watermark_col is None
        assert step.existing is False
        assert step.schema is None

    def test_silver_step_validation_success(self):
        """Test SilverStep validation with valid data."""
        step = SilverStep(
            name="valid_silver",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules={},
            table_name="silver_table"
        )
        
        # Should not raise any exception
        step.validate()

    def test_silver_step_validation_empty_name(self):
        """Test SilverStep creation with empty name should fail."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        with pytest.raises(ValidationError, match="Step name must be a non-empty string"):
            SilverStep(
                name="",
                source_bronze="bronze_step",
                transform=lambda spark, df, silvers: df,
                rules=rules,
                table_name="silver_table"
            )

    def test_silver_step_validation_empty_source_bronze(self):
        """Test SilverStep creation with empty source_bronze should fail."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        with pytest.raises(ValidationError, match="Source bronze step name must be a non-empty string"):
            SilverStep(
                name="test",
                source_bronze="",
                transform=lambda spark, df, silvers: df,
                rules=rules,
                table_name="silver_table"
            )

    def test_silver_step_validation_none_transform(self):
        """Test SilverStep creation with None transform should fail."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        with pytest.raises(ValidationError, match="Transform function is required and must be callable"):
            SilverStep(
                name="test",
                source_bronze="bronze_step",
                transform=None,
                rules=rules,
                table_name="silver_table"
            )

    def test_silver_step_validation_invalid_rules_type(self):
        """Test SilverStep validation with invalid rules type."""
        step = SilverStep(
            name="test",
            source_bronze="bronze_step",
            transform=lambda spark, df, silvers: df,
            rules="invalid",
            table_name="silver_table"
        )
        
        with pytest.raises(PipelineValidationError, match="Rules must be a dictionary"):
            step.validate()

    def test_silver_step_validation_empty_table_name(self):
        """Test SilverStep creation with empty table_name should fail."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        with pytest.raises(ValidationError, match="Table name must be a non-empty string"):
            SilverStep(
                name="test",
                source_bronze="bronze_step",
                transform=lambda spark, df, silvers: df,
                rules=rules,
                table_name=""
            )


class TestGoldStep:
    """Test GoldStep class."""

    def test_gold_step_creation(self):
        """Test GoldStep creation with valid parameters."""
        step = GoldStep(
            name="test_gold",
            transform=lambda spark, silvers: silvers["silver_step"],
            rules={"id": [F.col("id").isNotNull()]},
            table_name="gold_table",
            source_silvers=["silver_step"],
            schema="test_schema"
        )
        
        assert step.name == "test_gold"
        assert step.table_name == "gold_table"
        assert step.source_silvers == ["silver_step"]
        assert step.schema == "test_schema"
        assert callable(step.transform)

    def test_gold_step_creation_minimal(self):
        """Test GoldStep creation with minimal parameters."""
        rules = {"id": [F.col("id").isNotNull()]}
        step = GoldStep(
            name="minimal_gold",
            transform=lambda spark, silvers: silvers["silver_step"],
            rules=rules,
            table_name="gold_table"
        )
        
        assert step.name == "minimal_gold"
        assert step.table_name == "gold_table"
        assert step.source_silvers is None
        assert step.schema is None

    def test_gold_step_validation_success(self):
        """Test GoldStep validation with valid data."""
        rules = {"id": [F.col("id").isNotNull()]}
        step = GoldStep(
            name="valid_gold",
            transform=lambda spark, silvers: silvers["silver_step"],
            rules=rules,
            table_name="gold_table"
        )
        
        # Should not raise any exception
        step.validate()

    def test_gold_step_validation_empty_name(self):
        """Test GoldStep creation with empty name should fail."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        with pytest.raises(ValidationError, match="Step name must be a non-empty string"):
            GoldStep(
                name="",
                transform=lambda spark, silvers: silvers["silver_step"],
                rules=rules,
                table_name="gold_table"
            )

    def test_gold_step_validation_none_transform(self):
        """Test GoldStep creation with None transform should fail."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        with pytest.raises(ValidationError, match="Transform function is required and must be callable"):
            GoldStep(
                name="test",
                transform=None,
                rules=rules,
                table_name="gold_table"
            )

    def test_gold_step_validation_invalid_rules_type(self):
        """Test GoldStep creation with invalid rules type should fail."""
        with pytest.raises(ValidationError, match="Rules must be a non-empty dictionary"):
            GoldStep(
                name="test",
                transform=lambda spark, silvers: silvers["silver_step"],
                rules="invalid",
                table_name="gold_table"
            )

    def test_gold_step_validation_empty_table_name(self):
        """Test GoldStep creation with empty table_name should fail."""
        rules = {"id": [F.col("id").isNotNull()]}
        
        with pytest.raises(ValidationError, match="Table name must be a non-empty string"):
            GoldStep(
                name="test",
                transform=lambda spark, silvers: silvers["silver_step"],
                rules=rules,
                table_name=""
            )


class TestStepResult:
    """Test StepResult class."""

    def test_step_result_creation(self):
        """Test StepResult creation with all parameters."""
        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)
        
        result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=start_time,
            end_time=end_time,
            duration_secs=300.0,
            rows_processed=1000,
            rows_written=950,
            validation_rate=95.0,
            error_message=None
        )
        
        assert result.step_name == "test_step"
        assert result.phase == PipelinePhase.BRONZE
        assert result.success is True
        assert result.start_time == start_time
        assert result.end_time == end_time
        assert result.duration_secs == 300.0
        assert result.rows_processed == 1000
        assert result.rows_written == 950
        assert result.validation_rate == 95.0
        assert result.error_message is None

    def test_step_result_is_high_quality(self):
        """Test is_high_quality property."""
        start_time = datetime.now()
        end_time = datetime.now()
        
        # High quality result
        high_quality = StepResult(
            step_name="test",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=start_time,
            end_time=end_time,
            duration_secs=100.0,
            rows_processed=1000,
            rows_written=1000,
            validation_rate=98.0
        )
        assert high_quality.is_high_quality is True
        
        # Low quality result
        low_quality = StepResult(
            step_name="test",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=start_time,
            end_time=end_time,
            duration_secs=100.0,
            rows_processed=1000,
            rows_written=800,
            validation_rate=90.0
        )
        assert low_quality.is_high_quality is False

    def test_step_result_create_success(self):
        """Test create_success class method."""
        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)
        
        result = StepResult.create_success(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            start_time=start_time,
            end_time=end_time,
            rows_processed=1000,
            rows_written=950,
            validation_rate=95.0
        )
        
        assert result.step_name == "test_step"
        assert result.phase == PipelinePhase.BRONZE
        assert result.success is True
        assert result.duration_secs == 300.0
        assert result.rows_processed == 1000
        assert result.rows_written == 950
        assert result.validation_rate == 95.0
        assert result.error_message is None

    def test_step_result_create_failure(self):
        """Test create_failure class method."""
        start_time = datetime(2024, 1, 15, 10, 30, 0)
        end_time = datetime(2024, 1, 15, 10, 35, 0)
        
        result = StepResult.create_failure(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            start_time=start_time,
            end_time=end_time,
            error_message="Test error"
        )
        
        assert result.step_name == "test_step"
        assert result.phase == PipelinePhase.BRONZE
        assert result.success is False
        assert result.duration_secs == 300.0
        assert result.rows_processed == 0
        assert result.rows_written == 0
        assert result.validation_rate == 0.0
        assert result.error_message == "Test error"


class TestPipelineMetrics:
    """Test PipelineMetrics class."""

    def test_pipeline_metrics_creation_default(self):
        """Test PipelineMetrics creation with default values."""
        metrics = PipelineMetrics()
        
        assert metrics.total_steps == 0
        assert metrics.successful_steps == 0
        assert metrics.failed_steps == 0
        assert metrics.skipped_steps == 0
        assert metrics.total_duration == 0.0
        assert metrics.bronze_duration == 0.0
        assert metrics.silver_duration == 0.0
        assert metrics.gold_duration == 0.0
        assert metrics.total_rows_processed == 0
        assert metrics.total_rows_written == 0
        assert metrics.parallel_efficiency == 0.0
        assert metrics.cache_hit_rate == 0.0
        assert metrics.error_count == 0
        assert metrics.retry_count == 0

    def test_pipeline_metrics_creation_custom(self):
        """Test PipelineMetrics creation with custom values."""
        metrics = PipelineMetrics(
            total_steps=10,
            successful_steps=8,
            failed_steps=1,
            skipped_steps=1,
            total_duration=300.0,
            bronze_duration=100.0,
            silver_duration=150.0,
            gold_duration=50.0,
            total_rows_processed=10000,
            total_rows_written=9500,
            parallel_efficiency=0.85,
            cache_hit_rate=0.90,
            error_count=2,
            retry_count=1
        )
        
        assert metrics.total_steps == 10
        assert metrics.successful_steps == 8
        assert metrics.failed_steps == 1
        assert metrics.skipped_steps == 1
        assert metrics.total_duration == 300.0
        assert metrics.bronze_duration == 100.0
        assert metrics.silver_duration == 150.0
        assert metrics.gold_duration == 50.0
        assert metrics.total_rows_processed == 10000
        assert metrics.total_rows_written == 9500
        assert metrics.parallel_efficiency == 0.85
        assert metrics.cache_hit_rate == 0.90
        assert metrics.error_count == 2
        assert metrics.retry_count == 1


class TestPipelineConfig:
    """Test PipelineConfig class."""

    def test_pipeline_config_creation_default(self):
        """Test PipelineConfig creation with default values."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel = ParallelConfig(enabled=True, max_workers=4)
        config = PipelineConfig(
            schema="default",
            thresholds=thresholds,
            parallel=parallel
        )
        
        assert config.schema == "default"
        assert config.thresholds == thresholds
        assert config.parallel == parallel
        assert config.verbose is True

    def test_pipeline_config_creation_custom(self):
        """Test PipelineConfig creation with custom values."""
        thresholds = ValidationThresholds(bronze=90.0, silver=95.0, gold=98.0)
        parallel = ParallelConfig(enabled=False, max_workers=2)
        
        config = PipelineConfig(
            schema="custom_schema",
            thresholds=thresholds,
            parallel=parallel,
            verbose=False
        )
        
        assert config.schema == "custom_schema"
        assert config.thresholds == thresholds
        assert config.parallel == parallel
        assert config.verbose is False


class TestSilverDependencyInfo:
    """Test SilverDependencyInfo class."""

    def test_silver_dependency_info_creation(self):
        """Test SilverDependencyInfo creation."""
        dep_info = SilverDependencyInfo(
            step_name="silver_step",
            source_bronze="bronze_step",
            depends_on_silvers={"other_silver"},
            can_run_parallel=True,
            execution_group=1
        )
        
        assert dep_info.step_name == "silver_step"
        assert dep_info.source_bronze == "bronze_step"
        assert dep_info.depends_on_silvers == {"other_silver"}
        assert dep_info.can_run_parallel is True
        assert dep_info.execution_group == 1

    def test_silver_dependency_info_validation_success(self):
        """Test SilverDependencyInfo validation with valid data."""
        dep_info = SilverDependencyInfo(
            step_name="silver_step",
            source_bronze="bronze_step",
            depends_on_silvers=set(),
            can_run_parallel=True,
            execution_group=1
        )
        
        # Should not raise any exception
        dep_info.validate()

    def test_silver_dependency_info_validation_empty_step_name(self):
        """Test SilverDependencyInfo validation with empty step_name."""
        dep_info = SilverDependencyInfo(
            step_name="",
            source_bronze="bronze_step",
            depends_on_silvers=set(),
            can_run_parallel=True,
            execution_group=1
        )
        
        with pytest.raises(PipelineValidationError, match="Step name must be a non-empty string"):
            dep_info.validate()

    def test_silver_dependency_info_validation_empty_source_bronze(self):
        """Test SilverDependencyInfo validation with empty source_bronze."""
        dep_info = SilverDependencyInfo(
            step_name="silver_step",
            source_bronze="",
            depends_on_silvers=set(),
            can_run_parallel=True,
            execution_group=1
        )
        
        with pytest.raises(PipelineValidationError, match="Source bronze step name must be a non-empty string"):
            dep_info.validate()




class TestParallelConfig:
    """Test ParallelConfig class."""

    def test_parallel_config_creation(self):
        """Test ParallelConfig creation."""
        config = ParallelConfig(
            enabled=True,
            max_workers=4,
            timeout_secs=300
        )
        
        assert config.enabled is True
        assert config.max_workers == 4
        assert config.timeout_secs == 300

    def test_parallel_config_creation_minimal(self):
        """Test ParallelConfig creation with minimal parameters."""
        config = ParallelConfig(enabled=False, max_workers=1)
        
        assert config.enabled is False
        assert config.max_workers == 1
        assert config.timeout_secs == 300  # Default value

    def test_parallel_config_validation_success(self):
        """Test ParallelConfig validation with valid data."""
        config = ParallelConfig(
            enabled=True,
            max_workers=4,
            timeout_secs=300
        )
        
        # Should not raise any exception
        config.validate()

    def test_parallel_config_validation_negative_max_workers(self):
        """Test ParallelConfig validation with negative max_workers."""
        config = ParallelConfig(
            enabled=True,
            max_workers=-1
        )
        
        with pytest.raises(PipelineValidationError, match="max_workers must be at least 1"):
            config.validate()

    def test_parallel_config_validation_negative_worker_timeout(self):
        """Test ParallelConfig validation with negative timeout_secs."""
        config = ParallelConfig(
            enabled=True,
            max_workers=4,
            timeout_secs=-1
        )
        
        with pytest.raises(PipelineValidationError, match="timeout_secs must be at least 1"):
            config.validate()

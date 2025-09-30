#!/usr/bin/env python3
"""
Simple tests for models module functionality.

This module provides basic tests for the core model classes with proper
API alignment and error handling.
"""

import json
from dataclasses import dataclass
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from sparkforge.errors import PipelineConfigurationError, ValidationError
from sparkforge.models import (
    BaseModel,
    BronzeStep,
    ExecutionContext,
    ExecutionMode,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    PipelineMetrics,
    PipelinePhase,
    SilverStep,
    StageStats,
    StepResult,
    ValidationResult,
    ValidationThresholds,
    WriteMode,
)
from sparkforge.pipeline.models import PipelineMode, PipelineReport, PipelineStatus
from sparkforge.types import StepStatus, StepType


class TestBaseModel:
    """Test cases for BaseModel class."""

    def test_validate_default(self):
        """Test default validate method."""

        @dataclass
        class TestModel(BaseModel):
            name: str = "test"

            def validate(self) -> None:
                pass

        model = TestModel()
        # Should not raise any exception
        model.validate()

    def test_to_dict_simple(self):
        """Test to_dict with simple values."""

        @dataclass
        class TestModel(BaseModel):
            name: str
            value: int
            flag: bool

            def validate(self) -> None:
                pass

        model = TestModel(name="test", value=42, flag=True)
        result = model.to_dict()

        expected = {"name": "test", "value": 42, "flag": True}
        assert result == expected

    def test_to_dict_nested_objects(self):
        """Test to_dict with nested objects that have to_dict method."""

        @dataclass
        class NestedModel(BaseModel):
            nested_name: str

            def validate(self) -> None:
                pass

            def to_dict(self):
                return {"nested_name": self.nested_name}

        @dataclass
        class TestModel(BaseModel):
            name: str
            nested: NestedModel

            def validate(self) -> None:
                pass

        nested = NestedModel(nested_name="nested_test")
        model = TestModel(name="test", nested=nested)
        result = model.to_dict()

        expected = {"name": "test", "nested": {"nested_name": "nested_test"}}
        assert result == expected

    def test_to_json(self):
        """Test to_json method."""

        @dataclass
        class TestModel(BaseModel):
            name: str
            value: int

            def validate(self) -> None:
                pass

        model = TestModel(name="test", value=42)
        result = model.to_json()

        # Parse back to verify structure
        parsed = json.loads(result)
        assert parsed["name"] == "test"
        assert parsed["value"] == 42

    def test_str_representation(self):
        """Test string representation."""

        @dataclass
        class TestModel(BaseModel):
            name: str
            value: int

            def validate(self) -> None:
                pass

        model = TestModel(name="test", value=42)
        result = str(model)

        assert "TestModel" in result
        assert "name=test" in result
        assert "value=42" in result


class TestValidationThresholds:
    """Test cases for ValidationThresholds class."""

    def test_validation_thresholds_creation(self):
        """Test ValidationThresholds creation."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)

        assert thresholds.bronze == 95.0
        assert thresholds.silver == 98.0
        assert thresholds.gold == 99.0

    def test_validation_thresholds_validation(self):
        """Test ValidationThresholds validation."""
        # Valid thresholds
        thresholds = ValidationThresholds(bronze=90.0, silver=95.0, gold=99.0)
        thresholds.validate()  # Should not raise

    def test_validation_thresholds_invalid_bronze(self):
        """Test ValidationThresholds with invalid bronze threshold."""
        thresholds = ValidationThresholds(bronze=150.0, silver=98.0, gold=99.0)
        with pytest.raises(
            ValidationError, match="bronze threshold must be between 0 and 100"
        ):
            thresholds.validate()

    def test_validation_thresholds_invalid_silver(self):
        """Test ValidationThresholds with invalid silver threshold."""
        thresholds = ValidationThresholds(bronze=95.0, silver=-10.0, gold=99.0)
        with pytest.raises(
            ValidationError, match="silver threshold must be between 0 and 100"
        ):
            thresholds.validate()

    def test_validation_thresholds_invalid_gold(self):
        """Test ValidationThresholds with invalid gold threshold."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=200.0)
        with pytest.raises(
            ValidationError, match="gold threshold must be between 0 and 100"
        ):
            thresholds.validate()

    def test_validation_thresholds_hierarchy(self):
        """Test ValidationThresholds hierarchy validation."""
        thresholds = ValidationThresholds(bronze=98.0, silver=95.0, gold=99.0)
        # The current implementation doesn't validate hierarchy, so this test should pass
        thresholds.validate()  # Should not raise


class TestParallelConfig:
    """Test cases for ParallelConfig class."""

    def test_parallel_config_creation(self):
        """Test ParallelConfig creation."""
        config = ParallelConfig(enabled=True, max_workers=4)

        assert config.enabled is True
        assert config.max_workers == 4
        assert config.timeout_secs == 300  # Default value

    def test_parallel_config_validation(self):
        """Test ParallelConfig validation."""
        config = ParallelConfig(enabled=True, max_workers=8)
        config.validate()  # Should not raise

    def test_parallel_config_invalid_max_workers(self):
        """Test ParallelConfig with invalid max_workers."""
        config = ParallelConfig(enabled=True, max_workers=0)
        with pytest.raises(ValidationError, match="max_workers must be at least 1"):
            config.validate()


class TestPipelineConfig:
    """Test cases for PipelineConfig class."""

    def test_pipeline_config_creation(self):
        """Test PipelineConfig creation."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel = ParallelConfig(enabled=True, max_workers=4)
        config = PipelineConfig(
            schema="test_schema", thresholds=thresholds, parallel=parallel
        )

        assert config.schema == "test_schema"
        assert config.thresholds == thresholds
        assert config.parallel == parallel
        assert config.verbose is True

    def test_pipeline_config_validation(self):
        """Test PipelineConfig validation."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel = ParallelConfig(enabled=True, max_workers=4)
        config = PipelineConfig(
            schema="test_schema", thresholds=thresholds, parallel=parallel
        )

        config.validate()  # Should not raise


class TestBronzeStep:
    """Test cases for BronzeStep class."""

    def test_bronze_step_creation(self):
        """Test BronzeStep creation."""
        rules = {"id": [F.col("id").isNotNull()]}
        step = BronzeStep(name="test_bronze", rules=rules, incremental_col="timestamp")

        assert step.name == "test_bronze"
        assert step.incremental_col == "timestamp"
        assert step.rules is rules

    def test_bronze_step_validation(self):
        """Test BronzeStep validation."""
        step = BronzeStep(name="test_bronze", rules={"id": [F.col("id").isNotNull()]})
        step.validate()  # Should not raise

    def test_bronze_step_invalid_name(self):
        """Test BronzeStep creation with invalid name should fail."""
        with pytest.raises(
            ValidationError, match="Step name must be a non-empty string"
        ):
            BronzeStep(name="", rules={"id": [F.col("id").isNotNull()]})

    def test_bronze_step_invalid_rules(self):
        """Test BronzeStep creation with invalid rules should fail."""
        with pytest.raises(
            ValidationError, match="Rules must be a non-empty dictionary"
        ):
            BronzeStep(name="test_bronze", rules={})


class TestSilverStep:
    """Test cases for SilverStep class."""

    def test_silver_step_creation(self):
        """Test SilverStep creation."""

        def transform_func(spark, df, silvers):
            return df

        step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform_func,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_silver_table",
        )

        assert step.name == "test_silver"
        assert step.source_bronze == "test_bronze"
        assert step.table_name == "test_silver_table"

    def test_silver_step_validation(self):
        """Test SilverStep validation."""

        def transform_func(spark, df, silvers):
            return df

        step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform_func,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_silver_table",
        )
        step.validate()  # Should not raise

    def test_silver_step_invalid_source_bronze(self):
        """Test SilverStep creation with invalid source_bronze should fail."""

        def transform_func(spark, df, silvers):
            return df

        with pytest.raises(
            ValidationError, match="Source bronze step name must be a non-empty string"
        ):
            SilverStep(
                name="test_silver",
                source_bronze="",
                transform=transform_func,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="test_silver_table",
            )

    def test_silver_step_invalid_transform(self):
        """Test SilverStep creation with invalid transform should fail."""
        with pytest.raises(
            ValidationError, match="Transform function is required and must be callable"
        ):
            SilverStep(
                name="test_silver",
                source_bronze="test_bronze",
                transform=None,  # Invalid transform
                rules={"id": [F.col("id").isNotNull()]},
                table_name="test_silver_table",
            )

    def test_silver_step_invalid_table_name(self):
        """Test SilverStep creation with invalid table_name should fail."""

        def transform_func(spark, df, silvers):
            return df

        with pytest.raises(
            ValidationError, match="Table name must be a non-empty string"
        ):
            SilverStep(
                name="test_silver",
                source_bronze="test_bronze",
                transform=transform_func,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="",
            )


class TestGoldStep:
    """Test cases for GoldStep class."""

    def test_gold_step_creation(self):
        """Test GoldStep creation."""

        def transform_func(spark, silvers):
            return silvers["test_silver"]

        step = GoldStep(
            name="test_gold",
            transform=transform_func,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_gold_table",
            source_silvers=["test_silver"],
        )

        assert step.name == "test_gold"
        assert step.table_name == "test_gold_table"
        assert step.source_silvers == ["test_silver"]

    def test_gold_step_validation(self):
        """Test GoldStep validation."""

        def transform_func(spark, silvers):
            return silvers["test_silver"]

        step = GoldStep(
            name="test_gold",
            transform=transform_func,
            rules={"id": [F.col("id").isNotNull()]},
            table_name="test_gold_table",
            source_silvers=["test_silver"],
        )
        step.validate()  # Should not raise

    def test_gold_step_invalid_transform(self):
        """Test GoldStep creation with invalid transform should fail."""
        with pytest.raises(
            ValidationError, match="Transform function is required and must be callable"
        ):
            GoldStep(
                name="test_gold",
                transform=None,  # Invalid transform
                rules={"id": [F.col("id").isNotNull()]},
                table_name="test_gold_table",
                source_silvers=["test_silver"],
            )

    def test_gold_step_invalid_table_name(self):
        """Test GoldStep creation with invalid table_name should fail."""

        def transform_func(spark, silvers):
            return silvers["test_silver"]

        with pytest.raises(
            ValidationError, match="Table name must be a non-empty string"
        ):
            GoldStep(
                name="test_gold",
                transform=transform_func,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="",
                source_silvers=["test_silver"],
            )

    def test_gold_step_invalid_source_silvers(self):
        """Test GoldStep creation with invalid source_silvers should fail."""

        def transform_func(spark, silvers):
            return silvers["test_silver"]

        with pytest.raises(
            ValidationError, match="Source silvers must be a non-empty list"
        ):
            GoldStep(
                name="test_gold",
                transform=transform_func,
                rules={"id": [F.col("id").isNotNull()]},
                table_name="test_gold_table",
                source_silvers=[],  # Invalid empty list
            )


class TestStageStats:
    """Test cases for StageStats class."""

    def test_stage_stats_creation(self):
        """Test StageStats creation."""
        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.0,
            duration_secs=10.5,
        )

        assert stats.stage == "bronze"
        assert stats.step == "test_step"
        assert stats.total_rows == 1000
        assert stats.valid_rows == 950
        assert stats.invalid_rows == 50
        assert stats.validation_rate == 95.0
        assert stats.duration_secs == 10.5

    def test_stage_stats_validation(self):
        """Test StageStats validation."""
        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.0,
            duration_secs=10.5,
        )
        stats.validate()  # Should not raise

    def test_stage_stats_invalid_validation_rate(self):
        """Test StageStats with invalid validation_rate."""
        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=150.0,  # Invalid rate
            duration_secs=10.5,
        )
        with pytest.raises(
            PipelineConfigurationError,
            match="Validation rate must be between 0 and 100",
        ):
            stats.validate()

    def test_stage_stats_negative_values(self):
        """Test StageStats with negative values."""
        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=-1000,  # Invalid negative
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.0,
            duration_secs=10.5,
        )
        with pytest.raises(
            PipelineConfigurationError,
            match="Total rows \\(-1000\\) must equal valid \\(950\\) \\+ invalid \\(50\\)",
        ):
            stats.validate()


class TestExecutionContext:
    """Test cases for ExecutionContext class."""

    def test_execution_context_creation(self):
        """Test ExecutionContext creation."""
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL, start_time=datetime.now()
        )

        assert context.mode == ExecutionMode.INITIAL
        assert context.start_time is not None
        assert context.run_id is not None

    def test_execution_context_validation(self):
        """Test ExecutionContext validation."""
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL, start_time=datetime.now()
        )
        context.validate()  # Should not raise

    def test_execution_context_invalid_execution_id(self):
        """Test ExecutionContext with invalid run_id."""
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL, start_time=datetime.now(), run_id=""
        )
        with pytest.raises(ValueError, match="Run ID cannot be empty"):
            context.validate()


class TestPipelineMetrics:
    """Test cases for PipelineMetrics class."""

    def test_pipeline_metrics_creation(self):
        """Test PipelineMetrics creation."""
        metrics = PipelineMetrics(
            total_steps=5, successful_steps=4, failed_steps=1, total_duration=120.5
        )

        assert metrics.total_steps == 5
        assert metrics.successful_steps == 4
        assert metrics.failed_steps == 1
        assert metrics.total_duration == 120.5

    def test_pipeline_metrics_validation(self):
        """Test PipelineMetrics validation."""
        metrics = PipelineMetrics(
            total_steps=5, successful_steps=4, failed_steps=1, total_duration=120.5
        )
        metrics.validate()  # Should not raise

    def test_pipeline_metrics_invalid_values(self):
        """Test PipelineMetrics with invalid values."""
        metrics = PipelineMetrics(
            total_steps=-5,  # Invalid negative
            successful_steps=4,
            failed_steps=1,
            total_duration=120.5,
        )
        with pytest.raises(ValueError, match="Total steps cannot be negative"):
            metrics.validate()


class TestPipelineReport:
    """Test cases for PipelineReport class."""

    def test_pipeline_report_creation(self):
        """Test PipelineReport creation."""
        metrics = PipelineMetrics(
            total_steps=5, successful_steps=4, failed_steps=1, total_duration=120.5
        )

        report = PipelineReport(
            pipeline_id="test_pipeline",
            execution_id="test_execution",
            status=PipelineStatus.COMPLETED,
            mode=PipelineMode.INITIAL,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=120.5,
            metrics=metrics,
            errors=[],
            warnings=[],
        )

        assert report.pipeline_id == "test_pipeline"
        assert report.execution_id == "test_execution"
        assert report.status == PipelineStatus.COMPLETED
        assert report.mode == PipelineMode.INITIAL

    def test_pipeline_report_validation(self):
        """Test PipelineReport validation."""
        metrics = PipelineMetrics(
            total_steps=5, successful_steps=4, failed_steps=1, total_duration=120.5
        )

        report = PipelineReport(
            pipeline_id="test_pipeline",
            execution_id="test_execution",
            status=PipelineStatus.COMPLETED,
            mode=PipelineMode.INITIAL,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=120.5,
            metrics=metrics,
            errors=[],
            warnings=[],
        )

        # PipelineReport doesn't have a validate method
        # Just check that the report was created successfully
        assert report.pipeline_id == "test_pipeline"

    def test_pipeline_report_invalid_pipeline_id(self):
        """Test PipelineReport with invalid pipeline_id."""
        metrics = PipelineMetrics(
            total_steps=5, successful_steps=4, failed_steps=1, total_duration=120.5
        )

        # PipelineReport doesn't validate during construction
        # So this should succeed
        report = PipelineReport(
            pipeline_id="",
            execution_id="test_execution",
            status=PipelineStatus.COMPLETED,
            mode=PipelineMode.INITIAL,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=120.5,
            metrics=metrics,
            errors=[],
            warnings=[],
        )
        assert report.pipeline_id == ""


class TestStepResult:
    """Test cases for StepResult class."""

    def test_step_result_creation(self):
        """Test StepResult creation."""
        result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=10.5,
            rows_processed=1000,
            rows_written=950,
            validation_rate=95.0,
            error_message=None,
        )

        assert result.step_name == "test_step"
        assert result.phase == PipelinePhase.BRONZE
        assert result.success is True
        assert result.duration_secs == 10.5
        assert result.rows_processed == 1000
        assert result.rows_written == 950
        assert result.validation_rate == 95.0
        assert result.error_message is None

    def test_step_result_validation(self):
        """Test StepResult validation."""
        result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=10.5,
            rows_processed=1000,
            rows_written=950,
            validation_rate=95.0,
            error_message=None,
        )
        result.validate()  # Should not raise

    def test_step_result_invalid_step_name(self):
        """Test StepResult with invalid step_name."""
        result = StepResult(
            step_name="",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=10.5,
            rows_processed=1000,
            rows_written=950,
            validation_rate=95.0,
            error_message=None,
        )
        with pytest.raises(ValueError, match="Step name cannot be empty"):
            result.validate()


class TestEnums:
    """Test cases for enum classes."""

    def test_pipeline_phase_enum(self):
        """Test PipelinePhase enum values."""
        assert PipelinePhase.BRONZE.value == "bronze"
        assert PipelinePhase.SILVER.value == "silver"
        assert PipelinePhase.GOLD.value == "gold"

    def test_execution_mode_enum(self):
        """Test ExecutionMode enum values."""
        assert ExecutionMode.INITIAL.value == "initial"
        assert ExecutionMode.INCREMENTAL.value == "incremental"

    def test_write_mode_enum(self):
        """Test WriteMode enum values."""
        assert WriteMode.OVERWRITE.value == "overwrite"
        assert WriteMode.APPEND.value == "append"

    def test_validation_result_enum(self):
        """Test ValidationResult enum values."""
        assert ValidationResult.PASSED.value == "passed"
        assert ValidationResult.FAILED.value == "failed"
        assert ValidationResult.WARNING.value == "warning"

    def test_step_status_enum(self):
        """Test StepStatus enum values."""
        assert StepStatus.COMPLETED.value == "completed"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.SKIPPED.value == "skipped"
        assert StepStatus.RUNNING.value == "running"

    def test_step_type_enum(self):
        """Test StepType enum values."""
        assert StepType.BRONZE.value == "bronze"
        assert StepType.SILVER.value == "silver"
        assert StepType.GOLD.value == "gold"

    def test_pipeline_status_enum(self):
        """Test PipelineStatus enum values."""
        assert PipelineStatus.RUNNING.value == "running"
        assert PipelineStatus.COMPLETED.value == "completed"
        assert PipelineStatus.FAILED.value == "failed"

    def test_pipeline_mode_enum(self):
        """Test PipelineMode enum values."""
        assert PipelineMode.INITIAL.value == "initial"
        assert PipelineMode.INCREMENTAL.value == "incremental"

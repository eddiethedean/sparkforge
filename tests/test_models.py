# test_models.py
"""
Unit tests for the models module.

This module tests all data models and type definitions.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from sparkforge.models import (
    ValidationThresholds,
    ParallelConfig,
    PipelineConfig,
    BronzeStep,
    SilverStep,
    GoldStep,
    ExecutionContext,
    StageStats,
    StepResult,
    PipelineMetrics,
    ExecutionResult,
    SilverDependencyInfo,
    PipelinePhase
)
from sparkforge.exceptions import PipelineValidationError


@pytest.fixture
def spark_session():
    """Create Spark session for testing."""
    return SparkSession.builder.master('local[*]').appName('test').getOrCreate()


class TestValidationThresholds:
    """Test ValidationThresholds model."""
    
    def test_validation_thresholds_creation(self):
        """Test creating ValidationThresholds."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        
        assert thresholds.bronze == 95.0
        assert thresholds.silver == 98.0
        assert thresholds.gold == 99.0
    
    def test_validation_thresholds_defaults(self):
        """Test ValidationThresholds with default values."""
        # ValidationThresholds requires all parameters, no defaults
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        
        assert thresholds.bronze == 95.0
        assert thresholds.silver == 98.0
        assert thresholds.gold == 99.0
    
    def test_validation_thresholds_validation(self):
        """Test ValidationThresholds validation."""
        # Valid thresholds
        thresholds = ValidationThresholds(bronze=90.0, silver=95.0, gold=99.0)
        thresholds.validate()  # Should not raise
        
        # Invalid thresholds - need to call validate() method
        invalid_thresholds = ValidationThresholds(bronze=101.0, silver=95.0, gold=99.0)
        with pytest.raises(PipelineValidationError):
            invalid_thresholds.validate()
        
        invalid_thresholds2 = ValidationThresholds(bronze=95.0, silver=-1.0, gold=99.0)
        with pytest.raises(PipelineValidationError):
            invalid_thresholds2.validate()


class TestParallelConfig:
    """Test ParallelConfig model."""
    
    def test_parallel_config_creation(self):
        """Test creating ParallelConfig."""
        config = ParallelConfig(enabled=True, max_workers=4, timeout_secs=300)
        
        assert config.enabled is True
        assert config.max_workers == 4
        assert config.timeout_secs == 300
    
    def test_parallel_config_defaults(self):
        """Test ParallelConfig with default values."""
        # ParallelConfig requires enabled and max_workers, timeout_secs has default
        config = ParallelConfig(enabled=True, max_workers=4)
        
        assert config.enabled is True
        assert config.max_workers == 4
        assert config.timeout_secs == 300


class TestPipelineConfig:
    """Test PipelineConfig model."""
    
    def test_pipeline_config_creation(self):
        """Test creating PipelineConfig."""
        thresholds = ValidationThresholds(bronze=90.0, silver=95.0, gold=99.0)
        parallel = ParallelConfig(enabled=True, max_workers=2)
        
        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            parallel=parallel,
            verbose=True
        )
        
        assert config.schema == "test_schema"
        assert config.thresholds == thresholds
        assert config.parallel == parallel
        assert config.verbose is True
    
    def test_pipeline_config_validation(self):
        """Test PipelineConfig validation."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel = ParallelConfig(enabled=True, max_workers=4)
        
        # Valid config
        config = PipelineConfig(schema="test", thresholds=thresholds, parallel=parallel)
        config.validate()  # Should not raise
        
        # Invalid schema
        with pytest.raises(PipelineValidationError):
            config = PipelineConfig(schema="", thresholds=thresholds, parallel=parallel)
            config.validate()


class TestBronzeStep:
    """Test BronzeStep model."""
    
    def test_bronze_step_creation(self):
        """Test creating BronzeStep."""
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        step = BronzeStep(name="test_step", rules=rules, incremental_col="created_at")
        
        assert step.name == "test_step"
        assert step.rules == rules
        assert step.incremental_col == "created_at"
    
    def test_bronze_step_no_incremental(self):
        """Test BronzeStep without incremental column."""
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        step = BronzeStep(name="test_step", rules=rules)
        
        assert step.name == "test_step"
        assert step.rules == rules
        assert step.incremental_col is None
    
    def test_bronze_step_has_incremental_capability(self):
        """Test has_incremental_capability property."""
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        
        step_with_incremental = BronzeStep(name="test", rules=rules, incremental_col="created_at")
        assert step_with_incremental.has_incremental_capability is True
        
        step_without_incremental = BronzeStep(name="test", rules=rules)
        assert step_without_incremental.has_incremental_capability is False
    
    def test_bronze_step_validation(self):
        """Test BronzeStep validation."""
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        
        # Valid step
        step = BronzeStep(name="test", rules=rules)
        step.validate()  # Should not raise
        
        # Invalid name
        with pytest.raises(PipelineValidationError):
            step = BronzeStep(name="", rules=rules)
            step.validate()


class TestSilverStep:
    """Test SilverStep model."""
    
    def test_silver_step_creation(self):
        """Test creating SilverStep."""
        def transform_func(spark, df, prior_silvers):
            return df
        
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        step = SilverStep(
            name="test_step",
            source_bronze="bronze_step",
            transform=transform_func,
            rules=rules,
            table_name="test_table",
            watermark_col="created_at"
        )
        
        assert step.name == "test_step"
        assert step.source_bronze == "bronze_step"
        assert step.transform == transform_func
        assert step.rules == rules
        assert step.table_name == "test_table"
        assert step.watermark_col == "created_at"
        assert step.existing is False
    
    def test_silver_step_existing(self):
        """Test SilverStep for existing table."""
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        step = SilverStep(
            name="existing_step",
            source_bronze=None,
            transform=None,
            rules=rules,
            table_name="existing_table",
            watermark_col="updated_at",
            existing=True
        )
        
        assert step.existing is True
        assert step.source_bronze is None
        assert step.transform is None
    
    def test_silver_step_validation(self):
        """Test SilverStep validation."""
        def transform_func(spark, df, prior_silvers):
            return df
        
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        
        # Valid step
        step = SilverStep(
            name="test",
            source_bronze="bronze",
            transform=transform_func,
            rules=rules,
            table_name="table"
        )
        step.validate()  # Should not raise
        
        # Invalid name
        with pytest.raises(PipelineValidationError):
            step = SilverStep(
                name="",
                source_bronze="bronze",
                transform=transform_func,
                rules=rules,
                table_name="table"
            )
            step.validate()


class TestGoldStep:
    """Test GoldStep model."""
    
    def test_gold_step_creation(self):
        """Test creating GoldStep."""
        def transform_func(spark, silvers):
            return silvers["silver1"]
        
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        step = GoldStep(
            name="test_step",
            transform=transform_func,
            rules=rules,
            table_name="test_table",
            source_silvers=["silver1", "silver2"]
        )
        
        assert step.name == "test_step"
        assert step.transform == transform_func
        assert step.rules == rules
        assert step.table_name == "test_table"
        assert step.source_silvers == ["silver1", "silver2"]
    
    def test_gold_step_no_source_silvers(self):
        """Test GoldStep without source silvers."""
        def transform_func(spark, silvers):
            return silvers["silver1"]
        
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        step = GoldStep(
            name="test_step",
            transform=transform_func,
            rules=rules,
            table_name="test_table"
        )
        
        assert step.source_silvers is None
    
    def test_gold_step_validation(self):
        """Test GoldStep validation."""
        def transform_func(spark, silvers):
            return silvers["silver1"]
        
        rules = {"user_id": [F.col("user_id").isNotNull()]}
        
        # Valid step
        step = GoldStep(
            name="test",
            transform=transform_func,
            rules=rules,
            table_name="table"
        )
        step.validate()  # Should not raise
        
        # Invalid name
        with pytest.raises(PipelineValidationError):
            step = GoldStep(
                name="",
                transform=transform_func,
                rules=rules,
                table_name="table"
            )
            step.validate()


class TestExecutionContext:
    """Test ExecutionContext model."""
    
    def test_execution_context_creation(self):
        """Test creating ExecutionContext."""
        context = ExecutionContext(
            run_id="test_run_123",
            mode="initial",
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            end_time=datetime(2023, 1, 1, 10, 5, 0)
        )
        
        assert context.run_id == "test_run_123"
        assert context.mode == "initial"
        assert context.start_time == datetime(2023, 1, 1, 10, 0, 0)
        assert context.end_time == datetime(2023, 1, 1, 10, 5, 0)


class TestStageStats:
    """Test StageStats model."""
    
    def test_stage_stats_creation(self):
        """Test creating StageStats."""
        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.0,
            duration_secs=300.0
        )
        
        assert stats.stage == "bronze"
        assert stats.step == "test_step"
        assert stats.total_rows == 1000
        assert stats.valid_rows == 950
        assert stats.invalid_rows == 50
        assert stats.validation_rate == 95.0
        assert stats.duration_secs == 300.0


class TestStepResult:
    """Test StepResult model."""
    
    def test_step_result_creation(self):
        """Test creating StepResult."""
        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            validation_rate=95.0,
            duration_secs=300.0
        )
        
        result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            end_time=datetime(2023, 1, 1, 10, 5, 0),
            duration_secs=300.0,
            rows_processed=1000,
            rows_written=950,
            validation_rate=95.0,
            error_message=None
        )
        
        assert result.step_name == "test_step"
        assert result.success is True
        assert result.phase == PipelinePhase.BRONZE
        assert result.duration_secs == 300.0


class TestPipelineMetrics:
    """Test PipelineMetrics model."""
    
    def test_pipeline_metrics_creation(self):
        """Test creating PipelineMetrics."""
        metrics = PipelineMetrics(
            total_steps=10,
            successful_steps=8,
            failed_steps=2,
            total_duration_secs=3600.0,
            total_rows_processed=100000,
            total_rows_written=95000,
            avg_validation_rate=95.5
        )
        
        assert metrics.total_steps == 10
        assert metrics.successful_steps == 8
        assert metrics.failed_steps == 2
        assert metrics.total_duration_secs == 3600.0
        assert metrics.total_rows_processed == 100000
        assert metrics.total_rows_written == 95000
        assert metrics.avg_validation_rate == 95.5


class TestExecutionResult:
    """Test ExecutionResult model."""
    
    def test_execution_result_creation(self):
        """Test creating ExecutionResult."""
        metrics = PipelineMetrics(
            total_steps=10,
            successful_steps=8,
            failed_steps=2,
            total_duration_secs=3600.0,
            total_rows_processed=100000,
            total_rows_written=95000,
            avg_validation_rate=95.5
        )
        
        context = ExecutionContext(
            run_id="test_run",
            mode="initial",
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            end_time=datetime(2023, 1, 1, 10, 5, 0)
        )
        
        result = ExecutionResult(
            context=context,
            step_results=[],
            metrics=metrics,
            success=True
        )
        
        assert result.success is True
        assert result.metrics == metrics
        assert result.context == context
        assert result.step_results == []


class TestSilverDependencyInfo:
    """Test SilverDependencyInfo model."""
    
    def test_silver_dependency_info_creation(self):
        """Test creating SilverDependencyInfo."""
        info = SilverDependencyInfo(
            step_name="silver1",
            source_bronze="bronze1",
            depends_on_silvers=set(),
            can_run_parallel=True,
            execution_group=1
        )
        
        assert info.step_name == "silver1"
        assert info.source_bronze == "bronze1"
        assert info.depends_on_silvers == set()
        assert info.execution_group == 1
        assert info.can_run_parallel is True
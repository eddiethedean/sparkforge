#!/usr/bin/env python3
"""
Comprehensive tests for the models module.

This module tests all data models, validation, serialization, and factory functions.
"""

import unittest
from datetime import datetime
from typing import Dict, Any

from pipeline_builder.models import (
    # Enums
    PipelinePhase, ExecutionMode, WriteMode, ValidationResult,
    
    # Exceptions
    PipelineValidationError, PipelineConfigurationError, PipelineExecutionError,
    
    # Configuration models
    ValidationThresholds, ParallelConfig, PipelineConfig,
    
    # Step models
    BronzeStep, SilverStep, GoldStep,
    
    # Execution models
    ExecutionContext, StageStats, StepResult, PipelineMetrics, ExecutionResult,
    
    # Dependency models
    SilverDependencyInfo,
    
    # Factory functions
    create_pipeline_config, create_execution_context,
    
    # Validation utilities
    validate_pipeline_config, validate_step_config,
    
    # Serialization utilities
    serialize_pipeline_config, deserialize_pipeline_config
)


class TestEnums(unittest.TestCase):
    """Test enum classes."""
    
    def test_pipeline_phase_enum(self):
        """Test PipelinePhase enum."""
        self.assertEqual(PipelinePhase.BRONZE.value, "bronze")
        self.assertEqual(PipelinePhase.SILVER.value, "silver")
        self.assertEqual(PipelinePhase.GOLD.value, "gold")
    
    def test_execution_mode_enum(self):
        """Test ExecutionMode enum."""
        self.assertEqual(ExecutionMode.INITIAL.value, "initial")
        self.assertEqual(ExecutionMode.INCREMENTAL.value, "incremental")
    
    def test_write_mode_enum(self):
        """Test WriteMode enum."""
        self.assertEqual(WriteMode.OVERWRITE.value, "overwrite")
        self.assertEqual(WriteMode.APPEND.value, "append")
    
    def test_validation_result_enum(self):
        """Test ValidationResult enum."""
        self.assertEqual(ValidationResult.PASSED.value, "passed")
        self.assertEqual(ValidationResult.FAILED.value, "failed")
        self.assertEqual(ValidationResult.WARNING.value, "warning")


class TestValidationThresholds(unittest.TestCase):
    """Test ValidationThresholds class."""
    
    def test_validation_thresholds_creation(self):
        """Test creating validation thresholds."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        self.assertEqual(thresholds.bronze, 95.0)
        self.assertEqual(thresholds.silver, 98.0)
        self.assertEqual(thresholds.gold, 99.0)
    
    def test_validation_thresholds_validation_success(self):
        """Test successful validation."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        thresholds.validate()  # Should not raise
    
    def test_validation_thresholds_validation_failure(self):
        """Test validation failure."""
        with self.assertRaises(PipelineValidationError):
            ValidationThresholds(bronze=101.0, silver=98.0, gold=99.0).validate()
        
        with self.assertRaises(PipelineValidationError):
            ValidationThresholds(bronze=-1.0, silver=98.0, gold=99.0).validate()
    
    def test_get_threshold(self):
        """Test getting threshold for specific phase."""
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        self.assertEqual(thresholds.get_threshold(PipelinePhase.BRONZE), 95.0)
        self.assertEqual(thresholds.get_threshold(PipelinePhase.SILVER), 98.0)
        self.assertEqual(thresholds.get_threshold(PipelinePhase.GOLD), 99.0)
    
    def test_factory_methods(self):
        """Test factory methods."""
        default = ValidationThresholds.create_default()
        self.assertEqual(default.bronze, 95.0)
        self.assertEqual(default.silver, 98.0)
        self.assertEqual(default.gold, 99.0)
        
        strict = ValidationThresholds.create_strict()
        self.assertEqual(strict.bronze, 99.0)
        self.assertEqual(strict.silver, 99.5)
        self.assertEqual(strict.gold, 99.9)
        
        loose = ValidationThresholds.create_loose()
        self.assertEqual(loose.bronze, 80.0)
        self.assertEqual(loose.silver, 85.0)
        self.assertEqual(loose.gold, 90.0)


class TestParallelConfig(unittest.TestCase):
    """Test ParallelConfig class."""
    
    def test_parallel_config_creation(self):
        """Test creating parallel config."""
        config = ParallelConfig(enabled=True, max_workers=4, timeout_secs=300)
        self.assertTrue(config.enabled)
        self.assertEqual(config.max_workers, 4)
        self.assertEqual(config.timeout_secs, 300)
    
    def test_parallel_config_validation_success(self):
        """Test successful validation."""
        config = ParallelConfig(enabled=True, max_workers=4, timeout_secs=300)
        config.validate()  # Should not raise
    
    def test_parallel_config_validation_failure(self):
        """Test validation failure."""
        with self.assertRaises(PipelineValidationError):
            ParallelConfig(enabled=True, max_workers=0).validate()
        
        with self.assertRaises(PipelineValidationError):
            ParallelConfig(enabled=True, max_workers=33).validate()
        
        with self.assertRaises(PipelineValidationError):
            ParallelConfig(enabled=True, max_workers=4, timeout_secs=0).validate()
    
    def test_factory_methods(self):
        """Test factory methods."""
        default = ParallelConfig.create_default()
        self.assertTrue(default.enabled)
        self.assertEqual(default.max_workers, 4)
        
        sequential = ParallelConfig.create_sequential()
        self.assertFalse(sequential.enabled)
        self.assertEqual(sequential.max_workers, 1)
        
        high_perf = ParallelConfig.create_high_performance()
        self.assertTrue(high_perf.enabled)
        self.assertEqual(high_perf.max_workers, 16)


class TestPipelineConfig(unittest.TestCase):
    """Test PipelineConfig class."""
    
    def test_pipeline_config_creation(self):
        """Test creating pipeline config."""
        thresholds = ValidationThresholds.create_default()
        parallel = ParallelConfig.create_default()
        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            parallel=parallel,
            verbose=True
        )
        self.assertEqual(config.schema, "test_schema")
        self.assertEqual(config.thresholds, thresholds)
        self.assertEqual(config.parallel, parallel)
        self.assertTrue(config.verbose)
    
    def test_pipeline_config_validation_success(self):
        """Test successful validation."""
        config = PipelineConfig.create_default("test_schema")
        config.validate()  # Should not raise
    
    def test_pipeline_config_validation_failure(self):
        """Test validation failure."""
        with self.assertRaises(PipelineValidationError):
            PipelineConfig(
                schema="",
                thresholds=ValidationThresholds.create_default(),
                parallel=ParallelConfig.create_default()
            ).validate()
    
    def test_factory_methods(self):
        """Test factory methods."""
        default = PipelineConfig.create_default("test_schema")
        self.assertEqual(default.schema, "test_schema")
        self.assertTrue(default.verbose)
        
        high_perf = PipelineConfig.create_high_performance("test_schema")
        self.assertEqual(high_perf.schema, "test_schema")
        self.assertFalse(high_perf.verbose)
        self.assertTrue(high_perf.parallel.enabled)
        
        conservative = PipelineConfig.create_conservative("test_schema")
        self.assertEqual(conservative.schema, "test_schema")
        self.assertTrue(conservative.verbose)
        self.assertFalse(conservative.parallel.enabled)


class TestBronzeStep(unittest.TestCase):
    """Test BronzeStep class."""
    
    def test_bronze_step_creation(self):
        """Test creating bronze step."""
        step = BronzeStep(
            name="test_bronze",
            rules={"user_id": ["not_null"]},
            incremental_col="timestamp"
        )
        self.assertEqual(step.name, "test_bronze")
        self.assertEqual(step.rules, {"user_id": ["not_null"]})
        self.assertEqual(step.incremental_col, "timestamp")
    
    def test_bronze_step_validation_success(self):
        """Test successful validation."""
        step = BronzeStep(
            name="test_bronze",
            rules={"user_id": ["not_null"]}
        )
        step.validate()  # Should not raise
    
    def test_bronze_step_validation_failure(self):
        """Test validation failure."""
        with self.assertRaises(PipelineValidationError):
            BronzeStep(name="", rules={}).validate()
        
        with self.assertRaises(PipelineValidationError):
            BronzeStep(name="test", rules="invalid").validate()


class TestSilverStep(unittest.TestCase):
    """Test SilverStep class."""
    
    def test_silver_step_creation(self):
        """Test creating silver step."""
        def transform(df):
            return df
        
        step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform,
            rules={"user_id": ["not_null"]},
            table_name="test_silver",
            watermark_col="event_date"
        )
        self.assertEqual(step.name, "test_silver")
        self.assertEqual(step.source_bronze, "test_bronze")
        self.assertEqual(step.table_name, "test_silver")
        self.assertEqual(step.watermark_col, "event_date")
    
    def test_silver_step_validation_success(self):
        """Test successful validation."""
        def transform(df):
            return df
        
        step = SilverStep(
            name="test_silver",
            source_bronze="test_bronze",
            transform=transform,
            rules={"user_id": ["not_null"]},
            table_name="test_silver"
        )
        step.validate()  # Should not raise
    
    def test_silver_step_validation_failure(self):
        """Test validation failure."""
        with self.assertRaises(PipelineValidationError):
            SilverStep(
                name="",
                source_bronze="test_bronze",
                transform=lambda x: x,
                rules={},
                table_name="test_silver"
            ).validate()
        
        with self.assertRaises(PipelineValidationError):
            SilverStep(
                name="test",
                source_bronze="",
                transform=lambda x: x,
                rules={},
                table_name="test_silver"
            ).validate()


class TestGoldStep(unittest.TestCase):
    """Test GoldStep class."""
    
    def test_gold_step_creation(self):
        """Test creating gold step."""
        def transform(silvers):
            return silvers["test_silver"]
        
        step = GoldStep(
            name="test_gold",
            transform=transform,
            rules={"user_id": ["not_null"]},
            table_name="test_gold",
            source_silvers=["test_silver"]
        )
        self.assertEqual(step.name, "test_gold")
        self.assertEqual(step.table_name, "test_gold")
        self.assertEqual(step.source_silvers, ["test_silver"])
    
    def test_gold_step_validation_success(self):
        """Test successful validation."""
        def transform(silvers):
            return silvers["test_silver"]
        
        step = GoldStep(
            name="test_gold",
            transform=transform,
            rules={"user_id": ["not_null"]},
            table_name="test_gold",
            source_silvers=["test_silver"]
        )
        step.validate()  # Should not raise
    
    def test_gold_step_validation_failure(self):
        """Test validation failure."""
        with self.assertRaises(PipelineValidationError):
            GoldStep(
                name="",
                transform=lambda x: x,
                rules={},
                table_name="test_gold",
                source_silvers=[]
            ).validate()
        
        with self.assertRaises(PipelineValidationError):
            GoldStep(
                name="test",
                transform=lambda x: x,
                rules={},
                table_name="",
                source_silvers=[]
            ).validate()


class TestExecutionContext(unittest.TestCase):
    """Test ExecutionContext class."""
    
    def test_execution_context_creation(self):
        """Test creating execution context."""
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.utcnow()
        )
        self.assertEqual(context.mode, ExecutionMode.INITIAL)
        self.assertIsNotNone(context.start_time)
        self.assertIsNone(context.end_time)
        self.assertIsNone(context.duration_secs)
        self.assertIsNotNone(context.run_id)
    
    def test_execution_context_properties(self):
        """Test execution context properties."""
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.utcnow()
        )
        self.assertFalse(context.is_finished)
        self.assertTrue(context.is_running)
        
        context.finish()
        self.assertTrue(context.is_finished)
        self.assertFalse(context.is_running)
        self.assertIsNotNone(context.end_time)
        self.assertIsNotNone(context.duration_secs)


class TestStageStats(unittest.TestCase):
    """Test StageStats class."""
    
    def test_stage_stats_creation(self):
        """Test creating stage stats."""
        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
            validation_rate=95.0,
            duration_secs=10.0
        )
        self.assertEqual(stats.stage, "bronze")
        self.assertEqual(stats.step, "test_step")
        self.assertEqual(stats.total_rows, 100)
        self.assertEqual(stats.valid_rows, 95)
        self.assertEqual(stats.invalid_rows, 5)
        self.assertEqual(stats.validation_rate, 95.0)
        self.assertEqual(stats.duration_secs, 10.0)
    
    def test_stage_stats_validation_success(self):
        """Test successful validation."""
        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
            validation_rate=95.0,
            duration_secs=10.0
        )
        stats.validate()  # Should not raise
    
    def test_stage_stats_validation_failure(self):
        """Test validation failure."""
        with self.assertRaises(PipelineValidationError):
            StageStats(
                stage="bronze",
                step="test_step",
                total_rows=100,
                valid_rows=60,
                invalid_rows=50,
                validation_rate=95.0,
                duration_secs=10.0
            ).validate()
        
        with self.assertRaises(PipelineValidationError):
            StageStats(
                stage="bronze",
                step="test_step",
                total_rows=100,
                valid_rows=95,
                invalid_rows=5,
                validation_rate=101.0,
                duration_secs=10.0
            ).validate()
    
    def test_stage_stats_properties(self):
        """Test stage stats properties."""
        stats = StageStats(
            stage="bronze",
            step="test_step",
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
            validation_rate=95.0,
            duration_secs=10.0
        )
        self.assertTrue(stats.is_valid)
        self.assertEqual(stats.error_rate, 5.0)
    
    def test_factory_methods(self):
        """Test factory methods."""
        bronze_stats = StageStats.create_bronze_stats(
            step="test_bronze",
            total_rows=100,
            valid_rows=95,
            duration_secs=10.0
        )
        self.assertEqual(bronze_stats.stage, "bronze")
        self.assertEqual(bronze_stats.total_rows, 100)
        self.assertEqual(bronze_stats.valid_rows, 95)
        self.assertEqual(bronze_stats.invalid_rows, 5)
        
        silver_stats = StageStats.create_silver_stats(
            step="test_silver",
            total_rows=100,
            valid_rows=95,
            duration_secs=10.0
        )
        self.assertEqual(silver_stats.stage, "silver")
        
        gold_stats = StageStats.create_gold_stats(
            step="test_gold",
            total_rows=100,
            valid_rows=95,
            duration_secs=10.0
        )
        self.assertEqual(gold_stats.stage, "gold")


class TestStepResult(unittest.TestCase):
    """Test StepResult class."""
    
    def test_step_result_creation(self):
        """Test creating step result."""
        start_time = datetime.utcnow()
        end_time = datetime.utcnow()
        
        result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=start_time,
            end_time=end_time,
            duration_secs=10.0,
            rows_processed=100,
            rows_written=95,
            validation_rate=95.0
        )
        self.assertEqual(result.step_name, "test_step")
        self.assertEqual(result.phase, PipelinePhase.BRONZE)
        self.assertTrue(result.success)
        self.assertEqual(result.rows_processed, 100)
        self.assertEqual(result.rows_written, 95)
        self.assertEqual(result.validation_rate, 95.0)
    
    def test_step_result_properties(self):
        """Test step result properties."""
        start_time = datetime.utcnow()
        end_time = datetime.utcnow()
        
        result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=start_time,
            end_time=end_time,
            duration_secs=10.0,
            rows_processed=100,
            rows_written=95,
            validation_rate=95.0
        )
        self.assertTrue(result.is_valid)
    
    def test_factory_methods(self):
        """Test factory methods."""
        start_time = datetime.utcnow()
        end_time = datetime.utcnow()
        
        success_result = StepResult.create_success(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            start_time=start_time,
            end_time=end_time,
            rows_processed=100,
            rows_written=95,
            validation_rate=95.0
        )
        self.assertTrue(success_result.success)
        self.assertEqual(success_result.rows_processed, 100)
        
        failure_result = StepResult.create_failure(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            start_time=start_time,
            end_time=end_time,
            error_message="Test error"
        )
        self.assertFalse(failure_result.success)
        self.assertEqual(failure_result.error_message, "Test error")


class TestPipelineMetrics(unittest.TestCase):
    """Test PipelineMetrics class."""
    
    def test_pipeline_metrics_creation(self):
        """Test creating pipeline metrics."""
        metrics = PipelineMetrics(
            total_steps=10,
            successful_steps=9,
            failed_steps=1,
            total_duration_secs=100.0,
            total_rows_processed=1000,
            total_rows_written=950,
            avg_validation_rate=95.0
        )
        self.assertEqual(metrics.total_steps, 10)
        self.assertEqual(metrics.successful_steps, 9)
        self.assertEqual(metrics.failed_steps, 1)
        self.assertEqual(metrics.total_duration_secs, 100.0)
        self.assertEqual(metrics.total_rows_processed, 1000)
        self.assertEqual(metrics.total_rows_written, 950)
        self.assertEqual(metrics.avg_validation_rate, 95.0)
    
    def test_pipeline_metrics_properties(self):
        """Test pipeline metrics properties."""
        metrics = PipelineMetrics(
            total_steps=10,
            successful_steps=9,
            failed_steps=1,
            total_duration_secs=100.0,
            total_rows_processed=1000,
            total_rows_written=950,
            avg_validation_rate=95.0
        )
        self.assertEqual(metrics.success_rate, 90.0)
        self.assertEqual(metrics.failure_rate, 10.0)
    
    def test_from_step_results(self):
        """Test creating metrics from step results."""
        start_time = datetime.utcnow()
        end_time = datetime.utcnow()
        
        step_results = [
            StepResult.create_success(
                step_name="step1",
                phase=PipelinePhase.BRONZE,
                start_time=start_time,
                end_time=end_time,
                rows_processed=100,
                rows_written=95,
                validation_rate=95.0
            ),
            StepResult.create_success(
                step_name="step2",
                phase=PipelinePhase.SILVER,
                start_time=start_time,
                end_time=end_time,
                rows_processed=100,
                rows_written=95,
                validation_rate=95.0
            )
        ]
        
        metrics = PipelineMetrics.from_step_results(step_results)
        self.assertEqual(metrics.total_steps, 2)
        self.assertEqual(metrics.successful_steps, 2)
        self.assertEqual(metrics.failed_steps, 0)
        self.assertEqual(metrics.success_rate, 100.0)


class TestSilverDependencyInfo(unittest.TestCase):
    """Test SilverDependencyInfo class."""
    
    def test_silver_dependency_info_creation(self):
        """Test creating silver dependency info."""
        info = SilverDependencyInfo(
            step_name="test_silver",
            source_bronze="test_bronze",
            depends_on_silvers={"other_silver"},
            can_run_parallel=True,
            execution_group=0
        )
        self.assertEqual(info.step_name, "test_silver")
        self.assertEqual(info.source_bronze, "test_bronze")
        self.assertEqual(info.depends_on_silvers, {"other_silver"})
        self.assertTrue(info.can_run_parallel)
        self.assertEqual(info.execution_group, 0)
    
    def test_silver_dependency_info_validation_success(self):
        """Test successful validation."""
        info = SilverDependencyInfo(
            step_name="test_silver",
            source_bronze="test_bronze",
            depends_on_silvers=set(),
            can_run_parallel=True,
            execution_group=0
        )
        info.validate()  # Should not raise
    
    def test_silver_dependency_info_validation_failure(self):
        """Test validation failure."""
        with self.assertRaises(PipelineValidationError):
            SilverDependencyInfo(
                step_name="",
                source_bronze="test_bronze",
                depends_on_silvers=set(),
                can_run_parallel=True,
                execution_group=0
            ).validate()
        
        with self.assertRaises(PipelineValidationError):
            SilverDependencyInfo(
                step_name="test_silver",
                source_bronze="",
                depends_on_silvers=set(),
                can_run_parallel=True,
                execution_group=0
            ).validate()


class TestFactoryFunctions(unittest.TestCase):
    """Test factory functions."""
    
    def test_create_pipeline_config(self):
        """Test create_pipeline_config factory function."""
        config = create_pipeline_config(
            schema="test_schema",
            bronze_threshold=90.0,
            silver_threshold=95.0,
            gold_threshold=98.0,
            enable_parallel=True,
            max_workers=8,
            verbose=False
        )
        self.assertEqual(config.schema, "test_schema")
        self.assertEqual(config.thresholds.bronze, 90.0)
        self.assertEqual(config.thresholds.silver, 95.0)
        self.assertEqual(config.thresholds.gold, 98.0)
        self.assertTrue(config.parallel.enabled)
        self.assertEqual(config.parallel.max_workers, 8)
        self.assertFalse(config.verbose)
    
    def test_create_execution_context(self):
        """Test create_execution_context factory function."""
        context = create_execution_context(ExecutionMode.INITIAL)
        self.assertEqual(context.mode, ExecutionMode.INITIAL)
        self.assertIsNotNone(context.start_time)
        self.assertIsNotNone(context.run_id)


class TestValidationUtilities(unittest.TestCase):
    """Test validation utility functions."""
    
    def test_validate_pipeline_config_success(self):
        """Test successful pipeline config validation."""
        config = PipelineConfig.create_default("test_schema")
        validate_pipeline_config(config)  # Should not raise
    
    def test_validate_pipeline_config_failure(self):
        """Test pipeline config validation failure."""
        config = PipelineConfig(
            schema="",
            thresholds=ValidationThresholds.create_default(),
            parallel=ParallelConfig.create_default()
        )
        with self.assertRaises(PipelineConfigurationError):
            validate_pipeline_config(config)
    
    def test_validate_step_config_success(self):
        """Test successful step config validation."""
        step = BronzeStep(
            name="test_step",
            rules={"user_id": ["not_null"]}
        )
        validate_step_config(step)  # Should not raise
    
    def test_validate_step_config_failure(self):
        """Test step config validation failure."""
        step = BronzeStep(name="", rules={})
        with self.assertRaises(PipelineConfigurationError):
            validate_step_config(step)


class TestSerializationUtilities(unittest.TestCase):
    """Test serialization utility functions."""
    
    def test_serialize_pipeline_config(self):
        """Test pipeline config serialization."""
        config = PipelineConfig.create_default("test_schema")
        json_str = serialize_pipeline_config(config)
        self.assertIsInstance(json_str, str)
        self.assertIn("test_schema", json_str)
    
    def test_deserialize_pipeline_config(self):
        """Test pipeline config deserialization."""
        config = PipelineConfig.create_default("test_schema")
        json_str = serialize_pipeline_config(config)
        deserialized_config = deserialize_pipeline_config(json_str)
        self.assertEqual(deserialized_config.schema, config.schema)
        self.assertEqual(deserialized_config.thresholds.bronze, config.thresholds.bronze)
        self.assertEqual(deserialized_config.parallel.enabled, config.parallel.enabled)


def run_models_tests():
    """Run all models tests."""
    print("🧪 Running Models Tests")
    print("=" * 50)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_classes = [
        TestEnums,
        TestValidationThresholds,
        TestParallelConfig,
        TestPipelineConfig,
        TestBronzeStep,
        TestSilverStep,
        TestGoldStep,
        TestExecutionContext,
        TestStageStats,
        TestStepResult,
        TestPipelineMetrics,
        TestSilverDependencyInfo,
        TestFactoryFunctions,
        TestValidationUtilities,
        TestSerializationUtilities
    ]
    
    for test_class in test_classes:
        test_suite.addTest(unittest.makeSuite(test_class))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {result.testsRun - len(result.failures) - len(result.errors)} passed, {len(result.failures)} failed, {len(result.errors)} errors")
    
    if result.failures:
        print("\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n❌ Errors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_models_tests()
    if success:
        print("\n🎉 All models tests passed!")
    else:
        print("\n❌ Some models tests failed!")

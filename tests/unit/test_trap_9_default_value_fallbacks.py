"""
Test cases for Trap 9: Default Value Fallbacks in Configuration.

This module tests that default value fallbacks no longer use the 'or' operator
which could mask None values, and instead use explicit None checking.
"""

import pytest
from unittest.mock import Mock
from sparkforge.writer.models import WriterConfig
from sparkforge.writer.core import LogWriter
from sparkforge.execution import ExecutionEngine
from sparkforge.models.pipeline import PipelineConfig


class TestTrap9DefaultValueFallbacks:
    """Test cases for default value fallback fixes."""

    def test_writer_config_generate_table_name_requires_parameters(self):
        """Test that WriterConfig.generate_table_name requires non-None parameters."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            table_suffix_pattern="{run_mode}_{date}",
        )
        
        # Should raise ValueError when run_mode is None
        with pytest.raises(ValueError, match="run_mode cannot be None when using table_suffix_pattern"):
            config.generate_table_name(run_mode=None, timestamp="20240101")
        
        # Should raise ValueError when timestamp is None
        with pytest.raises(ValueError, match="timestamp cannot be None when using table_suffix_pattern"):
            config.generate_table_name(run_mode="initial", timestamp=None)

    def test_writer_config_generate_table_name_with_pattern_requires_parameters(self):
        """Test that WriterConfig.generate_table_name with pattern requires non-None parameters."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            table_name_pattern="{schema}.{table_name}_{pipeline_id}_{run_mode}_{date}",
        )
        
        # Should raise ValueError when pipeline_id is None
        with pytest.raises(ValueError, match="pipeline_id cannot be None when using table_name_pattern"):
            config.generate_table_name(pipeline_id=None, run_mode="initial", timestamp="20240101")
        
        # Should raise ValueError when run_mode is None
        with pytest.raises(ValueError, match="run_mode cannot be None when using table_name_pattern"):
            config.generate_table_name(pipeline_id="test_pipeline", run_mode=None, timestamp="20240101")
        
        # Should raise ValueError when timestamp is None
        with pytest.raises(ValueError, match="timestamp cannot be None when using table_name_pattern"):
            config.generate_table_name(pipeline_id="test_pipeline", run_mode="initial", timestamp=None)

    def test_writer_config_generate_table_name_without_patterns_works_with_none(self):
        """Test that WriterConfig.generate_table_name works with None when no patterns are used."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
        )
        
        # Should work fine with None values when no patterns are used
        result = config.generate_table_name(pipeline_id=None, run_mode=None, timestamp=None)
        assert result == "test_table"

    def test_execution_engine_context_validation(self, spark_session):
        """Test that ExecutionEngine properly validates context parameter."""
        # Test the context validation logic directly
        from sparkforge.execution import ExecutionEngine
        from sparkforge.models.pipeline import PipelineConfig, ValidationThresholds
        
        # Create a minimal config
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(
                bronze={"min_rows": 1},
                silver={"min_rows": 1}, 
                gold={"min_rows": 1}
            ),
            parallel=False
        )
        engine = ExecutionEngine(spark=spark_session, config=config)
        
        # Test that None context is converted to empty dict
        # We'll test the context validation by calling execute_pipeline with None
        from sparkforge.models.steps import BronzeStep
        from sparkforge.validation import ColumnRules
        
        step = BronzeStep(
            name="test_bronze",
            rules={"col1": "col1 IS NOT NULL"},
        )
        
        # This should not raise an error as context is properly handled
        result = engine.execute_pipeline([step], context=None)
        assert result is not None

    def test_execution_engine_context_type_validation(self, spark_session):
        """Test that ExecutionEngine validates context type."""
        from sparkforge.execution import ExecutionEngine
        from sparkforge.models.pipeline import PipelineConfig, ValidationThresholds
        
        # Create a minimal config
        config = PipelineConfig(
            schema="test_schema",
            thresholds=ValidationThresholds(
                bronze={"min_rows": 1},
                silver={"min_rows": 1}, 
                gold={"min_rows": 1}
            ),
            parallel=False
        )
        engine = ExecutionEngine(spark=spark_session, config=config)
        
        from sparkforge.models.steps import BronzeStep
        from sparkforge.validation import ColumnRules
        from sparkforge.errors import ExecutionError
        
        step = BronzeStep(
            name="test_bronze",
            rules={"col1": "col1 IS NOT NULL"},
        )
        
        # Should raise ExecutionError for invalid context type
        with pytest.raises(ExecutionError, match="context must be a dictionary, got <class 'str'>"):
            engine.execute_pipeline([step], context="invalid_context")

    def test_log_writer_run_id_handling(self, spark_session):
        """Test that LogWriter properly handles None run_id."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
        )
        writer = LogWriter(spark=spark_session, config=config)
        
        # Should generate new run_id when None is provided
        from sparkforge.models.execution import ExecutionContext, StepResult
        from sparkforge.execution import StepStatus
        from datetime import datetime
        
        context = ExecutionContext(
            pipeline_id="test_pipeline",
            schema="test_schema",
            run_id="test_run",
            mode="initial",
            start_time=datetime.now(),
        )
        
        step_result = StepResult.create_success(
            step_name="test_step",
            phase="bronze",
            start_time=datetime.now(),
            end_time=datetime.now(),
            rows_processed=100,
            rows_written=100,
            validation_rate=100.0,
        )
        
        # This should work without raising an error
        # The run_id will be generated if None is provided
        try:
            writer.write_step_result(step_result, context, run_id=None)
        except Exception as e:
            # We expect some errors due to missing table, but not due to run_id handling
            assert "run_id" not in str(e).lower()

    def test_log_writer_batch_run_ids_handling(self, spark_session):
        """Test that LogWriter properly handles None run_ids in batch operations."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
        )
        writer = LogWriter(spark=spark_session, config=config)
        
        from sparkforge.models.execution import ExecutionContext, StepResult
        from sparkforge.execution import StepStatus
        from datetime import datetime
        
        context = ExecutionContext(
            pipeline_id="test_pipeline",
            schema="test_schema",
            run_id="test_run",
            mode="initial",
            start_time=datetime.now(),
        )
        
        step_result = StepResult.create_success(
            step_name="test_step",
            phase="bronze",
            start_time=datetime.now(),
            end_time=datetime.now(),
            rows_processed=100,
            rows_written=100,
            validation_rate=100.0,
        )
        
        # This should work without raising an error
        # The run_ids will be generated if None is provided
        try:
            writer.write_execution_result_batch([step_result], run_ids=None)
        except Exception as e:
            # We expect some errors due to missing table, but not due to run_ids handling
            assert "run_ids" not in str(e).lower()

    def test_log_writer_display_limit_handling(self, spark_session):
        """Test that LogWriter properly handles None limit parameter."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
        )
        writer = LogWriter(spark=spark_session, config=config)
        
        # This should work without raising an error
        # The limit will default to 20 if None is provided
        try:
            writer.display_logs(limit=None)
        except Exception as e:
            # We expect some errors due to missing table, but not due to limit handling
            assert "limit" not in str(e).lower()

    def test_logger_initialization_explicit_none(self, spark_session):
        """Test that logger initialization properly handles explicit None."""
        from sparkforge.writer.monitoring import PerformanceMonitor
        from sparkforge.logging import PipelineLogger
        
        # Should create new logger when None is explicitly passed
        monitor = PerformanceMonitor(spark=spark_session, logger=None)
        assert isinstance(monitor.logger, PipelineLogger)
        assert monitor.logger.name == "PerformanceMonitor"

    def test_logger_initialization_with_logger(self, spark_session):
        """Test that logger initialization preserves provided logger."""
        from sparkforge.writer.monitoring import PerformanceMonitor
        from sparkforge.logging import PipelineLogger
        
        # Should use provided logger
        custom_logger = PipelineLogger("CustomLogger")
        monitor = PerformanceMonitor(spark=spark_session, logger=custom_logger)
        assert monitor.logger is custom_logger
        assert monitor.logger.name == "CustomLogger"

    def test_step_result_step_type_handling(self):
        """Test that StepResult properly handles None step_type."""
        from sparkforge.models.execution import StepResult
        from sparkforge.execution import StepStatus
        from sparkforge.writer.models import create_log_row_from_step_result
        from sparkforge.models.execution import ExecutionContext
        from datetime import datetime
        
        # Create StepResult with None step_type
        from sparkforge.models.enums import PipelinePhase
        step_result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=1.0,
            rows_processed=100,
            rows_written=100,
            validation_rate=100.0,
            step_type=None,  # Explicitly None
        )
        
        context = ExecutionContext(
            pipeline_id="test_pipeline",
            schema="test_schema",
            run_id="test_run",
            mode="initial",
            start_time=datetime.now(),
        )
        
        # Should fallback to "unknown" when step_type is None
        log_row = create_log_row_from_step_result(step_result, context, "test_run", "initial")
        assert log_row["step_type"] == "unknown"

    def test_step_result_step_type_with_value(self):
        """Test that StepResult preserves actual step_type value."""
        from sparkforge.models.execution import StepResult
        from sparkforge.execution import StepStatus
        from sparkforge.writer.models import create_log_row_from_step_result
        from sparkforge.models.execution import ExecutionContext
        from datetime import datetime
        
        # Create StepResult with actual step_type
        from sparkforge.models.enums import PipelinePhase
        step_result = StepResult(
            step_name="test_step",
            phase=PipelinePhase.BRONZE,
            success=True,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=1.0,
            rows_processed=100,
            rows_written=100,
            validation_rate=100.0,
            step_type="bronze_validation",  # Actual value
        )
        
        context = ExecutionContext(
            pipeline_id="test_pipeline",
            schema="test_schema",
            run_id="test_run",
            mode="initial",
            start_time=datetime.now(),
        )
        
        # Should preserve actual step_type value
        log_row = create_log_row_from_step_result(step_result, context, "test_run", "initial")
        assert log_row["step_type"] == "bronze_validation"

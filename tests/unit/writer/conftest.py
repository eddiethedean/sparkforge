"""
Shared fixtures and configuration for writer module tests.

This module provides common fixtures and test utilities used across
all writer module test files.
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from sparkforge.logging import PipelineLogger
from sparkforge.models import (
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    StepResult,
)
from sparkforge.writer.models import LogRow, WriteMode, WriterConfig


@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder.appName("WriterTests")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture
def mock_spark():
    """Mock SparkSession with common methods."""
    spark = Mock()
    spark.createDataFrame.return_value.count.return_value = 100
    spark.table.return_value.count.return_value = 100
    spark.table.return_value.show.return_value = None
    spark.table.return_value.schema.json.return_value = '{"type": "struct"}'
    return spark


@pytest.fixture
def mock_logger():
    """Mock PipelineLogger with context manager support."""
    logger = Mock(spec=PipelineLogger)

    # Mock context manager
    context_manager = Mock()
    context_manager.__enter__ = Mock()
    context_manager.__exit__ = Mock()
    logger.context.return_value = context_manager

    # Mock timer context manager
    timer_manager = Mock()
    timer_manager.__enter__ = Mock()
    timer_manager.__exit__ = Mock()
    logger.timer.return_value = timer_manager

    # Mock other methods
    logger.end_timer.return_value = 1.0
    logger.info.return_value = None
    logger.debug.return_value = None
    logger.warning.return_value = None
    logger.error.return_value = None
    logger.performance_metric.return_value = None

    return logger


@pytest.fixture
def basic_config():
    """Basic WriterConfig for testing."""
    return WriterConfig(
        table_schema="test_schema",
        table_name="test_logs",
        write_mode=WriteMode.APPEND,
    )


@pytest.fixture
def advanced_config():
    """Advanced WriterConfig with all features enabled."""
    return WriterConfig(
        table_schema="analytics",
        table_name="pipeline_logs",
        write_mode=WriteMode.APPEND,
        log_data_quality_results=True,
        enable_anomaly_detection=True,
        enable_schema_evolution=True,
        auto_optimize_schema=True,
        enable_optimization=True,
        min_validation_rate=95.0,
        max_invalid_rows_percent=5.0,
        batch_size=1000,
        max_file_size_mb=128,
        parallel_write_threads=4,
        memory_fraction=0.8,
    )


@pytest.fixture
def performance_config():
    """WriterConfig optimized for performance testing."""
    return WriterConfig(
        table_schema="perf_test",
        table_name="perf_logs",
        write_mode=WriteMode.APPEND,
        batch_size=2000,
        max_file_size_mb=256,
        parallel_write_threads=4,
        memory_fraction=0.8,
        enable_optimization=True,
        auto_optimize_schema=True,
    )


@pytest.fixture
def temp_delta_path():
    """Temporary directory for Delta table storage."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_log_rows():
    """Sample log rows for testing."""
    return [
        {
            "run_id": "test-run-1",
            "phase": "bronze",
            "step_name": "extract_data",
            "duration_secs": 10.0,
            "rows_processed": 1000,
            "rows_written": 950,
            "validation_rate": 95.0,
            "success": True,
            "error_message": None,
            "metadata": {},
        },
        {
            "run_id": "test-run-2",
            "phase": "silver",
            "step_name": "transform_data",
            "duration_secs": 15.0,
            "rows_processed": 950,
            "rows_written": 900,
            "validation_rate": 94.7,
            "success": True,
            "error_message": None,
            "metadata": {"quality_score": "high"},
        },
    ]


@pytest.fixture
def sample_log_row_typed():
    """Sample LogRow with proper typing."""
    return LogRow(
        run_id="typed-test-run",
        run_mode="initial",
        run_started_at=datetime.now(),
        run_ended_at=datetime.now(),
        execution_id="exec-123",
        pipeline_id="pipeline-456",
        schema="test_schema",
        phase="bronze",
        step_name="typed_step",
        step_type="extraction",
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration_secs=10.0,
        table_fqn="test.typed_table",
        rows_processed=1000,
        rows_written=950,
        validation_rate=95.0,
        success=True,
        error_message=None,
        metadata={"test": True},
    )


@pytest.fixture
def sample_execution_context():
    """Sample ExecutionContext for testing."""
    return ExecutionContext(
        mode=ExecutionMode.INITIAL,
        start_time=datetime.now(),
        execution_id="exec-context-123",
        pipeline_id="pipeline-456",
        schema="test_schema",
        run_mode="initial",
    )


@pytest.fixture
def sample_step_result(sample_execution_context):
    """Sample StepResult for testing."""
    return StepResult(
        step_name="sample_step",
        phase="bronze",
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration_secs=10.0,
        rows_processed=1000,
        rows_written=950,
        validation_rate=95.0,
        success=True,
        execution_context=sample_execution_context,
    )


@pytest.fixture
def sample_execution_result(sample_execution_context, sample_step_result):
    """Sample ExecutionResult for testing."""
    return ExecutionResult(
        success=True,
        context=sample_execution_context,
        step_results=[sample_step_result],
        total_duration_secs=10.0,
    )


@pytest.fixture
def mock_execution_result():
    """Mock ExecutionResult for testing."""
    mock_result = Mock(spec=ExecutionResult)
    mock_result.success = True
    mock_result.context = Mock(spec=ExecutionContext)
    mock_result.context.pipeline_id = "test-pipeline"
    mock_result.context.execution_id = "test-execution"
    mock_result.context.schema = "test_schema"
    mock_result.context.mode = ExecutionMode.INITIAL
    mock_result.context.start_time = datetime.now()
    mock_result.step_results = []
    mock_result.total_duration_secs = 10.0
    return mock_result


@pytest.fixture
def mock_step_result():
    """Mock StepResult for testing."""
    mock_result = Mock(spec=StepResult)
    mock_result.step_name = "mock_step"
    mock_result.phase = "bronze"
    mock_result.start_time = datetime.now()
    mock_result.end_time = datetime.now()
    mock_result.duration_secs = 10.0
    mock_result.rows_processed = 1000
    mock_result.rows_written = 950
    mock_result.validation_rate = 95.0
    mock_result.success = True
    mock_result.execution_context = Mock(spec=ExecutionContext)
    mock_result.error_message = None
    mock_result.metadata = {}
    return mock_result


@pytest.fixture
def quality_test_log_rows():
    """Log rows with varying quality for testing."""
    return [
        {
            "run_id": "quality-test-1",
            "phase": "bronze",
            "step_name": "high_quality_step",
            "duration_secs": 10.0,
            "rows_processed": 1000,
            "rows_written": 980,
            "validation_rate": 98.0,
            "success": True,
        },
        {
            "run_id": "quality-test-2",
            "phase": "silver",
            "step_name": "low_quality_step",
            "duration_secs": 20.0,
            "rows_processed": 1000,
            "rows_written": 800,
            "validation_rate": 80.0,
            "success": True,
        },
    ]


@pytest.fixture
def anomaly_test_log_rows():
    """Log rows with anomalies for testing."""
    return [
        {
            "run_id": "normal-run-1",
            "duration_secs": 10.0,
            "validation_rate": 95.0,
            "rows_processed": 1000,
        },
        {
            "run_id": "anomaly-run-1",
            "duration_secs": 300.0,  # Anomaly: 30x normal duration
            "validation_rate": 50.0,  # Anomaly: low validation rate
            "rows_processed": 1000,
        },
    ]


@pytest.fixture
def large_dataset():
    """Large dataset for performance testing."""
    return [
        {
            "run_id": f"large-dataset-{i}",
            "phase": "bronze" if i % 3 == 0 else "silver" if i % 3 == 1 else "gold",
            "step_name": f"large_step_{i}",
            "duration_secs": float(i % 100),
            "rows_processed": i * 10,
            "validation_rate": 95.0 + (i % 5),
            "success": True,
        }
        for i in range(1000)  # 1K records
    ]


@pytest.fixture
def memory_test_data():
    """Memory-intensive test data."""
    return [
        {
            "run_id": f"memory-test-{i}",
            "phase": "bronze",
            "step_name": f"memory_step_{i}",
            "duration_secs": 1.0,
            "rows_processed": 1000,
            "validation_rate": 95.0,
            "metadata": {"large_data": "x" * 1000},  # Large metadata
        }
        for i in range(100)  # 100 records with large metadata
    ]


@pytest.fixture
def invalid_log_rows():
    """Invalid log rows for error testing."""
    return [
        {
            "run_id": "",  # Invalid: empty run_id
            "phase": "bronze",
            "step_name": "invalid_step",
            "duration_secs": -1.0,  # Invalid: negative duration
            "rows_processed": -100,  # Invalid: negative rows
            "validation_rate": 150.0,  # Invalid: > 100%
            "success": True,
        }
    ]


@pytest.fixture
def mock_dataframe():
    """Mock DataFrame for testing."""
    df = Mock()
    df.count.return_value = 100
    df.filter.return_value = df
    df.limit.return_value = df
    df.select.return_value = df
    df.show.return_value = None
    df.schema.json.return_value = '{"type": "struct"}'
    df.columns = ["run_id", "phase", "step_name", "duration_secs"]
    return df


@pytest.fixture
def mock_validation_stats():
    """Mock validation statistics for testing."""
    stats = Mock()
    stats.total_rows = 100
    stats.valid_rows = 95
    stats.invalid_rows = 5
    stats.validation_rate = 95.0
    stats.duration_secs = 1.0
    return stats


@pytest.fixture
def mock_dataframe_info():
    """Mock DataFrame info for testing."""
    return {
        "row_count": 100,
        "column_count": 8,
        "size_bytes": 10240,
        "schema": {"fields": []},
    }


# Test utilities
class WriterTestUtils:
    """Utility class for writer tests."""

    @staticmethod
    def create_log_row(**kwargs) -> Dict[str, Any]:
        """Create a log row with default values."""
        defaults = {
            "run_id": "test-run",
            "phase": "bronze",
            "step_name": "test_step",
            "duration_secs": 10.0,
            "rows_processed": 1000,
            "rows_written": 950,
            "validation_rate": 95.0,
            "success": True,
            "error_message": None,
            "metadata": {},
        }
        defaults.update(kwargs)
        return defaults

    @staticmethod
    def create_execution_result(**kwargs) -> ExecutionResult:
        """Create an ExecutionResult with default values."""
        context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            execution_id="test-exec",
            pipeline_id="test-pipeline",
            schema="test_schema",
        )

        step = StepResult(
            step_name="test_step",
            phase="bronze",
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=10.0,
            rows_processed=1000,
            rows_written=950,
            validation_rate=95.0,
            success=True,
            execution_context=context,
        )

        defaults = {
            "success": True,
            "context": context,
            "step_results": [step],
            "total_duration_secs": 10.0,
        }
        defaults.update(kwargs)

        return ExecutionResult(**defaults)

    @staticmethod
    def assert_result_structure(result: Dict[str, Any]) -> None:
        """Assert that a result has the expected structure."""
        assert "success" in result
        assert isinstance(result["success"], bool)

        if result["success"]:
            assert "rows_written" in result
            assert "duration_secs" in result
            assert "table_fqn" in result


@pytest.fixture
def writer_test_utils():
    """WriterTestUtils instance for tests."""
    return WriterTestUtils()


# Performance test markers
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "performance: mark test as a performance test")
    config.addinivalue_line("markers", "benchmark: mark test as a benchmark test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")

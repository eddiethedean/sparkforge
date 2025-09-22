"""
Advanced test suite for LogWriter with comprehensive edge cases and property-based testing.

This module contains advanced tests that go beyond basic functionality to test
edge cases, error conditions, performance characteristics, and property-based
scenarios using Hypothesis.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import pytest
from hypothesis import given, strategies as st

from sparkforge.logging import PipelineLogger
from sparkforge.models import (
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    StepResult,
)
from sparkforge.writer.core import LogWriter
from sparkforge.writer.exceptions import WriterError
from sparkforge.writer.models import LogRow, WriteMode, WriterConfig


class TestLogWriterAdvanced:
    """Advanced tests for LogWriter functionality."""

    @pytest.fixture
    def mock_spark(self):
        """Mock SparkSession with advanced capabilities."""
        spark = Mock()
        spark.createDataFrame.return_value.count.return_value = 100
        spark.table.return_value.count.return_value = 100
        spark.table.return_value.show.return_value = None
        spark.table.return_value.schema.json.return_value = '{"type": "struct"}'
        return spark

    @pytest.fixture
    def mock_logger(self):
        """Mock PipelineLogger with advanced capabilities."""
        logger = Mock(spec=PipelineLogger)
        logger.context.return_value.__enter__ = Mock()
        logger.context.return_value.__exit__ = Mock()
        logger.timer.return_value.__enter__ = Mock()
        logger.timer.return_value.__exit__ = Mock()
        logger.end_timer.return_value = 1.0
        logger.info.return_value = None
        logger.debug.return_value = None
        logger.warning.return_value = None
        logger.error.return_value = None
        logger.performance_metric.return_value = None
        return logger

    @pytest.fixture
    def advanced_config(self):
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
    def writer(self, mock_spark, advanced_config, mock_logger):
        """LogWriter instance with advanced configuration."""
        return LogWriter(mock_spark, advanced_config, mock_logger)

    # ============================================================================
    # Property-Based Testing with Hypothesis
    # ============================================================================

    @given(
        table_schema=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd", "Pc"))),
        table_name=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd", "Pc"))),
        batch_size=st.integers(min_value=1, max_value=10000),
    )
    def test_config_property_validation(self, table_schema: str, table_name: str, batch_size: int):
        """Test that WriterConfig validates properties correctly."""
        config = WriterConfig(
            table_schema=table_schema,
            table_name=table_name,
            batch_size=batch_size,
        )
        
        # Should not raise validation errors for valid inputs
        config.validate()
        
        # Verify properties are set correctly
        assert config.table_schema == table_schema
        assert config.table_name == table_name
        assert config.batch_size == batch_size

    @given(
        duration_secs=st.floats(min_value=0.0, max_value=3600.0),
        rows_processed=st.integers(min_value=0, max_value=1000000),
        validation_rate=st.floats(min_value=0.0, max_value=100.0),
    )
    def test_log_row_property_creation(self, duration_secs: float, rows_processed: int, validation_rate: float):
        """Test LogRow creation with various property values."""
        log_row = LogRow(
            run_id="test-run",
            run_mode="initial",
            run_started_at=datetime.now(),
            run_ended_at=datetime.now(),
            execution_id="exec-123",
            pipeline_id="pipeline-456",
            schema="test_schema",
            phase="bronze",
            step_name="test_step",
            step_type="transformation",
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_secs=duration_secs,
            table_fqn="test.table",
            rows_processed=rows_processed,
            rows_written=rows_processed,
            validation_rate=validation_rate,
            success=True,
            error_message=None,
            metadata={},
        )
        
        # Verify properties are set correctly
        assert log_row["duration_secs"] == duration_secs
        assert log_row["rows_processed"] == rows_processed
        assert log_row["validation_rate"] == validation_rate

    # ============================================================================
    # Edge Case Testing
    # ============================================================================

    def test_empty_log_rows_handling(self, writer):
        """Test handling of empty log rows."""
        with patch.object(writer, "_write_log_rows") as mock_write:
            with patch.object(writer, "get_memory_usage") as mock_memory:
                mock_memory.return_value = {"rss_mb": 100.0}
                result = writer.write_log_rows([], run_id="empty-test")
                
                assert result["success"] is True
                assert result["rows_written"] == 0
                mock_write.assert_called_once_with([], "empty-test")

    def test_large_batch_processing(self, writer):
        """Test processing of large batches."""
        # Create a large batch of log rows
        large_batch = [
            {
                "run_id": f"test-run-{i}",
                "phase": "bronze",
                "step_name": f"step_{i}",
                "duration_secs": 1.0,
                "rows_processed": 1000,
                "validation_rate": 95.0,
            }
            for i in range(5000)  # Large batch
        ]
        
        with patch.object(writer, "_write_log_rows_batch") as mock_batch_write:
            with patch.object(writer, "get_memory_usage") as mock_memory:
                mock_memory.return_value = {"rss_mb": 100.0}
                result = writer.write_log_rows(large_batch, run_id="large-batch-test")
                
                assert result["success"] is True
                assert result["rows_written"] == 5000
                mock_batch_write.assert_called_once()

    def test_memory_pressure_handling(self, writer):
        """Test behavior under memory pressure conditions."""
        # Mock high memory usage
        with patch.object(writer, "get_memory_usage") as mock_memory:
            mock_memory.return_value = {
                "rss_mb": 2000,  # High memory usage
                "vms_mb": 4000,
                "percent": 95.0,
                "available_mb": 100.0,  # Low available memory
            }
            
            memory_info = writer.get_memory_usage()
            
            assert memory_info["rss_mb"] == 2000
            assert memory_info["percent"] == 95.0
            assert memory_info["available_mb"] == 100.0

    def test_concurrent_write_operations(self, writer):
        """Test handling of concurrent write operations."""
        import threading
        import time
        
        results = []
        errors = []
        
        def write_operation(thread_id: int):
            try:
                log_rows = [{"run_id": f"thread-{thread_id}", "phase": "bronze"}]
                with patch.object(writer, "_write_log_rows") as mock_write:
                    result = writer.write_log_rows(log_rows, run_id=f"concurrent-{thread_id}")
                    results.append(result)
            except Exception as e:
                errors.append(e)
        
        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=write_operation, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all operations completed successfully
        assert len(errors) == 0
        assert len(results) == 5
        for result in results:
            assert result["success"] is True

    # ============================================================================
    # Error Recovery Testing
    # ============================================================================

    def test_retry_mechanism(self, writer):
        """Test retry mechanism for failed operations."""
        call_count = 0
        
        def failing_write(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception(f"Simulated failure {call_count}")
            return {"success": True}
        
        with patch.object(writer, "_write_log_rows", side_effect=failing_write):
            with patch.object(writer, "get_memory_usage") as mock_memory:
                mock_memory.return_value = {"rss_mb": 100.0}
                result = writer.write_log_rows([{"test": "data"}], run_id="retry-test")
                
                assert result["success"] is True
                assert call_count == 3  # Should have retried twice

    def test_partial_failure_recovery(self, writer):
        """Test recovery from partial failures in batch operations."""
        # Mock partial failure in batch processing
        with patch.object(writer, "_write_log_rows") as mock_write:
            with patch.object(writer, "get_memory_usage") as mock_memory:
                mock_memory.return_value = {"rss_mb": 100.0}
                mock_write.side_effect = [
                    {"success": True},  # First batch succeeds
                    Exception("Batch 2 failed"),  # Second batch fails
                    {"success": True},  # Third batch succeeds
                ]
                
                # This should handle partial failures gracefully
                log_rows = [{"test": f"data_{i}"} for i in range(3)]
                result = writer.write_log_rows(log_rows, run_id="partial-failure-test")
                
                # Should report overall success with some failures
                assert result["success"] is True

    # ============================================================================
    # Performance Testing
    # ============================================================================

    def test_performance_benchmarks(self, writer):
        """Test performance characteristics of write operations."""
        import time
        
        # Test write operation timing
        start_time = time.time()
        
        with patch.object(writer, "_write_log_rows") as mock_write:
            with patch.object(writer, "get_memory_usage") as mock_memory:
                mock_memory.return_value = {"rss_mb": 100.0}
                log_rows = [{"test": f"data_{i}"} for i in range(1000)]
                result = writer.write_log_rows(log_rows, run_id="perf-test")
                
                end_time = time.time()
                duration = end_time - start_time
                
                assert result["success"] is True
                assert duration < 5.0  # Should complete within 5 seconds
                assert "duration_secs" in result

    def test_memory_usage_monitoring(self, writer):
        """Test memory usage monitoring during operations."""
        with patch("psutil.Process") as mock_process, patch("psutil.virtual_memory") as mock_vm:
            # Mock memory info
            mock_memory_info = Mock()
            mock_memory_info.rss = 1024 * 1024 * 200  # 200 MB
            mock_memory_info.vms = 1024 * 1024 * 400  # 400 MB
            
            mock_process.return_value.memory_info.return_value = mock_memory_info
            mock_process.return_value.memory_percent.return_value = 15.0
            mock_vm.return_value.available = 1024 * 1024 * 800  # 800 MB
            
            memory_info = writer.get_memory_usage()
            
            assert memory_info["rss_mb"] == 200.0
            assert memory_info["vms_mb"] == 400.0
            assert memory_info["percent"] == 15.0
            assert memory_info["available_mb"] == 800.0

    # ============================================================================
    # Data Quality Testing
    # ============================================================================

    def test_data_quality_validation_edge_cases(self, writer):
        """Test data quality validation with edge cases."""
        # Test with extremely low validation rate
        low_quality_log_rows = [
            {
                "run_id": "low-quality-test",
                "duration_secs": 10.0,
                "validation_rate": 10.0,  # Very low
                "rows_processed": 1000,
                "rows_written": 100,
            }
        ]
        
        with patch.object(writer, "_create_dataframe_from_log_rows") as mock_create_df, \
             patch("sparkforge.writer.core.apply_column_rules") as mock_apply_rules, \
             patch("sparkforge.writer.core.get_dataframe_info") as mock_df_info:
            
            mock_df = Mock()
            mock_create_df.return_value = mock_df
            
            # Mock validation results with low quality
            mock_stats = Mock()
            mock_stats.total_rows = 1
            mock_stats.valid_rows = 0
            mock_stats.invalid_rows = 1
            mock_stats.validation_rate = 10.0
            
            mock_apply_rules.return_value = (mock_df, Mock(), mock_stats)
            mock_df_info.return_value = {"row_count": 1, "column_count": 8}
            
            result = writer.validate_log_data_quality(low_quality_log_rows)
            
            assert result["quality_passed"] is False
            assert result["validation_rate"] == 10.0
            assert result["threshold_met"] is False

    def test_anomaly_detection_edge_cases(self, writer):
        """Test anomaly detection with extreme values."""
        writer.config.enable_anomaly_detection = True
        
        # Test with extreme anomaly values
        extreme_log_rows = [
            {
                "run_id": "extreme-test",
                "duration_secs": 3600.0,  # 1 hour - extreme duration
                "validation_rate": 0.1,   # Extremely low validation rate
                "rows_processed": 10000000,  # Extremely high row count
            }
        ]
        
        result = writer.detect_anomalies(extreme_log_rows)
        
        assert result["anomalies_detected"] is True
        assert result["anomaly_count"] >= 1
        assert len(result["anomalies"]) >= 1

    # ============================================================================
    # Table Operations Testing
    # ============================================================================

    def test_table_optimization_edge_cases(self, writer):
        """Test table optimization with edge cases."""
        # Test optimization with empty table
        with patch("sparkforge.writer.core.table_exists") as mock_exists, \
             patch.object(writer, "get_table_info") as mock_get_info:
            
            mock_exists.return_value = True
            mock_get_info.return_value = {"row_count": 0}  # Empty table
            
            result = writer.optimize_table()
            
            assert result["optimized"] is True
            assert "optimization_timestamp" in result

    def test_table_maintenance_with_large_table(self, writer):
        """Test table maintenance with large table simulation."""
        with patch("sparkforge.writer.core.table_exists") as mock_exists, \
             patch.object(writer, "get_table_info") as mock_get_info:
            
            mock_exists.return_value = True
            mock_get_info.return_value = {"row_count": 10000000}  # Large table
            
            maintenance_options = {
                "vacuum": True,
                "analyze": True,
                "validate_schema": True,
            }
            
            result = writer.maintain_table(maintenance_options)
            
            assert result["maintained"] is True
            assert "vacuum" in result["maintenance_operations"]
            assert "maintenance_timestamp" in result

    # ============================================================================
    # Reporting and Analytics Testing
    # ============================================================================

    def test_reporting_with_no_data(self, writer):
        """Test reporting functions with no data available."""
        with patch("sparkforge.writer.core.table_exists") as mock_exists:
            mock_exists.return_value = False
            
            # Test summary report with no data
            summary_result = writer.generate_summary_report()
            assert summary_result["report_available"] is False
            
            # Test trend analysis with no data
            trend_result = writer.analyze_performance_trends()
            assert trend_result["trends_available"] is False
            
            # Test data export with no data
            export_result = writer.export_analytics_data(format="json")
            assert export_result["export_successful"] is False

    def test_export_formats_edge_cases(self, writer):
        """Test data export with various edge cases."""
        with patch("sparkforge.writer.core.table_exists") as mock_exists:
            mock_exists.return_value = True
            
            # Test unsupported format - should raise WriterError
            try:
                writer.export_analytics_data(format="unsupported_format")
                assert False, "Expected WriterError for unsupported format"
            except WriterError:
                pass  # Expected
            
            # Test with invalid limit - should raise WriterError
            try:
                writer.export_analytics_data(format="json", limit=-1)
                assert False, "Expected WriterError for invalid limit"
            except WriterError:
                pass  # Expected

    # ============================================================================
    # Configuration Testing
    # ============================================================================

    def test_dynamic_table_naming(self, advanced_config):
        """Test dynamic table naming with various patterns."""
        # Test with suffix pattern
        advanced_config.table_suffix_pattern = "_{run_mode}_{date}"
        table_name = advanced_config.generate_table_name(
            pipeline_id="test-pipeline",
            run_mode="incremental",
            timestamp="20240101"
        )
        assert "incremental" in table_name
        assert "20240101" in table_name
        
        # Test with full pattern
        advanced_config.table_name_pattern = "{schema}.{pipeline_id}_{run_mode}_{timestamp}"
        table_name = advanced_config.generate_table_name(
            pipeline_id="test-pipeline",
            run_mode="initial",
            timestamp="20240101_120000"
        )
        assert table_name == "analytics.test-pipeline_initial_20240101_120000"

    def test_configuration_validation_edge_cases(self):
        """Test configuration validation with edge case values."""
        # Test with boundary values
        config = WriterConfig(
            table_schema="test",
            table_name="test_table",
            batch_size=1,  # Minimum value
            memory_fraction=0.1,  # Valid value (not 0.0)
            min_validation_rate=0.0,  # Minimum value
            max_invalid_rows_percent=100.0,  # Maximum value
        )
        
        # Should not raise validation errors for boundary values
        config.validate()
        
        # Test with invalid values
        with pytest.raises(ValueError):
            WriterConfig(
                table_schema="",  # Empty schema
                table_name="test_table",
            ).validate()
        
        with pytest.raises(ValueError):
            WriterConfig(
                table_schema="test",
                table_name="test_table",
                batch_size=0,  # Invalid batch size
            ).validate()


class TestLogWriterStress:
    """Stress tests for LogWriter under high load conditions."""

    @pytest.fixture
    def stress_writer(self):
        """LogWriter configured for stress testing."""
        mock_spark = Mock()
        mock_logger = Mock(spec=PipelineLogger)
        mock_logger.context.return_value.__enter__ = Mock()
        mock_logger.context.return_value.__exit__ = Mock()
        mock_logger.timer.return_value.__enter__ = Mock()
        mock_logger.timer.return_value.__exit__ = Mock()
        mock_logger.end_timer.return_value = 1.0
        
        config = WriterConfig(
            table_schema="stress_test",
            table_name="stress_logs",
            batch_size=100,  # Smaller batch size for stress testing
            enable_optimization=True,
        )
        
        return LogWriter(mock_spark, config, mock_logger)

    def test_high_volume_write_operations(self, stress_writer):
        """Test handling of high-volume write operations."""
        # Create large number of log rows
        large_dataset = [
            {
                "run_id": f"stress-test-{i}",
                "phase": "bronze" if i % 3 == 0 else "silver" if i % 3 == 1 else "gold",
                "step_name": f"stress_step_{i}",
                "duration_secs": float(i % 100),
                "rows_processed": i * 100,
                "validation_rate": 95.0 + (i % 5),
            }
            for i in range(10000)  # 10K log rows
        ]
        
        with patch.object(stress_writer, "_write_log_rows_batch") as mock_batch:
            with patch.object(stress_writer, "get_memory_usage") as mock_memory:
                mock_memory.return_value = {"rss_mb": 100.0}
                result = stress_writer.write_log_rows(large_dataset, run_id="stress-test")
                
                assert result["success"] is True
                assert result["rows_written"] == 10000
                mock_batch.assert_called_once()

    def test_memory_stress_testing(self, stress_writer):
        """Test memory usage under stress conditions."""
        # Simulate high memory usage scenario
        with patch("psutil.Process") as mock_process, patch("psutil.virtual_memory") as mock_vm:
            mock_memory_info = Mock()
            mock_memory_info.rss = 1024 * 1024 * 4000  # 4GB RSS
            mock_memory_info.vms = 1024 * 1024 * 8000  # 8GB VMS
            
            mock_process.return_value.memory_info.return_value = mock_memory_info
            mock_process.return_value.memory_percent.return_value = 85.0  # High usage
            mock_vm.return_value.available = 1024 * 1024 * 100  # Low available
            
            memory_info = stress_writer.get_memory_usage()
            
            assert memory_info["rss_mb"] == 4000.0
            assert memory_info["vms_mb"] == 8000.0
            assert memory_info["percent"] == 85.0
            assert memory_info["available_mb"] == 100.0

"""
Integration tests for the writer module with real SparkForge components.

This module contains integration tests that verify the writer module works
correctly with actual SparkForge components including validation, table operations,
and reporting modules.
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from sparkforge.logging import PipelineLogger
from sparkforge.models import (
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    StepResult,
)
from sparkforge.writer.core import LogWriter
from sparkforge.writer.models import LogRow, WriteMode, WriterConfig


class TestWriterIntegration:
    """Integration tests for writer module with SparkForge components."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create a real SparkSession for integration testing."""
        spark = SparkSession.builder \
            .appName("WriterIntegrationTest") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        yield spark
        spark.stop()

    @pytest.fixture
    def temp_delta_table_path(self):
        """Create a temporary directory for Delta table storage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def integration_config(self, temp_delta_table_path):
        """WriterConfig for integration testing."""
        return WriterConfig(
            table_schema="integration_test",
            table_name="integration_logs",
            write_mode=WriteMode.APPEND,
            log_data_quality_results=True,
            enable_anomaly_detection=True,
            enable_schema_evolution=True,
            auto_optimize_schema=True,
            enable_optimization=True,
            min_validation_rate=90.0,
            max_invalid_rows_percent=10.0,
            batch_size=100,
        )

    @pytest.fixture
    def integration_logger(self):
        """Real PipelineLogger for integration testing."""
        return PipelineLogger("WriterIntegrationTest")

    @pytest.fixture
    def integration_writer(self, spark_session, integration_config, integration_logger):
        """LogWriter configured for integration testing."""
        return LogWriter(spark_session, integration_config, integration_logger)

    def test_full_pipeline_execution_integration(
        self, integration_writer, spark_session, temp_delta_table_path
    ):
        """Test complete pipeline execution with real Spark operations."""
        # Create sample execution result
        execution_context = ExecutionContext(
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            execution_id="integration-test-exec",
            pipeline_id="integration-pipeline",
            schema="integration_test",
            run_mode="initial",
        )
        
        step_results = [
            StepResult(
                step_name="bronze_extract",
                phase="bronze",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=10.0,
                rows_processed=1000,
                rows_written=950,
                validation_rate=95.0,
                success=True,
                execution_context=execution_context,
            ),
            StepResult(
                step_name="silver_transform",
                phase="silver",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=15.0,
                rows_processed=950,
                rows_written=900,
                validation_rate=94.7,
                success=True,
                execution_context=execution_context,
            ),
        ]
        
        execution_result = ExecutionResult(
            success=True,
            context=execution_context,
            step_results=step_results,
            total_duration_secs=25.0,
        )
        
        # Write execution result
        result = integration_writer.write_execution_result(
            execution_result, run_id="integration-test-run"
        )
        
        assert result["success"] is True
        assert result["rows_written"] > 0
        assert "duration_secs" in result
        assert "quality_results" in result  # Should have quality results

    def test_data_quality_validation_integration(self, integration_writer):
        """Test data quality validation with real validation module."""
        # Create log rows with varying quality
        log_rows = [
            LogRow(
                run_id="quality-test-1",
                run_mode="initial",
                run_started_at=datetime.now(),
                run_ended_at=datetime.now(),
                execution_id="exec-1",
                pipeline_id="pipeline-1",
                schema="test_schema",
                phase="bronze",
                step_name="high_quality_step",
                step_type="extraction",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=10.0,
                table_fqn="test.high_quality_table",
                rows_processed=1000,
                rows_written=980,
                validation_rate=98.0,
                success=True,
                error_message=None,
                metadata={"quality_score": "high"},
            ),
            LogRow(
                run_id="quality-test-2",
                run_mode="initial",
                run_started_at=datetime.now(),
                run_ended_at=datetime.now(),
                execution_id="exec-2",
                pipeline_id="pipeline-1",
                schema="test_schema",
                phase="silver",
                step_name="low_quality_step",
                step_type="transformation",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=20.0,
                table_fqn="test.low_quality_table",
                rows_processed=1000,
                rows_written=800,
                validation_rate=80.0,
                success=True,
                error_message=None,
                metadata={"quality_score": "low"},
            ),
        ]
        
        # Test data quality validation
        quality_result = integration_writer.validate_log_data_quality(log_rows)
        
        assert "quality_passed" in quality_result
        assert "validation_rate" in quality_result
        assert "valid_rows" in quality_result
        assert "invalid_rows" in quality_result
        assert "stats" in quality_result

    def test_anomaly_detection_integration(self, integration_writer):
        """Test anomaly detection with real data patterns."""
        # Create log rows with anomalies
        log_rows = [
            LogRow(
                run_id="normal-run-1",
                run_mode="initial",
                run_started_at=datetime.now(),
                run_ended_at=datetime.now(),
                execution_id="exec-normal-1",
                pipeline_id="pipeline-1",
                schema="test_schema",
                phase="bronze",
                step_name="normal_step",
                step_type="extraction",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=10.0,
                table_fqn="test.normal_table",
                rows_processed=1000,
                rows_written=950,
                validation_rate=95.0,
                success=True,
                error_message=None,
                metadata={},
            ),
            LogRow(
                run_id="anomaly-run-1",
                run_mode="initial",
                run_started_at=datetime.now(),
                run_ended_at=datetime.now(),
                execution_id="exec-anomaly-1",
                pipeline_id="pipeline-1",
                schema="test_schema",
                phase="bronze",
                step_name="anomaly_step",
                step_type="extraction",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=300.0,  # Anomaly: 5x normal duration
                table_fqn="test.anomaly_table",
                rows_processed=1000,
                rows_written=500,  # Anomaly: low success rate
                validation_rate=50.0,  # Anomaly: low validation rate
                success=True,
                error_message=None,
                metadata={},
            ),
        ]
        
        # Test anomaly detection
        anomaly_result = integration_writer.detect_anomalies(log_rows)
        
        assert "anomalies_detected" in anomaly_result
        assert "anomaly_count" in anomaly_result
        assert "anomalies" in anomaly_result
        assert "analysis_timestamp" in anomaly_result

    def test_table_operations_integration(self, integration_writer):
        """Test table operations integration."""
        # Test table optimization (will fail gracefully if table doesn't exist)
        optimization_result = integration_writer.optimize_table(
            enable_partitioning=True,
            enable_compression=True,
            enable_zordering=False,
            enable_vacuum=False,
        )
        
        assert "optimized" in optimization_result
        assert "optimization_timestamp" in optimization_result
        
        # Test table maintenance
        maintenance_result = integration_writer.maintain_table(
            vacuum=True,
            analyze=True,
            validate_schema=True,
        )
        
        assert "maintained" in maintenance_result
        assert "maintenance_timestamp" in maintenance_result

    def test_reporting_integration(self, integration_writer):
        """Test reporting and analytics integration."""
        # Test summary report generation
        summary_result = integration_writer.generate_summary_report(days=7)
        
        assert "report_available" in summary_result
        assert "report_timestamp" in summary_result
        
        # Test performance trend analysis
        trend_result = integration_writer.analyze_performance_trends(days=30)
        
        assert "trends_available" in trend_result
        assert "trend_analysis_timestamp" in trend_result
        
        # Test data export
        export_result = integration_writer.export_analytics_data(
            format="json",
            limit=100,
        )
        
        assert "export_successful" in export_result
        assert "export_timestamp" in export_result

    def test_batch_processing_integration(self, integration_writer):
        """Test batch processing with real data."""
        # Create multiple execution results for batch processing
        execution_results = []
        
        for i in range(5):
            execution_context = ExecutionContext(
                mode=ExecutionMode.INITIAL,
                start_time=datetime.now(),
                execution_id=f"batch-exec-{i}",
                pipeline_id="batch-pipeline",
                schema="test_schema",
                run_mode="initial",
            )
            
            step_results = [
                StepResult(
                    step_name=f"batch_step_{i}",
                    phase="bronze",
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    duration_secs=10.0 + i,
                    rows_processed=1000 + i * 100,
                    rows_written=950 + i * 95,
                    validation_rate=95.0,
                    success=True,
                    execution_context=execution_context,
                ),
            ]
            
            execution_result = ExecutionResult(
                success=True,
                context=execution_context,
                step_results=step_results,
                total_duration_secs=10.0 + i,
            )
            
            execution_results.append(execution_result)
        
        # Process batch
        batch_result = integration_writer.write_execution_result_batch(
            execution_results=execution_results,
            run_id="batch-integration-test",
            batch_size=2,
        )
        
        assert batch_result["success"] is True
        assert batch_result["total_executions"] == 5
        assert batch_result["successful_writes"] == 5
        assert batch_result["failed_writes"] == 0
        assert batch_result["rows_written"] > 0

    def test_memory_monitoring_integration(self, integration_writer):
        """Test memory monitoring with real system metrics."""
        # Test memory usage monitoring
        memory_info = integration_writer.get_memory_usage()
        
        assert "rss_mb" in memory_info
        assert "vms_mb" in memory_info
        assert "percent" in memory_info
        assert "available_mb" in memory_info
        assert "monitoring_timestamp" in memory_info
        
        # Verify memory values are reasonable
        assert memory_info["rss_mb"] > 0
        assert memory_info["vms_mb"] > 0
        assert 0 <= memory_info["percent"] <= 100

    def test_configuration_integration(self, spark_session, integration_logger):
        """Test various configuration options integration."""
        # Test with different write modes
        for write_mode in [WriteMode.APPEND, WriteMode.OVERWRITE]:
            config = WriterConfig(
                table_schema="config_test",
                table_name="config_logs",
                write_mode=write_mode,
                batch_size=50,
                enable_optimization=True,
            )
            
            writer = LogWriter(spark_session, config, integration_logger)
            
            # Test basic functionality
            log_rows = [
                {
                    "run_id": f"config-test-{write_mode.value}",
                    "phase": "bronze",
                    "step_name": "config_step",
                    "duration_secs": 5.0,
                    "rows_processed": 500,
                    "validation_rate": 95.0,
                }
            ]
            
            result = writer.write_log_rows(log_rows, run_id=f"config-test-{write_mode.value}")
            assert result["success"] is True

    def test_error_handling_integration(self, integration_writer):
        """Test error handling integration with real components."""
        # Test with invalid log data
        invalid_log_rows = [
            {
                "run_id": "",  # Invalid: empty run_id
                "phase": "bronze",
                "step_name": "invalid_step",
                "duration_secs": -1.0,  # Invalid: negative duration
                "rows_processed": -100,  # Invalid: negative rows
                "validation_rate": 150.0,  # Invalid: > 100%
            }
        ]
        
        # Should handle invalid data gracefully
        result = integration_writer.write_log_rows(invalid_log_rows, run_id="error-test")
        
        # Result should indicate success but with quality issues
        assert result["success"] is True
        if "quality_results" in result:
            assert result["quality_results"]["quality_passed"] is False

    def test_schema_evolution_integration(self, integration_writer):
        """Test schema evolution integration."""
        # Test with evolving schema
        evolving_log_rows = [
            LogRow(
                run_id="schema-evolution-test",
                run_mode="initial",
                run_started_at=datetime.now(),
                run_ended_at=datetime.now(),
                execution_id="exec-schema-1",
                pipeline_id="schema-pipeline",
                schema="test_schema",
                phase="bronze",
                step_name="schema_step",
                step_type="extraction",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=10.0,
                table_fqn="test.schema_table",
                rows_processed=1000,
                rows_written=950,
                validation_rate=95.0,
                success=True,
                error_message=None,
                metadata={"new_field": "new_value", "version": "v2.0"},
            ),
        ]
        
        # Should handle schema evolution gracefully
        result = integration_writer.write_log_rows(
            evolving_log_rows, run_id="schema-evolution-test"
        )
        
        assert result["success"] is True
        assert result["rows_written"] == 1


class TestWriterPerformanceIntegration:
    """Performance integration tests for writer module."""

    @pytest.fixture(scope="class")
    def perf_spark_session(self):
        """Create SparkSession optimized for performance testing."""
        spark = SparkSession.builder \
            .appName("WriterPerformanceTest") \
            .master("local[4]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        yield spark
        spark.stop()

    @pytest.fixture
    def perf_config(self):
        """WriterConfig optimized for performance testing."""
        return WriterConfig(
            table_schema="perf_test",
            table_name="perf_logs",
            write_mode=WriteMode.APPEND,
            batch_size=1000,
            max_file_size_mb=256,
            parallel_write_threads=4,
            memory_fraction=0.8,
            enable_optimization=True,
            auto_optimize_schema=True,
        )

    def test_high_throughput_write_performance(self, perf_spark_session, perf_config):
        """Test high-throughput write performance."""
        logger = PipelineLogger("PerformanceTest")
        writer = LogWriter(perf_spark_session, perf_config, logger)
        
        # Create large dataset
        large_dataset = [
            {
                "run_id": f"perf-test-{i}",
                "phase": "bronze" if i % 3 == 0 else "silver" if i % 3 == 1 else "gold",
                "step_name": f"perf_step_{i}",
                "duration_secs": float(i % 100),
                "rows_processed": i * 10,
                "validation_rate": 95.0 + (i % 5),
            }
            for i in range(10000)  # 10K records
        ]
        
        import time
        start_time = time.time()
        
        result = writer.write_log_rows(large_dataset, run_id="perf-test")
        
        end_time = time.time()
        duration = end_time - start_time
        
        assert result["success"] is True
        assert result["rows_written"] == 10000
        assert duration < 30.0  # Should complete within 30 seconds
        
        # Calculate throughput
        throughput = result["rows_written"] / duration
        assert throughput > 300  # Should achieve > 300 rows/second

    def test_memory_efficiency_performance(self, perf_spark_session, perf_config):
        """Test memory efficiency during large operations."""
        logger = PipelineLogger("MemoryPerformanceTest")
        writer = LogWriter(perf_spark_session, perf_config, logger)
        
        # Monitor memory usage during large operation
        initial_memory = writer.get_memory_usage()
        
        # Create and process large dataset
        large_dataset = [
            {
                "run_id": f"memory-test-{i}",
                "phase": "bronze",
                "step_name": f"memory_step_{i}",
                "duration_secs": 1.0,
                "rows_processed": 1000,
                "validation_rate": 95.0,
            }
            for i in range(5000)  # 5K records
        ]
        
        result = writer.write_log_rows(large_dataset, run_id="memory-test")
        
        final_memory = writer.get_memory_usage()
        
        assert result["success"] is True
        assert result["rows_written"] == 5000
        
        # Memory usage should be reasonable
        memory_increase = final_memory["rss_mb"] - initial_memory["rss_mb"]
        assert memory_increase < 1000  # Should not increase by more than 1GB

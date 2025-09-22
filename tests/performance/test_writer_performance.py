"""
Performance tests and benchmarks for the writer module.

This module contains performance tests that measure execution time, memory usage,
throughput, and other performance characteristics of the writer module.
"""

from __future__ import annotations

import time
from datetime import datetime
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


class TestWriterPerformance:
    """Performance tests for writer module."""

    @pytest.fixture(scope="class")
    def perf_spark(self):
        """SparkSession optimized for performance testing."""
        spark = SparkSession.builder \
            .appName("WriterPerformanceTest") \
            .master("local[4]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        yield spark
        spark.stop()

    @pytest.fixture
    def perf_logger(self):
        """Logger configured for performance testing."""
        return PipelineLogger("PerformanceTest")

    @pytest.fixture
    def perf_config(self):
        """WriterConfig optimized for performance."""
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
    def perf_writer(self, perf_spark, perf_config, perf_logger):
        """LogWriter configured for performance testing."""
        return LogWriter(perf_spark, perf_config, perf_logger)

    # ============================================================================
    # Throughput Performance Tests
    # ============================================================================

    @pytest.mark.benchmark(group="write_throughput")
    def test_single_write_throughput(self, perf_writer, benchmark):
        """Benchmark single write operation throughput."""
        log_rows = [
            {
                "run_id": "throughput-test",
                "phase": "bronze",
                "step_name": "throughput_step",
                "duration_secs": 10.0,
                "rows_processed": 1000,
                "validation_rate": 95.0,
            }
            for _ in range(1000)  # 1K records
        ]
        
        def write_operation():
            return perf_writer.write_log_rows(log_rows, run_id="throughput-test")
        
        result = benchmark(write_operation)
        assert result["success"] is True
        assert result["rows_written"] == 1000

    @pytest.mark.benchmark(group="write_throughput")
    def test_large_batch_throughput(self, perf_writer, benchmark):
        """Benchmark large batch write throughput."""
        large_batch = [
            {
                "run_id": f"large-batch-{i}",
                "phase": "bronze" if i % 3 == 0 else "silver" if i % 3 == 1 else "gold",
                "step_name": f"large_step_{i}",
                "duration_secs": float(i % 100),
                "rows_processed": i * 10,
                "validation_rate": 95.0 + (i % 5),
            }
            for i in range(10000)  # 10K records
        ]
        
        def write_large_batch():
            return perf_writer.write_log_rows(large_batch, run_id="large-batch-test")
        
        result = benchmark(write_large_batch)
        assert result["success"] is True
        assert result["rows_written"] == 10000

    @pytest.mark.benchmark(group="batch_processing")
    def test_batch_processing_performance(self, perf_writer, benchmark):
        """Benchmark batch processing performance."""
        # Create multiple execution results
        execution_results = []
        
        for i in range(50):  # 50 execution results
            execution_context = ExecutionContext(
                mode=ExecutionMode.INITIAL,
                start_time=datetime.now(),
                execution_id=f"batch-exec-{i}",
                pipeline_id="batch-pipeline",
                schema="perf_test",
                run_mode="initial",
            )
            
            step_results = [
                StepResult(
                    step_name=f"batch_step_{i}",
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
            ]
            
            execution_result = ExecutionResult(
                success=True,
                context=execution_context,
                step_results=step_results,
                total_duration_secs=10.0,
            )
            
            execution_results.append(execution_result)
        
        def batch_processing():
            return perf_writer.write_execution_result_batch(
                execution_results=execution_results,
                run_id="batch-perf-test",
                batch_size=10,
            )
        
        result = benchmark(batch_processing)
        assert result["success"] is True
        assert result["total_executions"] == 50

    # ============================================================================
    # Memory Performance Tests
    # ============================================================================

    @pytest.mark.benchmark(group="memory_usage")
    def test_memory_efficiency(self, perf_writer, benchmark):
        """Benchmark memory efficiency during operations."""
        def memory_intensive_operation():
            # Create memory-intensive dataset
            large_dataset = [
                {
                    "run_id": f"memory-test-{i}",
                    "phase": "bronze",
                    "step_name": f"memory_step_{i}",
                    "duration_secs": 1.0,
                    "rows_processed": 1000,
                    "validation_rate": 95.0,
                    "metadata": {"large_data": "x" * 1000},  # Large metadata
                }
                for i in range(5000)  # 5K records with large metadata
            ]
            
            result = perf_writer.write_log_rows(large_dataset, run_id="memory-test")
            
            # Get memory usage after operation
            memory_info = perf_writer.get_memory_usage()
            
            return {
                "result": result,
                "memory_mb": memory_info["rss_mb"],
            }
        
        benchmark_result = benchmark(memory_intensive_operation)
        assert benchmark_result["result"]["success"] is True
        assert benchmark_result["memory_mb"] < 2000  # Should use less than 2GB

    @pytest.mark.benchmark(group="memory_usage")
    def test_memory_leak_detection(self, perf_writer):
        """Test for memory leaks during repeated operations."""
        initial_memory = perf_writer.get_memory_usage()
        
        # Perform multiple operations
        for i in range(10):
            log_rows = [
                {
                    "run_id": f"leak-test-{i}-{j}",
                    "phase": "bronze",
                    "step_name": f"leak_step_{j}",
                    "duration_secs": 1.0,
                    "rows_processed": 100,
                    "validation_rate": 95.0,
                }
                for j in range(100)  # 100 records per iteration
            ]
            
            result = perf_writer.write_log_rows(log_rows, run_id=f"leak-test-{i}")
            assert result["success"] is True
        
        final_memory = perf_writer.get_memory_usage()
        
        # Memory increase should be reasonable (less than 500MB)
        memory_increase = final_memory["rss_mb"] - initial_memory["rss_mb"]
        assert memory_increase < 500

    # ============================================================================
    # Latency Performance Tests
    # ============================================================================

    @pytest.mark.benchmark(group="latency")
    def test_single_record_latency(self, perf_writer, benchmark):
        """Benchmark latency for single record operations."""
        single_record = [{
            "run_id": "latency-test",
            "phase": "bronze",
            "step_name": "latency_step",
            "duration_secs": 1.0,
            "rows_processed": 1,
            "validation_rate": 95.0,
        }]
        
        def single_write():
            return perf_writer.write_log_rows(single_record, run_id="latency-test")
        
        result = benchmark(single_write)
        assert result["success"] is True
        assert result["rows_written"] == 1

    @pytest.mark.benchmark(group="latency")
    def test_batch_latency(self, perf_writer, benchmark):
        """Benchmark latency for batch operations."""
        batch_records = [
            {
                "run_id": f"batch-latency-{i}",
                "phase": "bronze",
                "step_name": f"batch_latency_step_{i}",
                "duration_secs": 1.0,
                "rows_processed": 100,
                "validation_rate": 95.0,
            }
            for i in range(100)  # 100 records batch
        ]
        
        def batch_write():
            return perf_writer.write_log_rows(batch_records, run_id="batch-latency-test")
        
        result = benchmark(batch_write)
        assert result["success"] is True
        assert result["rows_written"] == 100

    # ============================================================================
    # Concurrent Performance Tests
    # ============================================================================

    def test_concurrent_write_performance(self, perf_writer):
        """Test performance under concurrent write operations."""
        import threading
        import time
        
        results = []
        start_time = time.time()
        
        def concurrent_write(thread_id: int):
            log_rows = [
                {
                    "run_id": f"concurrent-{thread_id}-{i}",
                    "phase": "bronze",
                    "step_name": f"concurrent_step_{i}",
                    "duration_secs": 1.0,
                    "rows_processed": 100,
                    "validation_rate": 95.0,
                }
                for i in range(100)  # 100 records per thread
            ]
            
            result = perf_writer.write_log_rows(log_rows, run_id=f"concurrent-{thread_id}")
            results.append(result)
        
        # Start multiple threads
        threads = []
        for i in range(5):  # 5 concurrent threads
            thread = threading.Thread(target=concurrent_write, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Verify all operations completed successfully
        assert len(results) == 5
        total_rows_written = sum(result["rows_written"] for result in results)
        assert total_rows_written == 500  # 5 threads * 100 records each
        
        # Calculate concurrent throughput
        concurrent_throughput = total_rows_written / total_duration
        assert concurrent_throughput > 100  # Should achieve > 100 rows/second concurrently

    # ============================================================================
    # Data Quality Performance Tests
    # ============================================================================

    @pytest.mark.benchmark(group="data_quality")
    def test_data_quality_validation_performance(self, perf_writer, benchmark):
        """Benchmark data quality validation performance."""
        # Create dataset with mixed quality
        mixed_quality_data = [
            LogRow(
                run_id=f"quality-perf-{i}",
                run_mode="initial",
                run_started_at=datetime.now(),
                run_ended_at=datetime.now(),
                execution_id=f"exec-{i}",
                pipeline_id="quality-pipeline",
                schema="perf_test",
                phase="bronze" if i % 3 == 0 else "silver" if i % 3 == 1 else "gold",
                step_name=f"quality_step_{i}",
                step_type="extraction",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_secs=10.0 + (i % 50),
                table_fqn=f"test.quality_table_{i}",
                rows_processed=1000 + i * 10,
                rows_written=950 + i * 9,
                validation_rate=90.0 + (i % 10),
                success=True,
                error_message=None,
                metadata={"quality_index": i},
            )
            for i in range(1000)  # 1K records with mixed quality
        ]
        
        def quality_validation():
            return perf_writer.validate_log_data_quality(mixed_quality_data)
        
        result = benchmark(quality_validation)
        assert "quality_passed" in result
        assert "validation_rate" in result

    @pytest.mark.benchmark(group="anomaly_detection")
    def test_anomaly_detection_performance(self, perf_writer, benchmark):
        """Benchmark anomaly detection performance."""
        # Create dataset with some anomalies
        anomaly_data = [
            {
                "run_id": f"anomaly-perf-{i}",
                "duration_secs": 10.0 if i % 10 != 0 else 100.0,  # Anomaly every 10th record
                "validation_rate": 95.0 if i % 15 != 0 else 50.0,  # Anomaly every 15th record
                "rows_processed": 1000 + i * 10,
            }
            for i in range(1000)  # 1K records with anomalies
        ]
        
        def anomaly_detection():
            return perf_writer.detect_anomalies(anomaly_data)
        
        result = benchmark(anomaly_detection)
        assert "anomalies_detected" in result
        assert "anomaly_count" in result

    # ============================================================================
    # Table Operations Performance Tests
    # ============================================================================

    @pytest.mark.benchmark(group="table_operations")
    def test_table_optimization_performance(self, perf_writer, benchmark):
        """Benchmark table optimization performance."""
        def table_optimization():
            return perf_writer.optimize_table(
                enable_partitioning=True,
                enable_compression=True,
                enable_zordering=False,
                enable_vacuum=True,
            )
        
        result = benchmark(table_optimization)
        assert "optimized" in result

    @pytest.mark.benchmark(group="table_operations")
    def test_table_maintenance_performance(self, perf_writer, benchmark):
        """Benchmark table maintenance performance."""
        def table_maintenance():
            return perf_writer.maintain_table(
                vacuum=True,
                analyze=True,
                validate_schema=True,
            )
        
        result = benchmark(table_maintenance)
        assert "maintained" in result

    # ============================================================================
    # Reporting Performance Tests
    # ============================================================================

    @pytest.mark.benchmark(group="reporting")
    def test_summary_report_performance(self, perf_writer, benchmark):
        """Benchmark summary report generation performance."""
        def summary_report():
            return perf_writer.generate_summary_report(days=7)
        
        result = benchmark(summary_report)
        assert "report_available" in result

    @pytest.mark.benchmark(group="reporting")
    def test_trend_analysis_performance(self, perf_writer, benchmark):
        """Benchmark performance trend analysis."""
        def trend_analysis():
            return perf_writer.analyze_performance_trends(days=30)
        
        result = benchmark(trend_analysis)
        assert "trends_available" in result

    @pytest.mark.benchmark(group="reporting")
    def test_data_export_performance(self, perf_writer, benchmark):
        """Benchmark data export performance."""
        def data_export():
            return perf_writer.export_analytics_data(
                format="json",
                limit=1000,
            )
        
        result = benchmark(data_export)
        assert "export_successful" in result

    # ============================================================================
    # Configuration Performance Tests
    # ============================================================================

    def test_different_batch_sizes_performance(self, perf_spark, perf_logger):
        """Test performance with different batch sizes."""
        batch_sizes = [100, 500, 1000, 2000, 5000]
        results = []
        
        for batch_size in batch_sizes:
            config = WriterConfig(
                table_schema="batch_size_test",
                table_name="batch_size_logs",
                write_mode=WriteMode.APPEND,
                batch_size=batch_size,
                enable_optimization=True,
            )
            
            writer = LogWriter(perf_spark, config, perf_logger)
            
            # Create test data
            test_data = [
                {
                    "run_id": f"batch-size-{batch_size}-{i}",
                    "phase": "bronze",
                    "step_name": f"batch_size_step_{i}",
                    "duration_secs": 1.0,
                    "rows_processed": 100,
                    "validation_rate": 95.0,
                }
                for i in range(1000)  # 1K records
            ]
            
            start_time = time.time()
            result = writer.write_log_rows(test_data, run_id=f"batch-size-{batch_size}")
            end_time = time.time()
            
            duration = end_time - start_time
            throughput = result["rows_written"] / duration
            
            results.append({
                "batch_size": batch_size,
                "duration": duration,
                "throughput": throughput,
                "success": result["success"],
            })
        
        # Verify all batch sizes work correctly
        for result in results:
            assert result["success"] is True
            assert result["throughput"] > 0

    def test_different_parallel_threads_performance(self, perf_spark, perf_logger):
        """Test performance with different parallel thread counts."""
        thread_counts = [1, 2, 4, 8]
        results = []
        
        for thread_count in thread_counts:
            config = WriterConfig(
                table_schema="thread_test",
                table_name="thread_logs",
                write_mode=WriteMode.APPEND,
                parallel_write_threads=thread_count,
                batch_size=1000,
                enable_optimization=True,
            )
            
            writer = LogWriter(perf_spark, config, perf_logger)
            
            # Create test data
            test_data = [
                {
                    "run_id": f"thread-{thread_count}-{i}",
                    "phase": "bronze",
                    "step_name": f"thread_step_{i}",
                    "duration_secs": 1.0,
                    "rows_processed": 100,
                    "validation_rate": 95.0,
                }
                for i in range(1000)  # 1K records
            ]
            
            start_time = time.time()
            result = writer.write_log_rows(test_data, run_id=f"thread-{thread_count}")
            end_time = time.time()
            
            duration = end_time - start_time
            throughput = result["rows_written"] / duration
            
            results.append({
                "thread_count": thread_count,
                "duration": duration,
                "throughput": throughput,
                "success": result["success"],
            })
        
        # Verify all thread counts work correctly
        for result in results:
            assert result["success"] is True
            assert result["throughput"] > 0

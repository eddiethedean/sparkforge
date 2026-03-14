#!/usr/bin/env python3
"""
Test for Trap 4: Broad Exception Catching in Writer Components fix.

This test verifies that writer components properly raise specific exceptions
instead of returning generic error responses that mask real issues.
"""

from unittest.mock import Mock, patch

import pytest

from pipeline_builder.writer.analytics import DataQualityAnalyzer
from pipeline_builder.writer.core import LogWriter
from pipeline_builder.writer.exceptions import WriterError, WriterTableError
from pipeline_builder.writer.models import WriterConfig
from pipeline_builder.writer.monitoring import PerformanceMonitor
from pipeline_builder.writer.storage import StorageManager


class TestTrap4BroadExceptionCatching:
    """Test that writer components raise specific exceptions instead of generic responses."""

    def test_core_writer_raises_specific_exceptions(self, spark):
        """Test that LogWriter raises WriterError instead of returning generic responses."""
        # Create LogWriter
        writer = LogWriter(
            spark=spark,
            schema="test_schema",
            table_name="test_logs",
        )

        # Mock storage manager to raise an exception
        with patch.object(
            writer.storage_manager,
            "get_table_info",
            side_effect=RuntimeError("Database connection failed"),
        ):
            # Should raise WriterError, not return {"error": "..."}
            with pytest.raises(WriterError) as excinfo:
                writer.get_table_info()

            # Verify the error message is helpful
            error_msg = str(excinfo.value)
            assert "Failed to get table info" in error_msg
            assert "Database connection failed" in error_msg

    def test_core_writer_analytics_raises_specific_exceptions(self, spark):
        """Test that analytics methods raise WriterError instead of returning generic responses."""
        writer = LogWriter(
            spark=spark,
            schema="test_schema",
            table_name="test_logs",
        )

        # Mock storage manager to raise an exception
        with patch.object(
            writer.storage_manager,
            "query_logs",
            side_effect=RuntimeError("Query failed"),
        ):
            # Should raise WriterError, not return {"error": "..."}
            with pytest.raises(WriterError) as excinfo:
                writer.analyze_quality_trends()

            error_msg = str(excinfo.value)
            assert "Failed to analyze quality trends" in error_msg
            assert "Query failed" in error_msg

    def test_storage_manager_raises_specific_exceptions(self, spark):
        """Test that StorageManager raises WriterTableError (patch abstraction, not spark.sql)."""
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_logs",
        )
        storage = StorageManager(
            spark=spark,
            config=config,
        )

        # Patch abstraction so we don't patch read-only spark.sql
        with patch.object(
            storage,
            "_get_table_row_count",
            side_effect=RuntimeError("SQL execution failed"),
        ):
            with pytest.raises(WriterTableError) as excinfo:
                storage.get_table_info()

            error_msg = str(excinfo.value)
            assert "Failed to get table info for test_schema.test_logs" in error_msg
            assert "Failed to get table info" in error_msg

    def test_analytics_raises_specific_exceptions(self, spark, spark_imports):
        """Test that DataQualityAnalyzer raises WriterError (patch abstraction, not df.count)."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        StringType = spark_imports.StringType

        analyzer = DataQualityAnalyzer(
            spark=spark,
            logger=Mock(),
        )
        schema = StructType([StructField("test_col", StringType(), True)])
        mock_df = spark.createDataFrame([], schema)

        # Patch the query builder so we don't patch read-only DataFrame.count
        with patch(
            "pipeline_builder.writer.analytics.QueryBuilder.build_quality_trends_query",
            side_effect=RuntimeError("DataFrame operation failed"),
        ):
            with pytest.raises(WriterError) as excinfo:
                analyzer.analyze_quality_trends(mock_df)

            error_msg = str(excinfo.value)
            assert "Failed to analyze quality trends" in error_msg

    def test_monitoring_raises_specific_exceptions(self, spark):
        """Test that PerformanceMonitor raises WriterError instead of returning generic responses."""
        monitor = PerformanceMonitor(
            spark=spark,
            logger=Mock(),
        )

        # Mock psutil to raise an exception
        with patch(
            "psutil.virtual_memory", side_effect=RuntimeError("Memory info unavailable")
        ):
            # Should raise WriterError, not return {"error": "..."}
            with pytest.raises(WriterError) as excinfo:
                monitor.get_memory_usage()

            error_msg = str(excinfo.value)
            assert "Failed to get memory usage" in error_msg
            assert "Memory info unavailable" in error_msg

    def test_exception_chaining_preserves_original_error(self, spark):
        """Test that exceptions are properly chained to preserve the original error."""
        writer = LogWriter(
            spark=spark,
            schema="test_schema",
            table_name="test_logs",
        )

        original_error = RuntimeError("Original database error")

        with patch.object(
            writer.storage_manager, "get_table_info", side_effect=original_error
        ):
            with pytest.raises(WriterError) as excinfo:
                writer.get_table_info()

            # Verify the exception is chained
            assert excinfo.value.__cause__ is original_error
            assert "Original database error" in str(excinfo.value)

    def test_no_generic_error_responses_returned(self, spark):
        """Test that no methods return generic error responses."""
        writer = LogWriter(
            spark=spark,
            schema="test_schema",
            table_name="test_logs",
        )

        # Mock all methods to raise exceptions
        with patch.object(
            writer.storage_manager,
            "get_table_info",
            side_effect=RuntimeError("Test error"),
        ):
            with patch.object(
                writer.storage_manager,
                "query_logs",
                side_effect=RuntimeError("Test error"),
            ):
                # All methods should raise exceptions, not return error responses
                with pytest.raises(WriterError):
                    writer.get_table_info()

                with pytest.raises(WriterError):
                    writer.analyze_quality_trends()

                with pytest.raises(WriterError):
                    writer.analyze_execution_trends()

                with pytest.raises(WriterError):
                    writer.detect_quality_anomalies()

                with pytest.raises(WriterError):
                    writer.generate_performance_report()

    def test_error_logging_before_raising(self, spark):
        """Test that errors are logged before raising exceptions."""
        writer = LogWriter(
            spark=spark,
            schema="test_schema",
            table_name="test_logs",
        )

        with patch.object(
            writer.storage_manager,
            "get_table_info",
            side_effect=RuntimeError("Test error"),
        ):
            with patch.object(writer.logger, "error") as mock_logger:
                with pytest.raises(WriterError):
                    writer.get_table_info()

                # Verify error was logged
                mock_logger.assert_called_once()
                log_call = mock_logger.call_args[0][0]
                assert "Failed to get table info" in log_call
                assert "Test error" in log_call

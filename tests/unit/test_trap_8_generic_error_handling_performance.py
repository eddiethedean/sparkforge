"""
Test cases for Trap 8: Generic Error Handling in Performance Monitoring.

This module tests that performance monitoring no longer uses generic error handling
that masks real issues, and instead raises specific exceptions with proper error chaining.
"""

import pytest
from unittest.mock import Mock, patch
from sparkforge.writer.monitoring import PerformanceMonitor
from sparkforge.writer.exceptions import WriterError


class TestTrap8GenericErrorHandlingPerformance:
    """Test cases for generic error handling fixes in performance monitoring."""

    def test_start_operation_raises_specific_exception_on_failure(self, spark_session):
        """Test that start_operation raises WriterError instead of silently failing."""
        # Create performance monitor
        monitor = PerformanceMonitor(spark=spark_session, logger=Mock())
        
        # Mock time.time to raise an exception
        with patch('time.time', side_effect=Exception("Time service unavailable")):
            # Should raise WriterError with proper error chaining
            with pytest.raises(WriterError) as exc_info:
                monitor.start_operation("test_op", "test_type")
            
            error_msg = str(exc_info.value)
            assert "Failed to start monitoring operation test_op" in error_msg
            assert "Time service unavailable" in error_msg
            # Check that the original exception is chained
            assert exc_info.value.__cause__ is not None

    def test_end_operation_raises_specific_exception_on_failure(self, spark_session):
        """Test that end_operation raises WriterError instead of returning empty dict."""
        # Create performance monitor
        monitor = PerformanceMonitor(spark=spark_session, logger=Mock())
        
        # Mock the operation_start_times to raise an exception
        monitor.operation_start_times = Mock()
        monitor.operation_start_times.__getitem__ = Mock(side_effect=Exception("Key error"))
        
        # Should raise WriterError with proper error chaining
        with pytest.raises(WriterError) as exc_info:
            monitor.end_operation("test_op", True, 100)
        
        error_msg = str(exc_info.value)
        assert "Failed to end monitoring operation test_op" in error_msg
        assert "Key error" in error_msg
        # Check that the original exception is chained
        assert exc_info.value.__cause__ is not None

    def test_check_performance_thresholds_raises_specific_exception_on_failure(self, spark_session):
        """Test that check_performance_thresholds raises WriterError instead of returning generic error."""
        # Create performance monitor
        monitor = PerformanceMonitor(spark=spark_session, logger=Mock())
        
        # Mock the metrics to raise an exception
        monitor.metrics = Mock()
        monitor.metrics.__getitem__ = Mock(side_effect=Exception("Metrics unavailable"))
        
        # Should raise WriterError with proper error chaining
        with pytest.raises(WriterError) as exc_info:
            monitor.check_performance_thresholds({"test": "metrics"})
        
        error_msg = str(exc_info.value)
        assert "Failed to check performance thresholds" in error_msg
        assert "Metrics unavailable" in error_msg
        # Check that the original exception is chained
        assert exc_info.value.__cause__ is not None

    def test_get_memory_usage_raises_specific_exception_on_failure(self, spark_session):
        """Test that get_memory_usage raises WriterError instead of returning empty dict."""
        # Create performance monitor
        monitor = PerformanceMonitor(spark=spark_session, logger=Mock())
        
        # Mock psutil to raise an exception
        with patch('psutil.virtual_memory', side_effect=Exception("Memory service unavailable")):
            # Should raise WriterError with proper error chaining
            with pytest.raises(WriterError) as exc_info:
                monitor.get_memory_usage()
            
            error_msg = str(exc_info.value)
            assert "Failed to get memory usage" in error_msg
            assert "Memory service unavailable" in error_msg
            # Check that the original exception is chained
            assert exc_info.value.__cause__ is not None

    def test_analyze_execution_trends_raises_specific_exception_on_failure(self, spark_session):
        """Test that analyze_execution_trends raises WriterError instead of returning empty dict."""
        # Create analytics engine
        from sparkforge.writer.monitoring import AnalyticsEngine
        analytics = AnalyticsEngine(spark=spark_session, logger=Mock())
        
        # Mock QueryBuilder.build_daily_trends_query to raise an exception
        with patch('sparkforge.writer.monitoring.QueryBuilder.build_daily_trends_query', 
                   side_effect=Exception("Query failed")):
            # Should raise WriterError with proper error chaining
            with pytest.raises(WriterError) as exc_info:
                analytics.analyze_execution_trends(Mock())  # Mock DataFrame
        
        error_msg = str(exc_info.value)
        assert "Failed to analyze execution trends" in error_msg
        assert "Query failed" in error_msg
        # Check that the original exception is chained
        assert exc_info.value.__cause__ is not None

    def test_detect_anomalies_raises_specific_exception_on_failure(self, spark_session):
        """Test that detect_anomalies raises WriterError instead of returning empty dict."""
        # Create analytics engine
        from sparkforge.writer.monitoring import AnalyticsEngine
        analytics = AnalyticsEngine(spark=spark_session, logger=Mock())
        
        # Mock QueryBuilder.calculate_statistics to raise an exception
        with patch('sparkforge.writer.monitoring.QueryBuilder.calculate_statistics', 
                   side_effect=Exception("Anomaly detection failed")):
            # Should raise WriterError with proper error chaining
            with pytest.raises(WriterError) as exc_info:
                analytics.detect_anomalies(Mock())  # Mock DataFrame
        
        error_msg = str(exc_info.value)
        assert "Failed to detect anomalies" in error_msg
        assert "Anomaly detection failed" in error_msg
        # Check that the original exception is chained
        assert exc_info.value.__cause__ is not None

    def test_generate_performance_report_raises_specific_exception_on_failure(self, spark_session):
        """Test that generate_performance_report raises WriterError instead of returning empty dict."""
        # Create analytics engine
        from sparkforge.writer.monitoring import AnalyticsEngine
        analytics = AnalyticsEngine(spark=spark_session, logger=Mock())
        
        # Mock QueryBuilder.get_common_aggregations to raise an exception
        with patch('sparkforge.writer.monitoring.QueryBuilder.get_common_aggregations', 
                   side_effect=Exception("Report generation failed")):
            # Should raise WriterError with proper error chaining
            with pytest.raises(WriterError) as exc_info:
                analytics.generate_performance_report(Mock())  # Mock DataFrame
        
        error_msg = str(exc_info.value)
        assert "Failed to generate performance report" in error_msg
        assert "Report generation failed" in error_msg
        # Check that the original exception is chained
        assert exc_info.value.__cause__ is not None

    def test_error_chaining_preserves_original_exception(self, spark_session):
        """Test that error chaining preserves the original exception information."""
        # Create performance monitor
        monitor = PerformanceMonitor(spark=spark_session, logger=Mock())
        
        # Create a specific exception
        original_error = ValueError("Specific error message")
        
        # Mock time.time to raise the specific exception
        with patch('time.time', side_effect=original_error):
            with pytest.raises(WriterError) as exc_info:
                monitor.start_operation("test_op", "test_type")
            
            # Check that the original exception is properly chained
            assert exc_info.value.__cause__ is original_error
            assert isinstance(exc_info.value.__cause__, ValueError)
            assert str(original_error) in str(exc_info.value)

    def test_logging_still_occurs_before_exception_raising(self, spark_session):
        """Test that logging still occurs before raising exceptions."""
        # Create a mock logger
        mock_logger = Mock()
        monitor = PerformanceMonitor(spark=spark_session, logger=mock_logger)
        
        # Mock time.time to raise an exception
        with patch('time.time', side_effect=Exception("Test error")):
            with pytest.raises(WriterError):
                monitor.start_operation("test_op", "test_type")
            
            # Verify that logging occurred
            mock_logger.error.assert_called_once()
            log_call = mock_logger.error.call_args[0][0]
            assert "Failed to start monitoring operation test_op" in log_call
            assert "Test error" in log_call

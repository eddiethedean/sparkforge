"""
Writer monitoring module for performance tracking and metrics collection.

This module handles performance monitoring, metrics collection, and
analytics for the writer operations.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List

import psutil  # type: ignore[import-untyped]
from pyspark.sql import DataFrame, SparkSession

from ..logging import PipelineLogger
from .exceptions import WriterError
from .models import WriterMetrics
from .query_builder import QueryBuilder


class PerformanceMonitor:
    """Handles performance monitoring and metrics collection."""

    def __init__(self, spark: SparkSession, logger: PipelineLogger | None = None):
        """Initialize the performance monitor."""
        self.spark = spark
        if logger is None:
            self.logger = PipelineLogger("PerformanceMonitor")
        else:
            self.logger = logger
        self.metrics: WriterMetrics = {
            "total_writes": 0,
            "successful_writes": 0,
            "failed_writes": 0,
            "total_duration_secs": 0.0,
            "avg_write_duration_secs": 0.0,
            "total_rows_written": 0,
            "memory_usage_peak_mb": 0.0,
        }
        self.operation_start_times: Dict[str, float] = {}

    def start_operation(self, operation_id: str, operation_type: str) -> None:
        """
        Start monitoring an operation.

        Args:
            operation_id: Unique identifier for the operation
            operation_type: Type of operation being monitored
        """
        try:
            self.operation_start_times[operation_id] = time.time()
            self.logger.info(
                f"Started monitoring {operation_type} operation: {operation_id}"
            )

        except Exception as e:
            self.logger.error(
                f"Failed to start monitoring operation {operation_id}: {e}"
            )
            raise WriterError(f"Failed to start monitoring operation {operation_id}: {e}") from e

    def end_operation(
        self,
        operation_id: str,
        success: bool,
        rows_written: int = 0,
        error_message: str | None = None,
    ) -> Dict[str, Any]:
        """
        End monitoring an operation and update metrics.

        Args:
            operation_id: Unique identifier for the operation
            success: Whether the operation was successful
            rows_written: Number of rows written
            error_message: Error message if operation failed

        Returns:
            Dictionary containing operation metrics
        """
        try:
            if operation_id not in self.operation_start_times:
                self.logger.warning(f"Operation {operation_id} was not being monitored")
                return {}

            # Calculate duration
            start_time = self.operation_start_times[operation_id]
            duration = time.time() - start_time

            # Update metrics
            self.metrics["total_writes"] += 1
            if success:
                self.metrics["successful_writes"] += 1
            else:
                self.metrics["failed_writes"] += 1

            self.metrics["total_duration_secs"] += duration
            self.metrics["total_rows_written"] += rows_written

            # Calculate average duration
            if self.metrics["total_writes"] > 0:
                self.metrics["avg_write_duration_secs"] = (
                    self.metrics["total_duration_secs"] / self.metrics["total_writes"]
                )

            # Update peak memory usage
            current_memory = self.get_memory_usage()["used_mb"]
            if current_memory > self.metrics["memory_usage_peak_mb"]:
                self.metrics["memory_usage_peak_mb"] = current_memory

            # Create operation metrics
            operation_metrics = {
                "operation_id": operation_id,
                "success": success,
                "duration_secs": duration,
                "rows_written": rows_written,
                "memory_usage_mb": current_memory,
                "error_message": error_message,
                "timestamp": datetime.now().isoformat(),
            }

            # Clean up
            del self.operation_start_times[operation_id]

            self.logger.info(
                f"Completed monitoring {operation_id}: {duration:.2f}s, {rows_written} rows"
            )
            return operation_metrics

        except Exception as e:
            self.logger.error(f"Failed to end monitoring operation {operation_id}: {e}")
            raise WriterError(f"Failed to end monitoring operation {operation_id}: {e}") from e

    def get_metrics(self) -> WriterMetrics:
        """Get current performance metrics."""
        return self.metrics.copy()

    def reset_metrics(self) -> None:
        """Reset performance metrics."""
        self.metrics = {
            "total_writes": 0,
            "successful_writes": 0,
            "failed_writes": 0,
            "total_duration_secs": 0.0,
            "avg_write_duration_secs": 0.0,
            "total_rows_written": 0,
            "memory_usage_peak_mb": 0.0,
        }
        self.logger.info("Performance metrics reset")

    def get_memory_usage(self) -> Dict[str, Any]:
        """
        Get current memory usage information.

        Returns:
            Dictionary containing memory usage details
        """
        try:
            # Get system memory info
            memory = psutil.virtual_memory()

            # Get Spark memory info if available
            spark_memory = {}
            try:
                spark_context = self.spark.sparkContext
                spark_memory = {
                    "executor_memory": spark_context.getConf().get(
                        "spark.executor.memory", "N/A"
                    ),
                    "driver_memory": spark_context.getConf().get(
                        "spark.driver.memory", "N/A"
                    ),
                }
            except Exception:
                pass

            memory_info = {
                "total_mb": round(memory.total / (1024 * 1024), 2),
                "available_mb": round(memory.available / (1024 * 1024), 2),
                "used_mb": round(memory.used / (1024 * 1024), 2),
                "percentage": memory.percent,
                "spark_memory": spark_memory,
            }

            return memory_info

        except Exception as e:
            self.logger.error(f"Failed to get memory usage: {e}")
            raise WriterError(f"Failed to get memory usage: {e}") from e

    def check_performance_thresholds(
        self, operation_metrics: Dict[str, Any]
    ) -> List[str]:
        """
        Check if performance thresholds are exceeded.

        Args:
            operation_metrics: Metrics for the operation

        Returns:
            List of threshold violations
        """
        violations = []

        try:
            # Check duration threshold (5 minutes)
            if operation_metrics.get("duration_secs", 0) > 300:
                violations.append("Operation duration exceeded 5 minutes")

            # Check memory usage threshold (8GB)
            if operation_metrics.get("memory_usage_mb", 0) > 8192:
                violations.append("Memory usage exceeded 8GB")

            # Check success rate threshold (95%)
            if self.metrics["total_writes"] > 0:
                success_rate = (
                    self.metrics["successful_writes"] / self.metrics["total_writes"]
                ) * 100
                if success_rate < 95.0:
                    violations.append(f"Success rate below 95%: {success_rate:.1f}%")

            return violations

        except Exception as e:
            self.logger.error(f"Failed to check performance thresholds: {e}")
            raise WriterError(f"Failed to check performance thresholds: {e}") from e


class AnalyticsEngine:
    """Handles analytics and trend analysis for writer operations."""

    def __init__(self, spark: SparkSession, logger: PipelineLogger | None = None):
        """Initialize the analytics engine."""
        self.spark = spark
        if logger is None:
            self.logger = PipelineLogger("AnalyticsEngine")
        else:
            self.logger = logger

    def analyze_execution_trends(self, df: DataFrame) -> Dict[str, Any]:
        """
        Analyze execution trends from log data.

        Args:
            df: DataFrame containing log data

        Returns:
            Dictionary containing trend analysis
        """
        try:
            self.logger.info("Analyzing execution trends")

            # Use query builder for all trend analyses
            trends = {}

            # Success rate trend using query builder
            success_trend_df = QueryBuilder.build_daily_trends_query(df, 30)
            success_trend = success_trend_df.collect()

            trends["success_rate_trend"] = [
                {
                    "date": row["date"],
                    "success_rate": (
                        row["successful_executions"] / row["daily_executions"]
                    )
                    * 100,
                    "avg_validation_rate": row.get("avg_validation_rate", 0),
                    "avg_execution_time": row["avg_execution_time"],
                }
                for row in success_trend
            ]

            # Performance trends using query builder
            performance_trend_df = QueryBuilder.build_phase_trends_query(df, 30)
            performance_trend = performance_trend_df.collect()

            trends["performance_by_phase"] = [
                {
                    "phase": row["phase"],
                    "avg_execution_time": row["avg_execution_time"],
                    "avg_validation_rate": row["avg_validation_rate"],
                    "execution_count": row["execution_count"],
                }
                for row in performance_trend
            ]

            # Data quality trends using query builder
            quality_trend_df = QueryBuilder.build_quality_trends_query(df, 30)
            quality_trend = quality_trend_df.collect()

            trends["data_quality_trend"] = [
                {
                    "date": row["date"],
                    "avg_validation_rate": row["avg_validation_rate"],
                    "min_validation_rate": row["min_validation_rate"],
                    "max_validation_rate": row["max_validation_rate"],
                }
                for row in quality_trend
            ]

            self.logger.info("Execution trends analysis completed")
            return trends

        except Exception as e:
            self.logger.error(f"Failed to analyze execution trends: {e}")
            raise WriterError(f"Failed to analyze execution trends: {e}") from e

    def detect_anomalies(self, df: DataFrame) -> Dict[str, Any]:
        """
        Detect anomalies in execution data.

        Args:
            df: DataFrame containing log data

        Returns:
            Dictionary containing anomaly detection results
        """
        try:
            self.logger.info("Detecting anomalies in execution data")

            anomalies: Dict[str, Any] = {}

            # Calculate performance thresholds using query builder
            performance_stats = QueryBuilder.calculate_statistics(df, "execution_time")
            performance_threshold = performance_stats["avg"] + (
                2 * performance_stats["stddev"]
            )

            # Detect performance anomalies using query builder
            performance_anomalies_df = QueryBuilder.build_performance_anomaly_query(
                df, performance_threshold
            ).select("step", "execution_time", "validation_rate", "success")

            performance_anomalies = performance_anomalies_df.collect()

            anomalies["performance_anomalies"] = [
                {
                    "step": row["step"],
                    "execution_time": row["execution_time"],
                    "validation_rate": row["validation_rate"],
                    "success": row["success"],
                }
                for row in performance_anomalies
            ]

            # Detect data quality anomalies using query builder
            quality_anomalies_df = (
                QueryBuilder.build_quality_anomaly_query(df, 90.0)
                .select("step", "validation_rate", "valid_rows", "invalid_rows")
                .orderBy("validation_rate")
            )

            quality_anomalies = quality_anomalies_df.collect()

            anomalies["quality_anomalies"] = [
                {
                    "step": row["step"],
                    "validation_rate": row["validation_rate"],
                    "valid_rows": row["valid_rows"],
                    "invalid_rows": row["invalid_rows"],
                }
                for row in quality_anomalies
            ]

            # Calculate anomaly score
            total_executions = df.count()
            anomaly_count = len(performance_anomalies) + len(quality_anomalies)
            anomaly_score = (
                (anomaly_count / total_executions) * 100 if total_executions > 0 else 0
            )

            anomalies["anomaly_score"] = float(round(anomaly_score, 2))
            anomalies["total_anomalies"] = int(anomaly_count)
            anomalies["total_executions"] = int(total_executions)

            self.logger.info(
                f"Anomaly detection completed: {anomaly_count} anomalies found"
            )
            return anomalies

        except Exception as e:
            self.logger.error(f"Failed to detect anomalies: {e}")
            raise WriterError(f"Failed to detect anomalies: {e}") from e

    def generate_performance_report(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate comprehensive performance report.

        Args:
            df: DataFrame containing log data

        Returns:
            Dictionary containing performance report
        """
        try:
            self.logger.info("Generating performance report")

            # Overall statistics using query builder
            overall_stats_df = df.agg(**QueryBuilder.get_common_aggregations())
            overall_stats = overall_stats_df.collect()[0]

            # Phase-wise statistics using query builder
            phase_stats_df = QueryBuilder.build_phase_trends_query(df, 30)
            phase_stats = phase_stats_df.collect()

            # Recent performance using query builder
            recent_performance_df = QueryBuilder.build_recent_performance_query(df, 7)
            recent_performance = recent_performance_df.collect()

            report = {
                "overall_statistics": {
                    "total_executions": overall_stats["total_executions"],
                    "successful_executions": overall_stats["successful_executions"],
                    "success_rate": (
                        (
                            overall_stats["successful_executions"]
                            / overall_stats["total_executions"]
                        )
                        * 100
                        if overall_stats["total_executions"] > 0
                        else 0
                    ),
                    "avg_execution_time": overall_stats["avg_execution_time"],
                    "avg_validation_rate": overall_stats["avg_validation_rate"],
                    "total_rows_written": overall_stats["total_rows_written"],
                },
                "phase_statistics": [
                    {
                        "phase": row["phase"],
                        "execution_count": row["execution_count"],
                        "avg_execution_time": row["avg_execution_time"],
                        "avg_validation_rate": row["avg_validation_rate"],
                        "total_rows_written": row["total_rows_written"],
                    }
                    for row in phase_stats
                ],
                "recent_performance": [
                    {
                        "date": row["date"].strftime("%Y-%m-%d"),
                        "daily_executions": row["daily_executions"],
                        "avg_execution_time": row["avg_execution_time"],
                        "avg_validation_rate": row["avg_validation_rate"],
                    }
                    for row in recent_performance
                ],
                "generated_at": datetime.now().isoformat(),
            }

            self.logger.info("Performance report generated successfully")
            return report

        except Exception as e:
            self.logger.error(f"Failed to generate performance report: {e}")
            raise WriterError(f"Failed to generate performance report: {e}") from e

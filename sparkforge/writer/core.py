"""
Refactored LogWriter implementation with modular architecture.

This module contains the main LogWriter class that orchestrates the various
writer components for comprehensive logging functionality.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List

from pyspark.sql import SparkSession

from ..logging import PipelineLogger
from ..models import ExecutionResult, StepResult
from .analytics import DataQualityAnalyzer, TrendAnalyzer
from .exceptions import WriterConfigurationError, WriterError
from .models import LogRow, WriterConfig, WriterMetrics, create_log_schema
from .monitoring import AnalyticsEngine, PerformanceMonitor
from .operations import DataProcessor
from .storage import StorageManager


class LogWriter:
    """
    Refactored LogWriter with modular architecture.

    This class orchestrates the various writer components to provide
    comprehensive logging functionality for pipeline execution results.

    Components:
    - DataProcessor: Handles data processing and transformations
    - StorageManager: Manages Delta Lake storage operations
    - PerformanceMonitor: Tracks performance metrics
    - AnalyticsEngine: Provides analytics and trend analysis
    - DataQualityAnalyzer: Analyzes data quality metrics
    - TrendAnalyzer: Analyzes execution trends
    """

    def __init__(
        self,
        spark: SparkSession,
        config: WriterConfig,
        logger: PipelineLogger | None = None,
    ) -> None:
        """
        Initialize the LogWriter with modular components.

        Args:
            spark: Spark session
            config: Writer configuration
            logger: Pipeline logger (optional)

        Raises:
            WriterConfigurationError: If configuration is invalid
        """
        self.spark = spark
        self.config = config
        if logger is None:
            self.logger = PipelineLogger("LogWriter")
        else:
            self.logger = logger

        # Validate configuration
        try:
            self.config.validate()
        except ValueError as e:
            raise WriterConfigurationError(
                f"Invalid writer configuration: {e}",
                config_errors=[str(e)],
                context={"config": self.config.__dict__},
                suggestions=[
                    "Check configuration values",
                    "Ensure all required fields are provided",
                    "Verify numeric values are positive",
                ],
            ) from e

        # Initialize components
        self._initialize_components()

        # Initialize metrics
        self.metrics: WriterMetrics = {
            "total_writes": 0,
            "successful_writes": 0,
            "failed_writes": 0,
            "total_duration_secs": 0.0,
            "avg_write_duration_secs": 0.0,
            "total_rows_written": 0,
            "memory_usage_peak_mb": 0.0,
        }

        # Initialize schema
        self.schema = create_log_schema()

        self.logger.info(
            f"LogWriter initialized for table: {self.config.table_schema}.{self.config.table_name}"
        )

    def _initialize_components(self) -> None:
        """Initialize all writer components."""
        # Data processing component
        self.data_processor = DataProcessor(self.spark, self.logger)

        # Storage management component
        self.storage_manager = StorageManager(self.spark, self.config, self.logger)

        # Performance monitoring component
        self.performance_monitor = PerformanceMonitor(self.spark, self.logger)

        # Analytics components
        self.analytics_engine = AnalyticsEngine(self.spark, self.logger)
        self.quality_analyzer = DataQualityAnalyzer(self.spark, self.logger)
        self.trend_analyzer = TrendAnalyzer(self.spark, self.logger)

    def write_execution_result(
        self,
        execution_result: ExecutionResult,
        run_id: str | None = None,
        run_mode: str = "initial",
        metadata: dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """
        Write execution result to log table.

        Args:
            execution_result: The execution result to write
            run_id: Unique run identifier (generated if not provided)
            run_mode: Mode of the run (initial, incremental, etc.)
            metadata: Additional metadata

        Returns:
            Dict containing write results and metrics

        Raises:
            WriterValidationError: If validation fails
            WriterTableError: If table operations fail
            WriterPerformanceError: If performance thresholds exceeded
        """
        operation_id = str(uuid.uuid4())
        if run_id is None:
            run_id = str(uuid.uuid4())

        try:
            # Start performance monitoring
            self.performance_monitor.start_operation(
                operation_id, "write_execution_result"
            )

            # Log operation start
            self.logger.info(f"Writing execution result for run {run_id}")

            # Process execution result
            log_rows = self.data_processor.process_execution_result(
                execution_result, run_id, run_mode, metadata
            )

            # Create table if not exists
            self.storage_manager.create_table_if_not_exists(self.schema)

            # Write to storage
            write_result = self.storage_manager.write_batch(
                log_rows, self.config.write_mode
            )

            # Update metrics
            self._update_metrics(write_result, True)

            # End performance monitoring
            operation_metrics = self.performance_monitor.end_operation(
                operation_id, True, write_result.get("rows_written", 0)
            )

            # Check performance thresholds
            threshold_violations = (
                self.performance_monitor.check_performance_thresholds(operation_metrics)
            )
            if threshold_violations:
                self.logger.warning(
                    f"Performance threshold violations: {threshold_violations}"
                )

            result = {
                "success": True,
                "run_id": run_id,
                "operation_id": operation_id,
                "rows_written": write_result.get("rows_written", 0),
                "write_result": write_result,
                "operation_metrics": operation_metrics,
                "threshold_violations": threshold_violations,
            }

            self.logger.info(f"Successfully wrote execution result for run {run_id}")
            return result

        except Exception as e:
            # End performance monitoring with failure
            self.performance_monitor.end_operation(operation_id, False, 0, str(e))
            self._update_metrics({}, False)

            self.logger.error(f"Failed to write execution result for run {run_id}: {e}")
            raise

    def write_step_results(
        self,
        step_results: Dict[str, StepResult],
        run_id: str | None = None,
        run_mode: str = "initial",
        metadata: dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """
        Write step results to log table.

        Args:
            step_results: Dictionary of step results
            run_id: Unique run identifier (generated if not provided)
            run_mode: Mode of the run
            metadata: Additional metadata

        Returns:
            Dict containing write results and metrics
        """
        operation_id = str(uuid.uuid4())
        if run_id is None:
            run_id = str(uuid.uuid4())

        try:
            # Start performance monitoring
            self.performance_monitor.start_operation(operation_id, "write_step_results")

            # Log operation start
            self.logger.info(
                f"Writing {len(step_results)} step results for run {run_id}"
            )

            # Process step results
            log_rows = self.data_processor.process_step_results(
                step_results, run_id, run_mode, metadata
            )

            # Create table if not exists
            self.storage_manager.create_table_if_not_exists(self.schema)

            # Write to storage
            write_result = self.storage_manager.write_batch(
                log_rows, self.config.write_mode
            )

            # Update metrics
            self._update_metrics(write_result, True)

            # End performance monitoring
            operation_metrics = self.performance_monitor.end_operation(
                operation_id, True, write_result.get("rows_written", 0)
            )

            result = {
                "success": True,
                "run_id": run_id,
                "operation_id": operation_id,
                "rows_written": write_result.get("rows_written", 0),
                "write_result": write_result,
                "operation_metrics": operation_metrics,
            }

            self.logger.info(f"Successfully wrote step results for run {run_id}")
            return result

        except Exception as e:
            # End performance monitoring with failure
            self.performance_monitor.end_operation(operation_id, False, 0, str(e))
            self._update_metrics({}, False)

            self.logger.error(f"Failed to write step results for run {run_id}: {e}")
            raise

    def write_log_rows(
        self,
        log_rows: List[LogRow],
        run_id: str | None = None,
    ) -> Dict[str, Any]:
        """
        Write log rows directly to the table.

        Args:
            log_rows: List of log rows to write
            run_id: Unique run identifier (generated if not provided)

        Returns:
            Dict containing write results and metrics
        """
        operation_id = str(uuid.uuid4())
        if run_id is None:
            run_id = str(uuid.uuid4())

        try:
            # Start performance monitoring
            self.performance_monitor.start_operation(operation_id, "write_log_rows")

            # Log operation start
            self.logger.info(f"Writing {len(log_rows)} log rows for run {run_id}")

            # Create table if not exists
            self.storage_manager.create_table_if_not_exists(self.schema)

            # Write to storage
            write_result = self.storage_manager.write_batch(
                log_rows, self.config.write_mode
            )

            # Update metrics
            self._update_metrics(write_result, True)

            # End performance monitoring
            operation_metrics = self.performance_monitor.end_operation(
                operation_id, True, write_result.get("rows_written", 0)
            )

            result = {
                "success": True,
                "run_id": run_id,
                "operation_id": operation_id,
                "rows_written": write_result.get("rows_written", 0),
                "write_result": write_result,
                "operation_metrics": operation_metrics,
            }

            self.logger.info(f"Successfully wrote log rows for run {run_id}")
            return result

        except Exception as e:
            # End performance monitoring with failure
            self.performance_monitor.end_operation(operation_id, False, 0, str(e))
            self._update_metrics({}, False)

            self.logger.error(f"Failed to write log rows for run {run_id}: {e}")
            raise

    def write_execution_result_batch(
        self,
        execution_results: List[ExecutionResult],
        run_ids: List[str] | None = None,
        run_mode: str = "initial",
        metadata: dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """
        Write multiple execution results in batch.

        Args:
            execution_results: List of execution results to write
            run_ids: List of run identifiers (generated if not provided)
            run_mode: Mode of the runs
            metadata: Additional metadata

        Returns:
            Dict containing batch write results and metrics
        """
        operation_id = str(uuid.uuid4())
        if run_ids is None:
            run_ids = [str(uuid.uuid4()) for _ in execution_results]

        try:
            # Start performance monitoring
            self.performance_monitor.start_operation(
                operation_id, "write_execution_result_batch"
            )

            # Log operation start
            self.logger.info(
                f"Writing batch of {len(execution_results)} execution results"
            )

            # Process all execution results
            all_log_rows = []
            for i, execution_result in enumerate(execution_results):
                run_id = run_ids[i] if i < len(run_ids) else str(uuid.uuid4())
                log_rows = self.data_processor.process_execution_result(
                    execution_result, run_id, run_mode, metadata
                )
                all_log_rows.extend(log_rows)

            # Create table if not exists
            self.storage_manager.create_table_if_not_exists(self.schema)

            # Write to storage
            write_result = self.storage_manager.write_batch(
                all_log_rows, self.config.write_mode
            )

            # Update metrics
            self._update_metrics(write_result, True)

            # End performance monitoring
            operation_metrics = self.performance_monitor.end_operation(
                operation_id, True, write_result.get("rows_written", 0)
            )

            result = {
                "success": True,
                "operation_id": operation_id,
                "execution_results_count": len(execution_results),
                "total_rows_written": write_result.get("rows_written", 0),
                "write_result": write_result,
                "operation_metrics": operation_metrics,
            }

            self.logger.info(
                f"Successfully wrote batch of {len(execution_results)} execution results"
            )
            return result

        except Exception as e:
            # End performance monitoring with failure
            self.performance_monitor.end_operation(operation_id, False, 0, str(e))
            self._update_metrics({}, False)

            self.logger.error(f"Failed to write execution result batch: {e}")
            raise

    def show_logs(self, limit: int | None = None) -> None:
        """
        Display logs from the table.

        Args:
            limit: Maximum number of rows to display
        """
        try:
            self.logger.info(
                f"Displaying logs from {self.config.table_schema}.{self.config.table_name}"
            )

            # Query logs
            df = self.storage_manager.query_logs(limit=limit)

            # Show DataFrame
            display_limit = limit if limit is not None else 20
            df.show(display_limit, truncate=False)

            self.logger.info("Logs displayed successfully")

        except Exception as e:
            self.logger.error(f"Failed to display logs: {e}")
            raise

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the log table.

        Returns:
            Dictionary containing table information
        """
        try:
            return self.storage_manager.get_table_info()
        except Exception as e:
            self.logger.error(f"Failed to get table info: {e}")
            raise WriterError(f"Failed to get table info: {e}") from e

    def optimize_table(self) -> Dict[str, Any]:
        """
        Optimize the Delta table for better performance.

        Returns:
            Dictionary containing optimization results
        """
        try:
            self.logger.info("Optimizing Delta table")
            return self.storage_manager.optimize_table()
        except Exception as e:
            self.logger.error(f"Failed to optimize table: {e}")
            raise

    def vacuum_table(self, retention_hours: int = 168) -> Dict[str, Any]:
        """
        Vacuum the Delta table to remove old files.

        Args:
            retention_hours: Hours of retention for old files

        Returns:
            Dictionary containing vacuum results
        """
        try:
            self.logger.info(f"Vacuuming Delta table (retention: {retention_hours}h)")
            return self.storage_manager.vacuum_table(retention_hours)
        except Exception as e:
            self.logger.error(f"Failed to vacuum table: {e}")
            raise

    def analyze_quality_trends(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze data quality trends.

        Args:
            days: Number of days to analyze

        Returns:
            Dictionary containing quality trend analysis
        """
        try:
            self.logger.info(f"Analyzing quality trends for last {days} days")

            # Query recent logs
            df = self.storage_manager.query_logs()

            # Analyze quality trends
            return self.quality_analyzer.analyze_quality_trends(df, days)

        except Exception as e:
            self.logger.error(f"Failed to analyze quality trends: {e}")
            raise WriterError(f"Failed to analyze quality trends: {e}") from e

    def analyze_execution_trends(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze execution trends.

        Args:
            days: Number of days to analyze

        Returns:
            Dictionary containing execution trend analysis
        """
        try:
            self.logger.info(f"Analyzing execution trends for last {days} days")

            # Query recent logs
            df = self.storage_manager.query_logs()

            # Analyze execution trends
            return self.trend_analyzer.analyze_execution_trends(df, days)

        except Exception as e:
            self.logger.error(f"Failed to analyze execution trends: {e}")
            raise WriterError(f"Failed to analyze execution trends: {e}") from e

    def detect_quality_anomalies(self) -> Dict[str, Any]:
        """
        Detect data quality anomalies.

        Returns:
            Dictionary containing anomaly detection results
        """
        try:
            self.logger.info("Detecting quality anomalies")

            # Query logs
            df = self.storage_manager.query_logs()

            # Detect anomalies
            return self.quality_analyzer.detect_quality_anomalies(df)

        except Exception as e:
            self.logger.error(f"Failed to detect quality anomalies: {e}")
            raise WriterError(f"Failed to detect quality anomalies: {e}") from e

    def generate_performance_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive performance report.

        Returns:
            Dictionary containing performance report
        """
        try:
            self.logger.info("Generating performance report")

            # Query logs
            df = self.storage_manager.query_logs()

            # Generate report
            return self.analytics_engine.generate_performance_report(df)

        except Exception as e:
            self.logger.error(f"Failed to generate performance report: {e}")
            raise WriterError(f"Failed to generate performance report: {e}") from e

    def get_metrics(self) -> WriterMetrics:
        """Get current writer metrics."""
        return self.performance_monitor.get_metrics()

    def reset_metrics(self) -> None:
        """Reset writer metrics."""
        self.performance_monitor.reset_metrics()

    def get_memory_usage(self) -> Dict[str, Any]:
        """Get current memory usage information."""
        return self.performance_monitor.get_memory_usage()

    def _update_metrics(self, write_result: Dict[str, Any], success: bool) -> None:
        """Update writer metrics."""
        try:
            self.metrics["total_writes"] += 1
            if success:
                self.metrics["successful_writes"] += 1
            else:
                self.metrics["failed_writes"] += 1

            if "rows_written" in write_result:
                self.metrics["total_rows_written"] += write_result["rows_written"]

            # Update performance monitor metrics
            self.performance_monitor.metrics.update(self.metrics)

        except Exception as e:
            self.logger.error(f"Failed to update metrics: {e}")

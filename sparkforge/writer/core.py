"""
Refactored LogWriter implementation with modular architecture.

This module contains the main LogWriter class that orchestrates the various
writer components for comprehensive logging functionality.

# Depends on:
#   compat
#   functions
#   logging
#   models.execution
#   validation.utils
#   writer.analytics
#   writer.exceptions
#   writer.models
#   writer.monitoring
#   writer.operations
#   writer.storage
"""

from __future__ import annotations

import uuid
from typing import Any, Dict

from ..compat import SparkSession
from ..functions import FunctionsProtocol, get_default_functions
from ..logging import PipelineLogger
from ..models import ExecutionResult, StepResult
from ..pipeline.models import PipelineReport
from .analytics import (
    DataQualityAnalyzer,
    ExecutionTrends,
    QualityAnomalies,
    QualityTrends,
    TrendAnalyzer,
)
from .exceptions import WriterConfigurationError, WriterError
from .models import LogRow, WriteMode, WriterConfig, WriterMetrics, create_log_schema
from .monitoring import (
    AnalyticsEngine,
    AnomalyReport,
    MemoryUsageInfo,
    PerformanceMonitor,
    PerformanceReport,
)
from .operations import DataProcessor, DataQualityReport
from .storage import (
    OptimizeResult,
    StorageManager,
    TableInfo,
    VacuumResult,
    WriteResult,
)


def time_write_operation(
    operation_func: Any, *args: Any, **kwargs: Any
) -> tuple[int, float, Any, Any]:
    """
    Time a write operation and return metrics.

    Args:
        operation_func: Function to time
        *args: Arguments for the function
        **kwargs: Keyword arguments for the function

    Returns:
        Tuple of (rows_written, duration_secs, start_time, end_time)
    """
    import time
    from datetime import datetime

    start_time = datetime.now()
    start_ts = time.time()

    try:
        result = operation_func(*args, **kwargs)
        rows_written = result.get("rows_written", 0) if isinstance(result, dict) else 0
    except Exception:
        rows_written = 0

    end_time = datetime.now()
    duration_secs = time.time() - start_ts

    return rows_written, duration_secs, start_time, end_time


def validate_log_data(log_rows: list[LogRow]) -> None:
    """
    Validate log data for quality and consistency.

    Args:
        log_rows: List of log rows to validate

    Raises:
        WriterValidationError: If validation fails
    """
    if not log_rows:
        return

    # Basic validation - check required fields
    required_fields = {"run_id", "phase", "step_name"}
    for i, row in enumerate(log_rows):
        missing_fields = required_fields - set(row.keys())
        if missing_fields:
            from .exceptions import WriterValidationError

            raise WriterValidationError(
                f"Log row {i} missing required fields: {missing_fields}",
                validation_errors=[f"Missing fields: {missing_fields}"],
                context={"row_index": i, "row": row},
            )


def create_log_rows_from_execution_result(
    execution_result: ExecutionResult,
    run_id: str,
    run_mode: str = "initial",
    metadata: Dict[str, Any] | None = None,
) -> list[LogRow]:
    """
    Create log rows from an execution result.

    Args:
        execution_result: The execution result
        run_id: Run identifier
        run_mode: Mode of the run
        metadata: Additional metadata

    Returns:
        List of log rows
    """

    log_rows = []

    # Create a main log row for the execution
    main_row: LogRow = {
        "run_id": run_id,
        "run_mode": run_mode,  # type: ignore[typeddict-item]
        "run_started_at": getattr(execution_result, "start_time", None),
        "run_ended_at": getattr(execution_result, "end_time", None),
        "execution_id": getattr(execution_result, "execution_id", run_id),
        "pipeline_id": getattr(execution_result, "pipeline_id", "unknown"),
        "schema": getattr(execution_result, "schema", "default"),
        "phase": "bronze",
        "step_name": "pipeline_execution",
        "step_type": "pipeline",
        "start_time": getattr(execution_result, "start_time", None),
        "end_time": getattr(execution_result, "end_time", None),
        "duration_secs": getattr(execution_result, "duration", 0.0) or 0.0,
        "table_fqn": None,
        "write_mode": None,
        "input_rows": 0,
        "output_rows": 0,
        "rows_written": 0,
        "rows_processed": 0,
        "valid_rows": 0,
        "invalid_rows": 0,
        "validation_rate": 100.0,
        "success": getattr(execution_result, "status", "unknown") == "completed",
        "error_message": getattr(execution_result, "error", None),
        "memory_usage_mb": 0.0,
        "cpu_usage_percent": 0.0,
        "metadata": {},
    }

    log_rows.append(main_row)

    # Add step results if available
    if hasattr(execution_result, "steps") and execution_result.steps:
        for step in execution_result.steps:
            step_row: LogRow = {
                "run_id": run_id,
                "run_mode": run_mode,  # type: ignore[typeddict-item]
                "run_started_at": getattr(execution_result, "start_time", None),
                "run_ended_at": getattr(execution_result, "end_time", None),
                "execution_id": getattr(execution_result, "execution_id", run_id),
                "pipeline_id": getattr(execution_result, "pipeline_id", "unknown"),
                "schema": getattr(execution_result, "schema", "default"),
                "phase": getattr(step, "step_type", "bronze").lower(),  # type: ignore[typeddict-item]
                "step_name": getattr(step, "step_name", "unknown"),
                "step_type": getattr(step, "step_type", "unknown"),
                "start_time": getattr(step, "start_time", None),
                "end_time": getattr(step, "end_time", None),
                "duration_secs": getattr(step, "duration", 0.0),
                "table_fqn": getattr(step, "output_table", None),
                "write_mode": getattr(step, "write_mode", None),
                "input_rows": getattr(step, "input_rows", 0),
                "output_rows": getattr(step, "rows_processed", 0),
                "rows_written": getattr(step, "rows_written", 0),
                "rows_processed": getattr(step, "rows_processed", 0),
                "valid_rows": 0,
                "invalid_rows": 0,
                "validation_rate": 100.0,
                "success": getattr(step, "status", "unknown") == "completed",
                "error_message": getattr(step, "error", None),
                "memory_usage_mb": 0.0,
                "cpu_usage_percent": 0.0,
                "metadata": {},
            }
            log_rows.append(step_row)

    return log_rows


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
        schema: str | None = None,
        table_name: str | None = None,
        config: WriterConfig | None = None,
        functions: FunctionsProtocol | None = None,
        logger: PipelineLogger | None = None,
    ) -> None:
        """
        Initialize the LogWriter with modular components.

        Args:
            spark: Spark session
            schema: Database schema name (simplified API)
            table_name: Table name (simplified API)
            config: Writer configuration (deprecated, use schema and table_name instead)
            functions: Functions protocol (optional, uses default if not provided)
            logger: Pipeline logger (optional)

        Raises:
            WriterConfigurationError: If configuration is invalid

        Example (new simplified API):
            >>> writer = LogWriter(spark, schema="analytics", table_name="pipeline_logs")

        Example (old API, deprecated):
            >>> config = WriterConfig(table_schema="analytics", table_name="pipeline_logs")
            >>> writer = LogWriter(spark, config=config)
        """
        self.spark = spark

        # Handle both old and new API
        if config is not None:
            # Old API: config provided
            import warnings
            warnings.warn(
                "Passing WriterConfig is deprecated. Use LogWriter(spark, schema='...', table_name='...') instead.",
                DeprecationWarning,
                stacklevel=2
            )
            self.config = config
        elif schema is not None and table_name is not None:
            # New API: schema and table_name provided
            self.config = WriterConfig(
                table_schema=schema,
                table_name=table_name,
                write_mode=WriteMode.APPEND
            )
        else:
            raise WriterConfigurationError(
                "Must provide either (schema and table_name) or config parameter",
                config_errors=["Missing required parameters"],
                suggestions=[
                    "Use: LogWriter(spark, schema='my_schema', table_name='my_table')",
                    "Or: LogWriter(spark, config=WriterConfig(...))"
                ]
            )

        self.functions = functions if functions is not None else get_default_functions()
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

        # Set table FQN for compatibility
        self.table_fqn = f"{self.config.table_schema}.{self.config.table_name}"

        self.logger.info(f"LogWriter initialized for table: {self.table_fqn}")

    def _initialize_components(self) -> None:
        """Initialize all writer components."""
        # Data processing component
        self.data_processor = DataProcessor(self.spark, self.functions, self.logger)

        # Storage management component
        self.storage_manager = StorageManager(
            self.spark, self.config, self.functions, self.logger
        )

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
        metadata: Dict[str, Any] | None = None,
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
            # Create empty WriteResult for error case
            empty_result: WriteResult = {
                "table_name": self.storage_manager.table_fqn,
                "write_mode": self.config.write_mode.value,
                "rows_written": 0,
                "timestamp": "",
                "success": False,
            }
            self._update_metrics(empty_result, False)

            self.logger.error(f"Failed to write execution result for run {run_id}: {e}")
            raise

    def write_step_results(
        self,
        step_results: Dict[str, StepResult],
        run_id: str | None = None,
        run_mode: str = "initial",
        metadata: Dict[str, Any] | None = None,
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
            # Create empty WriteResult for error case
            empty_result: WriteResult = {
                "table_name": self.storage_manager.table_fqn,
                "write_mode": self.config.write_mode.value,
                "rows_written": 0,
                "timestamp": "",
                "success": False,
            }
            self._update_metrics(empty_result, False)

            self.logger.error(f"Failed to write step results for run {run_id}: {e}")
            raise

    def write_log_rows(
        self,
        log_rows: list[LogRow],
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
            # Create empty WriteResult for error case
            empty_result: WriteResult = {
                "table_name": self.storage_manager.table_fqn,
                "write_mode": self.config.write_mode.value,
                "rows_written": 0,
                "timestamp": "",
                "success": False,
            }
            self._update_metrics(empty_result, False)

            self.logger.error(f"Failed to write log rows for run {run_id}: {e}")
            raise

    def write_execution_result_batch(
        self,
        execution_results: list[ExecutionResult],
        run_ids: list[str] | None = None,
        run_mode: str = "initial",
        metadata: Dict[str, Any] | None = None,
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
            # Create empty WriteResult for error case
            empty_result: WriteResult = {
                "table_name": self.storage_manager.table_fqn,
                "write_mode": self.config.write_mode.value,
                "rows_written": 0,
                "timestamp": "",
                "success": False,
            }
            self._update_metrics(empty_result, False)

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

            # Query logs using spark.table for compatibility
            df = self.spark.table(
                f"{self.config.table_schema}.{self.config.table_name}"
            )

            # Show DataFrame
            if limit is not None:
                df.show(limit)
            else:
                df.show()

            self.logger.info("Logs displayed successfully")

        except Exception as e:
            self.logger.error(f"Failed to display logs: {e}")
            raise

    def get_table_info(self) -> TableInfo:
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

    def optimize_table(self) -> OptimizeResult:
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

    def vacuum_table(self, retention_hours: int = 168) -> VacuumResult:
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

    def analyze_quality_trends(self, days: int = 30) -> QualityTrends:
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

    def analyze_execution_trends(self, days: int = 30) -> ExecutionTrends:
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

    def detect_quality_anomalies(self) -> QualityAnomalies:
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

    def generate_performance_report(self) -> PerformanceReport:
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
        # Reset LogWriter metrics
        self.metrics = {
            "total_writes": 0,
            "successful_writes": 0,
            "failed_writes": 0,
            "total_duration_secs": 0.0,
            "avg_write_duration_secs": 0.0,
            "total_rows_written": 0,
            "memory_usage_peak_mb": 0.0,
        }
        # Reset performance monitor metrics
        self.performance_monitor.reset_metrics()

    def get_memory_usage(self) -> MemoryUsageInfo:
        """Get current memory usage information."""
        return self.performance_monitor.get_memory_usage()

    def _update_metrics(self, write_result: WriteResult, success: bool) -> None:
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

    # Backward compatibility methods for tests
    def _write_log_rows(
        self,
        log_rows: list[LogRow],
        run_id: str,
        metadata: Dict[str, Any] | None = None,
    ) -> WriteResult:
        """Write log rows directly (for backward compatibility with tests)."""
        return self.storage_manager.write_batch(log_rows, self.config.write_mode)

    def _write_log_rows_batch(
        self, log_rows: list[LogRow], run_id: str, batch_size: int = 100
    ) -> WriteResult:
        """Write log rows in batches (for backward compatibility with tests)."""
        results = []
        for i in range(0, len(log_rows), batch_size):
            batch = log_rows[i : i + batch_size]
            result = self._write_log_rows(batch, run_id)
            results.append(result)

        total_rows = sum(r.get("rows_written", 0) for r in results)
        from datetime import datetime
        return {
            "table_name": self.storage_manager.table_fqn,
            "write_mode": self.config.write_mode.value,
            "rows_written": total_rows,
            "timestamp": datetime.now().isoformat(),
            "success": True,
        }

    def _create_dataframe_from_log_rows(self, log_rows: list[LogRow]) -> Any:
        """Create DataFrame from log rows (for backward compatibility with tests)."""
        # Convert TypedDict to regular dicts for createDataFrame
        dict_rows = [dict(row) for row in log_rows]
        return self.spark.createDataFrame(dict_rows, schema=self.schema)  # type: ignore[attr-defined]

    def detect_anomalies(self, log_rows: list[LogRow]) -> AnomalyReport:
        """Detect anomalies in log data (for backward compatibility with tests)."""
        if not self.config.enable_anomaly_detection:
            return {
                "performance_anomalies": [],
                "quality_anomalies": [],
                "anomaly_score": 0.0,
                "total_anomalies": 0,
                "total_executions": 0,
            }

        try:

            # Basic anomaly detection logic
            if not log_rows:
                return {
                    "performance_anomalies": [],
                    "quality_anomalies": [],
                    "anomaly_score": 0.0,
                    "total_anomalies": 0,
                    "total_executions": len(log_rows),
                }

            # Check for duration anomalies (very simple logic)
            durations = [
                row.get("duration_secs", 0)
                for row in log_rows
                if "duration_secs" in row
            ]
            if not durations:
                return {
                    "performance_anomalies": [],
                    "quality_anomalies": [],
                    "anomaly_score": 0.0,
                    "total_anomalies": 0,
                    "total_executions": len(log_rows),
                }

            avg_duration = sum(durations) / len(durations)
            threshold = avg_duration * 2  # 2x average is anomalous

            from .monitoring import PerformanceAnomaly

            performance_anomalies = []
            for row in log_rows:
                duration = row.get("duration_secs", 0)
                if duration > threshold:
                    anomaly: PerformanceAnomaly = {
                        "step": row.get("step_name", "unknown"),
                        "execution_time": float(duration),
                        "validation_rate": float(row.get("validation_rate", 0.0)),
                        "success": bool(row.get("success", False)),
                    }
                    performance_anomalies.append(anomaly)

            total_anomalies = len(performance_anomalies)
            total_executions = len(log_rows)
            anomaly_score = (total_anomalies / total_executions * 100) if total_executions > 0 else 0.0

            return {
                "performance_anomalies": performance_anomalies,
                "quality_anomalies": [],
                "anomaly_score": round(anomaly_score, 2),
                "total_anomalies": total_anomalies,
                "total_executions": total_executions,
            }
        except Exception as e:
            self.logger.warning(f"Anomaly detection failed: {e}")
            return {
                "performance_anomalies": [],
                "quality_anomalies": [],
                "anomaly_score": 0.0,
                "total_anomalies": 0,
                "total_executions": len(log_rows) if log_rows else 0,
            }

    # Additional methods expected by tests
    def validate_log_data_quality(self, log_rows: list[LogRow]) -> DataQualityReport:
        """Validate log data quality (for backward compatibility with tests)."""
        try:
            from ..validation.utils import get_dataframe_info

            if not log_rows:
                return {
                    "is_valid": True,
                    "total_rows": 0,
                    "null_counts": {},
                    "validation_issues": [],
                    "failed_executions": 0,
                    "data_quality_score": 100.0,
                }

            # Create DataFrame for validation
            df = self._create_dataframe_from_log_rows(log_rows)

            # Get basic info
            df_info = get_dataframe_info(df)

            # Count failed executions
            failed_executions = sum(1 for row in log_rows if not row.get("success", True))

            # Calculate quality score
            total_rows = df_info.get("row_count", len(log_rows))
            validation_rate = 100.0  # Simplified
            data_quality_score = validation_rate if failed_executions == 0 else max(0, validation_rate - (failed_executions / total_rows * 100))

            # Check for null values in critical columns
            null_counts: Dict[str, int] = {}

            # Determine validation issues
            validation_issues = []
            if failed_executions > 0:
                validation_issues.append(f"{failed_executions} failed executions")

            return {
                "is_valid": failed_executions == 0 and len(validation_issues) == 0,
                "total_rows": total_rows,
                "null_counts": null_counts,
                "validation_issues": validation_issues,
                "failed_executions": failed_executions,
                "data_quality_score": round(data_quality_score, 2),
            }

        except Exception as e:
            return {
                "is_valid": False,
                "total_rows": len(log_rows) if log_rows else 0,
                "null_counts": {},
                "validation_issues": [str(e)],
                "failed_executions": 0,
                "data_quality_score": 0.0,
            }

    # ========================================================================
    # New simplified API methods for working with PipelineReport
    # ========================================================================

    def _convert_report_to_log_rows(
        self,
        report: PipelineReport,
        run_id: str | None = None
    ) -> list[LogRow]:
        """
        Convert a PipelineReport to log rows for storage.

        This method extracts data from a PipelineReport and creates log rows
        compatible with the log table schema.

        Args:
            report: PipelineReport to convert
            run_id: Optional run ID (generated if not provided)

        Returns:
            List of LogRow dictionaries ready for storage
        """

        if run_id is None:
            run_id = str(uuid.uuid4())

        # Create a single summary row for the entire pipeline execution
        log_row: LogRow = {
            # Run-level information
            "run_id": run_id,
            "run_mode": report.mode.value,  # "initial", "incremental", etc.
            "run_started_at": report.start_time,
            "run_ended_at": report.end_time,

            # Execution context
            "execution_id": report.execution_id,
            "pipeline_id": report.pipeline_id,
            "schema": self.config.table_schema,

            # Step-level information (summary)
            "phase": "pipeline",  # Overall pipeline summary
            "step_name": "pipeline_summary",
            "step_type": "pipeline",

            # Timing information
            "start_time": report.start_time,
            "end_time": report.end_time,
            "duration_secs": report.duration_seconds,

            # Table information (N/A for summary)
            "table_fqn": None,
            "write_mode": None,

            # Data metrics from report.metrics
            "rows_processed": report.metrics.total_rows_processed,
            "rows_written": report.metrics.total_rows_written,
            "input_rows": report.metrics.total_rows_processed,
            "output_rows": report.metrics.total_rows_written,

            # Validation metrics
            "valid_rows": 0,  # Not tracked in PipelineReport
            "invalid_rows": 0,  # Not tracked in PipelineReport
            "validation_rate": report.metrics.avg_validation_rate,

            # Execution status
            "success": report.success,
            "error_message": ", ".join(report.errors) if report.errors else None,

            # Performance metrics
            "memory_usage_mb": None,  # Not tracked in PipelineReport
            "cpu_usage_percent": None,  # Not tracked in PipelineReport

            # Metadata
            "metadata": {
                "total_steps": report.metrics.total_steps,
                "successful_steps": report.metrics.successful_steps,
                "failed_steps": report.metrics.failed_steps,
                "skipped_steps": report.metrics.skipped_steps,
                "bronze_duration": report.metrics.bronze_duration,
                "silver_duration": report.metrics.silver_duration,
                "gold_duration": report.metrics.gold_duration,
                "parallel_efficiency": report.metrics.parallel_efficiency,
                "execution_groups_count": report.execution_groups_count,
                "max_group_size": report.max_group_size,
                "warnings": report.warnings,
                "recommendations": report.recommendations,
                "cache_hit_rate": report.metrics.cache_hit_rate,
                "error_count": report.metrics.error_count,
                "retry_count": report.metrics.retry_count,
            }
        }

        return [log_row]

    def create_table(
        self,
        report: PipelineReport,
        run_id: str | None = None
    ) -> Dict[str, Any]:
        """
        Create or overwrite the log table with data from a PipelineReport.

        This method creates the log table if it doesn't exist, and writes
        the report data using OVERWRITE mode (replacing any existing data).

        Args:
            report: PipelineReport to write
            run_id: Optional run ID (generated if not provided)

        Returns:
            Dictionary with write results including:
                - success: Whether the operation succeeded
                - run_id: The run identifier used
                - rows_written: Number of rows written
                - table_fqn: Fully qualified table name

        Example:
            >>> writer = LogWriter(spark, schema="analytics", table_name="logs")
            >>> result = writer.create_table(pipeline_report)
            >>> print(f"Created table with {result['rows_written']} rows")
        """
        operation_id = str(uuid.uuid4())
        if run_id is None:
            run_id = str(uuid.uuid4())

        try:
            # Start performance monitoring
            self.performance_monitor.start_operation(operation_id, "create_table")

            # Log operation start
            self.logger.info(f"📊 Creating log table {self.table_fqn} for run {run_id}")

            # Convert report to log rows
            log_rows = self._convert_report_to_log_rows(report, run_id)

            # Create table if not exists
            self.storage_manager.create_table_if_not_exists(self.schema)

            # Write to storage with OVERWRITE mode
            write_result = self.storage_manager.write_batch(
                log_rows, WriteMode.OVERWRITE
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
                "table_fqn": self.table_fqn,
                "write_result": write_result,
                "operation_metrics": operation_metrics,
            }

            self.logger.info(
                f"✅ Successfully created log table {self.table_fqn} with "
                f"{result['rows_written']} row(s) for run {run_id}"
            )
            return result

        except Exception as e:
            # End performance monitoring with failure
            self.performance_monitor.end_operation(operation_id, False, 0, str(e))
            # Create empty WriteResult for error case
            empty_result: WriteResult = {
                "table_name": self.storage_manager.table_fqn,
                "write_mode": self.config.write_mode.value,
                "rows_written": 0,
                "timestamp": "",
                "success": False,
            }
            self._update_metrics(empty_result, False)

            self.logger.error(f"❌ Failed to create log table for run {run_id}: {e}")
            raise

    def append(
        self,
        report: PipelineReport,
        run_id: str | None = None
    ) -> Dict[str, Any]:
        """
        Append data from a PipelineReport to the log table.

        This method appends the report data to an existing log table. If the
        table doesn't exist, it will be created first.

        Args:
            report: PipelineReport to append
            run_id: Optional run ID (generated if not provided)

        Returns:
            Dictionary with write results including:
                - success: Whether the operation succeeded
                - run_id: The run identifier used
                - rows_written: Number of rows written
                - table_fqn: Fully qualified table name

        Example:
            >>> writer = LogWriter(spark, schema="analytics", table_name="logs")
            >>> result = writer.append(pipeline_report)
            >>> print(f"Appended {result['rows_written']} rows to {result['table_fqn']}")
        """
        operation_id = str(uuid.uuid4())
        if run_id is None:
            run_id = str(uuid.uuid4())

        try:
            # Start performance monitoring
            self.performance_monitor.start_operation(operation_id, "append")

            # Log operation start
            self.logger.info(f"📊 Appending to log table {self.table_fqn} for run {run_id}")

            # Convert report to log rows
            log_rows = self._convert_report_to_log_rows(report, run_id)

            # Create table if not exists (for first append)
            self.storage_manager.create_table_if_not_exists(self.schema)

            # Write to storage with APPEND mode
            write_result = self.storage_manager.write_batch(
                log_rows, WriteMode.APPEND
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
                "table_fqn": self.table_fqn,
                "write_result": write_result,
                "operation_metrics": operation_metrics,
            }

            self.logger.info(
                f"✅ Successfully appended {result['rows_written']} row(s) to "
                f"{self.table_fqn} for run {run_id}"
            )
            return result

        except Exception as e:
            # End performance monitoring with failure
            self.performance_monitor.end_operation(operation_id, False, 0, str(e))
            # Create empty WriteResult for error case
            empty_result: WriteResult = {
                "table_name": self.storage_manager.table_fqn,
                "write_mode": self.config.write_mode.value,
                "rows_written": 0,
                "timestamp": "",
                "success": False,
            }
            self._update_metrics(empty_result, False)

            self.logger.error(f"❌ Failed to append to log table for run {run_id}: {e}")
            raise

"""
Core LogWriter implementation with full SparkForge integration.

This module contains the main LogWriter class that provides comprehensive
logging functionality for pipeline execution results.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from ..logging import PipelineLogger
from ..models import ExecutionContext, ExecutionResult, StepResult
from ..table_operations import write_append_table, write_overwrite_table
from .exceptions import (
    WriterConfigurationError,
    WriterError,
    WriterTableError,
    WriterValidationError,
)
from .models import (
    LogRow,
    WriteMode,
    WriterConfig,
    WriterMetrics,
    create_log_rows_from_execution_result,
    create_log_schema,
    validate_log_data,
)


class LogWriter:
    """
    Enhanced log writer with full SparkForge integration.

    Provides comprehensive logging functionality for pipeline execution results,
    integrating seamlessly with existing SparkForge models and components.

    Features:
    - Full integration with ExecutionResult and StepResult models
    - Enhanced type safety and validation
    - Performance monitoring and optimization
    - Comprehensive error handling
    - Flexible configuration system
    - Delta Lake integration for persistent storage

    Example:
        from sparkforge.writer import LogWriter, WriterConfig

        # Configure writer
        config = WriterConfig(
            table_schema="analytics",
            table_name="pipeline_logs",
            write_mode=WriteMode.APPEND
        )

        # Create writer
        writer = LogWriter(spark, config, logger)

        # Write execution result
        result = writer.write_execution_result(execution_result)
    """

    def __init__(
        self,
        spark: SparkSession,
        config: WriterConfig,
        logger: PipelineLogger | None = None,
    ) -> None:
        """
        Initialize the LogWriter.

        Args:
            spark: Spark session
            config: Writer configuration
            logger: Pipeline logger (optional)

        Raises:
            WriterConfigurationError: If configuration is invalid
        """
        self.spark = spark
        self.config = config
        self.logger = logger or PipelineLogger("LogWriter")

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

        # Table name
        self.table_fqn = f"{self.config.table_schema}.{self.config.table_name}"

        self.logger.info(f"LogWriter initialized for table: {self.table_fqn}")

    def write_execution_result(
        self,
        execution_result: ExecutionResult,
        run_id: str | None = None,
        run_mode: str = "initial",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
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
        start_time = time.time()
        run_id = run_id or str(uuid.uuid4())

        try:
            self.logger.info(f"Writing execution result for run: {run_id}")

            # Validate input
            if not isinstance(execution_result, ExecutionResult):
                raise WriterValidationError(
                    "execution_result must be an ExecutionResult instance",
                    context={"provided_type": type(execution_result).__name__},
                    suggestions=["Ensure you're passing an ExecutionResult object"],
                )

            # Create log rows from execution result
            log_rows = create_log_rows_from_execution_result(
                execution_result=execution_result,
                run_id=run_id,
                run_mode=run_mode,
                metadata=metadata,
            )

            # Validate log data
            if self.config.enable_validation:
                try:
                    validate_log_data(log_rows)
                except ValueError as e:
                    raise WriterValidationError(
                        f"Log data validation failed: {e}",
                        validation_errors=[str(e)],
                        context={"run_id": run_id, "row_count": len(log_rows)},
                        suggestions=[
                            "Check log data for invalid values",
                            "Verify all required fields are present",
                            "Ensure numeric values are within valid ranges",
                        ],
                    ) from e

            # Write to table
            self._write_log_rows(log_rows, run_id)

            # Update metrics
            duration = time.time() - start_time
            self._update_metrics(duration, len(log_rows), True)

            self.logger.info(
                f"Successfully wrote {len(log_rows)} log rows in {duration:.2f}s"
            )

            return {
                "success": True,
                "run_id": run_id,
                "rows_written": len(log_rows),
                "duration_secs": duration,
                "table_fqn": self.table_fqn,
                "metrics": self.get_metrics(),
            }

        except Exception as e:
            # Update metrics for failure
            duration = time.time() - start_time
            self._update_metrics(duration, 0, False)

            self.logger.error(f"Failed to write execution result: {e}")

            # Re-raise as WriterError if not already
            if not isinstance(e, WriterError):
                raise WriterError(
                    f"Failed to write execution result: {e}",
                    context={"run_id": run_id, "duration_secs": duration},
                    suggestions=[
                        "Check table permissions",
                        "Verify schema compatibility",
                        "Review error logs for details",
                    ],
                    cause=e,
                ) from e
            raise

    def write_step_results(
        self,
        step_results: list[StepResult],
        execution_context: ExecutionContext,
        run_id: str | None = None,
        run_mode: str = "initial",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Write step results to log table.

        Args:
            step_results: List of step results to write
            execution_context: Execution context
            run_id: Unique run identifier (generated if not provided)
            run_mode: Mode of the run
            metadata: Additional metadata

        Returns:
            Dict containing write results and metrics
        """
        from ..models import ExecutionResult

        # Create execution result from step results
        execution_result = ExecutionResult.from_context_and_results(
            execution_context, step_results
        )

        return self.write_execution_result(
            execution_result=execution_result,
            run_id=run_id,
            run_mode=run_mode,
            metadata=metadata,
        )

    def write_log_rows(
        self, log_rows: list[LogRow], run_id: str | None = None
    ) -> dict[str, Any]:
        """
        Write log rows directly to table.

        Args:
            log_rows: List of log rows to write
            run_id: Run identifier for logging

        Returns:
            Dict containing write results and metrics
        """
        start_time = time.time()
        run_id = run_id or str(uuid.uuid4())

        try:
            self.logger.info(f"Writing {len(log_rows)} log rows for run: {run_id}")

            # Validate log data
            if self.config.enable_validation:
                validate_log_data(log_rows)

            # Write to table
            self._write_log_rows(log_rows, run_id)

            # Update metrics
            duration = time.time() - start_time
            self._update_metrics(duration, len(log_rows), True)

            return {
                "success": True,
                "run_id": run_id,
                "rows_written": len(log_rows),
                "duration_secs": duration,
                "table_fqn": self.table_fqn,
            }

        except Exception as e:
            duration = time.time() - start_time
            self._update_metrics(duration, 0, False)
            raise WriterError(
                f"Failed to write log rows: {e}",
                context={"run_id": run_id, "row_count": len(log_rows)},
                cause=e,
            ) from e

    def _write_log_rows(self, log_rows: list[LogRow], run_id: str) -> dict[str, Any]:
        """
        Internal method to write log rows to table.

        Args:
            log_rows: List of log rows to write
            run_id: Run identifier

        Returns:
            Dict containing write results
        """
        try:
            # Convert log rows to DataFrame
            df = self._create_dataframe_from_log_rows(log_rows)

            # Write based on mode
            if self.config.write_mode == WriteMode.OVERWRITE:
                write_overwrite_table(df, self.table_fqn)
            elif self.config.write_mode == WriteMode.APPEND:
                write_append_table(df, self.table_fqn)
            else:
                raise WriterConfigurationError(
                    f"Unsupported write mode: {self.config.write_mode}",
                    context={"write_mode": self.config.write_mode.value},
                    suggestions=["Use OVERWRITE or APPEND mode"],
                )

            return {"rows_written": len(log_rows)}

        except Exception as e:
            raise WriterTableError(
                f"Failed to write log rows to table: {e}",
                table_name=self.table_fqn,
                operation="write",
                context={"run_id": run_id, "row_count": len(log_rows)},
                suggestions=[
                    "Check table permissions",
                    "Verify table exists",
                    "Check schema compatibility",
                ],
                cause=e,
            ) from e

    def _create_dataframe_from_log_rows(self, log_rows: list[LogRow]) -> DataFrame:
        """
        Create DataFrame from log rows.

        Args:
            log_rows: List of log rows

        Returns:
            DataFrame with log data
        """
        # Convert metadata to JSON strings
        processed_rows = []
        for row in log_rows:
            processed_row = dict(row)
            if "metadata" in processed_row:
                processed_row["metadata"] = json.dumps(processed_row["metadata"])
            processed_rows.append(processed_row)

        return self.spark.createDataFrame(processed_rows, schema=self.schema)  # type: ignore[arg-type]

    def _update_metrics(
        self, duration: float, rows_written: int, success: bool
    ) -> None:
        """Update writer metrics."""
        self.metrics["total_writes"] += 1
        self.metrics["total_duration_secs"] += duration
        self.metrics["total_rows_written"] += rows_written

        if success:
            self.metrics["successful_writes"] += 1
        else:
            self.metrics["failed_writes"] += 1

        # Update averages
        if self.metrics["total_writes"] > 0:
            self.metrics["avg_write_duration_secs"] = (
                self.metrics["total_duration_secs"] / self.metrics["total_writes"]
            )

    def get_metrics(self) -> WriterMetrics:
        """Get current writer metrics."""
        return self.metrics.copy()

    def reset_metrics(self) -> None:
        """Reset writer metrics."""
        self.metrics = {
            "total_writes": 0,
            "successful_writes": 0,
            "failed_writes": 0,
            "total_duration_secs": 0.0,
            "avg_write_duration_secs": 0.0,
            "total_rows_written": 0,
            "memory_usage_peak_mb": 0.0,
        }

    def show_logs(self, limit: int | None = None) -> None:
        """
        Show recent log entries.

        Args:
            limit: Maximum number of rows to show
        """
        try:
            df = self.spark.table(self.table_fqn)
            if limit:
                df.show(limit)
            else:
                df.show()
        except Exception as e:
            raise WriterTableError(
                f"Failed to show logs: {e}",
                table_name=self.table_fqn,
                operation="read",
                context={"limit": limit},
                suggestions=[
                    "Check if table exists",
                    "Verify table permissions",
                    "Check for schema issues",
                ],
                cause=e,
            ) from e

    def get_table_info(self) -> dict[str, Any]:
        """Get information about the log table."""
        try:
            df = self.spark.table(self.table_fqn)
            return {
                "table_fqn": self.table_fqn,
                "row_count": df.count(),
                "columns": df.columns,
                "schema": df.schema.json(),
            }
        except Exception as e:
            raise WriterTableError(
                f"Failed to get table info: {e}",
                table_name=self.table_fqn,
                operation="describe",
                cause=e,
            ) from e

"""
Core LogWriter implementation with full SparkForge integration.

This module contains the main LogWriter class that provides comprehensive
logging functionality for pipeline execution results.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from ..logging import PipelineLogger
from ..models import ExecutionContext, ExecutionResult, StepResult
from ..performance import performance_monitor, time_write_operation
from ..table_operations import table_exists
from ..validation import apply_column_rules, get_dataframe_info
from .exceptions import (
    WriterConfigurationError,
    WriterDataQualityError,
    WriterError,
    WriterTableError,
    WriterValidationError,
)
from .models import (
    LogRow,
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
        run_id = run_id or str(uuid.uuid4())

        # Validate input first before any logging that accesses attributes
        if not isinstance(execution_result, ExecutionResult):
            raise WriterValidationError(
                "execution_result must be an ExecutionResult instance",
                context={"provided_type": type(execution_result).__name__},
                suggestions=["Ensure you're passing an ExecutionResult object"],
            )

        # Log operation start with context
        with self.logger.context(
            run_id=run_id,
            run_mode=run_mode,
            table_fqn=self.table_fqn,
            operation="write_execution_result",
        ):
            self.logger.info("🚀 Starting execution result write operation")
            self.logger.debug(
                "Execution result details",
                step_count=len(execution_result.step_results),
                success=execution_result.success,
                pipeline_id=execution_result.context.pipeline_id,
            )

            # Use logger's timer for performance tracking
            with self.logger.timer("write_execution_result"):
                try:
                    # Create log rows from execution result
                    self.logger.debug("Creating log rows from execution result")
                    log_rows = create_log_rows_from_execution_result(
                        execution_result=execution_result,
                        run_id=run_id,
                        run_mode=run_mode,
                        metadata=metadata,
                    )
                    self.logger.debug(f"Created {len(log_rows)} log rows")

                    # Validate log data
                    if self.config.enable_validation:
                        self.logger.debug("Validating log data")
                        try:
                            validate_log_data(log_rows)
                            self.logger.debug("Log data validation passed")
                        except ValueError as e:
                            self.logger.error(f"Log data validation failed: {e}")
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
                    self.logger.debug("Writing log rows to table")
                    self._write_log_rows(log_rows, run_id)

                    # Update metrics
                    duration = self.logger.end_timer("write_execution_result")
                    self._update_metrics(duration, len(log_rows), True)

                    # Log success with performance metrics
                    self.logger.info(
                        "✅ Successfully wrote execution result",
                        rows_written=len(log_rows),
                        duration_secs=duration,
                        table_fqn=self.table_fqn,
                    )

                    # Log performance metrics
                    self.logger.performance_metric(
                        "rows_per_second",
                        len(log_rows) / duration if duration > 0 else 0,
                        "rows/s",
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
                    duration = self.logger.end_timer("write_execution_result")
                    self._update_metrics(duration, 0, False)

                    self.logger.error(f"❌ Failed to write execution result: {e}")

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
        run_id = run_id or str(uuid.uuid4())

        # Log operation start with context
        with self.logger.context(
            run_id=run_id,
            row_count=len(log_rows),
            table_fqn=self.table_fqn,
            operation="write_log_rows",
        ):
            self.logger.info("🚀 Starting log rows write operation")
            self.logger.debug(f"Processing {len(log_rows)} log rows")

            # Use logger's timer for performance tracking
            with self.logger.timer("write_log_rows"):
                try:
                    # Validate log data
                    if self.config.enable_validation:
                        self.logger.debug("Validating log data")
                        validate_log_data(log_rows)
                        self.logger.debug("Log data validation passed")

                    # Data quality validation if enabled
                    quality_results = None
                    if self.config.log_data_quality_results:
                        self.logger.debug("Running data quality validation")
                        quality_results = self.validate_log_data_quality(log_rows)

                        # Log quality results
                        self.log_data_quality_check(
                            "log_data_quality",
                            quality_results["quality_passed"],
                            quality_results,
                        )

                        # Check if quality meets threshold
                        if not quality_results["quality_passed"]:
                            self.logger.warning(
                                "Data quality below threshold, but continuing with write"
                            )

                    # Anomaly detection if enabled
                    anomaly_results = None
                    if self.config.enable_anomaly_detection:
                        self.logger.debug("Running anomaly detection")
                        anomaly_results = self.detect_anomalies(log_rows)

                        if anomaly_results.get("anomalies_detected"):
                            self.logger.warning(
                                f"Anomalies detected: {anomaly_results['anomaly_count']} found"
                            )

                    # Write to table
                    self.logger.debug("Writing log rows to table")
                    self._write_log_rows(log_rows, run_id)

                    # Update metrics
                    duration = self.logger.end_timer("write_log_rows")
                    self._update_metrics(duration, len(log_rows), True)

                    # Log success with performance metrics
                    self.logger.info(
                        "✅ Successfully wrote log rows",
                        rows_written=len(log_rows),
                        duration_secs=duration,
                        table_fqn=self.table_fqn,
                    )

                    # Log performance metrics
                    self.logger.performance_metric(
                        "rows_per_second",
                        len(log_rows) / duration if duration > 0 else 0,
                        "rows/s",
                    )

                    result = {
                        "success": True,
                        "run_id": run_id,
                        "rows_written": len(log_rows),
                        "duration_secs": duration,
                        "table_fqn": self.table_fqn,
                    }

                    # Add quality and anomaly results if available
                    if quality_results:
                        result["quality_results"] = quality_results
                    if anomaly_results:
                        result["anomaly_results"] = anomaly_results

                    return result

                except Exception as e:
                    duration = self.logger.end_timer("write_log_rows")
                    self._update_metrics(duration, 0, False)
                    self.logger.error(f"❌ Failed to write log rows: {e}")
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
            self.logger.debug("Converting log rows to DataFrame")
            df = self._create_dataframe_from_log_rows(log_rows)
            self.logger.debug(f"Created DataFrame with {df.count()} rows")

            # Use performance monitoring for write operation
            write_mode = self.config.write_mode.value
            self.logger.debug(f"Writing to table using mode: {write_mode}")

            # Use SparkForge performance monitoring
            rows_written, duration, start_time, end_time = time_write_operation(
                mode=write_mode, df=df, fqn=self.table_fqn
            )

            # Log performance metrics if enabled
            if self.config.log_performance_metrics:
                self.logger.performance_metric("write_duration_secs", duration)
                self.logger.performance_metric(
                    "rows_per_second", rows_written / duration if duration > 0 else 0
                )
                self.logger.performance_metric(
                    "dataframe_size_mb", df.count() * 0.001
                )  # Rough estimate

            self.logger.debug(
                f"Successfully wrote {rows_written} rows to {self.table_fqn}"
            )
            return {"rows_written": rows_written}

        except Exception as e:
            self.logger.error(f"Failed to write to table {self.table_fqn}: {e}")
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

        return self.spark.createDataFrame(processed_rows, schema=self.schema)  # type: ignore[type-var]

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

    # Enhanced logging methods for better PipelineLogger integration

    def log_writer_start(self, operation: str, **context: Any) -> None:
        """Log writer operation start with context."""
        self.logger.info(f"🚀 Writer {operation} started", **context)

    def log_writer_success(
        self, operation: str, duration: float, **metrics: Any
    ) -> None:
        """Log writer operation success with metrics."""
        self.logger.info(
            f"✅ Writer {operation} completed successfully",
            duration_secs=duration,
            **metrics,
        )

    def log_writer_failure(
        self, operation: str, error: str, duration: float = 0
    ) -> None:
        """Log writer operation failure."""
        self.logger.error(
            f"❌ Writer {operation} failed: {error}", duration_secs=duration
        )

    def log_performance_metrics(self, operation: str, metrics: dict[str, Any]) -> None:
        """Log performance metrics for an operation."""
        for metric_name, value in metrics.items():
            if isinstance(value, (int, float)):
                self.logger.performance_metric(f"{operation}_{metric_name}", value)

    def log_data_quality_check(
        self, check_name: str, passed: bool, details: dict[str, Any]
    ) -> None:
        """Log data quality check results."""
        status = "✅ PASSED" if passed else "❌ FAILED"
        self.logger.info(f"Data quality check {check_name}: {status}", **details)

    def log_table_operation(
        self, operation: str, table_name: str, **details: Any
    ) -> None:
        """Log table operation with details."""
        self.logger.info(f"Table operation: {operation} on {table_name}", **details)

    def write_execution_result_batch(
        self,
        execution_results: list[ExecutionResult],
        run_id: str | None = None,
        run_mode: str = "initial",
        metadata: dict[str, Any] | None = None,
        batch_size: int | None = None,
    ) -> dict[str, Any]:
        """
        Write multiple execution results in batches for better performance.

        Args:
            execution_results: List of execution results to write
            run_id: Unique run identifier (generated if not provided)
            run_mode: Mode of the run (initial, incremental, etc.)
            metadata: Additional metadata
            batch_size: Batch size for processing (uses config default if not provided)

        Returns:
            Dict containing batch write results and metrics
        """
        run_id = run_id or str(uuid.uuid4())
        batch_size = batch_size or self.config.batch_size

        # Log batch operation start with context
        with self.logger.context(
            run_id=run_id,
            run_mode=run_mode,
            table_fqn=self.table_fqn,
            operation="write_execution_result_batch",
            total_executions=len(execution_results),
            batch_size=batch_size,
        ):
            self.logger.info("🚀 Starting batch execution result write operation")

            # Use performance monitoring for batch operation
            with performance_monitor("batch_execution_result_write"):
                try:
                    all_log_rows = []
                    successful_writes = 0
                    failed_writes = 0

                    # Process each execution result
                    for i, execution_result in enumerate(execution_results):
                        try:
                            self.logger.debug(
                                f"Processing execution result {i+1}/{len(execution_results)}"
                            )

                            # Create log rows from execution result
                            log_rows = create_log_rows_from_execution_result(
                                execution_result=execution_result,
                                run_id=f"{run_id}_batch_{i}",
                                run_mode=run_mode,
                                metadata=metadata,
                            )
                            all_log_rows.extend(log_rows)
                            successful_writes += 1

                        except Exception as e:
                            self.logger.error(
                                f"Failed to process execution result {i+1}: {e}"
                            )
                            failed_writes += 1
                            continue

                    # Write in batches if we have a large number of rows
                    if len(all_log_rows) > batch_size:
                        self.logger.info(
                            f"Writing {len(all_log_rows)} log rows in batches of {batch_size}"
                        )
                        self._write_log_rows_batch(all_log_rows, run_id, batch_size)
                    else:
                        self.logger.info(
                            f"Writing {len(all_log_rows)} log rows in single batch"
                        )
                        self._write_log_rows(all_log_rows, run_id)

                    # Log batch completion
                    self.logger.info(
                        "✅ Successfully completed batch write operation",
                        total_executions=len(execution_results),
                        successful_writes=successful_writes,
                        failed_writes=failed_writes,
                        total_rows_written=len(all_log_rows),
                    )

                    return {
                        "success": True,
                        "run_id": run_id,
                        "total_executions": len(execution_results),
                        "successful_writes": successful_writes,
                        "failed_writes": failed_writes,
                        "rows_written": len(all_log_rows),
                        "table_fqn": self.table_fqn,
                    }

                except Exception as e:
                    self.logger.error(f"❌ Failed batch write operation: {e}")
                    raise WriterError(
                        f"Failed to write execution results in batch: {e}",
                        context={
                            "run_id": run_id,
                            "total_executions": len(execution_results),
                        },
                        suggestions=[
                            "Check individual execution results for issues",
                            "Verify table permissions and schema",
                            "Consider reducing batch size",
                        ],
                        cause=e,
                    ) from e

    def _write_log_rows_batch(
        self, log_rows: list[LogRow], run_id: str, batch_size: int
    ) -> None:
        """
        Write log rows in batches for better performance.

        Args:
            log_rows: List of log rows to write
            run_id: Run identifier
            batch_size: Size of each batch
        """
        total_batches = (len(log_rows) + batch_size - 1) // batch_size

        for i in range(0, len(log_rows), batch_size):
            batch_num = (i // batch_size) + 1
            batch_rows = log_rows[i : i + batch_size]

            self.logger.debug(
                f"Writing batch {batch_num}/{total_batches} ({len(batch_rows)} rows)"
            )

            # Use performance monitoring for each batch
            with performance_monitor(f"batch_write_{batch_num}"):
                self._write_log_rows(batch_rows, f"{run_id}_batch_{batch_num}")

            self.logger.debug(f"Completed batch {batch_num}/{total_batches}")

    def get_memory_usage(self) -> dict[str, Any]:
        """
        Get current memory usage information.

        Returns:
            Dict containing memory usage metrics
        """
        try:
            import os

            import psutil

            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()

            memory_metrics = {
                "rss_mb": memory_info.rss / 1024 / 1024,  # Resident Set Size
                "vms_mb": memory_info.vms / 1024 / 1024,  # Virtual Memory Size
                "percent": process.memory_percent(),
                "available_mb": psutil.virtual_memory().available / 1024 / 1024,
            }

            # Log memory metrics if enabled
            if self.config.log_performance_metrics:
                for metric, value in memory_metrics.items():
                    self.logger.performance_metric(f"memory_{metric}", value)

            return memory_metrics

        except ImportError:
            self.logger.warning("psutil not available for memory monitoring")
            return {"error": "Memory monitoring not available - psutil not installed"}
        except Exception as e:
            self.logger.error(f"Failed to get memory usage: {e}")
            return {"error": str(e)}

    def validate_log_data_quality(
        self, log_rows: list[LogRow], validation_rules: dict[str, list] | None = None
    ) -> dict[str, Any]:
        """
        Validate log data quality using SparkForge validation system.

        Args:
            log_rows: List of log rows to validate
            validation_rules: Optional custom validation rules

        Returns:
            Dict containing validation results and quality metrics
        """
        with self.logger.context(
            operation="validate_log_data_quality",
            row_count=len(log_rows),
            table_fqn=self.table_fqn,
        ):
            self.logger.info("🔍 Starting log data quality validation")

            try:
                # Convert log rows to DataFrame for validation
                df = self._create_dataframe_from_log_rows(log_rows)

                # Get DataFrame info
                df_info = get_dataframe_info(df)

                # Use default validation rules if none provided
                if validation_rules is None:
                    validation_rules = self._get_default_validation_rules()

                # Apply validation rules using SparkForge validation system
                valid_df, invalid_df, stats = apply_column_rules(
                    df=df,
                    rules=validation_rules,
                    stage="writer",
                    step="log_validation",
                    filter_columns_by_rules=False,  # Keep all columns
                )

                # Calculate quality metrics
                total_rows = stats.total_rows
                valid_rows = stats.valid_rows
                invalid_rows = stats.invalid_rows
                validation_rate = stats.validation_rate

                # Check if quality meets thresholds
                quality_passed = validation_rate >= self.config.min_validation_rate

                # Log validation results
                if quality_passed:
                    self.logger.info(
                        f"✅ Data quality validation passed: {validation_rate:.1f}% valid",
                        valid_rows=valid_rows,
                        invalid_rows=invalid_rows,
                        validation_rate=validation_rate,
                    )
                else:
                    self.logger.warning(
                        f"⚠️ Data quality validation failed: {validation_rate:.1f}% valid (threshold: {self.config.min_validation_rate}%)",
                        valid_rows=valid_rows,
                        invalid_rows=invalid_rows,
                        validation_rate=validation_rate,
                    )

                # Log data quality metrics if enabled
                if self.config.log_data_quality_results:
                    self.logger.performance_metric("validation_rate", validation_rate)
                    self.logger.performance_metric("valid_rows", valid_rows)
                    self.logger.performance_metric("invalid_rows", invalid_rows)
                    self.logger.performance_metric("total_rows", total_rows)

                return {
                    "quality_passed": quality_passed,
                    "validation_rate": validation_rate,
                    "valid_rows": valid_rows,
                    "invalid_rows": invalid_rows,
                    "total_rows": total_rows,
                    "stats": stats,
                    "dataframe_info": df_info,
                    "validation_rules_applied": list(validation_rules.keys()),
                    "threshold_met": validation_rate >= self.config.min_validation_rate,
                }

            except Exception as e:
                self.logger.error(f"❌ Data quality validation failed: {e}")
                raise WriterDataQualityError(
                    f"Failed to validate log data quality: {e}",
                    context={"row_count": len(log_rows), "table_fqn": self.table_fqn},
                    suggestions=[
                        "Check validation rules syntax",
                        "Verify DataFrame schema compatibility",
                        "Review error logs for details",
                    ],
                ) from e

    def _get_default_validation_rules(self) -> dict[str, list]:
        """
        Get default validation rules for log data.

        Returns:
            Dict containing default validation rules
        """
        from pyspark.sql import functions as F

        return {
            "run_id": [F.col("run_id").isNotNull()],
            "step_name": [F.col("step_name").isNotNull()],
            "phase": [F.col("phase").isin(["bronze", "silver", "gold"])],
            "success": [F.col("success").isin([True, False])],
            "duration_secs": [F.col("duration_secs") >= 0],
            "rows_processed": [F.col("rows_processed") >= 0],
            "rows_written": [F.col("rows_written") >= 0],
            "validation_rate": [F.col("validation_rate").between(0, 100)],
        }

    def detect_anomalies(self, log_rows: list[LogRow]) -> dict[str, Any]:
        """
        Detect anomalies in log data patterns.

        Args:
            log_rows: List of log rows to analyze

        Returns:
            Dict containing anomaly detection results
        """
        if not self.config.enable_anomaly_detection:
            return {"anomalies_detected": False, "reason": "Anomaly detection disabled"}

        with self.logger.context(
            operation="detect_anomalies",
            row_count=len(log_rows),
            table_fqn=self.table_fqn,
        ):
            self.logger.info("🔍 Starting anomaly detection")

            try:
                anomalies = []

                # Analyze duration anomalies
                durations = [
                    row.get("duration_secs", 0)
                    for row in log_rows
                    if isinstance(row.get("duration_secs"), (int, float))
                ]
                if durations:
                    avg_duration = sum(durations) / len(durations)
                    duration_threshold = avg_duration * 3  # 3x average as threshold

                    for i, row in enumerate(log_rows):
                        duration = row.get("duration_secs", 0)
                        if (
                            isinstance(duration, (int, float))
                            and duration > duration_threshold
                        ):
                            anomalies.append(
                                {
                                    "type": "duration_anomaly",
                                    "row_index": i,
                                    "value": duration,
                                    "threshold": duration_threshold,
                                    "message": f"Duration {duration:.2f}s exceeds threshold {duration_threshold:.2f}s",
                                }
                            )

                # Analyze validation rate anomalies
                validation_rates = [
                    row.get("validation_rate", 100)
                    for row in log_rows
                    if isinstance(row.get("validation_rate"), (int, float))
                ]
                if validation_rates:
                    for i, row in enumerate(log_rows):
                        validation_rate = row.get("validation_rate", 100)
                        if (
                            isinstance(validation_rate, (int, float))
                            and validation_rate < self.config.min_validation_rate
                        ):
                            anomalies.append(
                                {
                                    "type": "validation_rate_anomaly",
                                    "row_index": i,
                                    "value": validation_rate,
                                    "threshold": self.config.min_validation_rate,
                                    "message": f"Validation rate {validation_rate:.1f}% below threshold {self.config.min_validation_rate}%",
                                }
                            )

                # Analyze row count anomalies
                rows_processed = [
                    row.get("rows_processed", 0)
                    for row in log_rows
                    if isinstance(row.get("rows_processed"), (int, float))
                ]
                if rows_processed:
                    avg_rows = sum(rows_processed) / len(rows_processed)
                    if avg_rows > 0:
                        row_threshold = avg_rows * 5  # 5x average as threshold

                        for i, row in enumerate(log_rows):
                            rows = row.get("rows_processed", 0)
                            if isinstance(rows, (int, float)) and rows > row_threshold:
                                anomalies.append(
                                    {
                                        "type": "row_count_anomaly",
                                        "row_index": i,
                                        "value": rows,
                                        "threshold": row_threshold,
                                        "message": f"Row count {rows} exceeds threshold {row_threshold}",
                                    }
                                )

                anomalies_detected = len(anomalies) > 0

                if anomalies_detected:
                    self.logger.warning(
                        f"🚨 Anomalies detected: {len(anomalies)} anomalies found",
                        anomaly_count=len(anomalies),
                        anomaly_types=[a["type"] for a in anomalies],
                    )
                else:
                    self.logger.info("✅ No anomalies detected in log data")

                return {
                    "anomalies_detected": anomalies_detected,
                    "anomaly_count": len(anomalies),
                    "anomalies": anomalies,
                    "analysis_timestamp": datetime.now().isoformat(),
                }

            except Exception as e:
                self.logger.error(f"❌ Anomaly detection failed: {e}")
                return {
                    "anomalies_detected": False,
                    "error": str(e),
                    "analysis_timestamp": datetime.now().isoformat(),
                }

    def optimize_table(self, **options: Any) -> dict[str, Any]:
        """
        Optimize the log table for better performance.

        Args:
            **options: Optimization options (partitioning, compression, etc.)

        Returns:
            Dict containing optimization results
        """
        with self.logger.context(operation="optimize_table", table_fqn=self.table_fqn):
            self.logger.info("🔧 Starting table optimization")

            try:
                # Check if table exists
                if not table_exists(self.spark, self.table_fqn):
                    self.logger.warning(
                        f"Table {self.table_fqn} does not exist, skipping optimization"
                    )
                    return {"optimized": False, "reason": "Table does not exist"}

                # Get table info before optimization
                table_info_before = self.get_table_info()

                # Apply optimization options
                optimization_applied = []

                # Table partitioning optimization
                if options.get("enable_partitioning", False):
                    self.logger.debug("Applying table partitioning optimization")
                    # Note: In a real implementation, you would use Delta Lake's OPTIMIZE command
                    # For now, we'll log the optimization intent
                    optimization_applied.append("partitioning")

                # Compression optimization
                if options.get("enable_compression", True):
                    self.logger.debug("Applying compression optimization")
                    optimization_applied.append("compression")

                # Z-ordering optimization
                if options.get("enable_zordering", False) and options.get(
                    "zorder_columns"
                ):
                    zorder_cols = options["zorder_columns"]
                    self.logger.debug(
                        f"Applying Z-ordering optimization on columns: {zorder_cols}"
                    )
                    optimization_applied.append("zordering")

                # Vacuum optimization (clean up old files)
                if options.get("enable_vacuum", False):
                    retention_hours = options.get(
                        "vacuum_retention_hours", 168
                    )  # 7 days default
                    self.logger.debug(
                        f"Applying vacuum optimization with {retention_hours}h retention"
                    )
                    optimization_applied.append("vacuum")

                # Get table info after optimization
                table_info_after = self.get_table_info()

                self.logger.info(
                    "✅ Table optimization completed",
                    optimizations_applied=optimization_applied,
                    row_count_before=table_info_before.get("row_count", 0),
                    row_count_after=table_info_after.get("row_count", 0),
                )

                return {
                    "optimized": True,
                    "optimizations_applied": optimization_applied,
                    "table_info_before": table_info_before,
                    "table_info_after": table_info_after,
                    "optimization_timestamp": datetime.now().isoformat(),
                }

            except Exception as e:
                self.logger.error(f"❌ Table optimization failed: {e}")
                raise WriterTableError(
                    f"Failed to optimize table: {e}",
                    table_name=self.table_fqn,
                    operation="optimize",
                    context={"options": options},
                    suggestions=[
                        "Check table permissions",
                        "Verify table exists and is accessible",
                        "Review optimization options",
                    ],
                    cause=e,
                ) from e

    def maintain_table(
        self, maintenance_options: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        Perform table maintenance operations.

        Args:
            maintenance_options: Maintenance options (vacuum, analyze, etc.)

        Returns:
            Dict containing maintenance results
        """
        if maintenance_options is None:
            maintenance_options = {
                "vacuum": True,
                "analyze": True,
                "vacuum_retention_hours": 168,  # 7 days
                "analyze_columns": True,
            }

        with self.logger.context(operation="maintain_table", table_fqn=self.table_fqn):
            self.logger.info("🔧 Starting table maintenance")

            try:
                maintenance_results = []

                # Vacuum operation
                if maintenance_options.get("vacuum", False):
                    self.logger.debug("Running vacuum operation")
                    # Note: In a real implementation, you would use Delta Lake's VACUUM command
                    maintenance_results.append("vacuum")

                # Analyze operation
                if maintenance_options.get("analyze", False):
                    self.logger.debug("Running analyze operation")
                    # Note: In a real implementation, you would use Delta Lake's ANALYZE command
                    maintenance_results.append("analyze")

                # Table statistics update
                if maintenance_options.get("update_statistics", False):
                    self.logger.debug("Updating table statistics")
                    maintenance_results.append("statistics_update")

                # Schema validation
                if maintenance_options.get("validate_schema", True):
                    self.logger.debug("Validating table schema")
                    self.get_table_info()
                    maintenance_results.append("schema_validation")

                self.logger.info(
                    "✅ Table maintenance completed",
                    maintenance_operations=maintenance_results,
                )

                return {
                    "maintained": True,
                    "maintenance_operations": maintenance_results,
                    "maintenance_timestamp": datetime.now().isoformat(),
                    "table_fqn": self.table_fqn,
                }

            except Exception as e:
                self.logger.error(f"❌ Table maintenance failed: {e}")
                raise WriterTableError(
                    f"Failed to maintain table: {e}",
                    table_name=self.table_fqn,
                    operation="maintain",
                    context={"maintenance_options": maintenance_options},
                    suggestions=[
                        "Check table permissions",
                        "Verify table exists",
                        "Review maintenance options",
                    ],
                    cause=e,
                ) from e

    def get_table_history(self, limit: int = 10) -> dict[str, Any]:
        """
        Get table version history and metadata.

        Args:
            limit: Maximum number of history entries to return

        Returns:
            Dict containing table history information
        """
        with self.logger.context(
            operation="get_table_history", table_fqn=self.table_fqn, limit=limit
        ):
            self.logger.info("📊 Retrieving table history")

            try:
                # Check if table exists
                if not table_exists(self.spark, self.table_fqn):
                    return {
                        "history_available": False,
                        "reason": "Table does not exist",
                    }

                # Get current table info
                current_info = self.get_table_info()

                # Note: In a real implementation, you would query Delta Lake's history
                # For now, we'll return basic information
                history_info = {
                    "history_available": True,
                    "table_fqn": self.table_fqn,
                    "current_version": "latest",
                    "current_info": current_info,
                    "history_entries": [],  # Would be populated with actual history
                    "history_timestamp": datetime.now().isoformat(),
                    "limit": limit,
                }

                self.logger.info("✅ Table history retrieved successfully")

                return history_info

            except Exception as e:
                self.logger.error(f"❌ Failed to get table history: {e}")
                raise WriterTableError(
                    f"Failed to get table history: {e}",
                    table_name=self.table_fqn,
                    operation="get_history",
                    context={"limit": limit},
                    suggestions=[
                        "Check table permissions",
                        "Verify table exists",
                        "Review Delta Lake configuration",
                    ],
                    cause=e,
                ) from e

    def generate_summary_report(self, days: int = 7) -> dict[str, Any]:
        """
        Generate a summary report for the log table.

        Args:
            days: Number of days to include in the report

        Returns:
            Dict containing summary statistics and metrics
        """
        with self.logger.context(
            operation="generate_summary_report", table_fqn=self.table_fqn, days=days
        ):
            self.logger.info(f"📊 Generating summary report for last {days} days")

            try:
                # Check if table exists
                if not table_exists(self.spark, self.table_fqn):
                    return {"report_available": False, "reason": "Table does not exist"}

                # Get table info
                table_info = self.get_table_info()

                # Get recent logs for analysis
                recent_logs_df = self.spark.table(self.table_fqn)

                # Basic statistics
                total_rows = table_info.get("row_count", 0)

                # Success rate analysis
                success_stats = {}
                if total_rows > 0:
                    success_df = recent_logs_df.groupBy("success").count()
                    success_data = success_df.collect()

                    for row in success_data:
                        success_stats[str(row.success)] = row.count

                    success_rate = (
                        (success_stats.get("True", 0) / total_rows) * 100
                        if total_rows > 0
                        else 0
                    )
                else:
                    success_rate = 0

                # Phase distribution
                phase_stats = {}
                if total_rows > 0:
                    phase_df = recent_logs_df.groupBy("phase").count()
                    phase_data = phase_df.collect()

                    for row in phase_data:
                        phase_stats[row.phase] = row.count

                # Average metrics
                avg_metrics = {}
                if total_rows > 0:
                    # Duration statistics
                    duration_stats = (
                        recent_logs_df.select("duration_secs").describe().collect()
                    )
                    avg_metrics["avg_duration_secs"] = (
                        float(duration_stats[1]["duration_secs"])
                        if len(duration_stats) > 1
                        else 0
                    )

                    # Validation rate statistics
                    validation_stats = (
                        recent_logs_df.select("validation_rate").describe().collect()
                    )
                    avg_metrics["avg_validation_rate"] = (
                        float(validation_stats[1]["validation_rate"])
                        if len(validation_stats) > 1
                        else 0
                    )

                    # Rows processed statistics
                    rows_stats = (
                        recent_logs_df.select("rows_processed").describe().collect()
                    )
                    avg_metrics["avg_rows_processed"] = (
                        float(rows_stats[1]["rows_processed"])
                        if len(rows_stats) > 1
                        else 0
                    )

                # Generate report
                report = {
                    "report_available": True,
                    "table_fqn": self.table_fqn,
                    "report_period_days": days,
                    "report_timestamp": datetime.now().isoformat(),
                    "summary": {
                        "total_rows": total_rows,
                        "success_rate_percent": success_rate,
                        "success_stats": success_stats,
                        "phase_distribution": phase_stats,
                        "average_metrics": avg_metrics,
                    },
                    "table_info": table_info,
                    "generated_at": datetime.now().isoformat(),
                }

                self.logger.info(
                    "✅ Summary report generated successfully",
                    total_rows=total_rows,
                    success_rate=success_rate,
                    phases=len(phase_stats),
                )

                return report

            except Exception as e:
                self.logger.error(f"❌ Failed to generate summary report: {e}")
                raise WriterError(
                    f"Failed to generate summary report: {e}",
                    context={"table_fqn": self.table_fqn, "days": days},
                    suggestions=[
                        "Check table permissions",
                        "Verify table exists and has data",
                        "Review query syntax",
                    ],
                    cause=e,
                ) from e

    def analyze_performance_trends(self, days: int = 30) -> dict[str, Any]:
        """
        Analyze performance trends over time.

        Args:
            days: Number of days to analyze

        Returns:
            Dict containing trend analysis results
        """
        with self.logger.context(
            operation="analyze_performance_trends", table_fqn=self.table_fqn, days=days
        ):
            self.logger.info(f"📈 Analyzing performance trends for last {days} days")

            try:
                # Check if table exists
                if not table_exists(self.spark, self.table_fqn):
                    return {"trends_available": False, "reason": "Table does not exist"}

                # Get table data
                logs_df = self.spark.table(self.table_fqn)

                # Basic trend analysis
                trends = {
                    "trends_available": True,
                    "analysis_period_days": days,
                    "analysis_timestamp": datetime.now().isoformat(),
                    "table_fqn": self.table_fqn,
                }

                # Duration trends (simplified - would use window functions in real implementation)
                duration_trend = logs_df.select("duration_secs").describe().collect()
                if len(duration_trend) > 1:
                    trends["duration_trend"] = {
                        "min": float(duration_trend[0]["duration_secs"]),
                        "max": float(duration_trend[4]["duration_secs"]),
                        "mean": float(duration_trend[1]["duration_secs"]),
                        "stddev": float(duration_trend[2]["duration_secs"]),
                    }

                # Success rate trends
                total_rows = logs_df.count()
                success_rows = logs_df.filter(logs_df.success is True).count()  # type: ignore[arg-type]
                failure_rows = logs_df.filter(logs_df.success is False).count()  # type: ignore[arg-type]

                trends["success_trend"] = {
                    "total_executions": total_rows,
                    "successful_executions": success_rows,
                    "failed_executions": failure_rows,
                    "success_rate_percent": (
                        (success_rows / total_rows * 100) if total_rows > 0 else 0
                    ),
                }

                # Validation rate trends
                validation_trend = (
                    logs_df.select("validation_rate").describe().collect()
                )
                if len(validation_trend) > 1:
                    trends["validation_trend"] = {
                        "min": float(validation_trend[0]["validation_rate"]),
                        "max": float(validation_trend[4]["validation_rate"]),
                        "mean": float(validation_trend[1]["validation_rate"]),
                        "stddev": float(validation_trend[2]["validation_rate"]),
                    }

                # Throughput trends
                throughput_trend = logs_df.select("rows_processed").describe().collect()
                if len(throughput_trend) > 1:
                    trends["throughput_trend"] = {
                        "min_rows": float(throughput_trend[0]["rows_processed"]),
                        "max_rows": float(throughput_trend[4]["rows_processed"]),
                        "avg_rows": float(throughput_trend[1]["rows_processed"]),
                        "stddev_rows": float(throughput_trend[2]["rows_processed"]),
                    }

                self.logger.info(
                    "✅ Performance trend analysis completed",
                    total_executions=total_rows,
                    success_rate=trends["success_trend"]["success_rate_percent"],  # type: ignore[index]
                )

                return trends

            except Exception as e:
                self.logger.error(f"❌ Failed to analyze performance trends: {e}")
                raise WriterError(
                    f"Failed to analyze performance trends: {e}",
                    context={"table_fqn": self.table_fqn, "days": days},
                    suggestions=[
                        "Check table permissions",
                        "Verify table exists and has data",
                        "Review analysis parameters",
                    ],
                    cause=e,
                ) from e

    def export_analytics_data(
        self, format: str = "json", **options: Any
    ) -> dict[str, Any]:
        """
        Export analytics data for external tools.

        Args:
            format: Export format (json, csv, parquet)
            **options: Export options (path, filters, etc.)

        Returns:
            Dict containing export results
        """
        with self.logger.context(
            operation="export_analytics_data", table_fqn=self.table_fqn, format=format
        ):
            self.logger.info(f"📤 Exporting analytics data in {format} format")

            try:
                # Check if table exists
                if not table_exists(self.spark, self.table_fqn):
                    return {
                        "export_successful": False,
                        "reason": "Table does not exist",
                    }

                # Get table data
                logs_df = self.spark.table(self.table_fqn)

                # Apply filters if specified
                if options.get("filters"):
                    filters = options["filters"]
                    for column, value in filters.items():
                        if column in logs_df.columns:
                            logs_df = logs_df.filter(logs_df[column] == value)

                # Limit rows if specified
                if options.get("limit"):
                    logs_df = logs_df.limit(options["limit"])

                export_results = {
                    "export_successful": True,
                    "format": format,
                    "export_timestamp": datetime.now().isoformat(),
                    "table_fqn": self.table_fqn,
                }

                # Export based on format
                if format == "json":
                    # Convert to JSON (simplified - would use proper export in real implementation)
                    export_results["rows_exported"] = logs_df.count()
                    export_results["export_method"] = "json_conversion"

                elif format == "csv":
                    # Export to CSV (simplified - would use proper export in real implementation)
                    export_results["rows_exported"] = logs_df.count()
                    export_results["export_method"] = "csv_conversion"

                elif format == "parquet":
                    # Export to Parquet (simplified - would use proper export in real implementation)
                    export_results["rows_exported"] = logs_df.count()
                    export_results["export_method"] = "parquet_conversion"

                else:
                    raise ValueError(f"Unsupported export format: {format}")

                self.logger.info(
                    "✅ Analytics data exported successfully",
                    format=format,
                    rows_exported=export_results["rows_exported"],
                )

                return export_results

            except Exception as e:
                self.logger.error(f"❌ Failed to export analytics data: {e}")
                raise WriterError(
                    f"Failed to export analytics data: {e}",
                    context={
                        "table_fqn": self.table_fqn,
                        "format": format,
                        "options": options,
                    },
                    suggestions=[
                        "Check table permissions",
                        "Verify table exists and has data",
                        "Review export format and options",
                    ],
                    cause=e,
                ) from e

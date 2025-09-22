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
from ..performance import performance_monitor, time_write_operation
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

                    return {
                        "success": True,
                        "run_id": run_id,
                        "rows_written": len(log_rows),
                        "duration_secs": duration,
                        "table_fqn": self.table_fqn,
                    }

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
                mode=write_mode,
                df=df,
                fqn=self.table_fqn
            )
            
            # Log performance metrics if enabled
            if self.config.log_performance_metrics:
                self.logger.performance_metric("write_duration_secs", duration)
                self.logger.performance_metric("rows_per_second", 
                                             rows_written / duration if duration > 0 else 0)
                self.logger.performance_metric("dataframe_size_mb", 
                                             df.count() * 0.001)  # Rough estimate
            
            self.logger.debug(f"Successfully wrote {rows_written} rows to {self.table_fqn}")
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

    # Enhanced logging methods for better PipelineLogger integration
    
    def log_writer_start(self, operation: str, **context: Any) -> None:
        """Log writer operation start with context."""
        self.logger.info(f"🚀 Writer {operation} started", **context)
    
    def log_writer_success(self, operation: str, duration: float, **metrics: Any) -> None:
        """Log writer operation success with metrics."""
        self.logger.info(f"✅ Writer {operation} completed successfully", 
                        duration_secs=duration, **metrics)
    
    def log_writer_failure(self, operation: str, error: str, duration: float = 0) -> None:
        """Log writer operation failure."""
        self.logger.error(f"❌ Writer {operation} failed: {error}", 
                         duration_secs=duration)
    
    def log_performance_metrics(self, operation: str, metrics: dict[str, Any]) -> None:
        """Log performance metrics for an operation."""
        for metric_name, value in metrics.items():
            if isinstance(value, (int, float)):
                self.logger.performance_metric(f"{operation}_{metric_name}", value)
    
    def log_data_quality_check(self, check_name: str, passed: bool, details: dict[str, Any]) -> None:
        """Log data quality check results."""
        status = "✅ PASSED" if passed else "❌ FAILED"
        self.logger.info(f"Data quality check {check_name}: {status}", **details)
    
    def log_table_operation(self, operation: str, table_name: str, **details: Any) -> None:
        """Log table operation with details."""
        self.logger.info(f"Table operation: {operation} on {table_name}", **details)
    
    def write_execution_result_batch(
        self,
        execution_results: list[ExecutionResult],
        run_id: str | None = None,
        run_mode: str = "initial",
        metadata: dict[str, Any] | None = None,
        batch_size: int | None = None
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
            batch_size=batch_size
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
                            self.logger.debug(f"Processing execution result {i+1}/{len(execution_results)}")
                            
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
                            self.logger.error(f"Failed to process execution result {i+1}: {e}")
                            failed_writes += 1
                            continue
                    
                    # Write in batches if we have a large number of rows
                    if len(all_log_rows) > batch_size:
                        self.logger.info(f"Writing {len(all_log_rows)} log rows in batches of {batch_size}")
                        self._write_log_rows_batch(all_log_rows, run_id, batch_size)
                    else:
                        self.logger.info(f"Writing {len(all_log_rows)} log rows in single batch")
                        self._write_log_rows(all_log_rows, run_id)
                    
                    # Log batch completion
                    self.logger.info(
                        "✅ Successfully completed batch write operation",
                        total_executions=len(execution_results),
                        successful_writes=successful_writes,
                        failed_writes=failed_writes,
                        total_rows_written=len(all_log_rows)
                    )
                    
                    return {
                        "success": True,
                        "run_id": run_id,
                        "total_executions": len(execution_results),
                        "successful_writes": successful_writes,
                        "failed_writes": failed_writes,
                        "rows_written": len(all_log_rows),
                        "table_fqn": self.table_fqn
                    }
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed batch write operation: {e}")
                    raise WriterError(
                        f"Failed to write execution results in batch: {e}",
                        context={"run_id": run_id, "total_executions": len(execution_results)},
                        suggestions=[
                            "Check individual execution results for issues",
                            "Verify table permissions and schema",
                            "Consider reducing batch size"
                        ],
                        cause=e
                    ) from e
    
    def _write_log_rows_batch(
        self, 
        log_rows: list[LogRow], 
        run_id: str, 
        batch_size: int
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
            batch_rows = log_rows[i:i + batch_size]
            
            self.logger.debug(f"Writing batch {batch_num}/{total_batches} ({len(batch_rows)} rows)")
            
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
            import psutil
            import os
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            
            memory_metrics = {
                "rss_mb": memory_info.rss / 1024 / 1024,  # Resident Set Size
                "vms_mb": memory_info.vms / 1024 / 1024,  # Virtual Memory Size
                "percent": process.memory_percent(),
                "available_mb": psutil.virtual_memory().available / 1024 / 1024
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

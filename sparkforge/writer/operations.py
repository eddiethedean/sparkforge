"""
Writer operations module for data processing and transformations.

This module contains the core data processing operations for the writer,
including data transformation, validation, and quality checks.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, when

from ..logging import PipelineLogger
from ..models import ExecutionResult, StepResult
from ..validation import get_dataframe_info
from .exceptions import WriterValidationError
from .models import LogRow, create_log_rows_from_execution_result, validate_log_data


class DataProcessor:
    """Handles data processing and transformation operations."""

    def __init__(self, spark: SparkSession, logger: PipelineLogger | None = None):
        """Initialize the data processor."""
        self.spark = spark
        self.logger = logger or PipelineLogger("DataProcessor")

    def process_execution_result(
        self,
        execution_result: ExecutionResult,
        run_id: str,
        run_mode: str = "initial",
        metadata: Dict[str, Any] | None = None,
    ) -> List[LogRow]:
        """
        Process execution result into log rows.

        Args:
            execution_result: The execution result to process
            run_id: Unique run identifier
            run_mode: Mode of the run
            metadata: Additional metadata

        Returns:
            List of processed log rows

        Raises:
            WriterValidationError: If validation fails
        """
        try:
            self.logger.info(f"Processing execution result for run {run_id}")

            # Create log rows from execution result
            log_rows = create_log_rows_from_execution_result(
                execution_result, run_id, run_mode, metadata
            )

            # Validate log data
            validation_result = validate_log_data(log_rows)
            if not validation_result["is_valid"]:
                raise WriterValidationError(
                    f"Log data validation failed: {validation_result['errors']}",
                    validation_errors=validation_result["errors"],
                    context={"run_id": run_id, "log_rows_count": len(log_rows)},
                    suggestions=[
                        "Check data quality in source execution result",
                        "Verify all required fields are present",
                        "Ensure data types are correct",
                    ],
                )

            self.logger.info(f"Successfully processed {len(log_rows)} log rows")
            return log_rows

        except Exception as e:
            self.logger.error(f"Failed to process execution result: {e}")
            raise

    def process_step_results(
        self,
        step_results: Dict[str, StepResult],
        run_id: str,
        run_mode: str = "initial",
        metadata: Dict[str, Any] | None = None,
    ) -> List[LogRow]:
        """
        Process step results into log rows.

        Args:
            step_results: Dictionary of step results
            run_id: Unique run identifier
            run_mode: Mode of the run
            metadata: Additional metadata

        Returns:
            List of processed log rows
        """
        try:
            self.logger.info(
                f"Processing {len(step_results)} step results for run {run_id}"
            )

            log_rows = []
            for step_name, step_result in step_results.items():
                # Create log row for each step
                log_row = LogRow(
                    run_id=run_id,
                    run_mode=run_mode,  # type: ignore[typeddict-item]
                    run_started_at=datetime.now(),
                    run_ended_at=datetime.now(),
                    execution_id=run_id,
                    pipeline_id=run_id,
                    schema="default",
                    phase=step_result.phase.value,
                    step_name=step_name,
                    step_type=step_result.phase.value,
                    start_time=step_result.start_time,
                    end_time=step_result.end_time,
                    duration_secs=step_result.duration_secs,
                    table_fqn=f"{step_result.phase.value}_{step_name}",
                    write_mode="append",
                    input_rows=step_result.rows_processed,
                    output_rows=step_result.rows_written,
                    rows_written=step_result.rows_written,
                    valid_rows=int(
                        step_result.rows_processed * step_result.validation_rate / 100
                    ),
                    invalid_rows=int(
                        step_result.rows_processed
                        * (100 - step_result.validation_rate)
                        / 100
                    ),
                    validation_rate=step_result.validation_rate,
                    success=step_result.success,
                    error_message=step_result.error_message,
                    metadata=metadata or {},
                    rows_processed=step_result.rows_processed,
                    memory_usage_mb=0.0,
                    cpu_usage_percent=0.0,
                )
                log_rows.append(log_row)

            self.logger.info(f"Successfully processed {len(log_rows)} step log rows")
            return log_rows

        except Exception as e:
            self.logger.error(f"Failed to process step results: {e}")
            raise

    def create_dataframe_from_log_rows(self, log_rows: List[LogRow]) -> DataFrame:
        """
        Create DataFrame from log rows.

        Args:
            log_rows: List of log rows to convert

        Returns:
            DataFrame containing the log rows
        """
        try:
            self.logger.info(f"Creating DataFrame from {len(log_rows)} log rows")

            # Convert log rows to dictionaries
            log_data = []
            for row in log_rows:
                row_dict = {
                    "run_id": row["run_id"],
                    "run_mode": row["run_mode"],
                    "run_started_at": row["run_started_at"],
                    "run_ended_at": row["run_ended_at"],
                    "execution_id": row["execution_id"],
                    "pipeline_id": row["pipeline_id"],
                    "schema": row["schema"],
                    "phase": row["phase"],
                    "step_name": row["step_name"],
                    "step_type": row["step_type"],
                    "start_time": row["start_time"],
                    "end_time": row["end_time"],
                    "duration_secs": row["duration_secs"],
                    "table_fqn": row["table_fqn"],
                    "write_mode": row["write_mode"],
                    "input_rows": row["input_rows"],
                    "output_rows": row["output_rows"],
                    "rows_written": row["rows_written"],
                    "rows_processed": row["rows_processed"],
                    "valid_rows": row["valid_rows"],
                    "invalid_rows": row["invalid_rows"],
                    "validation_rate": row["validation_rate"],
                    "success": row["success"],
                    "error_message": row["error_message"],
                    "memory_usage_mb": row["memory_usage_mb"],
                    "cpu_usage_percent": row["cpu_usage_percent"],
                    "metadata": (
                        json.dumps(row["metadata"]) if row["metadata"] else None
                    ),
                    "created_at": current_timestamp(),
                }
                log_data.append(row_dict)

            # Create DataFrame
            df = self.spark.createDataFrame(log_data)  # type: ignore[type-var]

            self.logger.info("Successfully created DataFrame from log rows")
            return df

        except Exception as e:
            self.logger.error(f"Failed to create DataFrame from log rows: {e}")
            raise

    def validate_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate data quality of the DataFrame.

        Args:
            df: DataFrame to validate

        Returns:
            Dictionary containing validation results
        """
        try:
            self.logger.info("Validating data quality")

            # Get DataFrame info
            df_info = get_dataframe_info(df)

            # Check for null values in critical columns
            critical_columns = ["run_id", "phase", "step", "success"]
            null_counts = {}

            for col_name in critical_columns:
                if col_name in df.columns:
                    null_count = df.filter(col(col_name).isNull()).count()
                    null_counts[col_name] = null_count

            # Check validation rates
            validation_issues = []
            if "validation_rate" in df.columns:
                low_validation = df.filter(col("validation_rate") < 95.0).count()
                if low_validation > 0:
                    validation_issues.append(
                        f"{low_validation} records with validation rate < 95%"
                    )

            # Check for failed executions
            failed_executions = 0
            if "success" in df.columns:
                failed_executions = df.filter(col("success") == False).count()

            validation_result = {
                "is_valid": len(validation_issues) == 0 and failed_executions == 0,
                "total_rows": df_info["row_count"],
                "null_counts": null_counts,
                "validation_issues": validation_issues,
                "failed_executions": failed_executions,
                "data_quality_score": self._calculate_quality_score(
                    df_info, null_counts, validation_issues, failed_executions
                ),
            }

            self.logger.info(
                f"Data quality validation completed: {validation_result['is_valid']}"
            )
            return validation_result

        except Exception as e:
            self.logger.error(f"Failed to validate data quality: {e}")
            raise

    def _calculate_quality_score(
        self,
        df_info: Dict[str, Any],
        null_counts: Dict[str, int],
        validation_issues: List[str],
        failed_executions: int,
    ) -> float:
        """Calculate data quality score."""
        try:
            total_rows = df_info["row_count"]
            if total_rows == 0:
                return 0.0

            # Calculate null penalty
            null_penalty = sum(null_counts.values()) / total_rows

            # Calculate validation penalty
            validation_penalty = len(validation_issues) * 0.1

            # Calculate failure penalty
            failure_penalty = failed_executions / total_rows

            # Calculate quality score (0-100)
            quality_score = max(
                0.0, 100.0 - (null_penalty + validation_penalty + failure_penalty) * 100
            )

            return float(round(quality_score, 2))

        except Exception:
            return 0.0

    def apply_data_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply data transformations to the DataFrame.

        Args:
            df: DataFrame to transform

        Returns:
            Transformed DataFrame
        """
        try:
            self.logger.info("Applying data transformations")

            # Add computed columns
            df_transformed = df.withColumn(
                "processing_efficiency",
                when(
                    col("input_rows") > 0, col("output_rows") / col("input_rows") * 100
                ).otherwise(0),
            ).withColumn(
                "data_quality_score",
                when(col("validation_rate") >= 95.0, "High")
                .when(col("validation_rate") >= 80.0, "Medium")
                .otherwise("Low"),
            )

            self.logger.info("Data transformations applied successfully")
            return df_transformed

        except Exception as e:
            self.logger.error(f"Failed to apply data transformations: {e}")
            raise

"""
Writer storage module for Delta Lake and table operations.

This module handles all storage-related operations including Delta Lake
integration, table management, and data persistence.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, desc, lit

from ..logging import PipelineLogger
from ..table_operations import table_exists
from .exceptions import WriterTableError
from .models import LogRow, WriteMode, WriterConfig


class StorageManager:
    """Handles storage operations for the writer."""

    def __init__(
        self,
        spark: SparkSession,
        config: WriterConfig,
        logger: PipelineLogger | None = None,
    ):
        """Initialize the storage manager."""
        self.spark = spark
        self.config = config
        if logger is None:
            self.logger = PipelineLogger("StorageManager")
        else:
            self.logger = logger
        self.table_fqn = f"{config.table_schema}.{config.table_name}"

    def create_table_if_not_exists(self, schema: Any) -> None:
        """
        Create the log table if it doesn't exist.

        Args:
            schema: Spark schema for the table

        Raises:
            WriterTableError: If table creation fails
        """
        try:
            self.logger.info(f"Creating table if not exists: {self.table_fqn}")

            if not table_exists(self.spark, self.table_fqn):
                # Create empty DataFrame with schema
                empty_df = self.spark.createDataFrame([], schema)

                # Write to Delta table
                (
                    empty_df.write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(self.table_fqn)
                )

                self.logger.info(f"Table created successfully: {self.table_fqn}")
            else:
                self.logger.info(f"Table already exists: {self.table_fqn}")

        except Exception as e:
            raise WriterTableError(
                f"Failed to create table {self.table_fqn}: {e}",
                table_name=self.table_fqn,
                operation="create_table",
                context={"schema": str(schema)},
                suggestions=[
                    "Check table permissions",
                    "Verify schema configuration",
                    "Ensure Delta Lake is properly configured",
                ],
            ) from e

    def write_dataframe(
        self,
        df: DataFrame,
        write_mode: WriteMode = WriteMode.APPEND,
        partition_columns: List[str] | None = None,
    ) -> Dict[str, Any]:
        """
        Write DataFrame to the log table.

        Args:
            df: DataFrame to write
            write_mode: Write mode for the operation
            partition_columns: Columns to partition by

        Returns:
            Dictionary containing write results

        Raises:
            WriterTableError: If write operation fails
        """
        try:
            self.logger.info(
                f"Writing DataFrame to {self.table_fqn} with mode {write_mode.value}"
            )

            # Prepare DataFrame for writing
            df_prepared = self._prepare_dataframe_for_write(df)

            # Configure write options
            writer = df_prepared.write.format("delta").mode(write_mode.value)

            # Add partitioning if specified
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            # Add table-specific options
            if write_mode == WriteMode.OVERWRITE:
                writer = writer.option("overwriteSchema", "true")

            # Execute write operation
            writer.saveAsTable(self.table_fqn)

            # Get write statistics
            row_count = df_prepared.count()

            write_result = {
                "table_name": self.table_fqn,
                "write_mode": write_mode.value,
                "rows_written": row_count,
                "timestamp": datetime.now().isoformat(),
                "success": True,
            }

            self.logger.info(f"Successfully wrote {row_count} rows to {self.table_fqn}")
            return write_result

        except Exception as e:
            raise WriterTableError(
                f"Failed to write DataFrame to {self.table_fqn}: {e}",
                table_name=self.table_fqn,
                operation="write_dataframe",
                context={"write_mode": write_mode.value, "row_count": df.count()},
                suggestions=[
                    "Check table permissions",
                    "Verify DataFrame schema matches table schema",
                    "Ensure sufficient storage space",
                    "Check for schema evolution conflicts",
                ],
            ) from e

    def write_batch(
        self, log_rows: List[LogRow], write_mode: WriteMode = WriteMode.APPEND
    ) -> Dict[str, Any]:
        """
        Write a batch of log rows to the table.

        Args:
            log_rows: List of log rows to write
            write_mode: Write mode for the operation

        Returns:
            Dictionary containing write results
        """
        try:
            self.logger.info(f"Writing batch of {len(log_rows)} log rows")

            # Convert log rows to DataFrame
            df = self._create_dataframe_from_log_rows(log_rows)

            # Write DataFrame
            return self.write_dataframe(df, write_mode)

        except Exception as e:
            self.logger.error(f"Failed to write batch: {e}")
            raise

    def optimize_table(self) -> Dict[str, Any]:
        """
        Optimize the Delta table for better performance.

        Returns:
            Dictionary containing optimization results
        """
        try:
            self.logger.info(f"Optimizing table: {self.table_fqn}")

            # Run OPTIMIZE command using SQL
            self.spark.sql(f"OPTIMIZE {self.table_fqn}")

            # Get table statistics
            table_info = self.get_table_info()

            optimization_result = {
                "table_name": self.table_fqn,
                "optimization_completed": True,
                "timestamp": datetime.now().isoformat(),
                "table_info": table_info,
            }

            self.logger.info(f"Table optimization completed: {self.table_fqn}")
            return optimization_result

        except Exception as e:
            self.logger.error(f"Failed to optimize table {self.table_fqn}: {e}")
            raise WriterTableError(
                f"Failed to optimize table {self.table_fqn}: {e}",
                table_name=self.table_fqn,
                operation="optimize_table",
                suggestions=[
                    "Check table permissions",
                    "Verify table exists",
                    "Ensure sufficient resources for optimization",
                ],
            ) from e

    def vacuum_table(self, retention_hours: int = 168) -> Dict[str, Any]:
        """
        Vacuum the Delta table to remove old files.

        Args:
            retention_hours: Hours of retention for old files

        Returns:
            Dictionary containing vacuum results
        """
        try:
            self.logger.info(
                f"Vacuuming table: {self.table_fqn} (retention: {retention_hours}h)"
            )

            # Run VACUUM command using Delta Lake API
            delta_table = DeltaTable.forName(self.spark, self.table_fqn)
            delta_table.vacuum(retentionHours=retention_hours)

            vacuum_result = {
                "table_name": self.table_fqn,
                "vacuum_completed": True,
                "retention_hours": retention_hours,
                "timestamp": datetime.now().isoformat(),
            }

            self.logger.info(f"Table vacuum completed: {self.table_fqn}")
            return vacuum_result

        except Exception as e:
            self.logger.error(f"Failed to vacuum table {self.table_fqn}: {e}")
            raise WriterTableError(
                f"Failed to vacuum table {self.table_fqn}: {e}",
                table_name=self.table_fqn,
                operation="vacuum_table",
                suggestions=[
                    "Check table permissions",
                    "Verify retention period is valid",
                    "Ensure table exists",
                ],
            ) from e

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the log table.

        Returns:
            Dictionary containing table information
        """
        try:
            self.logger.info(f"Getting table info for: {self.table_fqn}")

            # Get table details using Delta Lake API
            delta_table = DeltaTable.forName(self.spark, self.table_fqn)

            # Get table details using SQL
            table_details = self.spark.sql(
                f"DESCRIBE DETAIL {self.table_fqn}"
            ).collect()

            # Get table history
            table_history = delta_table.history().collect()

            # Get row count
            row_count = self.spark.table(self.table_fqn).count()

            table_info = {
                "table_name": self.table_fqn,
                "row_count": row_count,
                "details": [dict(row.asDict()) for row in table_details],
                "history_count": len(table_history),
                "last_modified": (
                    table_history[0]["timestamp"] if table_history else None
                ),
            }

            self.logger.info(f"Table info retrieved: {row_count} rows")
            return table_info

        except Exception as e:
            self.logger.error(f"Failed to get table info for {self.table_fqn}: {e}")
            raise WriterTableError(
                f"Failed to get table info for {self.table_fqn}: {e}",
                table_name=self.table_fqn,
                operation="get_table_info",
            ) from e

    def query_logs(
        self, limit: int | None = None, filters: Dict[str, Any] | None = None
    ) -> DataFrame:
        """
        Query logs from the table.

        Args:
            limit: Maximum number of rows to return
            filters: Filters to apply to the query

        Returns:
            DataFrame containing query results
        """
        try:
            self.logger.info(f"Querying logs from: {self.table_fqn}")

            # Start with the base table
            result_df = self.spark.table(self.table_fqn)

            # Apply filters if provided using PySpark functions
            if filters:
                for column, value in filters.items():
                    if isinstance(value, str):
                        result_df = result_df.filter(col(column) == lit(value))
                    else:
                        result_df = result_df.filter(col(column) == value)

            # Add ordering using PySpark functions
            result_df = result_df.orderBy(desc("created_at"))

            # Apply limit if specified
            if limit:
                result_df = result_df.limit(limit)

            self.logger.info(f"Query executed successfully: {result_df.count()} rows")
            return result_df

        except Exception as e:
            self.logger.error(f"Failed to query logs from {self.table_fqn}: {e}")
            raise WriterTableError(
                f"Failed to query logs: {e}",
                table_name=self.table_fqn,
                operation="query_logs",
                suggestions=[
                    "Check table exists",
                    "Verify query syntax",
                    "Check column names in filters",
                ],
            ) from e

    def _prepare_dataframe_for_write(self, df: DataFrame) -> DataFrame:
        """Prepare DataFrame for writing to Delta table."""
        try:
            # Add metadata columns if not present
            if "created_at" not in df.columns:
                df = df.withColumn("created_at", current_timestamp())

            if "updated_at" not in df.columns:
                df = df.withColumn("updated_at", current_timestamp())

            return df

        except Exception as e:
            self.logger.error(f"Failed to prepare DataFrame for write: {e}")
            raise

    def _create_dataframe_from_log_rows(self, log_rows: List[LogRow]) -> DataFrame:
        """Create DataFrame from log rows."""
        try:
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
                    "metadata": row["metadata"],
                    "created_at": current_timestamp(),
                }
                log_data.append(row_dict)

            # Create DataFrame
            return self.spark.createDataFrame(log_data)  # type: ignore[type-var]

        except Exception as e:
            self.logger.error(f"Failed to create DataFrame from log rows: {e}")
            raise

    @property
    def table_schema(self) -> str:
        """Get the table schema."""
        return self.config.table_schema

    @property
    def table_name(self) -> str:
        """Get the table name."""
        return self.config.table_name

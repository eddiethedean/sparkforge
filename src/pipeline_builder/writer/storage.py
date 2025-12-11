"""
Writer storage module for Delta Lake and table operations.

This module handles all storage-related operations including Delta Lake
integration, table management, and data persistence.

# Depends on:
#   compat
#   functions
#   logging
#   table_operations
#   writer.exceptions
#   writer.models
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional, TypedDict, Union, cast

from ..compat import DataFrame, SparkSession, types

# Handle optional Delta Lake dependency
try:
    from delta.tables import DeltaTable

    HAS_DELTA = True
except ImportError:
    DeltaTable = None  # type: ignore[misc, assignment]
    HAS_DELTA = False

from pipeline_builder_base.logging import PipelineLogger

from ..functions import FunctionsProtocol, get_default_functions
from ..table_operations import table_exists
from .exceptions import WriterTableError
from .models import LogRow, WriteMode, WriterConfig, create_log_schema

# ============================================================================
# TypedDict Definitions
# ============================================================================


class WriteResult(TypedDict):
    """Write operation result structure."""

    table_name: str
    write_mode: str
    rows_written: int
    timestamp: str
    success: bool


class OptimizeResultSkipped(TypedDict):
    """Optimize operation result when skipped."""

    table_name: str
    optimization_completed: bool  # False
    skipped: bool  # True
    reason: str
    timestamp: str


class TableInfo(TypedDict, total=False):
    """Table information structure."""

    table_name: str
    row_count: int
    details: list[dict[str, Union[str, int, float, Optional[bool]]]]
    history_count: int
    last_modified: Optional[str]
    history: list[dict[str, Union[str, int, float, Optional[bool]]]]
    timestamp: str


class OptimizeResultCompleted(TypedDict):
    """Optimize operation result when completed."""

    table_name: str
    optimization_completed: bool  # True
    timestamp: str
    table_info: TableInfo


# Union type for optimize result
OptimizeResult = Union[OptimizeResultSkipped, OptimizeResultCompleted]


class VacuumResultSkipped(TypedDict):
    """Vacuum operation result when skipped."""

    table_name: str
    vacuum_completed: bool  # False
    skipped: bool  # True
    reason: str
    retention_hours: int
    timestamp: str


class VacuumResultCompleted(TypedDict):
    """Vacuum operation result when completed."""

    table_name: str
    vacuum_completed: bool  # True
    retention_hours: int
    timestamp: str


# Union type for vacuum result
VacuumResult = Union[VacuumResultSkipped, VacuumResultCompleted]


class StorageManager:
    """Handles storage operations for the writer."""

    def __init__(
        self,
        spark: SparkSession,  # type: ignore[valid-type]
        config: WriterConfig,
        functions: Optional[FunctionsProtocol] = None,
        logger: Optional[PipelineLogger] = None,
    ):
        """Initialize the storage manager."""
        self.spark = spark
        self.config = config
        self.functions = functions if functions is not None else get_default_functions()
        if logger is None:
            self.logger = PipelineLogger("StorageManager")
        else:
            self.logger = logger
        self.table_fqn = f"{config.table_schema}.{config.table_name}"

    def create_table_if_not_exists(self, schema: types.StructType) -> None:
        """
        Create the log table if it doesn't exist.

        Args:
            schema: Spark schema for the table

        Raises:
            WriterTableError: If table creation fails
        """
        try:
            self.logger.info(f"Creating table if not exists: {self.table_fqn}")

            # Extract schema name from table_fqn (format: "schema.table")
            schema_name = (
                self.table_fqn.split(".")[0] if "." in self.table_fqn else None
            )

            # Ensure schema exists before creating table
            # This is especially important for LogWriter which creates tables in different schemas
            if schema_name:
                try:
                    # Use SQL to ensure schema exists (more reliable than storage API in some contexts)
                    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")  # type: ignore[attr-defined]
                except Exception as e:
                    # If SQL fails, try storage API
                    try:
                        if hasattr(self.spark, "storage") and hasattr(
                            self.spark.storage, "create_schema"
                        ):
                            self.spark.storage.create_schema(schema_name)
                    except Exception:
                        # If both fail, log warning but continue (schema might already exist)
                        self.logger.debug(
                            f"Could not create schema '{schema_name}': {e}"
                        )

            # Check if table exists and is a Delta table
            table_is_delta = False
            if table_exists(self.spark, self.table_fqn):
                try:
                    # Check if table is a Delta table by checking table properties
                    if HAS_DELTA:
                        try:
                            # Try to get table details using DESCRIBE DETAIL (more reliable)
                            detail_df = self.spark.sql(f"DESCRIBE DETAIL {self.table_fqn}")  # type: ignore[attr-defined]
                            detail_rows = detail_df.collect()
                            if detail_rows:
                                # Check if provider is delta
                                provider = detail_rows[0].get("provider", "")
                                if provider == "delta":
                                    table_is_delta = True
                                    self.logger.info(f"Table {self.table_fqn} exists and is a Delta table")
                                else:
                                    # Table exists but is not a Delta table - drop it
                                    self.logger.warning(
                                        f"Table {self.table_fqn} exists but is not a Delta table (provider: {provider}). Dropping and recreating."
                                    )
                                    self.spark.sql(f"DROP TABLE IF EXISTS {self.table_fqn}")  # type: ignore[attr-defined]
                            else:
                                # Could not get details, try DeltaTable.forName as fallback
                                try:
                                    DeltaTable.forName(self.spark, self.table_fqn)  # type: ignore[attr-defined]
                                    table_is_delta = True
                                    self.logger.info(f"Table {self.table_fqn} exists and is a Delta table (verified via DeltaTable)")
                                except Exception:
                                    # If both methods fail, assume it's not a Delta table
                                    self.logger.warning(
                                        f"Table {self.table_fqn} exists but could not verify as Delta table. Dropping and recreating."
                                    )
                                    self.spark.sql(f"DROP TABLE IF EXISTS {self.table_fqn}")  # type: ignore[attr-defined]
                        except Exception as e:
                            # DESCRIBE DETAIL failed, try DeltaTable.forName as fallback
                            try:
                                DeltaTable.forName(self.spark, self.table_fqn)  # type: ignore[attr-defined]
                                table_is_delta = True
                                self.logger.info(f"Table {self.table_fqn} exists and is a Delta table (verified via DeltaTable)")
                            except Exception:
                                # If both methods fail, log warning but don't drop - might be a temporary issue
                                self.logger.warning(
                                    f"Could not verify if table {self.table_fqn} is Delta: {e}. Assuming it's valid and continuing."
                                )
                                table_is_delta = True  # Assume it's valid to avoid dropping existing data
                    else:
                        # Delta Lake not available, but table exists - assume it's okay
                        table_is_delta = True
                except Exception as e:
                    # If all checks fail, assume table is valid to avoid data loss
                    self.logger.warning(
                        f"Could not verify table {self.table_fqn} Delta status: {e}. Assuming valid and continuing."
                    )
                    table_is_delta = True

            if not table_exists(self.spark, self.table_fqn):
                # Create empty DataFrame with schema
                empty_df = self.spark.createDataFrame([], schema)  # type: ignore[attr-defined]

                # Ensure schema exists RIGHT BEFORE saveAsTable
                if schema_name:
                    try:
                        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")  # type: ignore[attr-defined]
                    except Exception:
                        pass  # Schema might already exist

                # Check if Delta Lake is configured before creating Delta table
                delta_configured = False
                try:
                    # Check if Delta extensions are configured
                    extensions = self.spark.conf.get("spark.sql.extensions", "")  # type: ignore[attr-defined]
                    if "DeltaSparkSessionExtension" in extensions:
                        delta_configured = True
                except Exception:
                    pass

                if not delta_configured and HAS_DELTA:
                    # Try to verify Delta is available by checking if we can import and use it
                    try:
                        from delta.tables import DeltaTable
                        # If we can import, assume it's configured
                        delta_configured = True
                    except Exception:
                        pass

                if not delta_configured:
                    raise WriterTableError(
                        f"Delta Lake is not configured in SparkSession. Cannot create Delta table {self.table_fqn}",
                        table_name=self.table_fqn,
                        operation="create_table",
                        suggestions=[
                            "Ensure Delta Lake is properly configured in SparkSession",
                            "Use configure_spark_with_delta_pip() to set up Delta Lake",
                            "Check that spark.sql.extensions includes io.delta.sql.DeltaSparkSessionExtension",
                        ],
                    )

                # Write to Delta table
                (
                    empty_df.write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(self.table_fqn)  # type: ignore[attr-defined]
                )

                self.logger.info(f"Table created successfully: {self.table_fqn}")
            elif not table_is_delta:
                # Table exists but wasn't verified as Delta - this shouldn't happen after the check above
                self.logger.warning(f"Table {self.table_fqn} exists but Delta status unclear")

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
        df: DataFrame,  # type: ignore[valid-type]
        write_mode: WriteMode = WriteMode.APPEND,
        partition_columns: Optional[list[str]] = None,
    ) -> WriteResult:
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
            writer = df_prepared.write.format("delta").mode(write_mode.value)  # type: ignore[attr-defined]

            # Add partitioning if specified
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            # Add table-specific options
            if write_mode == WriteMode.OVERWRITE:
                writer = writer.option("overwriteSchema", "true")
            elif write_mode == WriteMode.APPEND:
                # Enable schema evolution for append mode to handle new columns
                writer = writer.option("mergeSchema", "true")

            # Execute write operation
            writer.saveAsTable(self.table_fqn)

            # Get write statistics
            row_count = df_prepared.count()  # type: ignore[attr-defined]

            write_result = {
                "table_name": self.table_fqn,
                "write_mode": write_mode.value,
                "rows_written": row_count,
                "timestamp": datetime.now().isoformat(),
                "success": True,
            }

            self.logger.info(f"Successfully wrote {row_count} rows to {self.table_fqn}")
            return cast(WriteResult, write_result)

        except Exception as e:
            # Safely get row count for error context
            try:
                row_count = df.count() if hasattr(df, "count") else 0  # type: ignore[attr-defined]
            except Exception:
                row_count = 0

            raise WriterTableError(
                f"Failed to write DataFrame to {self.table_fqn}: {e}",
                table_name=self.table_fqn,
                operation="write_dataframe",
                context={"write_mode": write_mode.value, "row_count": row_count},
                suggestions=[
                    "Check table permissions",
                    "Verify DataFrame schema matches table schema",
                    "Ensure sufficient storage space",
                    "Check for schema evolution conflicts",
                ],
            ) from e

    def write_batch(
        self, log_rows: list[LogRow], write_mode: WriteMode = WriteMode.APPEND
    ) -> WriteResult:
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

            # Convert log rows to DataFrame and write
            df = self._create_dataframe_from_log_rows(log_rows)
            # Write DataFrame
            return self.write_dataframe(df, write_mode)  # type: ignore[attr-defined]

        except Exception as e:
            self.logger.error(f"Failed to write batch: {e}")
            raise

    def optimize_table(self) -> OptimizeResult:
        """
        Optimize the Delta table for better performance.

        Returns:
            Dictionary containing optimization results
        """
        if not HAS_DELTA:
            self.logger.warning(
                f"Delta Lake not available, optimize operation skipped for {self.table_fqn}"
            )
            return {
                "table_name": self.table_fqn,
                "optimization_completed": False,
                "skipped": True,
                "reason": "Delta Lake not available",
                "timestamp": datetime.now().isoformat(),
            }

        try:
            self.logger.info(f"Optimizing table: {self.table_fqn}")

            # Run OPTIMIZE command using Delta Lake Python API
            delta_table = DeltaTable.forName(self.spark, self.table_fqn)
            # Note: optimize() method may not be available in all Delta Lake versions
            if hasattr(delta_table, "optimize"):
                delta_table.optimize()
            else:
                # Fallback: use SQL command
                self.spark.sql(f"OPTIMIZE {self.table_fqn}")  # type: ignore[attr-defined]

            # Get table statistics
            table_info = self.get_table_info()

            optimization_result = {
                "table_name": self.table_fqn,
                "optimization_completed": True,
                "timestamp": datetime.now().isoformat(),
                "table_info": table_info,
            }

            self.logger.info(f"Table optimization completed: {self.table_fqn}")
            return cast(OptimizeResult, optimization_result)

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

    def vacuum_table(self, retention_hours: int = 168) -> VacuumResult:
        """
        Vacuum the Delta table to remove old files.

        Args:
            retention_hours: Hours of retention for old files

        Returns:
            Dictionary containing vacuum results
        """
        if not HAS_DELTA:
            self.logger.warning(
                f"Delta Lake not available, vacuum operation skipped for {self.table_fqn}"
            )
            return {
                "table_name": self.table_fqn,
                "vacuum_completed": False,
                "skipped": True,
                "reason": "Delta Lake not available",
                "retention_hours": retention_hours,
                "timestamp": datetime.now().isoformat(),
            }

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
            return cast(VacuumResult, vacuum_result)

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

    def get_table_info(self) -> TableInfo:
        """
        Get information about the log table.

        Returns:
            Dictionary containing table information
        """
        if not HAS_DELTA:
            self.logger.warning(
                f"Delta Lake not available, using basic table info for {self.table_fqn}"
            )
            # Get basic info without Delta Lake
            row_count = self.spark.table(self.table_fqn).count()  # type: ignore[attr-defined]
            return {
                "table_name": self.table_fqn,
                "row_count": row_count,
                "details": [],
                "history": [],
                "timestamp": datetime.now().isoformat(),
            }

        try:
            self.logger.info(f"Getting table info for: {self.table_fqn}")

            # Get table details using Delta Lake API
            delta_table = DeltaTable.forName(self.spark, self.table_fqn)

            # Get table details using Delta Lake Python API
            # Note: detail() method may not be available in all Delta Lake versions
            if hasattr(delta_table, "detail"):
                table_details = delta_table.detail().collect()
            else:
                # Fallback: use SQL command
                table_details = self.spark.sql(
                    f"DESCRIBE DETAIL {self.table_fqn}"
                ).collect()  # type: ignore[attr-defined]

            # Get table history
            table_history = delta_table.history().collect()

            # Get row count
            row_count = self.spark.table(self.table_fqn).count()  # type: ignore[attr-defined]

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
            return cast(TableInfo, table_info)

        except Exception as e:
            self.logger.error(f"Failed to get table info for {self.table_fqn}: {e}")
            raise WriterTableError(
                f"Failed to get table info for {self.table_fqn}: {e}",
                table_name=self.table_fqn,
                operation="get_table_info",
            ) from e

    def query_logs(
        self,
        limit: Optional[int] = None,
        filters: Union[Dict[str, Union[str, int, float, bool]], None] = None,
    ) -> DataFrame:  # type: ignore[valid-type]
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
            result_df = self.spark.table(self.table_fqn)  # type: ignore[attr-defined]

            # Apply filters if provided using PySpark functions
            if filters:
                for column, value in filters.items():
                    if isinstance(value, str):
                        result_df = result_df.filter(
                            self.functions.col(column) == self.functions.lit(value)  # type: ignore[attr-defined]
                        )
                    else:
                        result_df = result_df.filter(
                            self.functions.col(column) == value  # type: ignore[attr-defined]
                        )

            # Add ordering using PySpark functions
            from ..compat import desc

            result_df = result_df.orderBy(desc("created_at"))

            # Apply limit if specified
            if limit:
                result_df = result_df.limit(limit)  # type: ignore[attr-defined]

            self.logger.info(f"Query executed successfully: {result_df.count()} rows")  # type: ignore[attr-defined]
            from typing import cast

            from ..compat import DataFrame

            return cast(DataFrame, result_df)  # type: ignore[valid-type]

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

    def _prepare_dataframe_for_write(self, df: DataFrame) -> DataFrame:  # type: ignore[valid-type]
        """Prepare DataFrame for writing to Delta table."""
        try:
            # Add metadata columns if not present
            from datetime import datetime

            current_time_str = datetime.now().isoformat()

            if "created_at" not in df.columns:  # type: ignore[attr-defined]
                df = df.withColumn("created_at", self.functions.lit(current_time_str))

            if "updated_at" not in df.columns:  # type: ignore[attr-defined]
                df = df.withColumn("updated_at", self.functions.lit(current_time_str))

            return df

        except Exception as e:
            self.logger.error(f"Failed to prepare DataFrame for write: {e}")
            raise

    def _create_dataframe_from_log_rows(self, log_rows: list[LogRow]) -> DataFrame:  # type: ignore[valid-type]
        """Create DataFrame from log rows."""
        try:
            # Convert log rows to dictionaries
            from datetime import datetime

            current_time_str = datetime.now().isoformat()

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
                    "table_total_rows": row.get(
                        "table_total_rows"
                    ),  # Include table_total_rows metric
                    "valid_rows": row["valid_rows"],
                    "invalid_rows": row["invalid_rows"],
                    "validation_rate": row["validation_rate"],
                    "success": row["success"],
                    "error_message": row["error_message"],
                    "memory_usage_mb": row["memory_usage_mb"],
                    "cpu_usage_percent": row["cpu_usage_percent"],
                    "metadata": row["metadata"],
                    "created_at": current_time_str,  # Include timestamp directly as string
                }
                log_data.append(row_dict)

            # Create DataFrame with explicit schema for type safety and None value handling
            schema = create_log_schema()
            df = self.spark.createDataFrame(log_data, schema)  # type: ignore[attr-defined]
            from typing import cast

            from ..compat import DataFrame

            return cast(DataFrame, df)  # type: ignore[valid-type]

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

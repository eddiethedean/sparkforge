"""Write service for handling all write operations.

This module provides a service for writing DataFrames to tables with proper
handling of write modes, schema overrides, and Delta Lake operations. The
WriteService centralizes all write logic, making it testable and maintainable.
"""

from __future__ import annotations

from typing import Any, Optional

from pipeline_builder_base.errors import ExecutionError
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import ExecutionMode

from ..compat import DataFrame, SparkSession
from ..table_operations import (
    create_dataframe_writer,
)
from .table_service import TableService


class WriteService:
    """Service for writing DataFrames to tables.

    Handles write modes, schema validation, schema overrides, and Delta Lake
    operations. Centralizes all write logic for Silver and Gold steps.

    Attributes:
        spark: SparkSession instance for DataFrame operations.
        table_service: TableService instance for table operations.
        logger: PipelineLogger instance for logging.

    Example:
        >>> from pipeline_builder.storage.write_service import WriteService
        >>> from pipeline_builder.storage.table_service import TableService
        >>> from pipeline_builder_base.models import ExecutionMode
        >>>
        >>> table_service = TableService(spark)
        >>> write_service = WriteService(spark, table_service)
        >>> rows = write_service.write_step_output(
        ...     df=output_df,
        ...     step=silver_step,
        ...     schema="analytics",
        ...     table_name="clean_events",
        ...     mode=ExecutionMode.INITIAL
        ... )
    """

    def __init__(
        self,
        spark: SparkSession,
        table_service: TableService,
        logger: Optional[PipelineLogger] = None,
    ):
        """Initialize the write service.

        Args:
            spark: Active SparkSession instance for DataFrame operations.
            table_service: TableService instance for table operations.
            logger: Optional PipelineLogger instance. If None, creates a
                default logger.
        """
        self.spark = spark
        self.table_service = table_service
        self.logger = logger or PipelineLogger()

    def write_step_output(
        self,
        df: DataFrame,
        step: Any,
        schema: str,
        table_name: str,
        mode: ExecutionMode,
    ) -> int:
        """Write step output to a table.

        Writes a DataFrame to a Delta Lake table with proper handling of write
        modes, schema validation, and schema overrides. Handles all the
        complexity of Delta Lake writes including overwrite semantics.

        Args:
            df: DataFrame to write.
            step: Step object (SilverStep or GoldStep) containing step
                configuration.
            schema: Schema name for the target table.
            table_name: Table name (without schema).
            mode: ExecutionMode enum value (INITIAL, INCREMENTAL, FULL_REFRESH,
                VALIDATION_ONLY).

        Returns:
            Number of rows written to the table.

        Raises:
            ExecutionError: If write fails, schema validation fails, or schema
                override application fails.

        Note:
            - Ensures schema exists before writing
            - Validates schema match for INCREMENTAL and FULL_REFRESH modes
            - Drops table in INITIAL mode for clean start
            - Applies schema override if provided in step
            - Handles Delta Lake overwrite semantics correctly
        """
        output_table = self.table_service.fqn(schema, table_name)

        # Ensure schema exists
        self.table_service.ensure_schema_exists(schema)

        # Determine write mode
        write_mode_str = self._determine_write_mode(step, mode)

        # Validate schema if needed
        if mode in (ExecutionMode.INCREMENTAL, ExecutionMode.FULL_REFRESH):
            self._validate_schema_for_mode(df, output_table, mode, step.name)

        # NOTE: We intentionally do NOT drop existing tables in INITIAL mode.
        # Dropping is destructive and can leave users with missing tables if a run fails
        # after the drop but before the overwrite commit. Delta overwrite is transactional.

        # Handle schema override if provided
        schema_override = getattr(step, "schema_override", None)
        if schema_override is not None:
            df = self._apply_schema_override(
                df, schema_override, step, output_table, write_mode_str
            )

        # Handle write based on step type and mode
        rows_written = self._execute_write(df, step, output_table, write_mode_str, mode)

        return rows_written

    def _determine_write_mode(
        self,
        step: Any,
        mode: ExecutionMode,
    ) -> str:
        """Determine the write mode for a step.

        Determines the appropriate write mode based on step type and execution
        mode. Gold steps always use overwrite, Silver steps use append for
        incremental mode and overwrite otherwise.

        Args:
            step: Step object (SilverStep or GoldStep).
            mode: ExecutionMode enum value.

        Returns:
            Write mode string ("overwrite" or "append").
        """
        # Gold steps always use overwrite to prevent duplicate aggregates
        if step.__class__.__name__ == "GoldStep":
            return "overwrite"
        elif mode == ExecutionMode.INCREMENTAL:
            return "append"
        else:  # INITIAL or FULL_REFRESH
            return "overwrite"

    def _validate_schema_for_mode(
        self,
        df: DataFrame,
        table_name: str,
        mode: ExecutionMode,
        step_name: str,
    ) -> None:
        """
        Validate schema for INCREMENTAL and FULL_REFRESH modes.

        Args:
            df: DataFrame to validate
            table_name: Fully qualified table name
            mode: Execution mode
            step_name: Name of the step

        Raises:
            ExecutionError: If schema validation fails
        """
        if not self.table_service.table_exists(table_name):
            return

        # Refresh table metadata
        self.table_service.refresh_table(table_name)

        # Validate schema match
        output_schema = df.schema
        self.table_service.validate_schema_match(
            table_name, output_schema, mode, step_name
        )

    def _apply_schema_override(
        self,
        df: DataFrame,
        schema_override: Any,
        step: Any,
        output_table: str,
        write_mode_str: str,
    ) -> DataFrame:
        """
        Apply schema override to DataFrame.

        Args:
            df: DataFrame to apply schema to
            schema_override: Schema to apply
            step: Step object
            output_table: Fully qualified table name
            write_mode_str: Write mode string

        Returns:
            DataFrame with schema override applied
        """
        try:
            # Cast DataFrame to the override schema
            df = self.spark.createDataFrame(df.rdd, schema_override)  # type: ignore[attr-defined]
        except Exception as e:
            raise ExecutionError(
                f"Failed to apply schema_override to step '{step.name}': {e}",
                context={
                    "step_name": step.name,
                    "table": output_table,
                    "schema_override": str(schema_override),
                },
                suggestions=[
                    "Verify that the schema_override matches the DataFrame structure",
                    "Check that all required columns are present in the DataFrame",
                    "Ensure data types are compatible",
                ],
            ) from e

        return df

    def _execute_write(
        self,
        df: DataFrame,
        step: Any,
        output_table: str,
        write_mode_str: str,
        mode: ExecutionMode,
    ) -> int:
        """
        Execute the actual write operation.

        Args:
            df: DataFrame to write
            step: Step object
            output_table: Fully qualified table name
            write_mode_str: Write mode string
            mode: Execution mode

        Returns:
            Number of rows written
        """
        # For overwrite mode with Delta tables, ensure table is dropped before writing
        # This prevents "Table does not support truncate in batch mode" errors
        if write_mode_str == "overwrite":
            from ..table_operations import prepare_delta_overwrite

            prepare_delta_overwrite(self.spark, output_table)

        writer = create_dataframe_writer(
            df, self.spark, write_mode_str, table_name=output_table
        )

        try:
            writer.saveAsTable(output_table)
            rows_written = df.count()
            return rows_written
        except Exception as e:
            # If write fails with truncate error, try dropping table and writing again
            error_msg = str(e).lower()
            if "truncate" in error_msg and "batch mode" in error_msg:
                self.logger.warning(
                    f"Write failed with truncate error for Delta table, "
                    f"dropping table and retrying: {e}"
                )
                try:
                    # Force drop the table (without CASCADE - not supported in all Spark versions)
                    self.spark.sql(f"DROP TABLE IF EXISTS {output_table}")  # type: ignore[attr-defined]
                    # Small delay to ensure catalog is updated
                    import time

                    time.sleep(0.1)
                    # Retry the write - table should not exist now
                    writer = create_dataframe_writer(
                        df, self.spark, write_mode_str, table_name=output_table
                    )
                    writer.saveAsTable(output_table)
                    rows_written = df.count()
                    self.logger.info(
                        f"Successfully wrote {rows_written} rows after retry"
                    )
                    return rows_written
                except Exception as retry_error:
                    raise ExecutionError(
                        f"Failed to write table '{output_table}' even after retry: {retry_error}",
                        context={
                            "step_name": step.name,
                            "table": output_table,
                            "mode": mode.value,
                            "write_mode": write_mode_str,
                            "original_error": str(e),
                        },
                    ) from retry_error

            raise ExecutionError(
                f"Failed to write table '{output_table}': {e}",
                context={
                    "step_name": step.name,
                    "table": output_table,
                    "mode": mode.value,
                    "write_mode": write_mode_str,
                },
            ) from e
